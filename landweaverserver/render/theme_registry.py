from dataclasses import dataclass
from typing import Dict, Optional, Any

import numpy as np
from landweaverserver.render.qml_palette import QmlPalette, _parse_color_attr
from scipy.ndimage import gaussian_filter

MEDIAN_FILTER_SIZE = 3
EPSILON = 1e-6

BACKGROUND_THEME_ID = 0
LUT_SIZE = 256
CLAIM_THRESHOLD = 0.2


# theme_registry.py

@dataclass(frozen=True, slots=True)
class ThemeRuntimeSpec:
    label: str
    theme_id: int
    rgb: tuple[int, int, int]

    max_opacity: float = 1.0
    blur_px: float = 0.0
    noise_amp: float = 0.0
    noise_id: str = "empty"
    contrast: float = 1.0

    smoothing_radius: float = 0.0

    surface_noise_id: Optional[str] = None
    surface_intensity: float = 0.0
    surface_shift_vector: tuple[float, float, float] = (0.0, 0.0, 0.0)

    enabled: bool = True


@dataclass(slots=True)
class ThemeTileContext:
    """Per-tile shared theme analysis.

    Args:
        theme_ids: Raw or smoothed theme ID raster for the tile.
        present_ids: Set of theme IDs present in this tile.
        active_specs: Active runtime specs that are both configured and present.
        masks_by_id: Binary float masks keyed by theme ID.
    """

    theme_ids: np.ndarray
    present_ids: set[int]
    active_specs: list[ThemeRuntimeSpec]
    masks_by_id: Dict[int, np.ndarray]


class ThemeRegistry:
    """Registry for categorical theme metadata and runtime rendering specs."""

    def __init__(self, cfg: Any):
        self.cfg = None
        # Identity Mappings
        self._name_to_id: Dict[str, int] = {}
        self._id_to_color: Dict[int, tuple[int, int, int]] = {}

        #  Runtime State
        self.qml_palette: Optional[Any] = None
        self.lut_rgb: Optional[np.ndarray] = None

        # Processed Specs
        self._runtime_specs_by_label: Dict[str, ThemeRuntimeSpec] = {}
        self._runtime_specs_by_id: Dict[int, ThemeRuntimeSpec] = {}

    @property
    def name_to_id(self) -> Dict[str, int]:
        """Return mapping from label -> theme ID."""
        return self._name_to_id

    @property
    def runtime_specs_by_label(self) -> Dict[str, ThemeRuntimeSpec]:
        """Return active/inactive runtime specs keyed by label."""
        return self._runtime_specs_by_label

    @property
    def runtime_specs_by_id(self) -> Dict[int, ThemeRuntimeSpec]:
        """Return runtime specs keyed by theme ID."""
        return self._runtime_specs_by_id

    def load_metadata(self, render_cfg: Any) -> None:
        """Methodical Ingestion: Extracts theme logic from the factor params."""
        self.cfg = render_cfg

        # 1. QML colors
        qml_path = render_cfg.path("theme_qml")
        if not qml_path or not qml_path.exists():
            raise FileNotFoundError(f"Theme QML not found: {qml_path}")
        self.qml_palette = QmlPalette.load(qml_path)

        # 2. Reset Internal Maps
        self._name_to_id.clear()
        self._id_to_color.clear()
        self._runtime_specs_by_label.clear()
        self._runtime_specs_by_id.clear()

        # 3. Synchronize Labels and Colors from QML
        self._name_to_id.update(self.qml_palette.value_by_label)
        for value_str, entry in self.qml_palette.entries_by_value.items():
            rgb = _parse_color_attr(entry.color_hex)
            if rgb:
                self._id_to_color[int(value_str)] = rgb

        # We search the factors list for the one that drives theme composition
        theme_factor = next(
            (f for f in render_cfg.factors if f.factor_builder == "theme_composite"), None
        )

        # 5. BUILD SPECS
        # If no factor found, we pass an empty dict to use defaults
        categories_cfg = theme_factor.params if theme_factor else {}
        self._build_runtime_specs(render_cfg, categories_cfg)

    def _build_runtime_specs(self, render_cfg: Any, categories_cfg: Dict[str, Any]) -> None:
        """Constructs ThemeRuntimeSpecs from the consolidated categories dictionary."""

        # We iterate over labels found in the QML palette to ensure 100% coverage
        for label, theme_id in self._name_to_id.items():
            # Get settings from factor params, fallback to _default_
            cat_cfg = categories_cfg.get(label)
            if cat_cfg is None:
                continue

            enabled = cat_cfg.get("enabled", True)
            if not enabled:
                continue

            rgb = self._id_to_color.get(theme_id, (0, 0, 0))
            surface_shift_vector = cat_cfg.get("surface_shift_vector", (0.0, 0.0, 0.0))

            try:
                # Build the unified spec containing  Smoothing and Rendering data
                spec = ThemeRuntimeSpec(
                    label=label, theme_id=theme_id, rgb=rgb, # Rendering Params
                    max_opacity=float(cat_cfg.get("max_opacity", 1.0)),
                    blur_px=float(cat_cfg.get("blur_px", 0.0)),
                    noise_amp=float(cat_cfg.get("noise_amp", 0.0)),
                    noise_id=str(cat_cfg.get("noise_id", "none")),
                    contrast=float(cat_cfg.get("contrast", 1.0)),
                    smoothing_radius=float(cat_cfg.get("smoothing_radius", 0.0)),
                    surface_noise_id=cat_cfg.get("surface_noise_id"),
                    surface_intensity=float(cat_cfg.get("surface_intensity", 0.0)),
                    surface_shift_vector=tuple(float(v) for v in surface_shift_vector), enabled=True
                )

                self._runtime_specs_by_label[label] = spec
                self._runtime_specs_by_id[theme_id] = spec

            except Exception as e:
                raise ValueError(f"❌ Theme Registry Error: [{label}] {e}")

    def _extract_theme_category_config(self, render_cfg: Any) -> Dict[str, Any]:
        """
        Extracts theme settings directly from the theme_render block.
        Filters out metadata keys by checking against known QML labels.
        """
        # 1. Get the structured attribute
        theme_render = getattr(render_cfg, "theme_render", {})

        if not theme_render:
            return {}

        # 2. Filter: Only return keys that are actual categories defined in QML.
        # This allows you to have keys like 'version' in
        # the same block without the spec-builder trying to render them.
        return {label: params for label, params in theme_render.items() if
                label in self._name_to_id}

    def load_theme_style(self) -> None:
        """Build dense RGB LUT in worker process."""
        if self.lut_rgb is not None:
            return

        lut = np.zeros((LUT_SIZE, 3), dtype=np.uint8)
        for theme_id, rgb in self._id_to_color.items():
            if not 0 <= theme_id < LUT_SIZE:
                raise ValueError(f"Theme ID {theme_id} is outside LUT range 0-{LUT_SIZE - 1}.")
            lut[theme_id] = rgb

        self.lut_rgb = lut

    def build_tile_context(self, theme_ids: np.ndarray) -> ThemeTileContext:
        present_ids = set(np.unique(theme_ids).tolist())
        active_specs: list[ThemeRuntimeSpec] = []
        masks_by_id: Dict[int, np.ndarray] = {}

        for theme_id in present_ids:
            if theme_id == BACKGROUND_THEME_ID:
                continue

            spec = self._runtime_specs_by_id.get(theme_id)
            if spec is None or not spec.enabled:
                continue

            active_specs.append(spec)
            masks_by_id[theme_id] = (theme_ids == theme_id).astype(np.float32)

        active_specs.sort(key=lambda item: item.theme_id)
        return ThemeTileContext(
            theme_ids=theme_ids, present_ids=present_ids, active_specs=active_specs,
            masks_by_id=masks_by_id, )

    def get_theme_surface(
            self, theme_ids: np.ndarray, ctx: Any,
            tile_ctx: Optional[ThemeTileContext] = None, ) -> np.ndarray:

        if self.lut_rgb is None:
            self.load_theme_style()

        if tile_ctx is None:
            tile_ctx = self.build_tile_context(theme_ids)

        indices = theme_ids.astype(np.uint8)
        rgb_float = self.lut_rgb[indices].astype(np.float32)

        noise_cache: Dict[str, np.ndarray] = {}

        for spec in tile_ctx.active_specs:
            if not spec.surface_noise_id or spec.surface_intensity <= 0.0:
                continue

            noise = noise_cache.get(spec.surface_noise_id)
            if noise is None:
                noise_provider = ctx.noises.get(spec.surface_noise_id)
                if noise_provider is None:
                    available = ctx.noises.keys()
                    raise KeyError(
                        f"Missing noise provider '{spec.surface_noise_id}' "
                        f"for theme '{spec.label}'.  Available: {available}"
                    )
                noise = np.squeeze(noise_provider.window_noise(ctx.window)).astype(np.float32)
                noise_cache[spec.surface_noise_id] = noise

            centered_noise = noise - 0.5
            shift = (centered_noise[..., np.newaxis] * np.asarray(
                spec.surface_shift_vector, dtype=np.float32
            ) * spec.surface_intensity)
            mask_3d = tile_ctx.masks_by_id[spec.theme_id][..., np.newaxis]
            rgb_float += shift * mask_3d

        rgb_float[theme_ids == BACKGROUND_THEME_ID] = 0.0
        return np.clip(rgb_float, 0.0, 255.0)


def refine_signal(mask: np.ndarray, params: Any, ctx: Any, diag_name: str = None) -> np.ndarray:
    if mask.ndim != 2:
        raise ValueError(
            f"refine_organic_signal shape violation: 'mask' must be 2D (H,W). "
            f"Got shape {mask.shape} for factor '{diag_name}'"
        )

    def get_p(key, default):
        if isinstance(params, dict): return params.get(key, default)
        return getattr(params, key, default)

    blur_px = float(get_p("blur_px", 0.0))
    noise_amp = float(get_p("noise_amp", 0.0))
    noise_id = get_p("noise_id", "none")
    contrast = float(get_p("contrast", 1.0))
    max_opacity = float(get_p("max_opacity", 1.0))

    # 3. EXECUTION STACK
    # Ensure raw binary 100 doesn't enter as 100.0
    signal = np.clip(mask.astype(np.float32), 0.0, 1.0)

    # A. Initial Melt
    if blur_px > 0.0:
        signal = gaussian_filter(signal, sigma=blur_px)

    # B. Noise Modulation
    if noise_amp > 0.0 and noise_id != "none":
        noise_provider = ctx.noises.get(noise_id)
        if noise_provider:
            noise_2d = noise_provider.window_noise(ctx.window).astype(np.float32)

            if noise_2d.ndim != 2:
                raise ValueError(
                    f"UPSTREAM ERROR: Noise Provider '{noise_id}' returned shape {noise_2d.shape}. "
                    f"refine_organic_signal requires strictly 2D (H,W) noise."
                )

            # Formula logic
            low_bound = signal * (1.0 - noise_amp)
            high_bound = signal  # Bound at the mask edge

            # Interpolate: signal = low + (noise * delta)
            signal = low_bound + (noise_2d * (high_bound - low_bound))

    # C. Signal Shaping
    if contrast != 1.0:
        signal = np.clip((signal - 0.5) * contrast + 0.5, 0.0, 1.0)

    final_res = np.clip(signal * max_opacity, 0.0, 1.0)

    return final_res
