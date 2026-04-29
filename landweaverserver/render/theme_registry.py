import collections
from dataclasses import dataclass
from typing import Dict, Optional, Any

import numpy as np

from landweaverserver.render.qml_palette import QmlPalette, _parse_color_attr
from landweaverserver.render.utils import optimized_blur

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
        """Extract theme logic from the factor categories."""
        self.cfg = render_cfg

        # 1. Get colors from QML file
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

        # 4. Find the factor that drives theme composition
        # (schema Stage 2 uses factor_builder; Stage 3 will change this to .op == "theme_composite")
        theme_factor = next(
            (f for f in render_cfg.factors if f.op == "theme_composite"), None
        )

        # 5. BUILD SPECS
        # SCHEMA V2: Use the dedicated categories attribute
        # Fallback to params to maintain compatibility during the transition
        categories_cfg = {}
        if theme_factor:
            categories_cfg = theme_factor.categories

        self._build_runtime_specs(render_cfg, categories_cfg)

    def _build_runtime_specs(self, render_cfg: Any, categories_cfg: Dict[str, Any]) -> None:
        """Constructs ThemeRuntimeSpecs from the consolidated categories dictionary."""

        for label, theme_id in self._name_to_id.items():
            # SCHEMA V2: Looking directly into the categories dictionary
            cat_cfg = categories_cfg.get(label)
            if cat_cfg is None:
                continue

            enabled = cat_cfg.get("enabled", True)
            if not enabled:
                continue

            rgb = self._id_to_color.get(theme_id, (0, 0, 0))

            # Use .get() with defaults for all tuning parameters
            surface_shift_vector = cat_cfg.get("surface_shift_vector", (0.0, 0.0, 0.0))

            try:
                spec = ThemeRuntimeSpec(
                    label=label,
                    theme_id=theme_id,
                    rgb=rgb,
                    max_opacity=float(cat_cfg.get("max_opacity", 1.0)),
                    blur_px=float(cat_cfg.get("blur_px", 0.0)),
                    noise_amp=float(cat_cfg.get("noise_amp", 0.0)),
                    noise_id=str(cat_cfg.get("noise_id", "none")),
                    contrast=float(cat_cfg.get("contrast", 1.0)),
                    smoothing_radius=float(cat_cfg.get("smoothing_radius", 0.0)),
                    surface_noise_id=cat_cfg.get("surface_noise_id"),
                    surface_intensity=float(cat_cfg.get("surface_intensity", 0.0)),
                    surface_shift_vector=tuple(float(v) for v in surface_shift_vector),
                    enabled=True
                )

                self._runtime_specs_by_label[label] = spec
                self._runtime_specs_by_id[theme_id] = spec

            except Exception as e:
                raise ValueError(f"❌ Theme Registry Error: [{label}] {e}")

    def _extract_theme_category_config(self, render_cfg: Any) -> Dict[str, Any]:
        """
        Extracts theme settings  from the theme_render block.
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
        """Build high-performance LUTs for both Color and Mottle-Shift."""
        if self.lut_rgb is not None:
            return

        # LUT 1: Base Colors
        self.lut_rgb = np.zeros((256, 3), dtype=np.uint8)

        # LUT 2: Mottle Vectors (Pre-multiplied by intensity)
        # We group these by Noise ID because different themes might use different noises
        self.lut_shifts_by_noise: Dict[str, np.ndarray] = collections.defaultdict(
            lambda: np.zeros((256, 3), dtype=np.float32)
        )

        for theme_id, spec in self._runtime_specs_by_id.items():
            if not 0 <= theme_id < 256: continue

            # Populate Color
            self.lut_rgb[theme_id] = spec.rgb

            # Populate Shift Vector for the specific noise this theme uses
            if spec.surface_noise_id and spec.surface_intensity > 0:
                baked_vec = np.array(
                    spec.surface_shift_vector, dtype=np.float32
                ) * spec.surface_intensity
                self.lut_shifts_by_noise[spec.surface_noise_id][theme_id] = baked_vec

        # Handle Background (Force 0)
        self.lut_rgb[BACKGROUND_THEME_ID] = (0, 0, 0)

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

        return ThemeTileContext(
            theme_ids=theme_ids, present_ids=present_ids, active_specs=active_specs,
            masks_by_id=masks_by_id, )

    def get_theme_surface(self, theme_ids: np.ndarray, ctx: Any, **kwargs) -> np.ndarray:
        if self.lut_rgb is None:
            self.load_theme_style()

        indices = theme_ids.astype(np.uint8, copy=False)

        # 1. Base Color Lookup (One single allocation for the result)
        rgb_float = self.lut_rgb[indices].astype(np.float32)

        # 2. Apply Noise-Groups Channel-Wise
        for nid, shift_lut in self.lut_shifts_by_noise.items():
            noise_provider = ctx.noises.get(nid)
            if not noise_provider: continue

            # ns is (H, W) - a direct view of the noise
            ns = noise_provider.get_noise_signal(
                int(ctx.window.row_off), int(ctx.window.col_off), indices.shape[0], indices.shape[1]
            )[..., 0]

            # Channel-wise math prevents massive float32 allocations
            for i in range(3):
                # shift_lut[:, i][indices] is a 2D (H, W) array
                rgb_float[..., i] += ns * shift_lut[:, i][indices]

        # 3. Final Cleanup
        if BACKGROUND_THEME_ID != 0:
            rgb_float[indices == BACKGROUND_THEME_ID] = 0.0

        return rgb_float.clip(0.0, 255.0, out=rgb_float)


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
    noise_atten_power = float(get_p("noise_atten_power", 1.0))
    noise_id = get_p("noise_id", "none")
    contrast = float(get_p("contrast", 1.0))
    max_opacity = float(get_p("max_opacity", 1.0))
    preserve_zero = bool(get_p("preserve_zero", False))

    # 3. EXECUTION STACK
    signal = np.clip(mask.astype(np.float32), 0.0, 1.0)

    # A. Initial Melt
    if blur_px > 0.0:
        signal = optimized_blur(signal, sigma=blur_px)

    # B. Noise Modulation
    if noise_amp > 0.0 and noise_id != "none":
        noise_provider = ctx.noises.get(noise_id)
        if noise_provider:
            noise_2d = noise_provider.window_noise(ctx.window).astype(np.float32)

            if noise_2d.ndim != 2:
                raise ValueError(
                    f"UPSTREAM ERROR: Noise Provider '{noise_id}' returned shape {noise_2d.shape}."
                )

            # --- PRESERVE ZERO LOGIC ---
            # If preserve_zero is enabled, we scale the intensity of the noise
            # by the current signal. In the "full" interior (1.0), noise is 100%.
            # At the vignetted edges (e.g., 0.1), the noise amplitude drops
            # significantly (e.g., to 0.07), ensuring the edge remains a smooth gradient.
            effective_noise_amp = noise_amp
            if preserve_zero:
                effective_noise_amp = noise_amp * (signal ** noise_atten_power)

            # Formula logic:
            # The low_bound moves closer to the high_bound (signal) as signal drops.
            low_bound = signal * (1.0 - effective_noise_amp)
            high_bound = signal

            # Interpolate: signal = low + (noise * delta)
            signal = low_bound + (noise_2d * (high_bound - low_bound))

    # C. Signal Shaping
    if contrast != 1.0:
        signal = np.clip((signal - 0.5) * contrast + 0.5, 0.0, 1.0)

    # Final enforcement: If preserve_zero is true, we multiply by the blurred mask
    # one last time. This acts as a secondary vignette that ensures any noise
    # artifacts created near the threshold are "crushed" back to zero.
    if preserve_zero:
        signal = signal * np.clip(mask.astype(np.float32), 0.0, 1.0)

    final_res = np.clip(signal * max_opacity, 0.0, 1.0)

    return final_res
