import hashlib
from pathlib import Path
import traceback
from typing import Dict, Any, Optional, Iterable, Final, Callable

import numpy as np
from rasterio.windows import Window

from landweaverserver.common.keys import RequiredResources, SurfaceKey, SourceKey
from landweaverserver.render.color_config import ColorConfig
from landweaverserver.render.color_ramp_hsv import get_ramp_from_yml
from landweaverserver.render.noise_engine import NoiseEngine
from landweaverserver.render.render_config import RenderConfig
from landweaverserver.render.surface_library import SURFACE_PROVIDER_REGISTRY, MODIFIER_REGISTRY, \
    SurfaceContext

# surface_engine.py
EXPECTED_BANDS = 3
OPAQUE_ALPHA: Final[int] = 255


def strip_alpha_or_fail(colors: np.ndarray, *, context: str) -> np.ndarray:
    """Normalize a ramp color table to RGB."""
    if colors.ndim != 2 or colors.shape[1] not in (3, 4):
        raise ValueError(f"{context}: expected colors (N,3) or (N,4), got {colors.shape}.")

    if colors.shape[1] == 3:
        return colors[:, :3]

    alpha_i = np.round(colors[:, 3]).astype("int64", copy=False)
    if np.any(alpha_i != OPAQUE_ALPHA):
        bad_idx = np.where(alpha_i != OPAQUE_ALPHA)[0][0]
        raise ValueError(
            f"{context}: non-opaque alpha at row {bad_idx} (val={alpha_i[bad_idx]}). "
            f"Surfaces must be RGB; move opacity into factors."
        )
    return colors[:, :3]


class FastRamp:
    def __init__(self, x_coords: np.ndarray, colors: np.ndarray, lut_size: int = 1024):
        # 1. Store domain for normalization (needed to map meters to LUT index)
        self.x = x_coords
        self.u_min = float(x_coords[0])
        self.u_max = float(x_coords[-1])
        self.range = self.u_max - self.u_min if self.u_max != self.u_min else 1.0

        # 2. BAKE THE LUT: Use Scipy ONCE right now to create the fast array
        from scipy.interpolate import interp1d
        f_temp = interp1d(x_coords, colors, axis=0, fill_value="extrapolate", kind="linear")

        # Create 1024 points covering the range from u_min to u_max
        lut_x = np.linspace(self.u_min, self.u_max, lut_size)
        self.lut = f_temp(lut_x).clip(0, 255).astype(np.uint8)
        self.lut_size_m1 = lut_size - 1

    def __call__(self, data: np.ndarray) -> np.ndarray:
        # 1. Map input (e.g., meters) to 0.0-1.0 range
        t = (data - self.u_min) / self.range

        # 2. Map 0.0-1.0 to integer indices [0 ... 1023]
        indices = (t * self.lut_size_m1).astype(np.int32)

        # 3. Clip ensures we handle values outside the original elevation range
        np.clip(indices, 0, self.lut_size_m1, out=indices)

        # 4. Near-instant memory lookup
        return np.take(self.lut, indices, axis=0)


class SurfaceEngine:
    def __init__(self, cfg: RenderConfig):
        self.cfg = cfg
        self.target_shape = None

        # Runtime registry of surface generators (Ramps, Themes, etc.)
        # Logic: Input(2D Data) -> Output(3D RGB)
        self.surfaces: Dict[SurfaceKey, Callable[[np.ndarray], np.ndarray]] = {}
        self.ramp_paths: Dict[str, Path] = {}
        self._modifier_cache: Dict[str, np.ndarray] = {}

        # Load registry from settings
        self.spec_registry = {s.key: s for s in cfg.surfaces}

        # --- DETERMINISTIC OFFSET CACHE ---
        # We calculate this once. Every worker process can use these to offset random effects
        self._offset_cache: Dict[SurfaceKey, int] = {}
        for skey in self.spec_registry.keys():
            seed_bytes = skey.encode('utf-8')
            stable_hash = hashlib.md5(seed_bytes).hexdigest()
            self._offset_cache[skey] = int(stable_hash[:8], 16) % 1000

    def generate_surfaces(
            self, data_2d: dict, masks_2d: dict, factors_2d: dict, style_engine: Any,
            surface_inputs: Iterable[SurfaceKey], noises: NoiseEngine, window: Window,
            anchor_key: SourceKey
    ) -> Dict[SurfaceKey, np.ndarray]:
        """
        Synthesizes the required RGB surfaces for the current tile.
        The comp engine will use blend_ops and factors to combine these into the final result
        """
        # Establish master geometry from the anchor
        anchor_data = data_2d.get(anchor_key)
        if anchor_data is None:
            raise KeyError(f"Surface Engine: Anchor '{anchor_key}' not found in data_2d.")

        target_h, target_w = anchor_data.shape[:2]
        self.target_shape = (target_h, target_w)

        ctx = SurfaceContext(
            cfg=self.cfg, noises=noises, window=window, surfaces=self.surfaces,
            target_shape=self.target_shape
        )

        rendered_surfaces = {}
        for srf_key in surface_inputs:
            spec = self.spec_registry.get(srf_key)
            if spec is None:
                available = list(self.spec_registry.keys())
                raise KeyError(
                    f"Surface Engine: Required surface '{srf_key}' not found in registry. "
                    f"Check your SURFACE_SPECS definition. Available: {available}"
                )

            provider_fn = SURFACE_PROVIDER_REGISTRY.get(spec.surface_builder)
            if not provider_fn:
                available = list(SURFACE_PROVIDER_REGISTRY.keys())
                raise ValueError(
                    f"Unknown provider '{spec.surface_builder}' for surface {srf_key}. Available: "
                    f"{available}"
                )

            try:
                # --- STAGE 1: SYNTHESIS ---
                # Generate the base RGB block from the provider (Ramp/Theme/etc)
                block = provider_fn(ctx, spec, data_2d, masks_2d, factors_2d, style_engine)

                # --- STAGE 2: MODIFICATION ---
                # Apply procedural textures if defined in config
                if spec.modifiers:
                    block = self._apply_modifiers(srf_key, spec, block, noises, window)

                # Validation and storage
                if block.shape != (target_h, target_w, 3):
                    raise ValueError(f"Shape mismatch in {srf_key}")

                rendered_surfaces[srf_key] = block

            except Exception as e:
                print(f"\n❌ Surface Engine Error: [{srf_key}]")
                traceback.print_exc()

                raise e

        return rendered_surfaces

    @staticmethod
    def get_ramp_paths(cfg: 'RenderConfig', resources: 'RequiredResources') -> Dict[str, Path]:
        """
        RESOLVER: Dynamically maps surface names to  files by
        inspecting the ramps_yml defined in the config.
        """
        paths: Dict[str, Path] = {}

        # 1. Load the secondary YAML (biome_ramps.yml)
        # FileKey.RAMPS_YML usually maps to the string "ramps_yml"
        ramps_cfg_path = cfg.path("ramps_yml")
        if not ramps_cfg_path or not ramps_cfg_path.exists():
            return {}  # Fallback or raise error

        import yaml
        with open(ramps_cfg_path, 'r') as f:
            ramps_data = yaml.safe_load(f).get("RAMPS", {})

        def resolve_to_physical_path(skey: str) -> Optional[Path]:
            """Helper to find the .txt path for a given key."""
            # Look up the key in the biome_ramps.yml data
            entry = ramps_data.get(skey)
            if not entry or entry.get("mode") != "file":
                return None

            # Get the filename (e.g., 'arid_base_color_ramp.txt')
            filename = entry.get("file")
            if not filename:
                return None

            # Use the directory of the ramps_yml to resolve the relative .txt path
            return ramps_cfg_path.parent / filename

        # 2. Resolve Primary Pivot (Fallback)
        primary_path = resolve_to_physical_path(resources.primary_surface)

        # 3. Resolve every required input
        for skey in resources.surface_inputs:
            spec = cfg.get_surface_spec(skey)
            if spec is None or spec.surface_builder != "ramp":
                continue

            # Check for explicit entry in the ramps YAML
            explicit_path = resolve_to_physical_path(skey)

            # Inheritance logic
            final_path = explicit_path if explicit_path else primary_path

            if final_path:
                paths[skey] = final_path

        return paths

    def load_surface_ramps(self, resources: RequiredResources):
        """
        LOADER:  reads the resolved paths and bakes LUTs.
        """
        # 1. Get the Map (String -> Path)
        ramp_map = self.get_ramp_paths(self.cfg, resources)

        self.ramp_paths.clear()
        self.surfaces.clear()

        # 2. Process every found ramp
        for skey, path in ramp_map.items():
            if not path.exists():
                raise FileNotFoundError(f"Ramp file for '{skey}' not found at {path}")

            z, c = ColorConfig.parse_ramp(str(path))
            c_rgb = strip_alpha_or_fail(c, context=f"surface ramp {skey}")

            # self.surfaces is now Dict[str, FastRamp]
            self.surfaces[skey] = FastRamp(z, c_rgb)
            self.ramp_paths[skey] = path

        # 3. CRITICAL: Validation
        # If a surface is a 'ramp' but didn't get a path, we MUST fail here.
        for skey in resources.surface_inputs:
            spec = self.cfg.get_surface_spec(skey)
            if spec and spec.surface_builder == "ramp" and skey not in self.surfaces:
                raise ValueError(
                    f"Configuration Error: Surface '{skey}' is a ramp, but no file path "
                    f"could be resolved. Add '{skey}: path/to/file.txt' to the 'files:' "
                    f"section or define a primary pivot."
                )

    @staticmethod
    def get_ramp_hash(cfg: RenderConfig, resources: RequiredResources) -> str:
        """
        HASH: Generates a fingerprint of the  assets used by the ramps.
        Used by JobResolver to detect if a color palette file was edited.
        """
        import hashlib

        # Call discovery module
        ramp_map = SurfaceEngine.get_ramp_paths(cfg, resources)

        # Collect paths and mtimes
        # We sort by key to ensure the hash is deterministic
        hash_parts = []
        for skey in sorted(ramp_map.keys()):
            path = ramp_map[skey].resolve()
            mtime = path.stat().st_mtime_ns
            hash_parts.append(f"{skey}:{path}:{mtime}")

        return hashlib.md5("|".join(hash_parts).encode("utf-8")).hexdigest()

    def _load_and_interpolate(self, skey, base_path, ramps_yml_path, out_dir):
        """Helper to resolve a ramp file and build the optimized FastRamp."""
        yaml_name = f"{skey}_color_ramp"
        mode, ramp_path = self._resolve_ramp_file(
            skey=skey, yaml_name=yaml_name, base_ramp_path=base_path, ramp_yml_path=ramps_yml_path,
            output_dir=out_dir
        )

        if ramp_path is None or not ramp_path.exists():
            raise FileNotFoundError(f"Ramp file {skey} not found at {ramp_path}")

        # 1. Parse the ramp file (raw elevation points and RGB colors)
        print(f"Load color ramp: key:'{skey}' {ramp_path}")
        z, c = ColorConfig.parse_ramp(str(ramp_path))
        c_rgb = strip_alpha_or_fail(c, context=f"surface ramp {skey}")

        # 2. Initialize FastRamp (it will bake its own LUT internally)
        self.surfaces[skey] = FastRamp(z, c_rgb)
        self.ramp_paths[skey] = ramp_path

    def configure_surface(self, resources: RequiredResources, output_dir: Optional[str] = None):
        self.load_surface_ramps(resources)
        self._modifier_cache.clear()

        # 1. Bake vectors for active profiles
        for p_id, profile in self.cfg.modifiers.items():
            if profile.intensity > 0:
                self._modifier_cache[p_id] = np.array(
                    profile.shift_vector, dtype="float32"
                ) * profile.intensity

        # 2. Build the Execution Plan for every surface
        self._modifier_plans = {}
        for s_key, spec in self.spec_registry.items():
            plan = []
            for mod_cfg in spec.modifiers:
                p_id = mod_cfg.get("mod_profile")
                baked_vec = self._modifier_cache.get(p_id)

                if baked_vec is not None:
                    # Pre-resolve everything into a simple tuple
                    plan.append(
                        (MODIFIER_REGISTRY[mod_cfg["effect"]],  # mod_fn
                         self.cfg.modifiers[p_id],  # profile
                         baked_vec,  # baked_vector
                         mod_cfg.get("noise_id")  # noise_id (optional override)
                         )
                    )
            if plan:
                self._modifier_plans[s_key] = plan

    def _apply_modifiers(
            self, srf_key: SurfaceKey, spec: Any, img_block: np.ndarray, noises: NoiseEngine,
            window: Window
    ) -> np.ndarray:
        # 1. Zero-overhead lookup
        plan = self._modifier_plans.get(srf_key)
        if not plan:
            return img_block

        offset = self._offset_cache.get(srf_key, 0)

        # 2. Iterate the pre-resolved plan
        for mod_fn, profile, baked_vector, noise_id_override in plan:
            # Noise provider lookup is still here, but could be pre-cached too
            nid = noise_id_override or profile.noise_id

            noise_2d = noises.get(nid).window_noise(
                window, row_off=offset, col_off=offset
            )

            # Library execution
            img_block = mod_fn(img_block, noise_2d[..., np.newaxis], profile, baked_vector)

        return img_block

    def _resolve_ramp_file(
            self, *, skey, yaml_name, base_ramp_path, ramp_yml_path, output_dir
    ):
        """Resolves path using ConfigMgr or derives a new one using ramps_yml."""
        # Check ConfigMgr for an explicit file path provided by user
        explicit_path = self.cfg.path(skey)

        if explicit_path and explicit_path.exists():
            return "file", explicit_path

        # Otherwise, attempt to derive using the color_ramp_hsv logic
        if ramp_yml_path is None:
            raise ValueError(
                f"Cannot derive ramp for '{skey}': ramps_yml path not provided."
            )

        out_path = output_dir / f"gen_{yaml_name}.txt"
        if out_path and out_path.exists():
            return "file", out_path
        try:
            mode, fname = get_ramp_from_yml(
                ramp_name=yaml_name, ramps_yml_settings=str(ramp_yml_path),
                base_ramp=str(base_ramp_path) if base_ramp_path else None, output_path=str(out_path)
            )
            return mode, Path(fname)
        except Exception as e:
            raise ValueError(f"Failed to derive ramp '{skey}': {e}")

    def _default_ramp_output_dir(self) -> Path:
        """Finds a safe place to dump derived text files."""
        out_path = self.cfg.path("output")
        if out_path:
            return out_path.parent
        return Path.cwd()
