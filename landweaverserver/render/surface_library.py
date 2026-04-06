from dataclasses import dataclass
from functools import wraps
from typing import Callable, Dict, Any, Tuple

import numpy as np
from rasterio.windows import Window
from scipy.interpolate import interp1d

# surface_library.py
SURFACE_PROVIDER_REGISTRY: Dict[str, Callable] = {}
MODIFIER_REGISTRY: Dict[str, Callable] = {}


@dataclass(frozen=True, slots=True)
class SurfaceContext:
    """Explicit contract for Surface Provider functions."""
    cfg: Any  # RenderConfig
    noises: Any  # NoiseLibrary (already attached to SHM)
    window: Window  # Current tile window
    surfaces: Dict[Any, interp1d]  # Pre-calculated color ramps
    target_shape: Tuple[int, int]  # (H, W) for the current tile


def surface_builder(surface_builder_nm: str):
    """
    Updated Decorator Contract:
    Receives 6 arguments (ctx, spec, data_2d, masks_2d, factors_2d, style_engine)
    Enforces (H, W, 3) float32 output.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(ctx, spec, data_2d, masks_2d, factors_2d, style_engine):
            # Pass all 6 arguments to the underlying provider function
            res = func(ctx, spec, data_2d, masks_2d, factors_2d, style_engine)

            if res is None:
                return np.zeros((*ctx.target_shape, 3), dtype="float32")

            # Coerce to RGB if needed
            if res.ndim == 2:
                res = np.stack([res] * 3, axis=-1)

            return res.astype("float32", copy=False)

        SURFACE_PROVIDER_REGISTRY[surface_builder_nm] = wrapper
        return wrapper

    return decorator


@surface_builder("theme")
def _theme_provider(ctx: SurfaceContext, spec, data_2d, masks_2d, factors_2d, style_engine):
    """Generate the theme surface for the current tile."""
    theme_ids = data_2d.get(spec.source)

    if theme_ids is None:
        available_keys = list(data_2d.keys())
        raise ValueError(
            f"Theme Provider: Source '{spec.source}' ({type(spec.source)}) not found. "
            f"Available keys: {available_keys}"
        )

    #smoothed_ids = style_engine.get_smoothed_ids(theme_ids)
    tile_ctx = style_engine.build_tile_context(theme_ids)

    return style_engine.get_theme_surface(
        theme_ids, ctx, tile_ctx=tile_ctx, )


@surface_builder("ramp")
def _ramp_provider(ctx: SurfaceContext, spec, data_2d, masks_2d, factors_2d, style_engine):
    # This factor is "elev_m" (Raw Meters)
    f_id = spec.input_factor
    if f_id is None:
        raise ValueError(f"input_factor is missing for surface {spec.key}")
    factor_val = factors_2d.get(f_id)
    if factor_val is None:
        raise ValueError(f"input_factor not found for surface {spec.key}")

    interp_func = ctx.surfaces.get(spec.key)
    if interp_func is None:
        print(f" Key='{spec.key}' Surfaces: {ctx.surfaces}")
        print(ctx)
    u_min, u_max = float(interp_func.x[0]), float(interp_func.x[-1])

    coords = np.clip(factor_val, u_min, u_max)
    return interp_func(coords)


def register_modifier(mod_id: str):
    def decorator(func):
        MODIFIER_REGISTRY[mod_id] = func
        return func

    return decorator


@register_modifier("color_mottle")
def _mottle_color(img_block, noise_rgb, profile):
    centered = noise_rgb - 0.5
    # (H,W,1) * (3,) results in (H,W,3) perfectly
    shift = (centered * np.array(profile.shift_vector, dtype="float32")) * profile.intensity
    return np.clip(img_block + shift, 0.0, 255.0)
