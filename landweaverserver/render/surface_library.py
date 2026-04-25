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

    # smoothed_ids = style_engine.get_smoothed_ids(theme_ids)
    tile_ctx = style_engine.build_tile_context(theme_ids)

    return style_engine.get_theme_surface(
        theme_ids, ctx, tile_ctx=tile_ctx, )


@surface_builder("ramp")
def _ramp_provider(ctx: SurfaceContext, spec, data_2d, masks_2d, factors_2d, style_engine):
    f_id = spec.input_factor
    factor_val = factors_2d.get(f_id)
    if factor_val is None:
        raise ValueError(f"input_factor {f_id} not found")

    interp_func = ctx.surfaces.get(spec.key)
    return interp_func(factor_val)


def register_modifier(mod_id: str):
    def decorator(func):
        MODIFIER_REGISTRY[mod_id] = func
        return func

    return decorator


@register_modifier("color_mottle")
def _mottle_color(img_block, noise_signal, profile, baked_vector):
    """
    Memory-Lean Mottle:
    - img_block: (H, W, 3) float32
    - noise_signal: (H, W, 1) float32
    """
    # noise_signal[..., 0] is a zero-cost view of (H, W)
    ns = noise_signal[..., 0]

    # Process channels individually to avoid allocating a large (H, W, 3) temporary
    # This is 4-6x faster than broadcasting in this specific case.
    img_block[..., 0] += ns * baked_vector[0]
    img_block[..., 1] += ns * baked_vector[1]
    img_block[..., 2] += ns * baked_vector[2]

    return img_block
