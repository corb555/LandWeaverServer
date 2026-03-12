from functools import wraps
from typing import Callable, Dict, Any

import numpy as np

from ThematicRender.keys import DriverKey

# surface_library.py
SURFACE_PROVIDER_REGISTRY: Dict[str, Callable] = {}


def spatial_surface(provider_id: str):
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

        SURFACE_PROVIDER_REGISTRY[provider_id] = wrapper
        return wrapper

    return decorator


@spatial_surface("ramp")
def _ramp_provider(ctx, spec, data_2d, masks_2d, factors_2d, style_engine):
    # This factor is "elev_m" (Raw Meters)
    f_id = spec.coord_factor
    factor_val = factors_2d.get(f_id)

    interp_func = ctx.surfaces.get(spec.key)
    u_min, u_max = float(interp_func.x[0]), float(interp_func.x[-1])

    coords = np.clip(factor_val, u_min, u_max)
    return interp_func(coords)

@spatial_surface("theme")
def _style_provider(ctx, spec, data_2d, masks_2d, factors_2d, style_engine):
    """
    Fetches categorical RGB.
    """
    # 1. Extract the specific thematic array from the  dictionary
    theme_ids = data_2d.get(DriverKey.THEME)

    if theme_ids is None:
        raise ValueError(f"Error: Surface Library 'style': {DriverKey.THEME} not found")

    # 2. Pass the ARRAY, not the DICTIONARY, to the style engine
    return style_engine.get_theme_surface(theme_ids)


MODIFIER_REGISTRY: Dict[str, Callable] = {}


def register_modifier(mod_id: str):
    def decorator(func):
        MODIFIER_REGISTRY[mod_id] = func
        return func

    return decorator


@register_modifier("mottle")
def _mottle_modifier(img_block: np.ndarray, noise: np.ndarray, profile: Any) -> np.ndarray:
    """
    Standard Mottle: Centered at 0 to provide dark and light variation.
    """
    # Shift noise from [0.0, 1.0] to [-0.5, 0.5]
    centered_noise = noise - 0.5

    # Calculate RGB shift
    shift = (centered_noise * np.array(profile.shift_vector, dtype="float32")) * profile.intensity

    # Apply and clip to valid 8-bit color range
    return np.clip(img_block + shift, 0, 255)
