from functools import wraps
from typing import Dict, Callable, Any

import numpy as np

# factor_library.py
from Render.spatial_math import normalize_step, lerp
from Render.theme_registry import refine_organic_signal
from Render.utils import SAFE_FUNCTIONS

# factor_library.py

FACTOR_REGISTRY: Dict[str, Callable] = {}


def spatial_factor(function_id: str):
    """
    Registers a library function and enforces the (H, W, 1) storage contract.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(data_2d, masks_2d, name, ctx):
            # The Engine passes a SimpleNamespace 'ctx' which includes the current 'spec'
            timer_key = f"Factor: {name}"
            if ctx.tmr: ctx.tmr.start(timer_key)

            # Execute the 2D math
            res_2d = func(data_2d, masks_2d, name, ctx)

            if ctx.tmr: ctx.tmr.end()

            # Promote to 3D for the storage layer firewall
            return res_2d[..., np.newaxis]

        FACTOR_REGISTRY[function_id] = wrapper
        return wrapper

    return decorator


# -----------------------------------------------------------------------------
# Logic Blocks
# -----------------------------------------------------------------------------

def _map_and_refine(data_2d, masks_2d, name, lib_ctx, driver_key):
    """
    'Remapping' factors
    Raw -> Normalize ->  Refine -> Masked Default.
    """
    params = lib_ctx.spec.params

    # we require start and full.
    if "start" not in params or "full" not in params:
        raise KeyError(
            f"Factor '{name}' uses a mapping function but is missing "
            f"required parameters 'start' and 'full' in its YAML params block."
        )

    # Identify the specific driver band requested
    raw_data = np.squeeze(data_2d[driver_key])
    if raw_data.ndim == 3:
        band_idx = int(params.get("band", 1)) - 1
        raw_plane = raw_data[:, :, band_idx]
    else:
        raw_plane = raw_data

    # 1. Normalize range: Linear normalization based on config start/full
    remapped = normalize_step(raw_plane, float(params.get("start")), float(params.get("full")))

    # 2. Blur, Noise, Contrast, and Power-curves
    refined = refine_organic_signal(
        mask=remapped, params=lib_ctx.spec.params, ctx=lib_ctx
    )

    # Ensure 'refined' hasn't picked up a trailing (..., 1) dimension
    # from noise sampling or internal math.
    refined_2d = np.squeeze(refined)
    valid_mask_2d = np.squeeze(masks_2d[driver_key])

    # 5. Gating
    default_val = float(params.get("default_fill", 0.0))

    # 6. Linear Interpolation (Now safe because both are 2D)
    result_2d = lerp(default_val, refined_2d, valid_mask_2d)

    return result_2d


# -----------------------------------------------------------------------------
# The Library
# -----------------------------------------------------------------------------

class FactorLibrary:
    @staticmethod
    @spatial_factor("raw_driver")
    def raw_driver(data_2d, masks_2d, name, lib_ctx):
        """
        Identity Operation: Promotes a physical driver to a logical factor.

        This function acts as a 'Semantic Airlock.' It moves raw data from the
        physical input dictionary (data_2d) to the logical factor dictionary
        (factors_2d) without modification.

        Architectural Purpose:
        1. DIMENSIONAL FIREWALL: Ensures the data is squeezed into a strictly
           2D NumPy array, removing any band or shared-memory slot dimensions.
        2. STABLE ALIASING: Allows the user to link a logical name (e.g., 'elev_m')
           to a physical file (e.g., 'terrain_v4.tif') so that changing files
           doesn't require updating every surface or pipeline step.
        3. PACKAGE ISOLATION: Allows the Rendering Engines to remain 'blind' to
           the physical Driver Pool, operating only on the Factor signal dictionary.

        Use Cases:
        - Providing 'elev_m' (raw meters) as a coordinate for color ramp sampling.
        - Passing through raw categorical 'theme_ids' for specialized masking.
        - Exposing raw physics values (like Slope) to the 'expression' factor.

        Drivers: [Target_Driver] (The first driver in the YAML list is used)
        """
        # 1. Physical Lookup
        # We use the explicit driver name provided in the factor's 'drivers' list
        if not lib_ctx.spec.drivers:
            raise ValueError(f"Factor '{name}' (raw_driver) requires at least one driver.")

        drv_key = list(lib_ctx.spec.drivers)[0]

        # 2. Extract Data
        # Use np.squeeze to ensure we fulfill the 2D Firewall contract
        raw_data = data_2d.get(drv_key)

        if raw_data is None:
            raise KeyError(f"Factor '{name}' requested driver '{drv_key}', but it is missing from memory.")

        # 3. Direct Return (Identity Math)
        return np.squeeze(raw_data)

    @staticmethod
    @spatial_factor("mapped_signal")
    def mapped_signal(data_2d, masks_2d, name, lib_ctx):
        """
        """
        driver_key = next(iter(lib_ctx.spec.drivers))
        return _map_and_refine(data_2d, masks_2d, name, lib_ctx, driver_key)

    @staticmethod
    @spatial_factor("theme_composite")
    def theme_composite(data_2d, masks_2d, name, lib_ctx):
        # Use the first driver for this factor
        drv_key = list(lib_ctx.spec.drivers)[0]

        """Aggregate configured thematic categories into a composite alpha."""
        theme_ids = data_2d[drv_key]

        tile_ctx = lib_ctx.themes.build_tile_context(theme_ids)
        composite_alpha = np.zeros(lib_ctx.target_shape, dtype=np.float32)

        for spec in tile_ctx.active_specs:
            binary_mask = tile_ctx.masks_by_id[spec.theme_id]
            if not np.any(binary_mask):
                continue

            cat_alpha = refine_organic_signal(
                mask=binary_mask, params=spec, ctx=lib_ctx
            )
            composite_alpha = np.maximum(composite_alpha, cat_alpha)

        return composite_alpha * np.squeeze(masks_2d[drv_key])

    @staticmethod
    @spatial_factor("protected_shaping")
    def protected_shaping(data_2d, masks_2d, name, lib_ctx):
        """
        Shapes a grayscale signal into a multiplicative factor while
        preserving highlights and shadows (mid-tone protection).

        This is used to modulate brightness (e.g. Hillshading or Texturing)
        without 'crushing' the underlying colors.
        """
        params = lib_ctx.spec.params
        driver_key = next(iter(lib_ctx.spec.drivers))
        raw_signal = data_2d.get(driver_key)

        if raw_signal is None:
            return np.ones(lib_ctx.target_shape, dtype="float32")

        # 1. Normalize Input
        # 'input_scale' allows handling both 8-bit (255) and float (1.0) drivers
        scale = float(params.get("input_scale", 255.0))
        val = np.clip(raw_signal / scale, 0.0, 1.0)

        # 2. Volume Adjustment (Gamma)
        gamma = float(params.get("gamma", 1.0))
        if gamma != 1.0:
            val = np.power(val, gamma)

        # 3. Protection Logic (The 'Airlock' for colors)
        # Prevents the signal from pushing pixels to pure black or pure white.

        # Shadow protection:
        t_low = (val - float(params["low_start"])) / max(
            float(params["low_end"]) - float(params["low_start"]), 1e-6
        )
        w_low = (1.0 - np.clip(t_low, 0, 1)) * float(params["protect_lows"])

        # Highlight protection:
        t_high = (val - float(params["high_start"])) / max(
            float(params["high_end"]) - float(params["high_start"]), 1e-6
        )
        w_high = np.clip(t_high, 0, 1) * float(params["protect_highs"])

        # Combine protection weights and apply to the signal
        m_protected = val + np.maximum(w_low, w_high) * (1.0 - val)

        # 4. Strength Scaling
        # 1.0 is the 'Neutral' state for multiplication.
        # Strength moves the signal further from or closer to 1.0.
        strength = float(params.get("strength", 1.0))
        m_final = 1.0 + strength * (m_protected - 1.0)

        # 5. Validity Masking
        # Ensures that NoData areas return exactly 1.0 (Multiply by 1.0 = No change)
        valid_mask = np.squeeze(masks_2d[driver_key])
        return 1.0 + valid_mask * (m_final - 1.0)

    @staticmethod
    @spatial_factor("specular_highlights")
    def specular_highlights(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.spec.params
        noise_id = lib_ctx.spec.required_noise
        noise_provider = lib_ctx.noises.get(noise_id)

        mask_key = lib_ctx.spec.required_factors[0] if lib_ctx.spec.required_factors else None
        mask = _get_required_factor(lib_ctx, mask_key) if mask_key else 1.0

        scale = float(params.get("scale", 6.0))
        floor = float(params.get("floor", 0.4))
        sensitivity = float(params.get("sensitivity", 2.0))

        noise = np.squeeze(noise_provider.window_noise(lib_ctx.window, scale_override=scale))

        # Math: Subtract floor, clip, then apply aggressive power curve
        n = np.clip(noise + floor - 0.5, 0, 1)
        glints = np.power(n, 10.0 / max(sensitivity, 0.1))

        return glints * mask

    @staticmethod
    @spatial_factor("noise_overlay")
    def noise_overlay(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.spec.params

        # Use the noise profile defined in the spec (e.g., "water")
        noise_id = lib_ctx.spec.required_noise
        noise_provider = lib_ctx.noises.get(noise_id)

        mask_key = lib_ctx.spec.required_factors[0] if lib_ctx.spec.required_factors else None
        mask = _get_required_factor(lib_ctx, mask_key) if mask_key else 1.0

        scale = float(params.get("scale", 3.0))
        noise = np.squeeze(noise_provider.window_noise(lib_ctx.window, scale_override=scale))

        # Pattern: 1.0 is neutral for multiply. Noise pushes it up or down.
        intensity = float(params.get("intensity", 0.2))
        shading = (1.0 - intensity) + (noise * intensity)

        # Blend shading only where the mask exists
        return 1.0 + mask * (shading - 1.0)

    @staticmethod
    @spatial_factor("proximity_power")
    def proximity_power(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.spec.params

        # Use the primary driver from the spec
        driver_key = next(iter(lib_ctx.spec.drivers))
        prox_data = data_2d.get(driver_key)

        # Use the dependency defined in the spec (usually "water")
        mask_key = lib_ctx.spec.required_factors[0] if lib_ctx.spec.required_factors else None
        mask = _get_required_factor(lib_ctx, mask_key) if mask_key else 1.0

        if prox_data is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # Parameters drive the curve
        max_d = float(params.get("max_range_px", 100.0))
        sensitivity = float(params.get("sensitivity", 1.0))

        res = np.clip(prox_data / max_d, 0.0, 1.0)
        if sensitivity != 1.0:
            res = np.power(res, 1.0 / max(sensitivity, 0.01))

        return res * mask

    @staticmethod
    @spatial_factor("categorical_mask")
    def categorical_mask(data_2d, masks_2d, name, lib_ctx):
        # Pull the label from the config for THIS factor (e.g., params['label'] = "water")
        params = lib_ctx.spec.params
        target_label = params.get("label", name)
        drv_key = list(lib_ctx.spec.drivers)[0]

        theme_ids = data_2d.get(drv_key)
        if theme_ids is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # Bridge between config logic and QML IDs
        target_val = lib_ctx.theme_registry.name_to_id.get(target_label)
        if target_val is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # Logic is  generic for any ID in the theme
        return (theme_ids == target_val).astype("float32")

    @staticmethod
    @spatial_factor("edge_fade")
    def edge_fade(data_2d, masks_2d, name, lib_ctx):
        """
        Creates an organic alpha transition based on proximity within a specific category.
        Useful for fading water at the shore or thinning forest at the tree-line.
        """
        params = lib_ctx.spec.params
        # Use the primary driver from the spec
        driver_key = next(iter(lib_ctx.spec.drivers))
        prox_data = data_2d.get(driver_key)
        theme_ids = data_2d.get(driver_key)

        # 1. Fetch Drivers from data dictionary
        # Proximity represents distance (in meters or pixels) from a feature boundary
        if prox_data is None or theme_ids is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # 2. Identify the target category from Config
        # Allows this function to work for named category
        target_label = params.get("label", name)
        target_id = lib_ctx.theme_registry.name_to_id.get(target_label.lower())

        if target_id is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # 3. Create the binary gate (Where is this feature?)
        binary_mask = (theme_ids == target_id).astype("float32")

        # 4. Calculate the Alpha Ramp
        # ramp_width: The distance over which the feature goes from 0% to 100% opaque.
        ramp_width = float(params.get("ramp_width", 15.0))
        alpha = np.clip(prox_data / max(ramp_width, 0.1), 0.0, 1.0)

        # 5. Apply Non-linear Shaping (Power Curve)
        # Allows for 'smooth' vs 'hard' transitions
        sensitivity = float(params.get("sensitivity", 1.0))
        if sensitivity != 1.0:
            alpha = np.power(alpha, 1.0 / max(sensitivity, 0.01))

        # Mask the alpha so it only exists inside the categorical boundary
        return alpha * binary_mask

    @staticmethod
    @spatial_factor("constrained_signal")
    def constrained_signal(data_2d, masks_2d, name, lib_ctx):
        """
        Procedural mask generator implementing the 'Apparent Boundary' pattern.

        This function creates organic transitions (like snowlines, tree-lines, or
        vegetation bands) by combining a primary geographic signal with stochastic
        jitter, then subjecting the result to a physical constraint.

        Logic Flow:
        1. PERTURBATION: The primary driver (e.g., Elevation) is displaced by a noise
           field. This creates 'Apparent Elevation,' where the boundary wanders
           naturally into valleys or up ridges instead of following rigid contours.
        2. THRESHOLDING: A smooth linear-step (ramp) is applied to the apparent
           signal to create a soft probability mask (0..1).
        3. CONSTRAINT: A secondary driver (e.g., Slope) acts as a physical penalty.
           If the constraint exceeds a limit (e.g., a cliff is too steep), the
           signal is stripped away, regardless of elevation.

        Parameters (params):
            threshold (float): The central value where the transition occurs.
            ramp (float): The vertical/horizontal width of the fade zone.
            jitter_amt (float): The maximum distance (in units) the noise wiggles the line.
            noise_id (str): Reference to the noise_profile for organic wandering.
            constraint_limit (float): The value where the physical penalty peaks.
            constraint_fade (float): The softness of the penalty transition.
            invert_threshold (bool): Optional. If True, signal is 1.0 BELOW threshold.

        Drivers: [Primary_Driver, Constraint_Driver]
        """
        params = lib_ctx.spec.params

        # 1. Map Drivers using the explicit indices from the YAML 'drivers' list
        # Example: [dem, slope] or [proximity, dem]
        primary_data = data_2d[lib_ctx.spec.drivers[0]]
        constraint_data = data_2d[lib_ctx.spec.drivers[1]]

        # 2. Generate Organic Jitter (The 'Wandering' component)
        noise_id = params.get("noise_id", "none")
        jitter_val = float(params.get("jitter_amt", 0.0))

        if noise_id != "none" and jitter_val > 0:
            noise_provider = lib_ctx.noises.get(noise_id)
            if noise_provider is None:
                raise KeyError(f"Factor '{name}' references unknown noise_id '{noise_id}'")

            noise = np.squeeze(noise_provider.window_noise(lib_ctx.window))
            # Transform Primary Data into an Apparent State
            # (noise-0.5)*2.0 centers the noise around 0.0 (-jitter to +jitter)
            effective_signal = primary_data + ((noise - 0.5) * 2.0 * jitter_val)
        else:
            effective_signal = primary_data

        # 3. Apply Boundary Threshold (The 'Probability' component)
        threshold = float(params.get("threshold", 0.0))
        ramp = float(params.get("ramp", 1.0))
        invert = params.get("invert_threshold", False)

        # Calculate linear distance from the edge of the ramp
        mask = np.clip((effective_signal - (threshold - ramp / 2)) / ramp, 0.0, 1.0)
        if invert:
            mask = 1.0 - mask

        # 4. Apply Physical Constraint (The 'Physics' component)
        # 1.0 is full adhesion; 0.0 is full stripping/penalty.
        limit = float(params.get("constraint_limit", 90.0))
        limit_fade = float(params.get("constraint_fade", 1.0))

        penalty = 1.0 - np.clip((constraint_data - (limit - limit_fade)) / limit_fade, 0.0, 1.0)

        # 5. Composite Final Factor (Ensure 2D firewall is maintained)
        return np.squeeze(mask * penalty)

    @staticmethod
    @spatial_factor("expression")
    def expression(data_2d, masks_2d, name, lib_ctx):
        """Evaluate a precompiled safe math expression for the current tile."""
        code = lib_ctx.expression_cache.get(name)
        if code is None:
            raise RuntimeError(f"Expression for '{name}' was not pre-compiled.")

        namespace: dict[str, Any] = dict(SAFE_FUNCTIONS)
        valid_mask = np.ones(lib_ctx.target_shape, dtype=np.float32)

        for d_key in lib_ctx.spec.drivers or []:
            arr = np.squeeze(np.asarray(data_2d[d_key], dtype=np.float32))
            if arr.shape != lib_ctx.target_shape:
                raise ValueError(
                    f"Driver '{d_key}' has shape {arr.shape}, "
                    f"expected {lib_ctx.target_shape}."
                )
            namespace[d_key] = arr

            mask = masks_2d.get(d_key)
            if mask is not None:
                valid_mask *= np.squeeze(np.asarray(mask, dtype=np.float32))

        for f_key in lib_ctx.spec.required_factors or []:
            if f_key not in lib_ctx.factors:
                raise KeyError(
                    f"Expression '{name}' requires factor '{f_key}', but it is unavailable."
                )
            arr = np.squeeze(np.asarray(lib_ctx.factors[f_key], dtype=np.float32))
            if arr.shape != lib_ctx.target_shape:
                raise ValueError(
                    f"Factor '{f_key}' has shape {arr.shape}, "
                    f"expected {lib_ctx.target_shape}."
                )
            namespace[f_key] = arr

        try:
            result = eval(code, {"__builtins__": {}}, namespace)
        except Exception as exc:
            raise RuntimeError(f"Math error in expression '{name}': {exc}") from exc

        result = np.asarray(result, dtype=np.float32)

        if result.shape == ():
            result = np.full(lib_ctx.target_shape, float(result), dtype=np.float32)

        if result.shape != lib_ctx.target_shape:
            raise ValueError(
                f"Expression '{name}' returned shape {result.shape}, "
                f"expected {lib_ctx.target_shape}."
            )

        return result * valid_mask

# -----------------------------------------------------------------------------
# Internal Helpers
# -----------------------------------------------------------------------------

def _get_required_factor(ctx, name):
    """
    Safely retrieves a previously computed factor.
    Provides high-fidelity error messages for dependency/sequence issues.
    """
    f = ctx.factors.get(name)  # Check the SimpleNamespace factors dict
    if f is not None:
        return np.squeeze(f)

    # If missing, investigate why to help the designer fix the pipeline
    # from ThematicRender.settings import FACTOR_SPECS
    all_defined = [s.name for s in FACTOR_SPECS]

    if name not in all_defined:
        raise KeyError(f"Factor Logic Error: '{name}' is used but not defined in settings.py.")
    else:
        raise KeyError(
            f"Factor Sequence Error: A factor tried to access '{name}', "
            f"but '{name}' hasn't been generated yet. Move '{name}' higher in FACTOR_SPECS."
        )
