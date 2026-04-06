from functools import wraps
from typing import Dict, Callable, Any

import numpy as np

# factor_library.py
from landweaverserver.render.spatial_math import normalize_step, lerp
from landweaverserver.render.theme_registry import refine_signal
from landweaverserver.render.utils import SAFE_FUNCTIONS
from scipy import ndimage

# factor_library.py

FACTOR_REGISTRY: Dict[str, Callable] = {}


def factor_builder(factor_builder_nm: str):
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

        FACTOR_REGISTRY[factor_builder_nm] = wrapper
        return wrapper

    return decorator


# -----------------------------------------------------------------------------
# Logic Blocks
# -----------------------------------------------------------------------------

def _mapped_signal(data_2d, masks_2d, name, lib_ctx, source_key):
    """
    'Remapping' factors
    Raw -> Normalize ->  Refine -> Masked Default.
    """
    params = lib_ctx.spec.params

    # requires start and full.
    if "start" not in params or "full" not in params:
        raise KeyError(
            f"Factor '{name}' uses a mapping function but is missing "
            f"required parameters 'start' and 'full' in its YAML params block."
        )

    # 1. FETCH SOURCE
    raw_data = data_2d.get(source_key)
    if raw_data is None:
        raise KeyError(f"Source '{source_key}' missing from data_2d")

    # 2.  BAND SELECTION
    # This logic allows multi-band rasters but selects exactly ONE plane.
    if raw_data.ndim == 3:
        band_idx = int(params.get("band", 1)) - 1
        # Check if the requested index is actually in the data
        if band_idx < 0 or band_idx >= raw_data.shape[2]:
            raise ValueError(
                f"Source '{source_key}' has {raw_data.shape[2]} bands. "
                f"Requested band {band_idx + 1} is out of range."
            )
        # Select the 2D plane explicitly
        raw_plane = raw_data[:, :, band_idx]
    else:
        # It's already a 2D array
        raw_plane = raw_data

    # ---  2D CONTRACT ---
    if raw_plane.ndim != 2:
        raise ValueError(
            f"UPSTREAM CONTRACT VIOLATION: After band selection, '{source_key}' "
            f"for factor '{name}' is still {raw_plane.shape}. "
            f"The Renderer requires strictly 2D (H,W) planes."
        )

    # 1. Normalize range: Linear normalization based on config start/full
    remapped = normalize_step(raw_plane, float(params.get("start")), float(params.get("full")))

    # 2. Blur, Noise, Contrast, and Power-curves
    if lib_ctx.cfg.raw_defs.get("refine_signal"):
        refined_2d = refine_signal(
            mask=remapped, params=lib_ctx.spec.params, ctx=lib_ctx, diag_name=name
        )
    else:
        refined_2d = remapped

    # 4. FETCH MASK
    valid_mask = masks_2d.get(source_key)
    if valid_mask is not None and valid_mask.ndim != 2:
        # Catch errors in the mask generation pipeline
        raise ValueError(
            f"UPSTREAM CONTRACT VIOLATION: Mask for '{source_key}' is {valid_mask.shape}. "
            f"Expected strictly 2D (H,W)."
        )

    # 5. Gating
    default_val = float(params.get("default_fill", 0.0))

    # 6. Linear Interpolation
    result_2d = lerp(default_val, refined_2d, valid_mask)

    return result_2d


# -----------------------------------------------------------------------------
# The Library
# -----------------------------------------------------------------------------

class FactorLibrary:
    @staticmethod
    @factor_builder("raw_source")
    def raw_source(data_2d, masks_2d, name, lib_ctx):
        """
        Identity Operation: Promotes a physical source to a logical factor.

        This function  moves raw data from the
        physical input dictionary (data_2d) to the logical factor dictionary
        (factors_2d) without modification.

        Architectural Purpose:
        1. DIMENSIONAL FIREWALL: Ensures the data is squeezed into a strictly
           2D NumPy array, removing any band or shared-memory slot dimensions.
        2. STABLE ALIASING: Allows the user to link a logical name (e.g., 'elev_m')
           to a physical file (e.g., 'terrain_v4.tif') so that changing files
           doesn't require updating every surface or pipeline step.
        3. PACKAGE ISOLATION: Allows the Rendering Engines to remain 'blind' to
           the physical Source Pool, operating only on the Factor signal dictionary.

        Use Cases:
        - Providing 'elev_m' (raw meters) as a coordinate for color ramp sampling.
        - Passing through raw categorical 'theme_ids' for specialized masking.
        - Exposing raw physics values (like Slope) to the 'expression' factor.

        Sources: [Target_Source] (The first source in the YAML list is used)
        """
        # 1. Physical Lookup
        # We use the explicit source name provided in the factor's 'sources' list
        if not lib_ctx.spec.sources:
            raise ValueError(f"Factor '{name}' (raw_source) requires at least one source.")

        drv_key = list(lib_ctx.spec.sources)[0]

        # 2. Extract Data
        # Use np.squeeze to ensure we fulfill the 2D Firewall contract
        raw_data = data_2d.get(drv_key)

        if raw_data is None:
            raise KeyError(f"Factor '{name}' requested source '{drv_key}', but it is missing from memory.")

        # 3. Direct Return (Identity Math)
        return np.squeeze(raw_data)

    @staticmethod
    @factor_builder("mapped_signal")
    def mapped_signal(data_2d, masks_2d, name, lib_ctx):
        """
        """
        source_key = next(iter(lib_ctx.spec.sources))
        return _mapped_signal(data_2d, masks_2d, name, lib_ctx, source_key)

    @staticmethod
    @factor_builder("theme_composite")
    def theme_composite(data_2d, masks_2d, name, lib_ctx):
        # Use the first source for this factor
        drv_key = list(lib_ctx.spec.sources)[0]

        """Aggregate configured thematic categories into a composite alpha."""
        theme_ids = data_2d[drv_key]

        tile_ctx = lib_ctx.themes.build_tile_context(theme_ids)
        composite_alpha = np.zeros(lib_ctx.target_shape, dtype=np.float32)

        for spec in tile_ctx.active_specs:
            binary_mask = tile_ctx.masks_by_id[spec.theme_id]
            if not np.any(binary_mask):
                continue

            cat_alpha = refine_signal(
                mask=binary_mask, params=spec, ctx=lib_ctx
            )
            composite_alpha = np.maximum(composite_alpha, cat_alpha)

        return composite_alpha * np.squeeze(masks_2d[drv_key])

    @staticmethod
    @factor_builder("protected_shaping")
    def protected_shaping(data_2d, masks_2d, name, lib_ctx):
        """
        Shapes a grayscale signal into a multiplicative factor while
        preserving highlights and shadows (mid-tone protection).

        This is used to modulate brightness (e.g. Hillshading or Texturing)
        without 'crushing' the underlying colors.
        """
        params = lib_ctx.spec.params
        source_key = next(iter(lib_ctx.spec.sources))
        raw_signal = data_2d.get(source_key)

        if raw_signal is None:
            return np.ones(lib_ctx.target_shape, dtype="float32")

        # 1. Normalize Input
        # 'input_scale' allows handling both 8-bit (255) and float (1.0) sources
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
        valid_mask = np.squeeze(masks_2d[source_key])
        return 1.0 + valid_mask * (m_final - 1.0)

    @staticmethod
    @factor_builder("specular_highlights")
    def specular_highlights(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.spec.params
        noise_id = lib_ctx.spec.noise_id
        noise_provider = lib_ctx.noises.get(noise_id)

        mask_key = params.get("mask_factor")
        mask = lib_ctx.factors.get(mask_key, 1.0) if mask_key else 1.0

        scale = float(params.get("scale", 6.0))
        floor = float(params.get("floor", 0.4))
        sensitivity = float(params.get("sensitivity", 2.0))

        noise = np.squeeze(noise_provider.window_noise(lib_ctx.window, scale_override=scale))

        # Math: Subtract floor, clip, then apply aggressive power curve
        n = np.clip(noise + floor - 0.5, 0, 1)
        glints = np.power(n, 10.0 / max(sensitivity, 0.1))

        return glints * mask

    @staticmethod
    @factor_builder("noise_overlay")
    def noise_overlay(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.spec.params

        # Use the noise profile defined in the spec (e.g., "water")
        noise_id = lib_ctx.spec.noise_id
        noise_provider = lib_ctx.noises.get(noise_id)

        mask_key = params.get("mask_factor")
        mask = lib_ctx.factors.get(mask_key, 1.0) if mask_key else 1.0

        scale = float(params.get("scale", 3.0))
        noise = np.squeeze(noise_provider.window_noise(lib_ctx.window, scale_override=scale))

        # Pattern: 1.0 is neutral for multiply. Noise pushes it up or down.
        intensity = float(params.get("intensity", 0.2))
        shading = (1.0 - intensity) + (noise * intensity)

        # Blend shading only where the mask exists
        return 1.0 + mask * (shading - 1.0)

    @staticmethod
    @factor_builder("proximity_power")
    def proximity_power(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.spec.params
        source_key = next(iter(lib_ctx.spec.sources))
        prox_data = data_2d.get(source_key)

        if prox_data is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # 1. KILL THE RIDGE LINE (The "Medial Axis" fix)
        # Blurring the distance field rounds off the sharp 'meeting point'
        # in the middle of the water body.
        blur = float(params.get("blur_sigma", 0.0))
        if blur > 0:
            # We work on a copy to avoid mutating the raw source for other factors
            prox_data = ndimage.gaussian_filter(prox_data.astype(np.float32), sigma=blur)

        # 2. DISTANCE NORMALIZATION
        # max_range_px: At what distance from shore do we reach 'deep water' (1.0)?
        max_d = float(params.get("max_range_px", 100.0))
        res = np.clip(prox_data / max(max_d, 0.1), 0.0, 1.0)

        # 3. RIVER BOOST (Non-linear Shaping)
        # sensitivity > 1.0 makes small proximity values (rivers) much stronger.
        # Sensitivity of 3.0-5.0 is usually ideal for rivers.
        sensitivity = float(params.get("sensitivity", 1.0))
        if sensitivity != 1.0:
            # We use 1/sens so that higher numbers 'pull' the curve toward 1.0 faster
            res = np.power(res, 1.0 / max(sensitivity, 0.01))

        # 4. FINAL MASKING
        mask_key = params.get("mask_factor")
        mask = lib_ctx.factors.get(mask_key, 1.0) if mask_key else 1.0

        return np.squeeze(res * mask)

    @staticmethod
    @factor_builder("categorical_mask")
    def categorical_mask(data_2d, masks_2d, name, lib_ctx):
        # Pull the label from the config for THIS factor (e.g., params['label'] = "water")
        params = lib_ctx.spec.params
        target_label = params.get("label", name)
        drv_key = list(lib_ctx.spec.sources)[0]

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
    @factor_builder("edge_fade")
    def edge_fade(data_2d, masks_2d, name, lib_ctx):
        """
        Creates an organic alpha transition based on proximity within a specific category.
        Useful for fading water at the shore or thinning forest at the tree-line.
        """
        params = lib_ctx.spec.params
        # Use the primary source from the spec
        source_key = next(iter(lib_ctx.spec.sources))
        prox_data = data_2d.get(source_key)
        theme_ids = data_2d.get(source_key)

        # 1. Fetch Sources from data dictionary
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

    """
    Procedural mask generator implementing the 'Apparent Boundary' pattern.

    This function creates organic transitions (like snowlines, tree-lines, or
    vegetation bands) by combining a primary geographic signal with stochastic
    jitter, then subjecting the result to a physical constraint.

    Logic Flow:
    1. PERTURBATION: The primary source (e.g., Elevation) is displaced by a noise
       field. This creates 'Apparent Elevation,' where the boundary wanders
       naturally into valleys or up ridges instead of following rigid contours.
    2. THRESHOLDING: A smooth linear-step (ramp) is applied to the apparent
       signal to create a soft probability mask (0..1).
    3. CONSTRAINT: A secondary source (e.g., Slope) acts as a physical penalty.
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
        Example:
        If you are using Slope as your constraint (units = Degrees):
        constraint_limit: 45.0 (Degrees)
        constraint_fade: 10.0 (Degrees)
        The Result: The signal is 100% solid up to 35° (
        45
        −
        10
        45−10
        ). It then fades linearly from 1.0 to 0.0 between 35° and 45°. At 45° and above, the signal is completely stripped away (0.0).

    Sources: [Primary_Source, Constraint_Source]
    """
    @staticmethod
    @factor_builder("constrained_signal")
    def constrained_signal(data_2d, masks_2d, name, lib_ctx):
        """
        Calculates a naturalized boundary mask with zero-safe logic.
        """
        params = lib_ctx.spec.params

        # 1. Map Sources using the explicit indices from the YAML 'sources' list
        primary_data = data_2d[lib_ctx.spec.sources[0]]
        constraint_data = data_2d[lib_ctx.spec.sources[1]]

        # 2. Generate Organic Jitter (The 'Wandering' component)
        noise_id = params.get("noise_id", "none")
        jitter_val = float(params.get("jitter_amt", 0.0))

        if noise_id != "none" and jitter_val > 0:
            noise_provider = lib_ctx.noises.get(noise_id)
            if noise_provider is None:
                raise KeyError(f"Factor '{name}' references unknown noise id '{noise_id}'")

            noise = np.squeeze(noise_provider.window_noise(lib_ctx.window))
            # Transform Primary Data into an Apparent State
            effective_signal = primary_data + ((noise - 0.5) * 2.0 * jitter_val)
        else:
            effective_signal = primary_data

        # 3. Apply Boundary Threshold (The 'Probability' component)
        threshold = float(params.get("threshold", 0.0))
        # Handle ramp=0 to avoid division by zero and provide a hard edge
        ramp = float(params.get("ramp", 0.0))
        invert = params.get("invert_threshold", False)

        if ramp > 0:
            # Smooth linear transition centered on the threshold
            mask = np.clip((effective_signal - (threshold - ramp / 2)) / ramp, 0.0, 1.0)
        else:
            # Hard binary edge at the threshold
            mask = (effective_signal >= threshold).astype(np.float32)

        if invert:
            mask = 1.0 - mask

        # --- UPDATED PHYSICS LOGIC ---
        limit = float(params.get("constraint_limit", 90.0))
        limit_fade = float(params.get("constraint_fade", 0.0))

        # Calculate the raw penalty based on slope
        if limit_fade > 0:
            penalty = 1.0 - np.clip((constraint_data - (limit - limit_fade)) / limit_fade, 0.0, 1.0)
        else:
            penalty = (constraint_data <= limit).astype(np.float32)

        # --- EROSION / BLUR STEP ---
        # This bleeds the '0' (cliff) into the '1' (narrow ridge)
        constraint_blur = float(params.get("constraint_blur", 0.0))
        if constraint_blur > 0:
            # We use a Gaussian blur on the penalty mask itself
            penalty = ndimage.gaussian_filter(penalty, sigma=constraint_blur)

            # Optional: Re-sharpen the penalty to keep the cliff edges crisp
            # but the narrow lines gone.
            #penalty = np.clip(penalty * 1.2, 0.0, 1.0)

        return np.squeeze(mask * penalty)

    @staticmethod
    @factor_builder("raster_calculator")
    def expression(data_2d, masks_2d, name, lib_ctx):
        """Evaluate a precompiled safe math expression for the current tile."""
        code = lib_ctx.expression_cache.get(name)
        if code is None:
            raise RuntimeError(f"raster_calculator for '{name}' was not pre-compiled.")

        namespace: dict[str, Any] = dict(SAFE_FUNCTIONS)
        valid_mask = np.ones(lib_ctx.target_shape, dtype=np.float32)

        for d_key in lib_ctx.spec.sources or []:
            arr = np.squeeze(np.asarray(data_2d[d_key], dtype=np.float32))
            if arr.shape != lib_ctx.target_shape:
                raise ValueError(
                    f"Source '{d_key}' has shape {arr.shape}, "
                    f"expected {lib_ctx.target_shape}."
                )
            namespace[d_key] = arr

            mask = masks_2d.get(d_key)
            if mask is not None:
                valid_mask *= np.squeeze(np.asarray(mask, dtype=np.float32))

        for f_key in lib_ctx.spec.required_factors or []:
            if f_key not in lib_ctx.factors:
                raise KeyError(
                    f"raster_calculator '{name}' requires factor '{f_key}', but it is unavailable."
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
            raise RuntimeError(f"Math error in raster_calculator expression '{name}': {exc}") from exc

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
    # from LandWeaverServer.settings import FACTOR_SPECS
    all_defined = [s.name for s in FACTOR_SPECS]

    if name not in all_defined:
        raise KeyError(f"Factor Logic Error: '{name}' is used but not defined in settings.py.")
    else:
        raise KeyError(
            f"Factor Sequence Error: A factor tried to access '{name}', "
            f"but '{name}' hasn't been generated yet. Move '{name}' higher in FACTOR_SPECS."
        )
