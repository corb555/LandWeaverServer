

@staticmethod
@spatial_factor("snow")
def snow(data_2d, masks_2d, name, ctx):
    # NOTE - this is going to get completely rewritten
    params = ctx.cfg.get_logic("snow")
    raw_dem = data_2d[DriverKey.DEM]

    start = float(params["snowline"]) - float(params["ramp"])
    end = float(params["snowline"]) + float(params["ramp"])
    density = np.clip((raw_dem - start) / (end - start + 1e-6), 0.0, 1.0)
    noise = data_2d.get("noise", 0.5)
    return np.clip(((density - noise) * 1.0) + 0.5, 0.0, 1.0)


@staticmethod
@spatial_factor("water_mask")
def water_mask(data_2d, masks_2d, name, ctx):
    theme_ids = data_2d.get(DriverKey.THEME)

    if theme_ids is None:
        return np.zeros(ctx.target_shape, dtype="float32")

    label_to_val = ctx.themes.label_to_id
    water_val = label_to_val.get("water")

    if water_val is None:
        print_once(
            "missing_water_id",
            "⚠️ Warning: 'water' label not found in QML. Water mask will be empty."
        )
        return np.zeros(ctx.target_shape, dtype="float32")

    idx = theme_ids[:, :, 0] if theme_ids.ndim == 3 else theme_ids
    mask = (idx == water_val).astype("float32")

    return mask

@staticmethod
@spatial_factor("add_depth")
def add_depth(data_2d, masks_2d, name, ctx):
    params = ctx.cfg.get_logic("water")
    prox_data = data_2d.get(DriverKey.WATER_PROXIMITY)

    water_mask = np.squeeze(ctx.factors_2d.get("water", 0.0))

    if prox_data is None:
        return np.zeros(ctx.target_shape, dtype="float32")

    max_d = float(params.get("max_depth_px", 100.0))
    res = np.clip(prox_data / max_d, 0.0, 1.0)

    sensitivity = float(params.get("depth_sensitivity", 1.0))
    if sensitivity != 1.0:
        res = np.power(res, 1.0 / max(sensitivity, 0.01))

    return res * water_mask

@staticmethod
@spatial_factor("add_ripples")
def add_ripples(data_2d, masks_2d, name, ctx):
    params = ctx.cfg.get_logic("water")
    noise_provider = ctx.noises.get("water")
    water_mask = _get_required_factor(ctx, "water")

    scale = float(params.get("ripple_scale", 3.0))
    noise = np.squeeze(noise_provider.window_noise(ctx.window, scale_override=scale))

    intensity = float(params.get("ripple_intensity", 0.2))
    shading = (1.0 - intensity) + (noise * intensity)
    res = 1.0 + water_mask * (shading - 1.0)

    return res

@staticmethod
@spatial_factor("add_glint")
def add_glint(data_2d, masks_2d, name, ctx):
    params = ctx.cfg.get_logic("water")
    noise_provider = ctx.noises.get("water")

    raw_water_mask = _get_required_factor(ctx, "water")
    if raw_water_mask is None:
        return np.zeros(ctx.target_shape)

    water_mask = np.squeeze(raw_water_mask)
    scale = float(params.get("glint_scale", 6.0))
    noise = np.squeeze(noise_provider.window_noise(ctx.window, scale_override=scale))
    n = np.clip(noise + float(params.get("glint_floor", 0.4)) - 0.5, 0, 1)
    sensitivity = float(params.get("glint_sensitivity", 2.0))
    glints = np.power(n, 10.0 / max(sensitivity, 0.1))

    return glints * water_mask