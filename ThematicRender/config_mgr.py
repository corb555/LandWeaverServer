from __future__ import annotations

import dataclasses
# config_mgr.py
from dataclasses import dataclass
from pathlib import Path
from typing import (Any, Tuple, Iterable, Set, Dict)

import numpy as np
from YMLEditor.yaml_reader import ConfigLoader

from ThematicRender.compositing_library import COMPOSITING_REGISTRY
from ThematicRender.keys import DriverKey, FileKey, NoiseSpec, RequiredResources, _BlendSpec, \
    SurfaceSpec, FactorSpec, PipelineRequirements
from ThematicRender.schema import RENDER_SCHEMA
from ThematicRender.settings import NOISE_SPECS, SURFACE_MODIFIER_SPECS
from ThematicRender.utils import DTYPE_ALIASES, GenMarkdown


# config_mgr.py
@dataclass(slots=True)
class ConfigMgr:
    """
    The Single Source of Truth for a build.
    All merging and path resolution is done at construction (build time).
    """
    logic: Dict[str, Any]  # Fused DRIVER_LOGIC_PARAMS
    specs: Dict[DriverKey, Any]  # Fused DRIVER_SPECS (DriverSpec objects)
    files: Dict[str, Path]  # Resolved absolute Paths
    raw_defs: Dict[str, Any]  # Top-level project settings (seed, etc.)

    @classmethod
    def build(cls, config_path: Path, prefix: str = "", output_override: str = None) -> "ConfigMgr":
        """
        The factory that 'Fuses' settings.py and YAML into a single store.
        """
        # 1. Load project YAML
        loader = ConfigLoader(RENDER_SCHEMA)
        defs = loader.read(config_file=config_path)

        # 2. Fuse Logic Parameters (Defaults from settings.py + Project Overrides)
        fused_logic = {}
        # Start with a deep copy of defaults
        # for k, v in DRIVER_LOGIC_PARAMS.items():
        #    fused_logic[k] = dict(v)

        # print(f"LOGIC CONFIG1:\n{fused_logic}\n\n")

        # Overlay YAML overrides (e.g. YAML 'drivers' block)
        yaml_drivers = defs.get("drivers", {})
        for key, params in yaml_drivers.items():
            if key in fused_logic:
                fused_logic[key].update(params)
            else:
                fused_logic[key] = params

        # 3. Fuse Driver Hardware Specs
        from ThematicRender.settings import DRIVER_SPECS
        fused_specs = {}
        yaml_specs = defs.get("driver_specs", {})
        for dkey in DriverKey:
            base = DRIVER_SPECS.get(dkey)
            # Update specific fields if YAML provides them (halo, cleanup_type, etc)
            override = yaml_specs.get(dkey.value, {})
            fused_specs[dkey] = dataclasses.replace(base, **override) if override else base

        # 4. Resolve Paths
        resolved_files = {}
        static_files = defs.get("files", {})
        prefixed_files = defs.get("prefixed_files", {})

        # Combine and expand paths
        for k, v in static_files.items():
            resolved_files[k] = Path(v).expanduser()
        for k, v in prefixed_files.items():
            resolved_files[k] = Path(f"{prefix}{v}").expanduser()

        # Set Final Output
        out_path = output_override or defs.get("output", "output.tif")
        resolved_files["output"] = Path(out_path).expanduser()

        return cls(
            logic=fused_logic, specs=fused_specs, files=resolved_files, raw_defs=defs
        )

    # --- Standard Accessors ---

    def get_logic(self, key: str) -> Dict[str, Any]:
        """Returns math params (start, full, noise_amp)."""
        return self.logic.get(key, {})

    def get_spec(self, key: DriverKey, default: Any = None) -> Any:
        """Returns hardware/storage specs (halo, cleanup_type)."""
        return self.specs.get(key, default)

    def get_smoothing_specs(self) -> Dict[str, Any]:
        """
        Returns the dictionary of thematic smoothing rules (precedence, radius, weight).
        """
        # 1. Try to pull from the 'theme_smoothing_specs' block in biome.yml
        return self.raw_defs.get("theme_smoothing_specs")

    def path(self, key: str) -> Path:
        """Returns the absolute Path for a file key."""
        p = self.files.get(key)
        if not p:
            # Note:  let the app handle the failure if a file is missing
            return None
        return p

    def get_global(self, key: str, default: Any = None) -> Any:
        """Access top-level project settings like 'seed'."""
        return self.raw_defs.get(key, default)


def derive_pipeline_requirements(
        pipeline: Iterable[_BlendSpec], surface_specs: Iterable[SurfaceSpec],
        factor_specs: Iterable[FactorSpec]
) -> PipelineRequirements:
    """
    Scans the pipeline recursively to find all required Factors and Surfaces.
    """
    req_factors: Set[str] = set()
    req_surfaces: Set[Any] = set()
    produced_surfaces: Set[Any] = set()

    ss_lookup = {ss.key: ss for ss in surface_specs}
    fs_lookup = {fs.name: fs for fs in factor_specs}

    active_steps = [s for s in pipeline if s.enabled]

    # PASS 1: Pipeline direct needs
    for step in active_steps:
        if step.factor_nm: req_factors.add(step.factor_nm)
        if step.output_surface: produced_surfaces.add(step.output_surface)
        if step.input_surfaces:
            for skey in step.input_surfaces: req_surfaces.add(skey)

    # PASS 2: Recursive Dependency Discovery
    processed_surfaces = set()
    processed_factors = set()

    while True:
        new_surfaces = req_surfaces - processed_surfaces
        new_factors = req_factors - processed_factors
        if not new_surfaces and not new_factors: break

        for skey in new_surfaces:
            spec = ss_lookup.get(skey)
            if spec:
                if spec.coord_factor: req_factors.add(spec.coord_factor)
                for f_req in spec.required_factors: req_factors.add(f_req)
            processed_surfaces.add(skey)

        for fname in new_factors:
            spec = fs_lookup.get(fname)
            if spec and spec.required_factors:
                for f_req in spec.required_factors: req_factors.add(f_req)
            processed_factors.add(fname)

    return PipelineRequirements(
        factor_names=req_factors, surface_inputs=req_surfaces - produced_surfaces
    )


Slice2D = Tuple[slice, slice]


def _parse_dtype(v: Any, *, where: str) -> np.dtype:
    """Parse dtype from config values."""
    if v is None:
        raise ValueError(f"{where}: dtype is None")

    if isinstance(v, np.dtype):
        return v

    if isinstance(v, type) and issubclass(v, np.generic):
        return np.dtype(v)

    if isinstance(v, str):
        key = v.strip().lower()
        if key in DTYPE_ALIASES:
            return np.dtype(DTYPE_ALIASES[key])
        raise ValueError(f"{where}: unknown dtype string '{v}'")

    raise ValueError(f"{where}: unsupported dtype {type(v).__name__}: {v!r}")


def _require_comp_ops(pipeline_list: list[_BlendSpec], required_ops: set[str]) -> None:
    enabled = [s for s in pipeline_list if getattr(s, "enabled", True)]
    enabled_ops = {getattr(s, "comp_op", None) or getattr(s, "action", None) for s in enabled}
    enabled_ops.discard(None)

    missing = required_ops - enabled_ops
    if missing:
        pretty_enabled = [
            f"{i}: comp_op={getattr(s, 'comp_op', None)!r} target={getattr(s, 'target', None)!r}"
            for i, s in enumerate(enabled)]
        raise ValueError(
            "\n❌ PIPELINE CONFIG ERROR\n"
            f"Missing required pipeline steps: {sorted(missing)}\n"
            "Enabled steps:\n  - " + "\n  - ".join(pretty_enabled) + "\n"
                                                                     "Your pipeline must include "
                                                                     "an enabled "
                                                                     "comp_op='create_buffer' step "
                                                                     "before "
                                                                     "comp_op='write_output'.\n"
        )


def derive_resources(
        *, cfg, pipeline: Iterable[_BlendSpec], factor_specs: Iterable[FactorSpec],
        surface_specs: Iterable[SurfaceSpec]
) -> RequiredResources:
    # materialize once so we can safely inspect it later
    pipeline_list = list(pipeline)
    _require_comp_ops(pipeline_list, {"create_buffer", "write_output"})

    # 1. Identify Demand
    preq = derive_pipeline_requirements(pipeline, surface_specs, factor_specs)
    fs_lookup = {fs.name: fs for fs in factor_specs}
    ss_lookup = {ss.key: ss for ss in surface_specs}

    req_drivers: Set[DriverKey] = set()
    req_files: Set[FileKey] = {FileKey.RAMPS_YML}
    requested_noise_ids: Set[str] = set()

    # 2. Gather from Factors
    for name in preq.factor_names:
        fs = fs_lookup.get(name)
        if not fs:
            continue
        req_drivers.update(fs.drivers)
        if fs.required_noise:
            requested_noise_ids.add(fs.required_noise)

    # 3. Gather from Surfaces (Modifier Dependencies)
    for sk in preq.surface_inputs:
        ss = ss_lookup.get(sk)
        if not ss:
            continue
        if ss.driver:
            req_drivers.add(ss.driver)
        if ss.files:
            req_files.update(ss.files)

        if ss.modifiers:
            for mod_cfg in ss.modifiers:
                profile_id = mod_cfg.get("profile_id")
                if not profile_id:
                    continue
                v_profile = SURFACE_MODIFIER_SPECS.get(profile_id)
                if v_profile is None:
                    available_vars = list(SURFACE_MODIFIER_SPECS.keys())
                    raise ValueError(
                        f"\n❌ CONFIG ERROR: Surface '{sk.value}' uses modifier profile "
                        f"'{profile_id}', but it doesn't exist in SURFACE_MODIFIER_PROFILES.\n"
                        f"👉 Available IDs: {available_vars}"
                    )
                requested_noise_ids.add(v_profile.noise_id)

    # 4. Fulfill Noise Profiles
    noise_profiles: Dict[str, NoiseSpec] = {}
    for nid in requested_noise_ids:
        profile = NOISE_SPECS.get(nid)
        if profile:
            noise_profiles[nid] = profile
        else:
            available_noises = list(NOISE_SPECS.keys())
            raise ValueError(
                f"\n❌ FATAL: Pipeline requires noise profile '{nid}', but it's not defined "
                f"in the NOISE_PROFILES table in settings.py.\n"
                f"👉 Ensure the ID matches exactly.\n"
                f"👉 Available Noise IDs: {available_noises}"
            )

    # 6. DETERMINE THE ANCHOR (Geometry)
    explicit_anchor = cfg.get_global("anchor")
    if explicit_anchor:
        anchor_key = DriverKey(explicit_anchor)
    elif DriverKey.DEM in req_drivers:
        anchor_key = DriverKey.DEM
    elif req_drivers:
        anchor_key = sorted(list(req_drivers))[0]
    else:
        print("❌ Error: No drivers found in pipeline. ")
        res = RequiredResources(
            drivers=req_drivers, files=req_files, anchor_key=None, noise_profiles=noise_profiles,
            factor_inputs=preq.factor_names, surface_inputs=preq.surface_inputs,
            primary_surface=None, )
        raise RuntimeError("❌ Error: No drivers found in pipeline. ")

    primary = None
    return RequiredResources(
        drivers=req_drivers, files=req_files, anchor_key=anchor_key, noise_profiles=noise_profiles,
        factor_inputs=preq.factor_names, surface_inputs=preq.surface_inputs,
        primary_surface=primary, )



def analyze_pipeline(ctx: Any) -> str:
    """
    Performs a deep logical audit and generates a high-fidelity pipeline report.

    Validates:
    - Logic/Config parity (Ensures biome.yml covers all FACTOR_SPECS).
    - Smoothing logic presence (Ensures explicit rules for categorical data).
    - Modifier validity (Ensures intensity and noise sources are defined).
    - Sequence integrity (Ensures buffers/surfaces exist before use).
    """
    from ThematicRender.settings import SURFACE_MODIFIER_SPECS
    md = GenMarkdown()

    # 1. PREPARE CONTEXTUAL LOOKUPS
    cfg = ctx.cfg
    pipeline = ctx.pipeline
    fs_lookup = {fs.name: fs for fs in ctx.factors_engine.specs}
    ss_lookup = ctx.surfaces_engine.spec_registry

    warnings = []
    step_with_warnings = set()

    def add_warning(idx, msg):
        warnings.append(msg)
        if isinstance(idx, int):
            step_with_warnings.add(idx)

    # 2. SIMULATED STATE TRACKING
    sim_buffers = set()
    sim_factors = set(fs_lookup.keys())
    sim_surfaces = set(ctx.surface_inputs)

    # --- 0. THE LOGIC LINTER (Strict Validation) ---

    # A. Check for Global Logic Parity
    for fs in ctx.factors_engine.specs:
        if fs.name not in cfg.logic:
            add_warning("Global", f"❌ **Missing Logic:** Factor `{fs.name}` has no entry in `driver_logic_params` (biome.yml).")

        # Explicit Theme Check
        if fs.function_id == "theme_composite":
            try:
                cfg.get_smoothing_specs()
            except SystemExit:
                add_warning("Global", f"❌ **Missing Resource:** `theme_smoothing_specs` block missing from biome.yml.")

    # B. Validate Pipeline Sequence
    for i, step in enumerate(pipeline):
        if not step.enabled: continue

        operator = COMPOSITING_REGISTRY.get(step.comp_op)
        if operator is None:
            add_warning(i, f"🔴 **Error:** Unknown operation `{step.comp_op}`.")
            continue

        # Check Surface Inputs
        for srf_key in (step.input_surfaces or []):
            if srf_key not in sim_surfaces:
                add_warning(i, f"⚠️ **Sequence Warning:** Surface `{srf_key.value}` used before creation.")

        # Check Factor Dependencies
        if step.factor_nm and step.factor_nm not in sim_factors:
            add_warning(i, f"🔴 **Logic Error:** Factor `{step.factor_nm}` not defined in FACTOR_SPECS.")

        # Check Buffer Integrity
        if "buffer" in operator.required_attrs and step.comp_op != "create_buffer":
            if step.buffer not in sim_buffers:
                add_warning(i, f"🔴 **Buffer Error:** `{step.buffer}` has not been initialized.")

        # Update State
        if step.comp_op == "create_buffer":
            sim_buffers.add(step.buffer)
        if step.output_surface:
            sim_surfaces.add(step.output_surface)

    # --- 1. REPORT HEADER ---
    md.header("Thematic Render Pipeline Report", 1)
    md.bullet(f"{md.bold('Output:')} `{cfg.path('output')}`")
    md.bullet(f"{md.bold('Anchor:')} `{ctx.anchor_key.value}` (Geometry reference)")

    md.header("🚨  Warnings", 2)
    if warnings:
        for w in warnings: md.bullet(w)
    else:
        md.text("✅ No errors.")
    md.text("---")

    # --- 2. EXECUTION NARRATIVE ---
    md.header("1. Compositing Sequence", 2)

    for i, step in enumerate(pipeline):
        if not step.enabled: continue
        target = step.output_surface.value if step.output_surface else step.buffer
        warn_icon = "⚠️ " if i in step_with_warnings else ""

        md.header(f"Step {i}) [{target}] {warn_icon}{step.desc}", 3)
        md.bullet(f"{md.bold('Op:')} `{step.comp_op}`")

        # Factor Logic Breakdown
        if step.factor_nm:
            fs = fs_lookup.get(step.factor_nm)
            params = cfg.get_logic(step.factor_nm)
            na = float(params.get("noise_amp", 0.0))
            nap = float(params.get("noise_atten_power", 1.0))
            con = float(params.get("contrast", 1.0))
            sen = float(params.get("sensitivity", 1.0))
            mo = float(params.get("max_opacity", 1.0))
            md.bullet(f"{md.bold('Factor:')} `{step.factor_nm}`")
            if fs:
                md.text(f"  * *Math:* `{fs.function_id}` using `{', '.join([d.value for d in fs.drivers])}`")

            param_str = ", ".join([f"{k}: {v}" for k, v in params.items()])
            md.text(f"  * *Parameters:* `{param_str or 'None'}`")
            look_desc = describe_lerp_parms(na, nap, con, sen, mo)
            md.text(f"  * *Look:* **{look_desc}**")
            md.text(f"  * *Pipeline Shaping:* Scale={step.scale}, Bias={step.bias}, Contrast={step.contrast}")

        # Inbound Surface Details
        if step.input_surfaces:
            for srf_key in step.input_surfaces:
                ss = ss_lookup.get(srf_key)
                if ss:
                    md.bullet(f"{md.bold('Surface:')} `{srf_key.value}` ({ss.provider_id})")
                    if ss.modifiers:
                        mods = ", ".join([f"{m['id']}({m['profile_id']})" for m in ss.modifiers])
                        md.text(f"    * *Modifiers:* {mods}")
                else:
                    md.bullet(f"{md.bold('Buffer:')} `{srf_key.value}`")

    # --- 3. RESOURCE APPENDIX ---
    md.header("2. Global Resource Registry", 2)

    # Physical Drivers
    md.header("Input Drivers", 3)
    md.tbl_hdr("Driver Key", "Halo", "Cleanup")
    for dkey in sorted(list(ctx.resources.drivers)):
        ds = cfg.get_spec(dkey)
        cleanup = f"{ds.cleanup_type} ({ds.smoothing_radius}px)" if ds.cleanup_type else "Raw"
        md.tbl_row(f"`{dkey.value}`", f"{ds.halo_px}px", cleanup)

    # Explicit Theme Smoothing
    md.header("Thematic Smoothing Rules", 3)
    md.tbl_hdr("Category", "Precedence", "Radius", "Grow Threshold")
    try:
        smooth_specs = cfg.get_smoothing_specs()
        for label, pspec in smooth_specs.items():
            md.tbl_row(label, pspec.get('precedence'), pspec.get('smoothing_radius'), pspec.get('expansion_weight'))
    except:
        md.text("*No smoothing rules defined.*")

    md.header("Surface Material Table", 2)
    md.tbl_hdr("Surface", "Base Provider", "Modifier ID", "Noise Source", "Shift (RGB)")
    for s_key in ctx.surface_inputs:
        ss = ss_lookup.get(s_key)
        if ss:
            mod = ss.modifiers[0] if ss.modifiers else None
            if mod:
                m_prof = SURFACE_MODIFIER_SPECS.get(mod["profile_id"])
                md.tbl_row(
                    s_key.value,
                    ss.provider_id,
                    mod["profile_id"],
                    m_prof.noise_id if m_prof else "None",
                    str(m_prof.shift_vector) if m_prof else "N/A"
                )
            else:
                md.tbl_row(s_key.value, ss.provider_id, "None", "N/A", "N/A")

    # Surface Modifiers
    md.header("Surface Modifier Profiles (Mottling)", 3)
    md.tbl_hdr("ID", "Intensity", "RGB Shift Vector", "Noise Source")
    for mid, mprof in SURFACE_MODIFIER_SPECS.items():
        md.tbl_row(f"`{mid}`", mprof.intensity, str(mprof.shift_vector), f"`{mprof.noise_id}`")

    # Noise Profiles
    md.header("Procedural Noise Profiles", 3)
    md.tbl_hdr("ID", "Sigmas", "Weights", "Stretch")
    for nid, nprof in ctx.resources.noise_profiles.items():
        md.tbl_row(f"`{nid}`", str(nprof.sigmas), str(nprof.weights), str(nprof.stretch))

    # Themes ---
    md.header("Thematic Categories", 2)
    md.tbl_hdr("Label", "ID", "Opacity", "Noise Amp", "Edge Softness", "Status")

    label_to_id = ctx.themes.label_to_id
    for label, cat_id in label_to_id.items():
        # Bridge QML Label to biome.yml Logic
        params = cfg.get_logic(label.lower())

        # Determine Status
        if not params:
            status = "🟡 Using Defaults"
            amp = 0.3; opac = 0.8; blur = "N/A"
        else:
            status = "🟢 Configured"
            amp = params.get("noise_amp", 0.0)
            opac = params.get("max_opacity", 1.0)
            blur = f"{params.get('blur_px', 0)}px"

        # Highlight Transparency Leaks
        # If noise_amp > 0, the layer is mathematically NOT solid.
        opacity_desc = f"{opac*100:.0f}%"
        if amp > 0:
            opacity_desc = f"**{opac*(1-amp)*100:.0f}% to {opac*100:.0f}%**"
            status += " (Transparent Holes)"

        md.tbl_row(label, cat_id, opacity_desc, f"{amp*100:.0f}%", blur, status)

    return md.render()

def describe_lerp_parms(noise_amp, noise_atten_power, contrast, sensitivity, max_opacity) -> str:
    """
    Translates mathematical parameters into a qualitative description of the artistic look.
    """
    parts = []

    # 1. Texture/Variation (Noise Amp)
    if noise_amp < 0.1: parts.append("Smooth/Solid")
    elif noise_amp < 0.3: parts.append("Subtle Grain")
    elif noise_amp < 0.6: parts.append("Organic Mottling")
    else: parts.append("Aggressive Patchiness")

    # 2. Edge Character (Contrast)
    if contrast < 0.9: parts.append("Faded Edges")
    elif contrast <= 1.1: parts.append("Natural Transitions")
    elif contrast <= 2.5: parts.append("Crisp Boundaries")
    else: parts.append("Sharp/Clamped Edges")

    # 3. Shape Curve (Sensitivity / Power)
    if sensitivity < 0.8: parts.append("Broad Presence")
    elif sensitivity > 1.2: parts.append("Silky/Refined Falloff")

    # 4. Global Weight (Max Opacity)
    if max_opacity < 0.4: parts.append("Ghostly/Thin")
    elif max_opacity < 0.8: parts.append("Balanced Density")
    else: parts.append("Heavily Opaque")

    return ", ".join(parts)