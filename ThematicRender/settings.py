from __future__ import annotations

# setttings.py
from typing import Dict, Final

import numpy as np

from ThematicRender.keys import (SurfaceKey, DriverKey, FileKey, DriverSpec, SurfaceSpec, \
                                 _BlendSpec, FactorSpec, NoiseSpec, SurfaceModifierSpec,
                                 ThemeSmoothingSpec, EdgeProfile, FactorKey, _GatedStepSpec)

BLEND_PIPELINE = [_BlendSpec(
    # ARID LAYER
    desc="Mix ARID_BASE and ARID_RED_BASE using lithology data",
    comp_op="lerp_surfaces", factor_nm="lith",
    input_surfaces=[SurfaceKey.ARID_BASE, SurfaceKey.ARID_RED_BASE],
    output_surface=SurfaceKey.ARID_COMPOSITE, enabled=True
), _BlendSpec(
    desc="Create the Canvas buffer with the ARID_COMPOSITE surface", enabled=True,
    comp_op="create_buffer", factor_nm=None, input_surfaces=[SurfaceKey.ARID_COMPOSITE]
), _BlendSpec(
    desc="Add ARID_VEGETATION to the arid region using the forest "
         " mask.", enabled=True, comp_op="lerp", factor_nm="forest",
    input_surfaces=[SurfaceKey.ARID_VEGETATION], scale=1.1, contrast=0.0
), # HUMID LAYER
    _BlendSpec(
        desc="Mix HUMID_BASE and ARID_RED_BASE using lithology data", comp_op="lerp_surfaces",
        factor_nm="lith", scale=1.0,
        input_surfaces=[SurfaceKey.HUMID_BASE, SurfaceKey.ARID_RED_BASE],
        output_surface=SurfaceKey.HUMID_COMPOSITE, enabled=True
    ), _BlendSpec(
        desc="Create the humid buffer with HUMID_COMPOSITE.", comp_op="create_buffer",
        factor_nm=None, input_surfaces=[SurfaceKey.HUMID_COMPOSITE], buffer="humid", enabled=True
    ), _BlendSpec(
        desc="Add humid vegetation to the humid buffer "
             "using "
             "the forest mask.", comp_op="lerp", enabled=True, factor_nm="forest",scale=1.1, contrast=0.0,
        input_surfaces=[SurfaceKey.HUMID_VEGETATION], buffer="humid", ), # MERGE HUMID AND ARID
    _BlendSpec(
        desc="Merge the Canvas (Arid) buffer and Humid buffers using the "
             "moisture gradient.", enabled=False, comp_op="lerp_buffers", factor_nm="precip",
        merge_buffer="humid", bias=-0.30
    ), _BlendSpec(
        desc="Add thematic classes (water, rock, ice) onto the terrain using "
             "smoothed masks.", enabled=True, comp_op="alpha_over", factor_nm="theme_composite",
        input_surfaces=[SurfaceKey.THEME_OVERLAY]
    ), _BlendSpec(
        desc="Mask in high-altitude snow and ice color ramps based on the jittered elevation "
             "snowline.", enabled=False, comp_op="lerp", factor_nm="snow",
        input_surfaces=[SurfaceKey.SNOW]
    ), _BlendSpec(
        desc="Simulate deep water by applying a darkening gradient to lake and river interiors "
             "based "
             "on proximity to shore.", comp_op="apply_zonal_gradient", enabled=False,
        factor_nm="water_depth", mask_nm="water", params={
            "color_0": [82, 90, 105],  # Shallow color
            "color_1": [58, 64, 74]  # Deep color
        }
    ), _BlendSpec(
        desc="Composite thematic water using the Shoreline Fade.", enabled=False,
        comp_op="alpha_over", factor_nm="water_alpha",  # Use the fade instead of the solid mask
        input_surfaces=[SurfaceKey.THEME_OVERLAY]
    ), _BlendSpec(
        desc="Apply wave structure (shadows) to the water surface.", comp_op="multiply",
        factor_nm="water_ripples", buffer="canvas", enabled=False
    ), _BlendSpec(
        desc="Water glint", comp_op="alpha_over", factor_nm="water_glint", buffer="canvas",
        params={
            "color": [58, 64, 74], "intensity": 0.8  # Overdrive the brightness
        }, enabled=False
    ), _BlendSpec(
        desc="Add hillshades", enabled=False, comp_op="multiply", factor_nm="hillshade",
        buffer="canvas"
    ), _BlendSpec(
        desc="Output the canvas buffer", enabled=True, comp_op="write_output", buffer="canvas"
    ), ]

FACTOR_SPECS: list[FactorSpec] = [FactorSpec(
    name="elev", function_id="mapped_signal", drivers=frozenset({DriverKey.DEM}),
    desc="Normalized 0..1 elevation", required_noise="biome"
), FactorSpec(
    name="elev_m", function_id="elevation_raw", drivers=frozenset({DriverKey.DEM}),
    desc="Raw physical elevation in meters for ramp sampling."
), FactorSpec(
    name="shoreline_fade", function_id="edge_fade",
    drivers=frozenset({DriverKey.WATER_PROXIMITY, DriverKey.THEME}),
    desc="Fades water opacity at the shoreline to reveal the bottom."
), FactorSpec(
    name="precip", function_id="mapped_signal", drivers=frozenset({DriverKey.PRECIP}),
    required_noise="biome",
    desc="Environmental gradient mask (Arid vs Humid) derived from precipitation data."
), FactorSpec(
    name="forest", function_id="mapped_signal", drivers=frozenset({DriverKey.FOREST}),
    required_noise="forest", desc="Biological mask defining vegetation density (Forest vs Meadow)."
), FactorSpec(
    name="lith", function_id="mapped_signal", required_noise="geology", drivers=frozenset({DriverKey.LITH}),
    desc="Organic transition mask for red-rock lithology regions."
), FactorSpec(
    name="snow", function_id="snow", drivers=frozenset({DriverKey.DEM}),
    desc="High-contrast mask for permanent snow and ice based on elevation jitter."
), FactorSpec(
    name="hillshade", function_id="hillshade", drivers=frozenset({DriverKey.HILLSHADE}),
    desc="A raster representing modeled topographic shading."
),
    FactorSpec(
        name="theme_composite",
        function_id="theme_composite",
        drivers=frozenset({DriverKey.THEME}),
        # EXPLICIT PARAMS:
        params={
            "qml_file_key": "theme_qml",
            "smoothing_logic_key": "primary_theme_smoothing"
        },
        desc="Smoothed opacity mask for thematic LandFire categories."
    ),

    FactorSpec(
    name="water", function_id="categorical_mask", drivers=frozenset({DriverKey.THEME}),
    desc="Binary mask for water bodies used for specialized water effects."
), FactorSpec(
    name="water_depth", function_id="proximity_power", drivers=frozenset({DriverKey.WATER_PROXIMITY}),
    desc="Distance-based gradient inside water bodies to simulate bathymetric darkening."
), FactorSpec(
    name="water_glint", function_id="specular_highlights", drivers=frozenset({DriverKey.THEME}),
    required_noise="water", desc="High-frequency specular highlights for water surfaces.",
    required_factors=("water",), ), FactorSpec(
    name="water_ripples", function_id="noise_overlay", drivers=frozenset({DriverKey.THEME}),
    required_noise="water", required_factors=("water",),
    desc="Base wave structure for water shading."
), ]

DRIVER_SPECS: Final[dict["DriverKey", DriverSpec]] = {
    DriverKey.DEM: DriverSpec(dtype=np.float32, halo_px=64),
    DriverKey.PRECIP: DriverSpec(dtype=np.float32, halo_px=64), DriverKey.LITH: DriverSpec(
        dtype=np.float32, halo_px=64, cleanup_type="continuous", smoothing_radius=8.0
        ), DriverKey.HILLSHADE: DriverSpec(dtype=np.float32, halo_px=64),
    DriverKey.FOREST: DriverSpec(
        dtype=np.float32, halo_px=64, cleanup_type="continuous", smoothing_radius=8.0
        ), DriverKey.WATER_PROXIMITY: DriverSpec(
        dtype=np.float32, halo_px=64, cleanup_type="continuous", smoothing_radius=15.0
        ), DriverKey.THEME: DriverSpec(dtype=np.uint8, halo_px=64, cleanup_type="categorical"),
}

# primes: 53,  71,  83,  97,  103, 113, 127, 131,  149, 151,  163, , 173,  181, 191, 199
# Use primes or non-multiples to break the 'grid' look
# 1.7 = fine grit
# 31.0 = mid clusters
# 173.0 = broad sweeps (Macro)
NOISE_SPECS: Final[Dict[str, NoiseSpec]] = {
    "biome": NoiseSpec(
        id="biome", sigmas=(1.0, 3.0, 8.0), weights=(0.7, 0.2, 0.1),
        desc="Organic noise for biome transitions and broad land-cover variety."
    ), "geology": NoiseSpec(
        id="geology",

        sigmas=(3.0, 31.0, 199.0), weights=(0.7, 0.0, 0.3), stretch=(1.0, 1.0),
        desc="Sand-swept sedimentary variation."
    ), "water": NoiseSpec(
        id="water", sigmas=(0.8, 1.5, 3.0), weights=(0.6, 0.3, 0.1), stretch=(1.0, 4.0),
        seed_offset=1,
        desc="Horizontally stretched noise to simulate water surface patterns and liquid flow."
    ), "fine_mottle": NoiseSpec(
        id="fine_mottle", sigmas=(1.0, 2.0), weights=(0.8, 0.2),
        desc="High-frequency granular noise for simulating surface grit and fine soil texture."
    ), "forest": NoiseSpec(
        id="forest", sigmas=(0.8, 3.5, 12.0), weights=(0.3, 0.5, 0.2), stretch=(1.0, 1.0),
        desc="Multi-scale noise blending fine tooth with medium clumps to simulate organic forest "
             "canopy."
    ),
}

SURFACE_SPECS: list[SurfaceSpec] = [SurfaceSpec(
    key=SurfaceKey.ARID_BASE, driver=DriverKey.DEM, coord_factor="elev_m",
    required_factors=("elev_m",), provider_id="ramp",
    modifiers=[{"id": "mottle", "profile_id": "arid_mottle"}],
    desc="Standard dry-climate soil and rock color ramp."
), SurfaceSpec(
    key=SurfaceKey.ARID_RED_BASE, driver=DriverKey.DEM, coord_factor="elev_m", provider_id="ramp",
    modifiers=[{"id": "mottle", "profile_id": "lith_mineral"}], required_factors=("elev_m",),
    desc="Iron-oxide rich (red rock) variant of the arid soil ramp."
), SurfaceSpec(
    key=SurfaceKey.ARID_VEGETATION, driver=DriverKey.DEM, coord_factor="elev_m", provider_id="ramp",
    required_factors=("elev_m",),modifiers=[{"id": "mottle", "profile_id": "arid_vegetation"}],
    desc="Dry-climate vegetation colors (sagebrush, scrub, dormant grasses)."
), SurfaceSpec(
    key=SurfaceKey.HUMID_BASE, driver=DriverKey.DEM, coord_factor="elev_m", provider_id="ramp",
    modifiers=[{"id": "mottle", "profile_id": "humid_vegetation"}],
    required_factors=("elev_m",), desc="Moist-climate forest floor and damp earth color ramp."
), SurfaceSpec(
    key=SurfaceKey.HUMID_VEGETATION, driver=DriverKey.DEM, coord_factor="elev_m",
    provider_id="ramp", required_factors=("elev_m",),
    modifiers=[{"id": "mottle", "profile_id": "forest_mottle"}],
    desc="Lush, chlorophyll-rich vegetation colors (conifers, rainforest, meadows)."
), SurfaceSpec(
    key=SurfaceKey.SNOW, driver=DriverKey.DEM, required_factors=("elev_m",), coord_factor="elev_m",
    provider_id="ramp", desc="High-altitude snow and ice color ramp."
), SurfaceSpec(
    key=SurfaceKey.THEME_OVERLAY, coord_factor=None, provider_id="theme", driver=DriverKey.THEME,
    required_factors=("theme_composite",),
    modifiers=[{"id": "mottle", "profile_id": "theme_mottle"}],
    files=frozenset({FileKey.THEME_QML}),
    desc="Categorical colors for specific features (water, rock, glacier) defined in QML."
), ]


SURFACE_MODIFIER_SPECS: Final[Dict[str, SurfaceModifierSpec]] = {
    "water": SurfaceModifierSpec(
        intensity=10.0, shift_vector=(0.2, 0.2, 1.0), noise_id="biome",
        desc="Cooling hue shifts to provide teal and deep blue variety to water bodies."
    ), "arid_mottle": SurfaceModifierSpec(
        intensity=20.0, shift_vector=(1.0, 0.8, 0.5), noise_id="biome",
        desc="Warm sandstone and tan staining for arid soil and desert regions."
    ), "forest_mottle": SurfaceModifierSpec(
        intensity=35.0, shift_vector=(0.1, 1.0, 0.2), noise_id="forest",
        desc="High-contrast canopy variation with vibrant green peaks and deep neutral shadows."
    ), "humid_vegetation": SurfaceModifierSpec(
        intensity=20.0, shift_vector=(0.8, 1.0, 0.5), noise_id="biome",
        desc="Chlorophyll-focused variation for lush, moisture-rich vegetation layers."
    ), "arid_vegetation": SurfaceModifierSpec(
        intensity=20.0, shift_vector=(1.0, 0.8, 0.2), noise_id="biome",
        desc="Desaturated, earthy color shifts for dry-climate scrub, sagebrush, and dormant grass."
    ), "arid_base_mod": SurfaceModifierSpec(
        intensity=20.0, shift_vector=(1.0, 0.9, 0.7), noise_id="biome",
        desc="Subtle mineral staining and variety for dry soil and base rock foundations."
    ), "rock": SurfaceModifierSpec(
        intensity=0.0, shift_vector=(0.1, 0.1, 0.1), noise_id="fine_mottle",
        desc="Neutral grey and mineral mottle for exposed geologic and rocky features."
    ), "glacier": SurfaceModifierSpec(
        intensity=6.0, shift_vector=(-0.5, 0.1, 1.0), noise_id="biome",
        desc="Deep blue and cool-white variation for permanent ice and glacial features."
    ), "volcanic": SurfaceModifierSpec(
        intensity=20.0, shift_vector=(1.0, 0.4, 0.2), noise_id="biome",
        desc="Aggressive warm and dark shifts for lava flows and volcanic ash deposits."
    ), "lith_mineral": SurfaceModifierSpec(
        intensity=35.0,  # Aggressive shift to see the variation
        shift_vector=(1.0, 0.5, 0.2),  # Shifts Red up, Green mid, Blue low (Oranges/Reds)
        noise_id="geology"  #
    ),
    "theme_mottle": SurfaceModifierSpec(
        intensity=0.0,
        shift_vector=(1.0, 1.0, 1.0),
        noise_id="biome"
    )
}
