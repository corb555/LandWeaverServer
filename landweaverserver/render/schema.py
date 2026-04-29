from typing import Any

# Registry of valid builders and ops to prevent typos in YAML
FACTOR_OPS = ["mapped_signal", "theme_composite", "raw_source", "categorical_mask",
                   "constrained_signal", "protected_shaping"]

BLEND_OPS = ["blend_surfaces", "create_buffer", "output_buffer", "blend_overlay", "blend_buffers",
             "alpha_over", "multiply", "gradient_fill"]

DATA_TYPES = ["float32", "uint8", "float64", "int32"]

# A shared schema for the visual refinement keys used in both
# standard params and thematic categories.
REFINEMENT_PARAMS = {
    "enabled": {"type": "boolean", "default": True},
    "blur_px": {"type": "float", "required": False},
    "noise_amp": {"type": "float", "required": False},
    "noise_id": {"type": "string", "required": False},
    "contrast": {"type": "float", "required": False},
    "max_opacity": {"type": "float", "required": False},
    "start": {"type": "float", "required": False},
    "full": {"type": "float", "required": False},
    "gamma": {"type": "float", "required": False},
    "noise_atten_power": {"type": "float", "required": False},
    "sensitivity": {"type": "float", "required": False},
    "low_start": {"type": "float", "required": False},
    "low_end": {"type": "float", "required": False},
    "high_start": {"type": "float", "required": False},
    "high_end": {"type": "float", "required": False},
    "protect_lows": {"type": "float", "required": False},
    "protect_highs": {"type": "float", "required": False},
    "strength": {"type": "float", "required": False},
    "band": {"type": "integer", "required": False},
    "preserve_zero": {"type": "boolean", "required": False},
    "constraint_blur": {"type": "float", "required": False},
    "input_scale": {"type": "float", "required": False},
    "constraint_fade": {"type": "float", "required": False},
    "constraint_limit": {"type": "float", "required": False},
    "jitter_amt": {"type": "float", "required": False},
    "surface_intensity": {"type": "float", "required": False},
    "ramp": {"type": "float", "required": False},
    "threshold": {"type": "float", "required": False},
    "label": {"type": "string", "required": False},          # Fixed: "str" -> "string"
    "surface_noise_id": {"type": "string", "required": False},
    "surface_shift_vector": {
        "type": "list",
        "schema": {"type": "float"},
        "minlength": 3,
        "maxlength": 3,
        "required": False
    }
}
RENDER_SCHEMA: dict[str, Any] = {
    "config_type": {"type": "string", "allowed": ["land_weaver"], "required": True},
    "version": {"type": "integer", "required": False, "default": 1},
    "seed": {"type": "integer", "required": False, "default": 1},
    "anchor": {"type": "string", "required": True},
    "debug_factors": {"type": "string", "allowed": ["true", "false"], "required": False},
    "refine_signal": {"type": "boolean", "required": False, "default": True},

    "files": {
        "type": "dict", "required": True, "valuesrules": {"type": "string"}
    },

    "sources": {
        "type": "dict", "required": True, "valuesrules": {"type": "string"}
    },

    "source_specs": {
        "type": "dict", "required": True, "valuesrules": {
            "type": "dict",
            "schema": {
                "dtype": {"type": "string", "allowed": DATA_TYPES, "required": True}
            }
        }
    },

    "surfaces": {
        "type": "dict", "required": True, "valuesrules": {
            "type": "dict",
            "schema": {
                "source": {"type": "string", "required": False},
                "input_factor": {"type": "string", "required": False},
                "op": {"type": "string", "required": True},
                "desc": {"type": "string", "required": False},
                "files": {"type": "list", "schema": {"type": "string"}, "required": False},
                "modifiers": {"type": "list", "schema": {"type": "string"}, "required": False},
            }
        }
    },

    "modifiers": {
        "type": "dict", "required": False, "valuesrules": {
            "type": "dict",
            "schema": {
                "op": {"type": "string", "required": True},
                "intensity": {"type": "float", "required": True},
                "shift_vector": {
                    "type": "list", "minlength": 3, "maxlength": 3, "schema": {"type": "float"}
                },
                "noise_id": {"type": "string", "required": True},
                "desc": {"type": "string", "required": False}
            }
        }
    },

    "factors": {
        "type": "dict",
        "required": False,
        "valuesrules": {
            "type": "dict",
            "schema": {
                "op": {
                    "type": "string",
                    "allowed": FACTOR_OPS,
                    "required": True
                },
                "sources": {
                    "type": "list",
                    "schema": {"type": "string"},
                    "required": True
                },
                "desc": {"type": "string", "required": False},
                "categories": {
                    "type": "dict",
                    "required": False,
                    "valuesrules": {
                        "type": "dict",
                        "schema": REFINEMENT_PARAMS
                    }
                },
                "params": {
                    "type": "dict",
                    "required": False,
                    "schema": REFINEMENT_PARAMS
                }
            }
        }
    },

    "noise_profiles": {
        "type": "dict", "required": False, "valuesrules": {
            "type": "dict",
            "schema": {
                "sigmas": {"type": "list", "schema": {"type": "float"}, "required": True},
                "weights": {"type": "list", "schema": {"type": "float"}, "required": True},
                "stretch": {
                    "type": "list", "minlength": 2, "maxlength": 2, "schema": {"type": "float"}
                },
                "seed_offset": {"type": "integer", "required": False},
                "desc": {"type": "string", "required": False}
            }
        }
    },

    "pipeline": {
        "type": "list", "required": True, "schema": {
            "type": "dict",
            "schema": {
                "name": {"type": "string", "required": True},
                "desc": {"type": "string", "required": False},
                "enabled": {"type": "boolean", "default": True},
                "op": {"type": "string", "allowed": BLEND_OPS, "required": True},
                "factor": {"type": "string", "required": False},
                "input_surfaces": {"type": "list", "schema": {"type": "string"}},
                "output_surface": {"type": "string", "required": False},
                "buffer": {"type": "string", "required": False},
                "merge_buffer": {"type": "string", "required": False},
                "scale": {"type": "float", "required": False},
                "bias": {"type": "float", "required": False},
                "contrast": {"type": "float", "required": False},
                "params": {"type": "dict", "required": False}
            }
        }
    },

    "output": {
        "type": "dict", "required": False, "default": {}, "schema": {
            "creation_options": {
                "type": "dict", "required": False, "default": {}, "valuesrules": {
                    "anyof": [
                        {"type": "string"},
                        {"type": "integer"},
                        {"type": "float"},
                        {"type": "boolean"}
                    ]
                },
            },
        },
    },
}