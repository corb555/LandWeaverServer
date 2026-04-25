from typing import Any

# Registry of valid builders and ops to prevent typos in YAML
FACTOR_BUILDERS = ["mapped_signal", "theme_composite", "raw_source", "categorical_mask",
                   "constrained_signal", "protected_shaping"]

BLEND_OPS = ["lerp_surfaces", "create_buffer", "output_buffer", "lerp", "lerp_buffers",
             "alpha_over", "multiply", "apply_zonal_gradient"]

DATA_TYPES = ["float32", "uint8", "float64", "int32"]

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
            "type": "dict", "schema": {
                "dtype": {"type": "string", "allowed": DATA_TYPES, "required": True}
            }
        }
    },

    "surfaces": {
        "type": "dict", "required": True, "valuesrules": {
            "type": "dict", "schema": {
                "source": {"type": "string", "required": False},
                "input_factor": {"type": "string", "required": False},
                "surface_builder": {"type": "string", "required": True},
                "desc": {"type": "string", "required": False},
                "files": {"type": "list", "schema": {"type": "string"}}, "modifiers": {
                    "type": "list", "required": False, "schema": {
                        "type": "dict", "schema": {
                            "effect": {"type": "string", "required": True},
                            "mod_profile": {"type": "string", "required": True}
                        }
                    }
                }
            }
        }
    },

    "modifier_profiles": {
        "type": "dict", "required": False, "valuesrules": {
            "type": "dict", "schema": {
                "intensity": {"type": "float", "required": True}, "shift_vector": {
                    "type": "list", "minlength": 3, "maxlength": 3, "schema": {"type": "float"}
                }, "noise_id": {"type": "string", "required": True},
                "desc": {"type": "string", "required": False}
            }
        }
    },

    "factors": {
        "type": "dict", "required": False, "valuesrules": {
            "type": "dict", "schema": {
                "factor_builder": {"type": "string", "allowed": FACTOR_BUILDERS, "required": True},
                "sources": {"type": "list", "schema": {"type": "string"}},
                "desc": {"type": "string", "required": False},
                "params": {"type": "dict", "required": False}
                # Flexible dict for builder-specific logic
            }
        }
    },

    "noise_profiles": {
        "type": "dict", "required": False, "valuesrules": {
            "type": "dict", "schema": {
                "sigmas": {"type": "list", "schema": {"type": "float"}, "required": True},
                "weights": {"type": "list", "schema": {"type": "float"}, "required": True},
                "stretch": {
                    "type": "list", "minlength": 2, "maxlength": 2, "schema": {"type": "float"}
                }, "seed_offset": {"type": "integer", "required": False},
                "desc": {"type": "string", "required": False}
            }
        }
    },

    "pipeline": {
        "type": "list", "required": True, "schema": {
            "type": "dict", "schema": {
                "name": {"type": "string", "required": True},
                "desc": {"type": "string", "required": False},
                "enabled": {"type": "boolean", "default": True},
                "blend_op": {"type": "string", "allowed": BLEND_OPS, "required": True},
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
                    "anyof": [{"type": "string"}, {"type": "integer"}, {"type": "float"},
                              {"type": "boolean"}]
                },
            },
        },
    },
}
