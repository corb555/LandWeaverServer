from enum import StrEnum
from typing import Any


def _allowed(enum_cls: type[StrEnum]) -> list[str]:
    return [e.value for e in enum_cls]


RENDER_SCHEMA: dict[str, Any] = {
    "version": {"type": "integer", "required": False, "default": 1},
    "anchor": {"type": "string", "required": True},
    "seed": {"type": "integer", "required": False, "default": 1},
    "override_factor": {"type": "string", "required": False},
    "debug_factors": {"type": "string", "required": False},
    "files": {"type": "dict", "required": True},
    "prefixed_files": {"type": "dict", "required": True},
    "theme_smoothing_specs": {"type": "dict", "required": False},
    "driver_specs": {"type": "dict", "required": True},
    "logic": {"type": "dict", "required": False},
    "factors": {"type": "dict", "required": False},
    "noise_profiles": {"type": "dict", "required": False},
    "pipeline": {"type": "list", "required": True},
    "surface_modifier_specs": {"type": "dict", "required": False},
    "surfaces": {"type": "dict", "required": True},
    "theme_render": {"type": "dict", "required": False},

    # ------------------------------------------------------------------
    # Output options
    # ------------------------------------------------------------------
    "output": {
        "type": "dict", "required": False, "default": {}, "schema": {
            "creation_options": {
                "type": "dict", "required": False, "default": {}, "valuesrules": {
                    "anyof": [{"type": "string"}, {"type": "integer"}, {"type": "float"},
                              {"type": "boolean"}, ]
                },
            },
        },
    },
}
