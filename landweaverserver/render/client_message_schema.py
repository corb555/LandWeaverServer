RENDER_REQUEST_SCHEMA = {
    "msg": {
        "type": "string", "allowed": ["render_request"], "required": True
    }, "job_id": {
        "type": "string", "regex": r"^\d{1,11}$", "required": True
    }, "params": {
        "type": "dict", "required": True, "schema": {
            "percent": {"type": "float", "min": 0.0, "max": 1.0, "required": True},
            "row": {"type": "float", "min": 0.0, "max": 1.0, "required": True},
            "col": {"type": "float", "min": 0.0, "max": 1.0, "required": True},
            "prefix": {"type": "string", "regex": r"^[a-zA-Z0-9_]+$", "required": True},
            "output_suffix": {"type": "string", "regex": r"^[a-zA-Z0-9_]+$", "required": True},
        },
    },
}
