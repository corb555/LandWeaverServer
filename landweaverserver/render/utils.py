import ast
from time import perf_counter
from typing import Any

import cv2
import numpy as np


def validate_path(path):
    if not path.exists():
        raise FileNotFoundError(f"Path not found at: {path}")

    if path.is_dir():
        raise IsADirectoryError(f"Path is a directory, not a file: {path}")

    if not path.is_file():
        raise ValueError(f"Path is not a regular file: {path}")


def optimized_blur(data: np.ndarray, sigma: float, mode: str = "wrap"):
    """
    Gaussian blur with manual support for BORDER_WRAP.
    """
    if sigma <= 0:
        return data

    h, w = data.shape[:2]

    # 1. Determine effective data and sigma (Pyramidal logic)
    if sigma >= 8.0:
        scale = 4
        # Note: slicing [::scale] is fine for the input to a blur
        work_data = data[::scale, ::scale]
        sigma_work = sigma / float(scale)
    else:
        scale = 1
        work_data = data
        sigma_work = sigma

    # 2. Handle BORDER_WRAP Special Case
    # OpenCV's GaussianBlur rejects BORDER_WRAP, so we pad manually
    if mode == "wrap":
        # A safe margin for Gaussian blur is 3.5 to 4.0 * sigma
        pad = int(sigma_work * 4.0)
        # Ensure padding isn't zero
        pad = max(pad, 1)

        # Manually create the circular wrap
        padded = cv2.copyMakeBorder(work_data, pad, pad, pad, pad, cv2.BORDER_WRAP)

        # Blur the padded version (Border type here doesn't matter much now)
        blurred = cv2.GaussianBlur(padded, (0, 0), sigmaX=sigma_work)

        # Crop back to the original work_data size
        work_result = blurred[pad:-pad, pad:-pad]
    else:
        # Standard borders (Reflect, Replicate) are supported natively
        border_map = {
            "reflect": cv2.BORDER_REFLECT_101, "constant": cv2.BORDER_CONSTANT,
            "nearest": cv2.BORDER_REPLICATE
        }
        cv2_border = border_map.get(mode, cv2.BORDER_DEFAULT)
        work_result = cv2.GaussianBlur(work_data, (0, 0), sigmaX=sigma_work, borderType=cv2_border)

    # 3. Final Upscale (if pyramidal)
    if scale > 1:
        return cv2.resize(work_result, (w, h), interpolation=cv2.INTER_LINEAR)

    return work_result


def dot_get(obj: Any, path: str, default: Any = None) -> Any:
    """
    Retrieves a nested value from a dictionary  using a dot-separated path.
    Example: dot_get(cfg, "sources.water.max_opacity")
    """
    if obj is None:
        return default

    # 1. If we were passed the RenderConfig object itself,
    # start the search inside its raw_defs dictionary.
    current = obj.raw_defs if hasattr(obj, 'raw_defs') else obj

    # 2. Split the path (e.g., "sources.water.max_opacity" -> ["sources", "water", "max_opacity"])
    keys = path.split(".")

    for key in keys:
        if isinstance(current, dict):
            # Move one level deeper into the dictionary
            current = current.get(key)
        elif hasattr(current, key):
            # Handle cases where it might be a nested dataclass/object
            current = getattr(current, key)
        else:
            return default

        # If at any point we hit a dead end, return the default
        if current is None:
            return default

    return current


class TimerStats:
    def __init__(self):
        self.stats = {}
        self.start_time = 0
        self.current_block = None

    def start(self, name):
        self.start_time = perf_counter()
        self.current_block = name

    def end(self):
        elapsed = perf_counter() - self.start_time
        self.stats[self.current_block] = self.stats.get(self.current_block, 0) + elapsed

    def summary(self):
        for name, total_time in self.stats.items():
            print(f"{name}: {total_time:.2f} seconds")


class GenMarkdown:
    def __init__(self):
        self.lines = []

    def header(self, txt, level=1):
        self.lines.append(f"\n{'#' * level} {txt} \n")

    @staticmethod
    def bold(txt):
        return f"**{txt}**"

    @staticmethod
    def italic(txt):
        return f"_{txt}_"

    def text(self, txt):
        self.lines.append(f"{txt} \n")

    def tbl_hdr(self, *cols):
        self.lines.append("| " + " | ".join(cols) + " |")
        self.lines.append("| " + " | ".join(["---"] * len(cols)) + " |")

    def tbl_row(self, *cols):
        # Clean up None values and ensure string conversion
        row = [str(c) if c is not None else "" for c in cols]
        self.lines.append("| " + " | ".join(row) + " |")

    def bullet(self, txt):
        self.lines.append(f"* {txt} ")

    @staticmethod
    def format_dict(d: dict) -> str:
        """Converts a dictionary to a compact string for table cells."""
        if not d: return ""
        return "<br>".join([f"{k}: {v}" for k, v in d.items()])

    def render(self):
        return "\n".join(self.lines)


# Globally track seen message IDs
_SEEN_MSGS = set()


def print_once(msg_id: str, *args, **kwargs):
    """Prints a message only the first time a specific msg_id is encountered."""
    if msg_id not in _SEEN_MSGS:
        print(*args, **kwargs)
        _SEEN_MSGS.add(msg_id)


def stats_once(tag, a):
    print_once(
        tag, f"{tag} shape={a.shape} min={float(a.min()):.4f} max={float(a.max()):.4f} mean="
             f"{float(a.mean()):.4f}"
    )


def reset_print_once():
    """Call this at the start of process_rasters if you want a fresh log per run."""
    _SEEN_MSGS.clear()


EPSILON = 1e-6


def lerp(a: np.ndarray, b: np.ndarray, t: np.ndarray) -> np.ndarray:
    """Linearly interpolate between arrays."""
    return a + t * (b - a)


def clamp(x: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Clamp values to a range."""
    return np.clip(x, lo, hi)


def smoothstep(e0: float, e1: float, x: np.ndarray) -> np.ndarray:
    """Hermite smoothstep."""
    denom = max(e1 - e0, EPSILON)
    t = np.clip((x - e0) / denom, 0.0, 1.0)
    return t * t * (3.0 - 2.0 * t)


SAFE_FUNCTIONS = {
    "clip": np.clip, "min": np.minimum, "max": np.maximum, "pow": np.power, "where": np.where,
    "abs": np.abs, "log": np.log, "sqrt": np.sqrt, "exp": np.exp, "lerp": lerp, "clamp": clamp,
    "smoothstep": smoothstep,
}

SAFE_NODE_TYPES = {ast.Expression, ast.BinOp, ast.UnaryOp, ast.Call, ast.Name, ast.Load,
                   ast.Constant, ast.keyword, ast.Tuple, ast.List, ast.USub, ast.UAdd, ast.Add,
                   ast.Sub, ast.Mult, ast.Div, ast.Pow, }


def compile_expression(expr_str: str) -> Any:
    """Audit and compile an expression string.

    Args:
        expr_str: User expression in restricted math syntax.

    Returns:
        Compiled code object.

    Raises:
        ValueError: If the expression contains disallowed syntax or names.
    """
    if not expr_str:
        raise ValueError("Expression string is empty.")

    tree = ast.parse(expr_str, mode="eval")

    for node in ast.walk(tree):
        if type(node) not in SAFE_NODE_TYPES:
            raise ValueError(
                f"Illegal expression component: {type(node).__name__}"
            )

        if isinstance(node, ast.Attribute):
            raise ValueError("Attribute access is not allowed.")

        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ValueError("Only direct safe function calls are allowed.")
            if node.func.id not in SAFE_FUNCTIONS:
                raise ValueError(f"Illegal function call: {node.func.id}")

    return compile(tree, "<expression>", "eval")
