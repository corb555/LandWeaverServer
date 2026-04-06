import pytest
import numpy as np
from dataclasses import dataclass
from typing import Dict

from landweaverserver.render.theme_registry import refine_signal


# --- 1. MOCKS & INFRASTRUCTURE ---

@dataclass
class MockNoiseProvider:
    """Returns a solid value instead of random noise for deterministic testing."""
    constant_value: float = 0.5

    def window_noise(self, window, scale_override=None):
        # Return a 2D array filled with the constant value
        # Size matches our test tile (10x10)
        return np.full((10, 10), self.constant_value, dtype=np.float32)

class MockContext:
    """Mimics the lib_ctx/WorkerContext structure."""
    def __init__(self, noise_map: Dict[str, float]):
        self.noises = {k: MockNoiseProvider(v) for k, v in noise_map.items()}
        self.window = None # Not used by mock provider

    def get(self, noise_id):
        return self.noises.get(noise_id)

@dataclass
class ThemeRuntimeSpecMock:
    """Mimics the ThemeRuntimeSpec dataclass."""
    blur_px: float = 0.0
    noise_amp: float = 0.0
    noise_id: str = "none"
    contrast: float = 1.0
    power_exponent: float = 0.0
    max_opacity: float = 1.0

# --- 2. THE TEST SUITE ---

def test_refine_neutral_identity():
    """Fact: If all parameters are neutral, the output must be identical to input."""
    input_mask = np.random.rand(10, 10).astype(np.float32)
    params = {"blur_px": 0, "noise_amp": 0, "contrast": 1.0, "max_opacity": 1.0}
    ctx = MockContext({})

    output = refine_signal(input_mask, params, ctx)

    assert np.array_equal(input_mask, output)
    assert output.dtype == np.float32

def test_refine_max_opacity():
    """Fact: max_opacity scales the foreground but preserves the background (0.0)."""
    # Create a test array with one 0.0 pixel and one 1.0 pixel
    input_mask = np.array([[0.0, 1.0]], dtype=np.float32)
    params = {"max_opacity": 0.6}
    ctx = MockContext({})

    output = refine_signal(input_mask, params, ctx)

    # Validation:
    # The background (0.0) must remain 0.0
    assert output[0, 0] == 0.0
    # The foreground (1.0) must be exactly the max_opacity
    assert output[0, 1] == pytest.approx(0.6)
    # Ensure no values exceed the ceiling
    assert np.max(output) <= 0.6

def test_refine_noise_modulation_math():
    """Fact: Noise modulation must follow: signal * (1.0 + (noise-0.5) * 2 * amp)."""
    input_mask = np.full((10, 10), 0.5, dtype=np.float32)

    # --- LEVEL 1: Standard Amplitude (0.4) ---
    amp_low = 0.4

    # Case A: Max Noise (1.0) -> Expected: 0.5 * (1.0 + 0.5 * 2 * 0.4) = 0.7
    ctx_white = MockContext({"noise": 1.0})
    out_white = refine_signal(input_mask, {"noise_amp": amp_low, "noise_id": "noise"}, ctx_white)
    assert np.all(out_white == pytest.approx(0.7))

    # Case B: Min Noise (0.0) -> Expected: 0.5 * (1.0 - 0.5 * 2 * 0.4) = 0.3
    ctx_black = MockContext({"noise": 0.0})
    out_black = refine_signal(input_mask, {"noise_amp": amp_low, "noise_id": "noise"}, ctx_black)
    assert np.all(out_black == pytest.approx(0.3))

    # --- LEVEL 2: Maximum Amplitude (1.0) ---
    # This level proves the noise can fully subtract or fully saturate the signal.
    amp_high = 1.0

    # Case C: Max Noise (1.0) -> Expected: 0.5 * (1.0 + 0.5 * 2 * 1.0) = 1.0
    out_max = refine_signal(input_mask, {"noise_amp": amp_high, "noise_id": "noise"}, ctx_white)
    assert np.all(out_max == pytest.approx(1.0))

    # Case D: Min Noise (0.0) -> Expected: 0.5 * (1.0 - 0.5 * 2 * 1.0) = 0.0
    # This is the "Shredder" test—proves noise can punch transparent holes.
    out_min = refine_signal(input_mask, {"noise_amp": amp_high, "noise_id": "noise"}, ctx_black)
    assert np.all(out_min == pytest.approx(0.0))

def test_refine_contrast_shaping():
    """Fact: Contrast > 1.0 pushes mid-gray values toward the extremes (0 and 1)."""
    # Create a small gradient [0.4, 0.5, 0.6]
    input_mask = np.array([[0.4, 0.5, 0.6]], dtype=np.float32)
    params = {"contrast": 2.0} # Aggressive S-curve
    ctx = MockContext({})

    output = refine_signal(input_mask, params, ctx)

    # 0.5 remains 0.5
    assert output[0, 1] == 0.5
    # 0.4 becomes (0.4-0.5)*2 + 0.5 = 0.3
    assert output[0, 0] < 0.4
    # 0.6 becomes (0.6-0.5)*2 + 0.5 = 0.7
    assert output[0, 2] > 0.6

def test_refine_power_exponent_shaping():
    """Fact: power_exponent creates non-symmetrical 'silky' transitions."""
    input_mask = np.full((10, 10), 0.5, dtype=np.float32)
    params = {"power_exponent": 2.0} # Smooth mode
    ctx = MockContext({})

    output = refine_signal(input_mask, params, ctx)

    # Calculation: 0.5 ^ (1.0 / 2.0) = sqrt(0.5) approx 0.707
    assert np.all(output == pytest.approx(0.707, abs=1e-3))

def test_refine_blur_softening():
    """Fact: blur_px must soften hard edges."""
    # Hard edge: left half 0, right half 1
    input_mask = np.zeros((10, 10), dtype=np.float32)
    input_mask[:, 5:] = 1.0
    params = {"blur_px": 1.5}
    ctx = MockContext({})

    output = refine_signal(input_mask, params, ctx)

    # Center pixels (index 4 and 5) should no longer be 0.0 and 1.0
    assert 0.0 < output[5, 4] < 0.5
    assert 0.5 < output[5, 5] < 1.0
    # Far edges should still be near 0 and 1
    assert output[5, 0] == pytest.approx(0.0, abs=1e-2)
    assert output[5, 9] == pytest.approx(1.0, abs=1e-2)

def test_interface_robustness():
    """Fact: Function must produce identical results for Dict and Dataclass params."""
    input_mask = np.full((10, 10), 0.5, dtype=np.float32)
    ctx = MockContext({"test": 0.8})

    dict_params = {
        "noise_amp": 0.2, "noise_id": "test", "max_opacity": 0.8
    }
    obj_params = ThemeRuntimeSpecMock(
        noise_amp=0.2, noise_id="test", max_opacity=0.8
    )

    res_dict = refine_signal(input_mask, dict_params, ctx)
    res_obj = refine_signal(input_mask, obj_params, ctx)

    assert np.array_equal(res_dict, res_obj)

def test_refine_error_on_missing_noise():
    """Fact: If noise_amp > 0, an invalid noise_id must raise a KeyError."""
    input_mask = np.ones((10, 10), dtype=np.float32)
    params = {"noise_amp": 0.5, "noise_id": "missing_ghost"}
    ctx = MockContext({}) # Empty context

    with pytest.raises(KeyError, match="Unknown noise_id 'missing_ghost'"):
        refine_signal(input_mask, params, ctx)