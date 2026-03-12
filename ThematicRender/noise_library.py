from dataclasses import dataclass
from typing import Dict, Any

import numpy as np


# noise_library.py

@dataclass(frozen=True, slots=True)
class NoiseProvider:
    """
    Standardized provider of procedural noise.
    """
    tile: np.ndarray

    def __post_init__(self) -> None:
        if self.tile.ndim != 2:
            raise ValueError("NoiseProvider.tile must be 2D")
        if self.tile.dtype != np.float32:
            raise ValueError("NoiseProvider.tile must be float32")

    @property
    def h(self) -> int:
        return self.tile.shape[0]

    @property
    def w(self) -> int:
        return self.tile.shape[1]

    def window_noise(self, window, *, row_off=0, col_off=0, scale_override=None) -> np.ndarray:
        h = int(window.height)
        w = int(window.width)
        r0 = int(window.row_off) + int(row_off)
        c0 = int(window.col_off) + int(col_off)

        s = scale_override if scale_override is not None else 1.0
        rows = (np.arange(r0, r0 + h) * s % self.h).astype(np.int64, copy=False)
        cols = (np.arange(c0, c0 + w) * s % self.w).astype(np.int64, copy=False)

        # Layer 1: The Base Noise
        noise1 = self.tile[np.ix_(rows, cols)]

        # Layer 2: The "Pattern Breaker" (Shifted and slightly scaled)
        # We use a prime offset (e.g., 503) and a different scale (e.g., 0.97)
        # to ensure it never aligns with the base layer
        rows2 = ((np.arange(r0, r0 + h) + 503) * (s * 0.97) % self.h).astype(np.int64)
        cols2 = ((np.arange(c0, c0 + w) + 503) * (s * 0.97) % self.w).astype(np.int64)
        noise2 = self.tile[np.ix_(rows2, cols2)]

        # Blend them: This mathematically destroys the 'grid' look
        return (noise1 * 0.7 + noise2 * 0.3)[..., np.newaxis]


class NoiseLibrary:
    """
    Engine responsible for procedural resource generation.
    """

    def __init__(self, cfg, profiles: Dict[str, Any]):
        self.providers: Dict[str, NoiseProvider] = {}
        self.profiles = profiles

        base_seed = cfg.get_global("seed", 42)

        for noise_id, profile in profiles.items():
            # Use a high-quality 2k tile as the basis for all lookups
            tile = generate_fbm_noise_tile(
                shape=(2048, 2048), sigmas=profile.sigmas, weights=profile.weights,
                stretch=profile.stretch, seed=base_seed + profile.seed_offset
            )
            self.providers[noise_id] = NoiseProvider(tile)

    def keys(self):
        return self.providers.keys()

    def get(self, noise_id: str) -> NoiseProvider:
        provider = self.providers.get(noise_id)
        if provider is None:
            raise KeyError(f"Noise ID '{noise_id}' not found in Registry.")
        return provider


def generate_fbm_noise_tile(
        shape: tuple[int, int], *, sigmas: tuple[float, ...] = (1.5, 4.0, 10.0),
        weights: tuple[float, ...] = (0.4, 0.3, 0.3), stretch: tuple[float, float] = (1.0, 1.0),
        seed: int = 42
) -> np.ndarray:
    from scipy.ndimage import gaussian_filter
    rng = np.random.default_rng(seed)
    out = np.zeros(shape, dtype="float32")

    for sigma, w in zip(sigmas, weights):
        if w <= 0: continue  # Optimization

        # 1. Generate unique noise for this octave
        n = rng.uniform(-0.5, 0.5, shape).astype("float32")

        # 2. Apply the blur
        s_y, s_x = sigma * stretch[0], sigma * stretch[1]
        n = gaussian_filter(n, sigma=(s_y, s_x), mode="wrap")

        # 3.  Per-Octave Normalization
        # We force this octave back to a 0.0-1.0 range so weights are meaningful
        n_min, n_max = n.min(), n.max()
        if n_max - n_min > 1e-6:
            n = (n - n_min) / (n_max - n_min)

        # 4. Add to composite based on weight
        out += float(w) * n

    # 5. Final Global Normalization
    mn, mx = out.min(), out.max()
    if mx - mn > 1e-6:
        out = (out - mn) / (mx - mn)
    return out.astype("float32")
