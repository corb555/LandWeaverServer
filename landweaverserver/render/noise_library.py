import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing.shared_memory import SharedMemory
from typing import Dict, Any, Optional, Tuple

import numpy as np


@dataclass(slots=True)
class NoiseProvider:
    """
    Standardized provider of procedural noise.
    The 'tile' is stored in Shared Memory to avoid massive I/O during IPC.
    """
    shm_name: str
    shape: Tuple[int, int]
    dtype: np.dtype

    # Internal handles (Not Pickled)
    _shm: Optional[SharedMemory] = field(default=None, init=False, repr=False)
    _tile: Optional[np.ndarray] = field(default=None, init=False, repr=False)

    def __getstate__(self):
        """Exclude live memory handles from the pickle stream."""
        return {
            "shm_name": self.shm_name, "shape": self.shape, "dtype": self.dtype,
        }

    def __setstate__(self, state):
        """Restore metadata; _shm and _tile remain None until attach() is called."""
        self.shm_name = state["shm_name"]
        self.shape = state["shape"]
        self.dtype = state["dtype"]
        self._shm = None
        self._tile = None

    def attach_shm(self) -> None:
        """WORKER SIDE: Map the shared memory into local process space."""
        if self._tile is not None:
            return
        self._shm = SharedMemory(name=self.shm_name)
        self._tile = np.ndarray(self.shape, dtype=self.dtype, buffer=self._shm.buf)

    def close(self) -> None:
        """WORKER SIDE: Close the local handle to shared memory."""
        if self._shm:
            self._shm.close()
            self._shm = None
            self._tile = None

    def unlink(self) -> None:
        """ORCHESTRATOR SIDE: Physically delete the shared memory segment."""
        self.close()
        try:
            temp_shm = SharedMemory(name=self.shm_name)
            temp_shm.close()
            temp_shm.unlink()
        except FileNotFoundError:
            pass

    def cleanup(self, unlink: bool = False):
        """Standardized cleanup for the entire library."""
        if not self.providers:
            return

        for name, provider in self.providers.items():
            try:
                # Call the specific cleanup code you provided
                provider.cleanup(unlink=unlink)
            except Exception as e:
                print(f"   ⚠️  Error cleaning up provider '{name}': {e}")

        self.providers.clear()

    @property
    def tile(self) -> np.ndarray:
        if self._tile is None:
            raise RuntimeError(f"NoiseProvider '{self.shm_name}' accessed before attach_shm().")
        return self._tile

    @property
    def h(self) -> int:
        return self.shape[0]

    @property
    def w(self) -> int:
        return self.shape[1]

    def window_noise(self, window, *, row_off=0, col_off=0, scale_override=None) -> np.ndarray:
            """Hot path: Returns a strictly 2D (H,W) noise patch."""
            h, w = int(window.height), int(window.width)
            r0, c0 = int(window.row_off) + int(row_off), int(window.col_off) + int(col_off)

            s = scale_override if scale_override is not None else 1.0

            # Periodic Wrapping logic
            rows = (np.arange(r0, r0 + h) * s % self.h).astype(np.int64, copy=False)
            cols = (np.arange(c0, c0 + w) * s % self.w).astype(np.int64, copy=False)

            noise1 = self.tile[np.ix_(rows, cols)]

            # Pattern breaker logic
            rows2 = ((np.arange(r0, r0 + h) + 503) * (s * 0.97) % self.h).astype(np.int64)
            cols2 = ((np.arange(c0, c0 + w) + 503) * (s * 0.97) % self.w).astype(np.int64)
            noise2 = self.tile[np.ix_(rows2, cols2)]

            # RETURN STRICTLY 2D
            return (noise1 * 0.7 + noise2 * 0.3)


class NoiseLibrary:
    def __init__(self, cfg, profiles: Dict[str, Any], create_shm: bool = False):
        self.previous_ts: datetime = None
        self.providers: Dict[str, NoiseProvider] = {}
        self.profiles = profiles
        self.noise_shape = (2048, 2048)
        base_seed = cfg.get_global("seed", 42)

        if create_shm:
            # 1. Dispatch all heavy generation and SHM tasks to a thread pool
            # NumPy/FFT release the GIL, so this provides true CPU parallelism.
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for noise_id, profile in profiles.items():
                    futures.append(
                        executor.submit(
                            self._generate_and_store_shm,
                            noise_id, profile, base_seed
                        )
                    )

                # Wait for all threads to complete before proceeding
                concurrent.futures.wait(futures)


        # 2. BOTH MODES: Register the Providers (Lightweight Metadata only)
        for noise_id in profiles.keys():
            self.providers[noise_id] = NoiseProvider(
                shm_name=f"tr_noise_{noise_id}",
                shape=self.noise_shape,
                dtype=np.float32
            )

    def _generate_and_store_shm(self, noise_id: str, profile: Any, base_seed: int):
        """Worker function run in parallel threads for pipeline boot."""
        shm_name = f"tr_noise_{noise_id}"

        # A. Heavy Math (FFT-based generation)
        tile_data = generate_fbm_noise_tile(
            shape=self.noise_shape,
            sigmas=profile.sigmas,
            weights=profile.weights,
            stretch=profile.stretch,
            seed=base_seed + profile.seed_offset
        )

        # B. SHM Resource Management
        try:
            old = SharedMemory(name=shm_name)
            old.close()
            old.unlink()
        except FileNotFoundError:
            pass

        shm = SharedMemory(create=True, size=tile_data.nbytes, name=shm_name)

        # C. Zero-Copy Transfer
        # Wrap the SHM buffer in a numpy view and copy pixels
        shm_view = np.ndarray(tile_data.shape, dtype=tile_data.dtype, buffer=shm.buf)
        shm_view[:] = tile_data[:]

        # Close handle (Unlink remains active in OS until daemon shutdown)
        shm.close()

    def attach_providers_shm(self):
        """Called by Workers during JIT Context Switch."""
        for provider in self.providers.values():
            provider.attach_shm()

    def detach_providers_shm(self):
        """Called by Workers during Job Finalization."""
        for provider in self.providers.values():
            provider.close()

    def get(self, noise_id: str) -> NoiseProvider:
        return self.providers.get(noise_id, None)

    def cleanup(self, unlink: bool = False):
        for provider in self.providers.values():
            provider.cleanup(unlink=unlink)

    def showtime(self, msg):
        wall_start = datetime.now()
        start_ts = wall_start.strftime("%H:%M:%S.%f")[:-3]
        if self.previous_ts is None:
            print(f"{start_ts} {msg}")
        else:
            print(f"{start_ts} {msg}. Elapsed: {wall_start - self.previous_ts}")
        self.previous_ts = wall_start

import numpy as np

def generate_fbm_noise_tile(
        shape: tuple[int, int], *, sigmas: tuple[float, ...] = (1.5, 4.0, 10.0),
        weights: tuple[float, ...] = (0.4, 0.3, 0.3), stretch: tuple[float, float] = (1.0, 1.0),
        seed: int = 42
) -> np.ndarray:
    """
    Generates a multi-scale 2D smooth noise tile using FFT-based Gaussian blurring.

    Performance: Constant time relative to sigma size.
    Quality: Native periodic wrapping (zero edge artifacts).
    """
    rng = np.random.default_rng(seed)
    h, w = shape
    out = np.zeros(shape, dtype="float32")

    # Pre-calculate frequency grids (normalized frequencies)
    # These are used to construct the Gaussian kernel in frequency space
    freq_y = np.fft.fftfreq(h)[:, np.newaxis]
    freq_x = np.fft.fftfreq(w)
    # Square frequencies once for efficiency
    freq_sq = (freq_y**2, freq_x**2)

    for sigma, w_val in zip(sigmas, weights):
        if w_val <= 0: continue

        # 1. Generate unique noise for this octave
        n = rng.uniform(-0.5, 0.5, shape).astype("float32")

        # 2. Apply the blur via FFT
        s_y, s_x = sigma * stretch[0], sigma * stretch[1]

        # Move to frequency domain
        n_fft = np.fft.fft2(n)

        # Construct the Gaussian Transfer Function: H(u,v) = exp(-2 * pi^2 * sigma^2 * f^2)
        # Note: 2 * pi^2 is the scaling constant for standard deviation in freq space
        kernel = np.exp(-2 * (np.pi**2) * (s_y**2 * freq_sq[0] + s_x**2 * freq_sq[1]))

        # Point-wise multiply and return to spatial domain
        n = np.fft.ifft2(n_fft * kernel).real

        # 3. Per-Octave Normalization
        n_min, n_max = n.min(), n.max()
        if n_max - n_min > 1e-6:
            n = (n - n_min) / (n_max - n_min)

        # 4. Add to composite based on weight
        out += float(w_val) * n

    # 5. Final Global Normalization
    mn, mx = out.min(), out.max()
    if mx - mn > 1e-6:
        out = (out - mn) / (mx - mn)

    return out.astype("float32")

def generate_fbm_noise_tile1(
        shape: tuple[int, int], *, sigmas: tuple[float, ...] = (1.5, 4.0, 10.0),
        weights: tuple[float, ...] = (0.4, 0.3, 0.3), stretch: tuple[float, float] = (1.0, 1.0),
        seed: int = 42
) -> np.ndarray:
    """
    This function generates a multi-scale 2D smooth noise tile by creating one independent
    uniform-random field
    per (sigma, weight) pair, Gaussian-blurring each field at a scale sigma * stretch,
    normalizing each blurred
    field individually to 0–1, weighting and summing the normalized fields, then normalizing the
    final composite
    again to 0–1. Smaller sigmas contribute finer texture, larger sigmas contribute broader
    structure, weights
    control the relative dominance of each spatial scale, stretch introduces anisotropy by
    scaling blur separately
    in Y and X, and mode="wrap" makes the blur tile-friendly across image edges. Because each
    octave is
    normalized before weighting and the final composite is normalized again, weights should be
    interpreted
    as controlling relative pattern dominance rather than simple output amplitude.
    """
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
