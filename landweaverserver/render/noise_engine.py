import concurrent.futures
from datetime import datetime
from multiprocessing.shared_memory import SharedMemory
from typing import Dict, Any

from landweaverserver.render.noise_provider import NoiseProvider


class NoiseEngine:
    def __init__(self, cfg, profiles: Dict[str, Any], create_shm: bool = False):
        self.previous_ts: datetime = None
        self.profiles = profiles

        # --- DIMENSION CONTRACT ---
        self.core_size = 2048
        self.pad_size = 512  # Must be larger than  max tile (384)
        self.padded_shape = (self.core_size + self.pad_size, self.core_size + self.pad_size)

        self.providers: Dict[str, NoiseProvider] = {}
        base_seed = cfg.get_global("seed", 42)

        if create_shm:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(self._generate_and_store_shm, nid, prof, base_seed) for
                           nid, prof in profiles.items()]
                concurrent.futures.wait(futures)

        # 2. Register Providers with the PADDED shape
        # This ensures workers map the entire buffer (2560x2560)
        for noise_id in profiles.keys():
            self.providers[noise_id] = NoiseProvider(
                shm_name=f"tr_noise_{noise_id}", shape=self.padded_shape, dtype=np.float32
            )

    def _generate_and_store_shm(self, noise_id: str, profile: Any, base_seed: int):
        shm_name = f"tr_noise_{noise_id}"
        core = self.core_size
        pad = self.pad_size

        # 1. Generate the core signal (2048x2048)
        raw_tile = generate_fbm_noise_tile(
            shape=(core, core), sigmas=profile.sigmas, weights=profile.weights,
            stretch=profile.stretch, seed=base_seed + profile.seed_offset
        )

        # 2. Pre-mix and Center
        r_shift, c_shift = 503, 503
        layer2 = np.roll(raw_tile, shift=(-r_shift, -c_shift), axis=(0, 1))
        mixed_tile = (raw_tile * 0.7 + layer2 * 0.3) - 0.5
        mixed_tile = mixed_tile.astype(np.float32)

        # 3. Create Padded SHM
        shm_size = (core + pad) * (core + pad) * 4
        try:
            old = SharedMemory(name=shm_name)
            old.unlink()
        except:
            pass

        shm = SharedMemory(create=True, size=shm_size, name=shm_name)
        # Create a view of the full padded buffer
        buf = np.ndarray((core + pad, core + pad), dtype=np.float32, buffer=shm.buf)

        # 4. STITCH GHOST BORDER
        buf[:core, :core] = mixed_tile  # Top-Left Core
        buf[:core, core:] = mixed_tile[:, :pad]  # Right Gutter
        buf[core:, :core] = mixed_tile[:pad, :]  # Bottom Gutter
        buf[core:, core:] = mixed_tile[:pad, :pad]  # Corner

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

    def cleanup(self, unlink: bool = False) -> None:
        """Release all noise provider shared-memory resources.

        Args:
            unlink: If True, also unlink the underlying shared-memory segments.
                This should normally only be used by the orchestrator / owner
                process during final shutdown. Worker processes should use the
                default behavior and only close their local handles.
        """
        for provider in list(self.providers.values()):
            try:
                if unlink:
                    provider.unlink()
                else:
                    provider.close()
            except FileNotFoundError:
                # Shared memory may already have been removed by another owner.
                pass
            except Exception as exc:
                print(f"⚠️ NoiseEngine cleanup warning for '{provider.shm_name}': {exc}")

        self.providers.clear()

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
    freq_sq = (freq_y ** 2, freq_x ** 2)

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
        kernel = np.exp(-2 * (np.pi ** 2) * (s_y ** 2 * freq_sq[0] + s_x ** 2 * freq_sq[1]))

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
