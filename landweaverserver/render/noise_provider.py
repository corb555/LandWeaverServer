from dataclasses import dataclass, field
from multiprocessing.shared_memory import SharedMemory
from typing import Optional, Tuple

import numpy as np


@dataclass(slots=True)
class NoiseProvider:
    """
    Standardized provider of procedural noise.
    The 'tile' is stored once for all processes in Shared Memory to avoid large I/O
    """
    shm_name: str
    shape: Tuple[int, int]
    dtype: np.dtype

    # Internal handles
    _shm: Optional[SharedMemory] = field(default=None, init=False, repr=False)
    _tile: Optional[np.ndarray] = field(default=None, init=False, repr=False)

    def __getstate__(self):
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

    def get_noise_signal(self, r_base: int, c_base: int, h: int, w: int) -> np.ndarray:
        """
        fast slice for modifiers and LUTs.
        Returns a strictly (H, W, 1) float32 view.
        """
        # 1. Normalize start coordinates against the core pattern size (2048)
        # This ensures we always start within the valid core.
        r0 = r_base % 2048
        c0 = c_base % 2048

        # 2. Slice from the PADDED shared memory buffer.
        # Guaranteed contiguous because buffer is 2560px (2048 + 512 pad).
        # Adding [..., np.newaxis] is a 0ms 'view' operation.
        return self.tile[r0: r0 + h, c0: c0 + w, np.newaxis]

    def close(self) -> None:
        """WORKER SIDE: Close the local handle to shared memory."""
        if self._shm:
            self._shm.close()
            self._shm = None
            self._tile = None

    def unlink(self) -> None:
        """ORCHESTRATOR SIDE:  delete the shared memory segment."""
        self.close()
        try:
            temp_shm = SharedMemory(name=self.shm_name)
            temp_shm.close()
            temp_shm.unlink()
        except FileNotFoundError:
            pass

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
        h, w = int(window.height), int(window.width)

        # Always modulo against the core (2048)
        # This ensures r0/c0 are always in the safe [0-2047] range
        r0 = (int(window.row_off) + int(row_off)) % 2048
        c0 = (int(window.col_off) + int(col_off)) % 2048

        # If a tile is 384 wide and starts at 2000,
        # it will look ahead into indices 2000 to 2384.
        # Since the buffer is 2560 wide, this is a safe, contiguous slice.
        return self.tile[r0: r0 + h, c0: c0 + w]
