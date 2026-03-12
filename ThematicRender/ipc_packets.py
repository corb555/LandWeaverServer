from dataclasses import dataclass
from typing import Optional, Tuple, Dict
import uuid

import numpy as np
from rasterio.windows import Window

from ThematicRender.keys import DriverKey

# ipc_blocks.py

Slice2D = Tuple[slice, slice]
WindowRect = Tuple[int, int, int, int]  # (col_off, row_off, width, height)


def rect_from_window(w: Window) -> WindowRect:
    return int(w.col_off), int(w.row_off), int(w.width), int(w.height)


def window_from_rect(r: WindowRect) -> Window:
    col, row, w, h = r
    return Window(col, row, w, h)


@dataclass(frozen=True)
class DriverBlockRef:
    slot_id: int
    data_h_w: Tuple[int, int]
    inner_slices: Optional[Tuple[slice, slice]] = None


@dataclass(frozen=True, slots=True)
class DriverBlockView:
    slot_id: int
    data: np.ndarray
    mask: np.ndarray
    data_h_w: Tuple[int, int]
    mask_h_w: Tuple[int, int]
    inner_slices: Optional[Slice2D] = None


@dataclass(frozen=True, slots=True)
class WorkPacket:
    """Reader -> Worker (IPC-friendly)."""
    seq: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    read_duration: float = 0.0


@dataclass(frozen=True, slots=True)
class ResultPacket:
    seq: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    img_block: Optional[np.ndarray] = None
    out_ref: Optional[DriverBlockRef] = None
    # Metrics
    read_duration: float = 0.0
    render_duration: float = 0.0
    write_duration: float = 0.0


@dataclass(frozen=True)
class PoolSpec:
    data_shape: tuple  # (Bands, H, W) e.g., (1, 384, 384)
    data_dtype: np.dtype
    mask_shape: tuple  # (Bands, H, W) e.g., (1, 384, 384)
    mask_dtype: np.dtype


class BlockPool:
    def __init__(self, spec: PoolSpec, slots: int) -> None:
        self.spec = spec
        self.slots = slots
        self._data = np.empty((slots, *spec.data_shape), dtype=spec.data_dtype)
        self._mask = np.empty((slots, *spec.mask_shape), dtype=spec.mask_dtype)
        self._free: list[int] = list(range(slots))

    def __getitem__(self, slot_id: int) -> 'DriverBlockView':
        return DriverBlockView(
            slot_id=slot_id, data=self._data[slot_id], mask=self._mask[slot_id], )

    def acquire(self) -> int:
        if not self._free:
            raise RuntimeError("BlockPool exhausted: no free slots.")
        return self._free.pop()

    def release(self, slot_id: int) -> None:
        if not (0 <= slot_id < self.slots):
            raise ValueError(f"slot_id out of range: {slot_id}")
        self._free.append(slot_id)

    def write(
            self, slot_id: int, data: np.ndarray, mask: np.ndarray,
            inner_slices: Optional[Tuple[slice, slice]] = None, pad_data: float = 0.0,
            pad_mask: float = 0.0
            ) -> 'DriverBlockRef':
        """
        Coerces any input into the (Band, H, W) storage slot.
        Accepts pad arguments to match RasterManager signature.
        """
        # Standardize Data to 3D (B, H, W)
        v_data = data[np.newaxis, ...] if data.ndim == 2 else data
        # Standardize Mask to 3D (B, H, W)
        v_mask = mask[np.newaxis, ...] if mask.ndim == 2 else mask

        vb, vh, vw = v_data.shape
        mb, mh, mw = v_mask.shape

        # 4D Write: [Slot, Band, Row, Col]
        self._data[slot_id, :vb, :vh, :vw] = v_data
        self._mask[slot_id, :mb, :mh, :mw] = v_mask

        return DriverBlockRef(
            slot_id=slot_id, data_h_w=(vh, vw), inner_slices=inner_slices
        )

    def view(self, ref: 'DriverBlockRef'):
        """Returns a namedtuple/object with .value and .valid as (B, H, W)"""
        # Slice the 4D buffer into a 3D view for the worker
        data = self._data[ref.slot_id, :, :ref.data_h_w[0], :ref.data_h_w[1]]
        mask = self._mask[ref.slot_id, :, :ref.data_h_w[0], :ref.data_h_w[1]]
        return type('View', (), {'data': data, 'mask': mask})


from multiprocessing import shared_memory, Queue


def _standardize_shape(shape: tuple) -> tuple:
    """Forces any 2D or 3D shape into a 3D (Bands, H, W) tuple."""
    if len(shape) == 2:  # (H, W) -> (1, H, W)
        return (1, shape[0], shape[1])
    if len(shape) == 3:
        if shape[2] <= 4:  # (H, W, B) -> (B, H, W)
            return (shape[2], shape[0], shape[1])
    return shape  # Already (B, H, W)


class SharedMemoryPool:
    def __init__(self, spec: PoolSpec, slots: int, prefix: str):
        self.prefix = prefix
        self.spec = spec
        self.slots = slots
        self.session_id = str(uuid.uuid4())[:8]

        # 1. Handshake
        sig_name = f"{prefix}_sig"
        self._sig_shm = self._create_shm(sig_name, 64)
        self._sig_shm.buf[:8] = self.session_id.encode('ascii')

        # 2. Define Shapes and Dtypes
        self._d_shape = (slots, *_standardize_shape(spec.data_shape))
        self._m_shape = (slots, *_standardize_shape(spec.mask_shape))
        self._d_name = f"{prefix}_data"
        self._m_name = f"{prefix}_mask"

        # 3. Allocation (Main Process)
        v_size = int(np.prod(self._d_shape) * np.dtype(spec.data_dtype).itemsize)
        m_size = int(np.prod(self._m_shape) * np.dtype(spec.mask_dtype).itemsize)

        self._d_shm = self._create_shm(self._d_name, v_size)
        self._m_shm = self._create_shm(self._m_name, m_size)

        # 4. Local Cache for NumPy Views
        # These will be None when the object is unpickled in a worker process
        self._d_buf_local = None
        self._m_buf_local = None

        # Queue for slot management
        self._available_slots = Queue()
        for i in range(slots):
            self._available_slots.put(i)

    @property
    def data_buf(self) -> np.ndarray:
        """Process-safe access to the Value buffer."""
        if self._d_buf_local is None:
            # Re-attach to the SHM handle (which was pickled/unpickled)
            # and wrap it in a fresh numpy array for this process.
            self._d_buf_local = np.ndarray(
                self._d_shape, dtype=self.spec.data_dtype, buffer=self._d_shm.buf
            )
        return self._d_buf_local

    @property
    def mask_buf(self) -> np.ndarray:
        """Process-safe access to the Validity buffer."""
        if self._m_buf_local is None:
            self._m_buf_local = np.ndarray(
                self._m_shape, dtype=self.spec.mask_dtype, buffer=self._m_shm.buf
            )
        return self._m_buf_local

    def write(self, slot_id: int, data: np.ndarray, mask: np.ndarray, **kwargs) -> 'DriverBlockRef':
        # Standardize inputs to (B, H, W)
        d_in = data[np.newaxis, ...] if data.ndim == 2 else data
        if d_in.ndim == 3 and d_in.shape[2] <= 4:
            d_in = d_in.transpose(2, 0, 1)

        m_in = mask[np.newaxis, ...] if mask.ndim == 2 else mask
        if m_in.ndim == 3 and m_in.shape[2] <= 4:
            m_in = m_in.transpose(2, 0, 1)

        db, dh, dw = d_in.shape
        mb, mh, mw = m_in.shape

        # Write to the lazy-attached buffers
        self.data_buf[slot_id, :db, :dh, :dw] = d_in
        self.mask_buf[slot_id, :mb, :mh, :mw] = m_in

        return DriverBlockRef(
            slot_id=slot_id, data_h_w=(dh, dw), inner_slices=kwargs.get('inner_slices')
        )

    def view(self, ref: 'DriverBlockRef'):
        """Returns a view of the 3D (B, H, W) data for a slot."""
        h, w = ref.data_h_w
        return type(
            'View', (), {
                'data': self.data_buf[ref.slot_id, :, :h, :w],  # Changed from 'value'
                'mask': self.mask_buf[ref.slot_id, :, :h, :w]  # Changed from 'valid'
            }
            )

    def _create_shm(self, name, size):
        try:
            return shared_memory.SharedMemory(name=name, create=True, size=size)
        except FileExistsError:
            ex = shared_memory.SharedMemory(name=name);
            ex.close();
            ex.unlink()
            return shared_memory.SharedMemory(name=name, create=True, size=size)

    def verify_connection(self):
        # Force re-binding during verification
        _ = self.data_buf
        _ = self.mask_buf
        try:
            sig_name = f"{self.prefix}_sig"
            temp_sig = shared_memory.SharedMemory(name=sig_name)
            content = bytes(temp_sig.buf[:8]).decode('ascii').strip('\x00')
            match = content == self.session_id
            temp_sig.close()
            return match
        except:
            return False

    def acquire(self):
        return self._available_slots.get()

    def release(self, i):
        self._available_slots.put(i)

    def cleanup(self):
        for s in [self._d_shm, self._m_shm, self._sig_shm]:
            s.close();
            s.unlink()
