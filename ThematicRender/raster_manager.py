from dataclasses import dataclass
from contextlib import ExitStack
from typing import Dict, Set, Tuple, Optional, Any
import numpy as np
import rasterio
from rasterio.windows import Window

# Standardized IPC Packets
from ThematicRender.ipc_packets import DriverBlockRef

@dataclass(frozen=True, slots=True)
class WindowRead:
    """Container for spatial coordinate context.

    Attributes:
        read_window: The expanded window actually read from disk (includes halo).
        inner_slices: The NumPy slices required to crop the halo back to the target tile size.
    """
    read_window: Window
    inner_slices: Tuple[slice, slice]


def _expand_window_for_halo(window: Window, *, halo_px: int, width: int, height: int) -> WindowRead:
    """Calculates an expanded coordinate window to ensure spatial continuity.

    Procedural noise and Gaussian filters require a 'halo' of neighbor pixels
    to prevent edge artifacts at tile boundaries. This function calculates the
    expanded read area and the corresponding crop slices.

    Args:
        window: The target output window.
        halo_px: Number of padding pixels required.
        width: Maximum width of the source dataset.
        height: Maximum height of the source dataset.

    Returns:
        WindowRead object containing the expanded window and inner crop coordinates.
    """
    if halo_px <= 0:
        return WindowRead(read_window=window, inner_slices=(slice(None), slice(None)))

    # Determine expanded boundaries clamped to the dataset extent
    col_off, row_off = int(window.col_off), int(window.row_off)
    w, h = int(window.width), int(window.height)

    left = max(0, col_off - halo_px)
    top = max(0, row_off - halo_px)
    right = min(width, col_off + w + halo_px)
    bottom = min(height, row_off + h + halo_px)

    read_w = Window(left, top, right - left, bottom - top)

    # Calculate local slices to extract the original 'inner' window from the expanded read
    inner_row0 = row_off - top
    inner_row1 = inner_row0 + h
    inner_col0 = col_off - left
    inner_col1 = inner_col0 + w

    return WindowRead(
        read_window=read_w,
        inner_slices=(slice(inner_row0, inner_row1), slice(inner_col0, inner_col1))
    )


class RasterManager:
    """Lifecycle manager for GIS input datasets.

    This class manages the opening and closing of multiple Rasterio datasets
    and orchestrates the standardized reading of data into the Inter-Process
    Communication (IPC) storage layer.
    """

    def __init__(self, cfg: Any, required_drivers: Set[Any], anchor_key: Any):
        """
        Args:
            cfg: The ConfigMgr providing file paths and specs.
            required_drivers: Set of DriverKeys needed for the current pipeline.
            anchor_key: The DriverKey defining the master resolution and CRS.
        """
        self.cfg = cfg
        self.required_drivers = required_drivers
        self.anchor_key = anchor_key
        self.sources: Dict[Any, rasterio.DatasetReader] = {}
        self._stack = ExitStack()

    def __enter__(self):
        """Opens all required drivers using a managed ExitStack."""
        print("🔓 Opening Input Drivers...")
        for dkey in self.required_drivers:
            p = self.cfg.path(dkey.value)

            if not p or not p.exists():
                raise FileNotFoundError(f"Driver '{dkey.value}' path missing or invalid: {p}")

            try:
                # Add the open handle to the ExitStack for guaranteed cleanup
                self.sources[dkey] = self._stack.enter_context(rasterio.open(p))

                status = "(ANCHOR)" if dkey == self.anchor_key else "        "
                print(f"   🔹 {status} {dkey.value.ljust(12)} -> {p.name}")
            except Exception as e:
                raise ValueError(f"Failed to open driver {dkey.value} at {p}: {e}")

        if self.anchor_key not in self.sources:
            raise RuntimeError(f"Anchor driver '{self.anchor_key.value}' failed to open.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes all file handles."""
        self._stack.close()

    @property
    def anchor_src(self) -> rasterio.DatasetReader:
        """Returns the primary dataset used for spatial reference."""
        return self.sources[self.anchor_key]

    def read_driver_block_ref(
            self, key: Any, src: rasterio.DatasetReader, window: Window, *,
            halo_override: Optional[int] = None, pool: Any
    ) -> DriverBlockRef:
        """Reads a spatial window from disk and commits it to Shared Memory.

        This is the primary ingestion point for the engine. It handles:
        1. Coordinate expansion (Halos).
        2. Boundless reading with NoData filling.
        3. Dtype standardization (promoting integers to floats for math).
        4. Validity mask construction (Alpha or NoData based).
        5. Handoff to the Shared Memory Pool.

        Args:
            key: The DriverKey for the raster being read.
            src: The open Rasterio DatasetReader handle.
            window: The target global window (without halo).
            halo_override: Optional pixel padding (defaults to config spec).
            pool: The SharedMemoryPool to receive the data.

        Returns:
            DriverBlockRef: A lightweight, pickle-safe pointer to the data in SHM.
        """
        # 1. SPATIAL SETUP
        drv_spec = self.cfg.get_spec(key)
        halo = halo_override if halo_override is not None else drv_spec.halo_px

        win_read = _expand_window_for_halo(
            window, halo_px=halo, width=src.width, height=src.height
        )

        # 2. DISK I/O (Data)
        fill = src.nodata if src.nodata is not None else 0
        raw = src.read(1, window=win_read.read_window, boundless=True, fill_value=fill)

        # 3. DTYPE STANDARDIZATION
        # Promote to float32 for math processing unless it is 8-bit categorical data
        data = raw.astype("float32", copy=False) if drv_spec.dtype != np.uint8 else raw
        h, w = data.shape

        # 4. MASK CONSTRUCTION (Presence Sensing)
        # Determine valid pixels using Alpha bands (2nd or 4th band) or NoData values
        if src.count in (2, 4):
            # Extract standard Alpha channel
            alpha_raw = src.read(
                src.count, window=win_read.read_window, boundless=True, fill_value=0
            )
            mask = (alpha_raw.astype("float32", copy=False) / 255.0)[np.newaxis, ...]
        else:
            # Default to solid mask (1.0)
            mask = np.ones((1, h, w), dtype="float32")

        # Overlay NoData mask if defined in the GeoTIFF metadata
        if src.nodata is not None:
            nodata_mask = (raw != src.nodata).astype("float32")[np.newaxis, ...]
            mask *= nodata_mask

        # 5. STORAGE HANDOFF (Shared Memory)
        # Acquire a binary slot from the pool (Blocks if pool is exhausted)
        slot_id = pool.acquire()

        try:
            # Commit the 2D local arrays into the 4D Shared Memory buffer
            # Return the lightweight reference for the worker processes
            return pool.write(
                slot_id=slot_id,
                data=data,
                mask=mask,
                inner_slices=win_read.inner_slices,
                # Metadata used for debugging/padding
                pad_data=(float(fill) if drv_spec.dtype != np.uint8 else int(fill)),
                pad_mask=0.0
            )
        except Exception:
            # Restore slot availability if the write operation fails
            pool.release(slot_id)
            raise