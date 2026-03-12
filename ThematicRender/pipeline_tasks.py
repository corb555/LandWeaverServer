from dataclasses import dataclass, field
from pathlib import Path
import sys
import time
import traceback
from typing import Dict, Any, Optional, Tuple, Set

import numpy as np
import rasterio
from rasterio.windows import Window
from scipy.ndimage import gaussian_filter


# pipeline_tasks.py
from ThematicRender.config_mgr import ConfigMgr, analyze_pipeline
from ThematicRender.ipc_packets import (WorkPacket, ResultPacket, rect_from_window,
                                        window_from_rect)
from ThematicRender.keys import DriverKey, SurfaceKey


# -----------------------------------------------------------------------------
# Context objects (The "Manifests")
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class ReaderContext:
    io: Any  # RasterManager
    pool_map: Dict[DriverKey, Any]
    _initialized: bool = False


@dataclass(slots=True)
class WorkerContext:
    cfg: ConfigMgr
    pool_map: Dict[DriverKey, Any]
    factors_engine: Any
    surfaces_engine: Any
    themes: Any
    compositor: Any
    pipeline: Any
    anchor_key: DriverKey
    surface_inputs: Set[Any]
    resources: Any
    noise_registry: Any
    out_pool: Optional[Any] = None
    _initialized: bool = False

    @property
    def initialized(self):
        return self._initialized


@dataclass(slots=True)
class WriterContext:
    output_path: Path
    output_profile: Dict[str, Any]
    pool_map: Dict[DriverKey, Any]
    out_pool: Optional[Any] = None
    write_offset_row: int = 0
    write_offset_col: int = 0
    _initialized: bool = False
    _dst: Any = field(default=None, init=False, repr=False)

    def get_dst(self):
        """Lazy-load the file handle locally in the writer process."""
        if self._dst is None:
            # We open in r+ because the file was created empty by the main engine
            self._dst = rasterio.open(self.output_path, "r+")
        return self._dst

    def close(self):
        """Cleanup handle."""
        if self._dst:
            self._dst.close()
            self._dst = None


def init_render_task(ctx: WorkerContext) -> None:
    """Performs one-time initialization"""
    if ctx._initialized:
        return

    # Load QML -> LUT
    # Scan FactorSpecs to see if any categorical themes are required
    for spec in ctx.factors_engine.specs:
        qml_key = spec.params.get("qml_file_key")
        if qml_key:
            # Load the style once here, outside the window loop
            ctx.themes.load_theme_style()

    # Create surfaces by interpolating Color Ramp definitions  (elev, RGB)
    ctx.surfaces_engine.load_surface_ramps(ctx.resources)
    ctx._initialized = True

def init_read_task(ctx: ReaderContext) -> None:
    """Performs one-time initialization """
    if ctx._initialized:
        return

    ctx._initialized = True

# -----------------------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------------------
def read_task(*, seq: int, window: Window, ctx: ReaderContext) -> WorkPacket:
    """Reads all required drivers from disk into SHM. """
    # INITIALIZE (one time)
    if not ctx._initialized:
        init_read_task(ctx)
    t_start = time.perf_counter()

    # 1. Calculate max halo for coordinate expansion
    max_halo = 0
    for dkey in ctx.io.sources:
        spec = ctx.io.cfg.get_spec(dkey)
        max_halo = max(max_halo, spec.halo_px)

    # 2. Iterate through all required drivers and read into their respective pools
    refs = {}
    for key, src in ctx.io.sources.items():
        refs[key] = ctx.io.read_driver_block_ref(
            key=key, src=src, window=window, halo_override=max_halo, pool=ctx.pool_map[key]
        )

    # 3. Return the WorkPacket with metadata for the Render task
    return WorkPacket(
        seq=seq, window_rect=rect_from_window(window), refs=refs,
        read_duration=time.perf_counter() - t_start
    )



def render_task(*, packet: WorkPacket, ctx: WorkerContext) -> ResultPacket:

    """
    Orchestrates the spatial compositing pipeline for a single image tile.

    This function handles the end-to-end rendering lifecycle for a tile: rehydrating
    spatial data from shared memory, applying pre-processing cleanup, generating
    procedural factors and surfaces, and performing the final stack blend.

    Args:
        packet (WorkPacket): The input work unit containing:
            - seq: The unique sequence ID for the tile.
            - window_rect: The global pixel coordinates for the output tile.
            - refs: A dictionary of `DriverBlockRef` pointers to input rasters
                stored in Shared Memory.
            - read_duration: Timing metadata passed from the reader process.
        ctx (WorkerContext): The persistent execution context containing:
            - pool_map: Access to Shared Memory pools for input drivers.
            - factors_engine/surfaces_engine: Procedural math providers.
            - themes: The categorical smoothing and style registry.
            - compositor: The engine responsible for final RGB blending.
            - out_pool: (Optional) The Shared Memory pool for output tiles.
            - _initialized: Boolean tracking process-local engine state.

    Returns:
        ResultPacket: An IPC-friendly result container containing:
            - out_ref: A `DriverBlockRef` pointing to the rendered result in
                the output Shared Memory pool (Multiprocessing mode).
            - img_block: The raw (B, H, W) uint8 array (Single-thread mode).
            - render_duration: High-precision timing of the compute work.
            - Various metadata for the writer process to correctly position
                the tile in the destination GeoTIFF.

    Note:
        This function implements "Firewall Squeezing," which forces all
        multi-band or 1-band inputs into strict 2D (H, W) arrays before
        engine execution. This prevents broadcasting errors common in
        NumPy-based spatial math.
    """
    t_start = time.perf_counter()

    # INITIALIZE (one time)
    if not ctx._initialized:
        init_render_task(ctx)

    # EXTRACT DATA from SHM and sets up the spatial compute window
    data_2d, masks_2d, compute_window, h, w = _prepare_compute_context(packet, ctx)

    # CLEAN raster driver data through smoothing and categorical generalization
    for drv_key in data_2d.keys():
        drv_spec = ctx.cfg.get_spec(drv_key)
        if not drv_spec.cleanup_type:
            continue

        if drv_spec.cleanup_type == "categorical":
            # Pass the explicit THEME_SMOOTHING_SPEC from settings/config
            data_2d[drv_key] = ctx.themes.get_smoothed_ids(
                data_2d[drv_key],
                ctx.cfg.get_smoothing_specs()
            )
        elif drv_spec.cleanup_type == "continuous":
            radius = drv_spec.smoothing_radius
            if radius and radius > 0:
                data_2d[drv_key] = gaussian_filter(data_2d[drv_key].astype(np.float32), sigma=radius)

    # GENERATE FACTORS (masks representing biomes, density, or gradients)
    raw_factors = ctx.factors_engine.generate_factors(data_2d, masks_2d, compute_window, ctx.anchor_key)
    factors_2d = {k: np.squeeze(f) for k, f in raw_factors.items()}

    # SYNTHESIZE SURFACES and apply procedural variation (mottling)
    surface_blocks = ctx.surfaces_engine.generate_surface_blocks(
        data_2d=data_2d,
        masks_2d=masks_2d,
        factors_2d=factors_2d,
        style_engine=ctx.themes,
        surface_inputs=ctx.surface_inputs,
        noises=ctx.factors_engine.noise_registry,
        window=compute_window,
        anchor_key=ctx.anchor_key
    )

    # CROP RESULTS to target size
    anchor_ref = packet.refs[ctx.anchor_key]
    slices = anchor_ref.inner_slices or (slice(None), slice(None))
    surfaces_in = _slice_collection(surface_blocks, slices)
    factors_in = _slice_collection(factors_2d, slices)

    # BLEND the stack
    img_block = ctx.compositor.blend_window(surfaces_in, factors_in, ctx.pipeline)

    # RETURN RESULT
    if ctx.out_pool is None:
        # Synchronous path: Return results as standard Python objects
        return ResultPacket(
            seq=packet.seq,
            window_rect=packet.window_rect,
            refs=packet.refs,
            img_block=img_block,
            read_duration=packet.read_duration,
            render_duration=time.perf_counter() - t_start
        )
    else:
        # Asynchronous path: Commit results to shared memory for the writer process
        out_slot = ctx.out_pool.acquire()
        try:
            out_ref = ctx.out_pool.write(
                out_slot,
                data=img_block,
                mask=np.ones((1, h, w), dtype=np.float32),
                inner_slices=None
            )
            return ResultPacket(
                seq=packet.seq,
                window_rect=packet.window_rect,
                refs=packet.refs,
                out_ref=out_ref,
                read_duration=packet.read_duration,
                render_duration=time.perf_counter() - t_start
            )
        except Exception as e:
            # Ensure slot availability is restored if a write failure occurs
            ctx.out_pool.release(out_slot)
            raise e

def write_task(*, packet: ResultPacket, ctx: WriterContext) -> float:
    """Writes to disk and releases  SHM slots."""

    # 1. COORDINATE TRANSLATION
    # window = Global GIS coordinates
    # local_window = Relative coordinates inside the output file
    t_start = time.perf_counter()

    window = window_from_rect(packet.window_rect)
    local_window = rasterio.windows.Window(
        col_off=int(window.col_off) - int(ctx.write_offset_col),
        row_off=int(window.row_off) - int(ctx.write_offset_row), width=int(window.width),
        height=int(window.height)
    )

    # 2. RESOLVE DATA
    if packet.img_block is not None:
        # Single-thread pass-through path
        data = packet.img_block
    elif packet.out_ref is not None:
        # Pull from SHM Output Pool (Standardized 4D -> 3D View)
        view = ctx.out_pool.view(packet.out_ref)

        # IMPORTANT: Crop to the actual valid data height/width.
        # This handles the "Edge Case" where the final tile in a row
        # is smaller than the standard 256x256 buffer.
        h, w = packet.out_ref.data_h_w
        data = view.data[:, :h, :w]
    else:
        raise ValueError("ResultPacket is empty: contains neither img_block nor out_ref.")

    # 3. WRITE (Rasterio expects B, H, W)
    dst = ctx.get_dst()
    dst.write(data, window=local_window)

    # 4. RESOURCE RELEASE
    # This returns the slot indices to the "Available" Queue
    if packet.out_ref:
        ctx.out_pool.release(packet.out_ref.slot_id)

    # Release all input buffers (DEM, Lithology, etc.) used for this  tile
    for dkey, ref in packet.refs.items():
        ctx.pool_map[dkey].release(ref.slot_id)

    return time.perf_counter() - t_start


def render_worker_task(packet, worker_ctx, result_queue):
    try:
        # Re-initialize the worker's SHM handles if needed
        result = render_task(packet=packet, ctx=worker_ctx)
        result_queue.put(result)
    except Exception as e:
        print(f"Render Error: {e}")
        traceback.print_exc()


def render_worker_loop(work_queue, result_queue, worker_ctx):
    # 1. THE HANDSHAKE
    # Check the Output Pool signature
    if worker_ctx.out_pool:
        if not worker_ctx.out_pool.verify_connection():
            return  # Exit if the session is invalid

    # Check all input pools
    for pool in worker_ctx.pool_map.values():
        # Force re-attachment of the numpy views for the new process
        pool._v_cache = None
        pool._m_cache = None
        if not pool.verify_connection():
            return

    # 2. INITIALIZE ENGINE (Once per core)
    if not worker_ctx._initialized:
        init_render_task(worker_ctx)

    while True:
        packet = work_queue.get()
        if packet is None:  # Sentinel to exit
            break

        try:
            result = render_task(packet=packet, ctx=worker_ctx)
            result_queue.put(result)
        except Exception as e:
            print(f" Worker Render Error: {e}")
            traceback.print_exc()
            sys.exit(3)


def writer_worker_loop(result_queue, writer_ctx):
    """The dedicated I/O process aggregates all telemetry."""
    stats = {
        "read": 0.0, "render": 0.0, "write": 0.0, "count": 0,
    }

    # Track when the writer actually started its loop
    proc_start = time.perf_counter()

    try:
        while True:
            packet = result_queue.get()
            if packet is None:
                break

            # 1. Measure the Write
            write_start = time.perf_counter()
            write_task(packet=packet, ctx=writer_ctx)
            write_duration = time.perf_counter() - write_start

            # 2. Accumulate from Packet
            stats["read"] += packet.read_duration
            stats["render"] += packet.render_duration
            stats["write"] += write_duration
            stats["count"] += 1

    finally:
        # 3. Final Flush
        f_start = time.perf_counter()
        writer_ctx.close()
        flush_duration = time.perf_counter() - f_start
        stats["write"] += flush_duration

        # 4. Total Wall Time (from the writer's perspective)
        # Note: Adding init_duration makes it comparable to the total run
        total_elapsed = (time.perf_counter() - proc_start)

        # 5. Print the Final Report
        _print_mp_report(stats, total_elapsed, flush_duration)


def _prepare_compute_context(packet: WorkPacket, ctx: WorkerContext):
    """
    Rehydrates shared memory and calculates the expanded spatial context.

    Returns:
        tuple: (data_2d, masks_2d, compute_window, target_h, target_w)
    """
    # 1. Determine the target output dimensions
    inner_window = window_from_rect(packet.window_rect)
    h, w = int(inner_window.height), int(inner_window.width)

    # 2. Map shared memory buffers into local process views (3D)
    raw_blocks = {k: ctx.pool_map[k].view(ref) for k, ref in packet.refs.items()}

    # 3. FIREWALL: Squeeze to strictly 2D working planes
    data_2d = {k: np.squeeze(blk.data[0]) for k, blk in raw_blocks.items()}
    masks_2d = {k: np.squeeze(blk.mask[0]) for k, blk in raw_blocks.items()}

    # 4. Coordinate Calculation (Halo / Padding logic)
    anchor_blk_ref = packet.refs[ctx.anchor_key]
    r_pad = anchor_blk_ref.inner_slices[0].start if anchor_blk_ref.inner_slices else 0
    c_pad = anchor_blk_ref.inner_slices[1].start if anchor_blk_ref.inner_slices else 0

    # Define the expanded spatial window used for noise sampling
    comp_h, comp_w = data_2d[ctx.anchor_key].shape[:2]
    compute_window = rasterio.windows.Window(
        col_off=inner_window.col_off - c_pad,
        row_off=inner_window.row_off - r_pad,
        width=comp_w,
        height=comp_h
    )

    return data_2d, masks_2d, compute_window, h, w


def _slice_collection(collection: Dict[Any, np.ndarray], slices: Tuple[slice, slice]):
    sy, sx = slices
    return {k: v[sy, sx, ...] for k, v in collection.items()}


def _print_mp_report(stats, total_elapsed, flush_duration):
    n = stats["count"] or 1
    print("\n" + "=" * 40)
    print(f"MP RENDER REPORT ({n} tiles)")
    print("-" * 40)
    print(f"Read :          {stats['read']:7.2f}s")
    print(f"Render :        {stats['render']:7.2f}s")
    print(f"Write :         {stats['write']:7.2f}s (inc. {flush_duration:.2f}s flush)")
    print("-" * 40)
    print(f"Wall Time:      {total_elapsed:7.2f}s")

    # The Parallelism Power:
    # Sum of all work / Wall time
    sum_work = stats['read'] + stats['render'] + stats['write']
    print(f"Efficiency:   {sum_work / total_elapsed:7.2f}x speedup")
    print("=" * 40 + "\n")

