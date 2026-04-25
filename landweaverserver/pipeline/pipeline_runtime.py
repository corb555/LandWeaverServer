from contextlib import ExitStack
import multiprocessing as mp
import traceback
from typing import Dict, List, Optional, Tuple

import numpy as np

from landweaverserver.common.ipc_packets import Envelope, Op
from landweaverserver.pipeline.job_context import JobContextStore
from landweaverserver.pipeline.job_loops import worker_loop, reader_loop, writer_loop
from landweaverserver.pipeline.shared_memory import PoolSpec, SharedMemoryPool, SlotRegistry
from landweaverserver.pipeline.system_config import SystemConfig
from landweaverserver.render.noise_engine import NoiseEngine


class PipelineRuntime:
    def __init__(self, engine_cfg: 'SystemConfig', source_specs):
        self._cleaned = False
        self._stopped = False
        self.engine_cfg = engine_cfg
        self.stack = ExitStack()
        self.system_params = self.engine_cfg.get("system", {})
        self.source_specs = source_specs

        # Resource Containers
        self.pool_map: Dict[str, SharedMemoryPool] = {}
        self.output_pool: Optional[SharedMemoryPool] = None
        self.registry: Optional[SlotRegistry] = None
        self.ctx_store: Optional['JobContextStore'] = None

        # TODO noise_eng should probably not be in this
        self.noise_eng: Optional['NoiseEngine'] = None

        self.ctx_mp = mp.get_context("spawn")

        # IPC Queues
        self.status_q = self.ctx_mp.Queue()
        self.reader_q = self.ctx_mp.Queue()
        self.worker_q = self.ctx_mp.Queue()
        self.writer_q = self.ctx_mp.Queue()
        self.response_q = self.ctx_mp.Queue()

        # Process Handles
        self.reader_procs: List[mp.Process] = []
        self.worker_procs: List[mp.Process] = []
        self.writer_proc: Optional[mp.Process] = None

    def start(self) -> None:
        print("[PipelineRuntime] Startup...")

        # Shared Memory Context Side-channel
        self.ctx_store = JobContextStore()
        self.stack.callback(self.ctx_store.cleanup)

        # Pre-allocate SHM Pools & Partition Registry
        self._initialize_shm_pools(
            input_slots=self.system_params.get("input_slots"),
            num_renderers=self.system_params.get("renderer_count"),
            num_readers=self.system_params.get("reader_count"),
            buffer_factor=self.system_params.get("transit_buffer_factor"),
            source_specs=self.source_specs
        )

        shm_name = self.ctx_store.shm.name

        self.reader_procs = [self.ctx_mp.Process(
            target=reader_loop, args=(self.reader_q, self.status_q, shm_name, self.pool_map),
            name=f"RasterRead_{i}", ) for i in range(self.system_params.get("reader_count"))]

        self.worker_procs = [self.ctx_mp.Process(
            target=worker_loop,
            args=(self.worker_q, self.writer_q, self.status_q, shm_name, self.output_pool,
                  self.pool_map,), name=f"RasterRender_{i}", ) for i in
            range(self.system_params.get("renderer_count"))]

        self.writer_proc = self.ctx_mp.Process(
            target=writer_loop, args=(self.writer_q, self.status_q, shm_name, self.output_pool),
            name="RasterWrite_1", )

        for proc in self.reader_procs + self.worker_procs + [self.writer_proc]:
            if proc is not None:
                proc.start()

        print(f"[PipelineRuntime] Workers Ready. SHM Store: {shm_name}")

    def stop(self) -> None:
        """
        Stop all worker processes and reclaim runtime resources.

        Shutdown sequence:
        1. Flip the shared global state to SHUTDOWN so active workers can
           observe cancellation immediately.
        2. Send one SHUTDOWN envelope per worker process.
        3. Join each process gracefully, then force terminate if needed.
        4. Unlink shared memory and close IPC via ``cleanup()``.

        This method is idempotent.
        """
        if self._stopped:
            return

        print("\n[PipelineRuntime] Stop initiated...")
        self._stopped = True

        # 1. Global shutdown signal
        if self.ctx_store:
            try:
                self.ctx_store.set_shutdown()
                print("   - Global state set to SHUTDOWN")
            except Exception as exc:
                print(f"   ⚠️ Failed to set shutdown state: {exc}")

        # 2. Poison pills
        print("   - Dispatching shutdown envelopes to worker queues...")
        self._send_shutdown_envelopes()

        # 3. Graceful join with forceful fallback
        print("   - Waiting for worker processes to exit...")
        self._stop_process_group(self.reader_procs)
        self._stop_process_group(self.worker_procs)
        self._stop_process_group([self.writer_proc])

        # 4. Final resource reclamation
        print("   - Reclaiming shared memory and IPC resources...")
        self.cleanup()

    def _send_shutdown_envelopes(self) -> None:
        """Send one shutdown envelope per worker process."""
        for _ in self.reader_procs:
            self.reader_q.put(Envelope(op=Op.SHUTDOWN, payload=None))

        for _ in self.worker_procs:
            self.worker_q.put(Envelope(op=Op.SHUTDOWN, payload=None))

        if self.writer_proc is not None:
            self.writer_q.put(Envelope(op=Op.SHUTDOWN, payload=None))

    @staticmethod
    def _stop_process_group(
            procs: List[Optional[mp.Process]], join_timeout_s: float = 1.0,
            terminate_join_timeout_s: float = 0.2, ) -> None:
        """
        Join a list of processes and force terminate any that remain alive.

        Args:
            procs: Process list to stop.
            join_timeout_s: Grace period for normal shutdown.
            terminate_join_timeout_s: Final join timeout after terminate().
        """
        for proc in procs:
            if proc is None:
                continue

            if not proc.is_alive():
                continue

            proc.join(timeout=join_timeout_s)

            if proc.is_alive():
                print(f"   ⚠️ Process {proc.name} unresponsive. Force terminating...")
                proc.terminate()
                proc.join(timeout=terminate_join_timeout_s)

    def sync_to_geography(self, region_id: str):
        """
        Purge SHM cache if the geographical region has changed.
        """
        if self.registry.context_id != region_id:
            print(f"[System] New region ({region_id[:8]}). Purging Slot Cache.")
            self.registry.reset_context(region_id)

    def _initialize_shm_pools(
            self, input_slots: int, num_renderers: int, num_readers: int, buffer_factor: float,
            source_specs, ) -> None:
        """Allocate SHM segments and calculate the static/transit partition."""
        block_h, block_w = 256, 256
        max_halo = self.engine_cfg.get("system.max_halo", 0)
        pool_h = block_h + 2 * max_halo
        pool_w = block_w + 2 * max_halo

        num_sources = len(source_specs)

        dtype_map = {
            "uint8": np.uint8, "float32": np.float32, "float64": np.float64, "int16": np.int16,
        }

        for drv_key, spec_cfg in source_specs.items():
            dtype_str = spec_cfg.dtype or "float32"
            val_dtype = dtype_map.get(dtype_str)
            if val_dtype is None:
                raise ValueError(
                    f"Unsupported dtype '{dtype_str}' for source '{drv_key}'."
                )

            spec = PoolSpec(
                data_shape=(1, pool_h, pool_w), data_dtype=np.dtype(val_dtype),
                mask_shape=(1, pool_h, pool_w), mask_dtype=np.dtype(np.float32), )

            pool = SharedMemoryPool(spec, input_slots, prefix=f"tr_{drv_key}")
            self.stack.callback(pool.cleanup)
            self.pool_map[drv_key] = pool

        out_slots = int(max(16, int(num_renderers * buffer_factor)))
        out_spec = PoolSpec(
            data_shape=(3, 256, 256), data_dtype=np.dtype(np.uint8), mask_shape=(1, 256, 256),
            mask_dtype=np.dtype(np.float32), )
        self.output_pool = SharedMemoryPool(out_spec, slots=out_slots, prefix="tr_output")
        self.stack.callback(self.output_pool.cleanup)

        static_count, transit_count = calculate_shm_partitions(
            total_slots=input_slots, num_renderers=num_renderers, num_readers=num_readers,
            num_sources=num_sources, buffer_factor=buffer_factor, )

        self.registry = SlotRegistry(
            pool_map=self.pool_map, context_id="boot", static_count=static_count, )

        emit_memory_plan_report(
            MemoryPlanReport(
                input_slots=input_slots, num_renderers=num_renderers, num_readers=num_readers,
                num_sources=num_sources, buffer_factor=buffer_factor, static_count=static_count,
                transit_count=transit_count, out_slots=out_slots, )
        )

    def manage_noise_engine(self, noise_eng: 'NoiseEngine'):
        """
        Register the active noise engine.

        If a prior noise engine exists, its SHM is unlinked immediately to
        prevent accumulation of orphaned segments.
        """
        if self.noise_eng is not None:
            print("   - Unlinking superseded Noise Library...")
            try:
                self.noise_eng.cleanup(unlink=True)
            except Exception as exc:
                print(f"   ⚠️ Warning unlinking old noise library: {exc}")

        self.noise_eng = noise_eng

    def cleanup(self):
        """
        Hard reclamation of all runtime resources.

        This unlinks shared memory segments and clears runtime-owned references.
        This method is idempotent.
        """
        if self._cleaned:
            return

        print("\n[PipelineRuntime] Cleaning up all resources...")

        # 1. Noise cleanup
        if self.noise_eng:
            print("   - Unlinking active Noise Library segments...")
            try:
                self.noise_eng.cleanup(unlink=True)
                self.noise_eng = None
            except Exception as exc:
                traceback.print_exc()
                print(f"   ⚠️ Noise cleanup error: {exc}")

        # 2. ExitStack resource unlink
        try:
            print("   - Unlinking Source Pools and closing IPC...")
            self.stack.close()
        except Exception as exc:
            print(f"   ⚠️ Error during ExitStack closure: {exc}")

        # 3. Reference clearing
        self.pool_map.clear()
        self.output_pool = None
        self.registry = None
        self.ctx_store = None
        self.status_q = None
        self.reader_q = None
        self.worker_q = None
        self.writer_q = None
        self.response_q = None

        self._cleaned = True
        print("✅ [PipelineRuntime] Cleanup complete.")

    def update_context(self, job_id: str, reader_data, worker_data, writer_data):
        """
        Publish the current job contexts into the SHM side-channel.

        This must be called before dispatching tile work.
        """
        if not self.ctx_store:
            raise RuntimeError("Engine not initialized.")

        self.ctx_store.write_contexts(job_id, reader_data, worker_data, writer_data)

    def cancel_active_job(self):
        """Interrupt workers by setting the SHM state to CANCEL."""
        if self.ctx_store:
            print("🛑 [Engine] Global State: CANCEL")
            self.ctx_store.set_job_cancel()

    def set_engine_idle(self):
        """Set the global SHM state to IDLE."""
        if self.ctx_store:
            self.ctx_store.set_idle()

    def set_engine_shutdown(self):
        """Set the global SHM state to SHUTDOWN."""
        if self.ctx_store:
            self.ctx_store.set_shutdown()


"""Shared-memory partition reporting utilities."""

from dataclasses import dataclass

STATIC_CACHE_WARN_RATIO = 0.20
CACHE_PADDING_SLOTS = 100
SEPARATOR_WIDTH = 60


@dataclass(frozen=True)
class MemoryPlanReport:
    """Computed values used for SHM partition reporting.

    Args:
        input_slots: Total input slots per source.
        num_renderers: Number of renderer workers.
        num_readers: Number of reader workers.
        num_sources: Number of configured input sources.
        buffer_factor: Transit/output buffer scaling factor.
        static_count: Number of static-cache slots.
        transit_count: Number of transit slots.
        out_slots: Number of output-pool slots.
    """

    input_slots: int
    num_renderers: int
    num_readers: int
    num_sources: int
    buffer_factor: float
    static_count: int
    transit_count: int
    out_slots: int

    @property
    def load(self) -> int:
        """Return the pipeline load estimate."""
        return self.num_renderers * self.num_sources + self.num_readers

    @property
    def static_cache_ratio(self) -> float:
        """Return the static-cache share of the input pool."""
        return 0.0 if self.input_slots <= 0 else self.static_count / self.input_slots

    @property
    def min_transit_slots(self) -> int:
        """Return the minimum healthy transit-slot count."""
        return self.num_renderers * self.num_sources


def emit_memory_plan_report(report: MemoryPlanReport) -> None:
    """Print a detailed shared-memory plan report.

    Args:
        report: Precomputed shared-memory plan values.
    """
    print("\n[MemoryPlan] Partitioning:")
    print(
        f"   - Total Pool Size:  {report.input_slots:4} slots per source "
        f"('config: input_slots')"
    )
    print(
        f"   - Transit Pool:     {report.transit_count:4} slots "
        f"(Load x transit_buffer_factor: {report.buffer_factor})"
    )
    print(
        f"          - Load:      {report.load} "
        f"({report.num_renderers} Renderers × {report.num_sources} Sources + "
        f"{report.num_readers} Readers)"
    )
    print(
        f"   - Static Cache:    {report.static_count:4} slots "
        f"(Remaining slots for Cache)\n"
    )
    print(
        f"   - Output Pool:     {report.out_slots:4} slots "
        f"(Write-buffer for {report.num_renderers} Renderers x "
        f"transit_buffer_factor)"
    )

    print("\n   💡 [Optimization Tips]:")

    if report.static_cache_ratio < STATIC_CACHE_WARN_RATIO:
        suggested_input_slots = report.transit_count + CACHE_PADDING_SLOTS
        print("   ⚠️  Warning: Static Cache is very small (<20%). Previews may be slow.")
        print(
            f"      To fix: Increase 'input_slots' in system.yml to at least "
            f"{suggested_input_slots}."
        )
    else:
        print("   ✅  Static Cache is healthy.")

    if report.transit_count < report.min_transit_slots:
        print("   🛑 Critical: Transit Pool is under-sized! Potential for pipeline deadlock.")
        print("      To fix: Increase 'input_slots' or decrease 'transit_buffer_factor'.")
    else:
        print("   ✅  Transit Pool is healthy.")

    print(
        f"   - To change Transit Pool, adjust 'config: transit_buffer_factor' "
        f"(Current: {report.buffer_factor})"
    )
    print("   - Source Count is sum of entries in source_specs")
    print("-" * SEPARATOR_WIDTH + "\n")


def calculate_shm_partitions(
        total_slots: int, num_renderers: int, num_readers: int,  # New parameter
        num_sources: int, buffer_factor: float = 2.0, max_transit_ratio: float = 0.8
) -> Tuple[int, int]:
    """
    Determines the split between Static Cache and Transit Pool.

    Formula:
    transit_floor = ((Renderers * Sources) + Readers) * buffer_factor
    """
    # 1. Calculate the 'Worst-Case' Transit Floor
    # Renderers need 'num_sources' slots each; Readers only need 1 slot each.
    active_demand = (num_renderers * num_sources) + num_readers
    transit_floor = int(active_demand * buffer_factor)

    # 2. Safety Cap
    max_transit = int(total_slots * max_transit_ratio)
    transit_count = min(transit_floor, max_transit)

    # 3. Final Static Cache Size
    static_count = max(0, total_slots - transit_count)

    return static_count, transit_count
