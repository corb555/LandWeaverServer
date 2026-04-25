from dataclasses import dataclass
from queue import Empty
from typing import Iterable, Dict, List, Callable, Optional, TypeAlias, Tuple

import rasterio
from rasterio.windows import Window

from landweaverserver.common.ipc_packets import (RenderPacket, SourceBlockRef, BlockReadPacket,
                                                 WindowRect, Envelope, Op)
from landweaverserver.common.keys import SourceKey
from landweaverserver.pipeline.io_manager import get_read_geometry
from landweaverserver.pipeline.job_control import JobManifest
from landweaverserver.pipeline.pipeline_runtime import PipelineRuntime

EnvelopeHandler: TypeAlias = Callable[[Envelope], None]


@dataclass(slots=True)
class DispatchResult:
    """Result of attempting to dispatch a single tile.

    This is the dispatcher's handoff object back to the orchestrator. A tile
    may require one or more source-block reads before it can be rendered.

    There are two main cases:

    1. Cache miss path
       - ``read_packets`` contains one or more ``LOAD_BLOCK`` envelopes
       - ``render_packet`` is ``None`` until all required source blocks arrive

    2. Cache hit / immediate render path
       - ``read_packets`` is empty
       - ``render_packet`` is ready immediately because every source block was
         already present in the shared-memory cache

    Args:
        tile_id: Tile identifier assigned by enumerating the current window list.
            ``None`` means no more tiles are available.
        read_packets: Reader work packets required before the tile can render.
        render_packet: Fully assembled render packet when the tile is ready
            immediately, otherwise ``None``.
    """

    tile_id: Optional[int]
    read_packets: List[Envelope]
    render_packet: Optional[RenderPacket]


class TileDispatcher:
    """Dispatch tiles into the read/render pipeline.

    A render tile depends on blocks from every configured source. The dispatcher
    is responsible for:

    - walking the tile/window list for the active job
    - deciding whether each required source block is already cached
    - emitting reader requests for cache misses
    - tracking per-tile pending source-block counts
    - creating a ``RenderPacket`` once a tile has all required inputs
    - releasing source-block slot references after a tile is written

    This is the orchestrator's tile-level state machine for
    "what inputs are still needed before tile X can render?"

    Attributes:
        runtime: Pipeline runtime containing worker queues and the shared-memory
            slot registry.
        max_tiles_in_flight: Maximum number of tiles to prime into the pipeline
            before the steady-state event loop takes over.
        unk_block_read: Count of late/unknown block completions whose tile is no
            longer tracked. This is mainly diagnostic.
        active_tiles: Per-tile tracking table keyed by tile_id. Each entry keeps
            the tile window, block map, and number of source blocks still pending.
        current_tile_iterator: Enumerator over the active job's window list.
        current_job_manifest: Manifest for the active job, or ``None`` when idle.
    """

    def __init__(
            self, *, runtime: "PipelineRuntime", max_in_flight: int = 10, ) -> None:
        """Initialize the tile dispatcher.

        Args:
            runtime: Active pipeline runtime used for queues and cache/slot
                management.
            max_in_flight: Number of tiles to pre-dispatch when a new job starts.
        """
        self.runtime = runtime
        self.max_tiles_in_flight = max_in_flight

        # Diagnostic counter for block completions received after the tile has
        # already been forgotten or aborted.
        self.unk_block_read = 0

        # Active per-tile bookkeeping for the current job.
        self.active_tiles: Dict[int, dict] = {}

        # State for the currently active job.
        self.current_tile_iterator: Optional[Iterable[Tuple[int, Window]]] = None
        self.current_job_manifest: Optional["JobManifest"] = None

    def initialize_job(
            self, job_manifest: "JobManifest", win_list: List[rasterio.windows.Window], ) -> None:
        """Reset dispatcher state for a newly started job.

        This clears any prior active-tile bookkeeping, installs the new manifest,
        resets the tile iterator, and best-effort drains stale worker-input
        queues before the next job begins.

        Args:
            job_manifest: Manifest describing the active render job.
            win_list: Ordered list of output windows/tiles to process.
        """
        print(f"[Dispatcher] Job Initialized with {len(win_list)} tiles.")
        self.current_job_manifest = job_manifest
        self.active_tiles.clear()
        self.current_tile_iterator = enumerate(win_list)

        # Best-effort cleanup of stale read/render work from the previous job.
        self.flush_queues()

    def get_priming_list(self, job_id: str) -> List[DispatchResult]:
        """Prime the pipeline with up to ``max_tiles_in_flight`` tiles.

        This is used immediately after job startup to create the initial burst of
        reader and renderer work. After priming, the orchestrator's main loop
        continues the pipeline incrementally as blocks load and tiles complete.

        Args:
            job_id: Active job identifier.

        Returns:
            Dispatch results for the initial batch of tiles. Each result may
            contain reader packets, an immediate render packet, or both/neither
            depending on cache state.
        """
        candidates: List[DispatchResult] = []

        for _ in range(self.max_tiles_in_flight):
            result = self.dispatch_next_tile(job_id)
            if result.tile_id is None:
                break
            candidates.append(result)

        return candidates

    def dispatch_next_tile(self, job_id: str) -> DispatchResult:
        """Dispatch the next tile from the active iterator.

        For each source required by the active job, this method asks the slot
        registry for a slot. If the block is already cached, no reader request is
        emitted for that source. Otherwise, a ``LOAD_BLOCK`` request is generated.

        The tile is then entered into ``active_tiles`` with a pending block count.
        If every source block was already cached, the tile can be rendered
        immediately and a ``RenderPacket`` is returned.

        Args:
            job_id: Active job identifier.

        Returns:
            A ``DispatchResult`` describing the next work to issue. If no more
            tiles are available, returns a result with ``tile_id=None``.
        """
        if self.current_tile_iterator is None or self.current_job_manifest is None:
            return DispatchResult(tile_id=None, read_packets=[], render_packet=None)

        try:
            tile_id, window = next(self.current_tile_iterator)
        except StopIteration:
            return DispatchResult(tile_id=None, read_packets=[], render_packet=None)

        block_table: Dict[SourceKey, SourceBlockRef] = {}
        read_requests: List[Envelope] = []
        pending_block_count = 0
        manifest = self.current_job_manifest

        for source_id in manifest.resources.sources:
            # Acquire or reuse a slot for this source/window pair.
            slot_id, is_cached = self.runtime.registry.get_or_allocate(source_id, window)

            halo = manifest.render_cfg.get_halo_for_source(source_id)
            meta = manifest.source_metadata[source_id]
            geom = get_read_geometry(window, halo, meta["width"], meta["height"])

            block_table[source_id] = SourceBlockRef(
                slot_id=slot_id, data_h_w=geom.full_h_w, inner_slices=geom.inner_slices, )

            # Cache hit: no reader work needed for this source.
            if is_cached:
                continue

            pending_block_count += 1
            block_req = BlockReadPacket(
                job_id=job_id, source_id=source_id, tile_id=tile_id, target_slot_id=slot_id,
                window_rect=self.rect_from_window(window), halo=halo, )
            read_requests.append(Envelope(op=Op.LOAD_BLOCK, payload=block_req))

        # Register the tile before returning any work so later block completions
        # can be reconciled correctly.
        self.active_tiles[tile_id] = {
            "pending_blocks": pending_block_count, "block_map": block_table, "window": window,
            "read_duration": 0.0,
        }

        # Fast path: all required source blocks were already cached, so the tile
        # is render-ready immediately.
        if pending_block_count == 0:
            return DispatchResult(
                tile_id=tile_id, read_packets=[], render_packet=RenderPacket(
                    job_id=job_id, tile_id=tile_id, window_rect=self.rect_from_window(window),
                    block_map=block_table, read_duration=0.0, ), )

        return DispatchResult(
            tile_id=tile_id, read_packets=read_requests, render_packet=None, )

    def get_cached_tile_render_packet(
            self, job_id: str, tile_id: int, ) -> Optional[RenderPacket]:
        """Return a render packet if a tracked tile has no pending reads.

        This supports the fast path where all source blocks were already cached
        when the tile was dispatched.

        Args:
            job_id: Active job identifier.
            tile_id: Tile to inspect.

        Returns:
            A ``RenderPacket`` if the tile is fully ready, else ``None``.
        """
        tile = self.active_tiles.get(tile_id)
        if tile is None:
            return None

        if tile["pending_blocks"] != 0:
            return None

        return RenderPacket(
            job_id=job_id, tile_id=tile_id, window_rect=self.rect_from_window(tile["window"]),
            block_map=tile["block_map"], read_duration=tile["read_duration"], )

    def on_source_block_loaded(
            self, job_id: str, tile_id: int, read_duration: float = 0.0, ) -> Optional[
        RenderPacket]:
        """Record completion of one source-block load for a tile.

        Each source block required by a tile decrements the tile's pending count
        when it arrives. Once that count reaches zero, the tile is ready to be
        rendered and a ``RenderPacket`` is returned.

        Args:
            job_id: Active job identifier.
            tile_id: Tile whose source block has finished loading.
            read_duration: Time spent loading this source block. Currently
                accepted for future accounting, though not accumulated here.

        Returns:
            A ``RenderPacket`` if this completion makes the tile render-ready,
            otherwise ``None``.

        Raises:
            ValueError: If the pending block count underflows, indicating an
                internal accounting error or duplicate completion event.
        """
        tile = self.active_tiles.get(tile_id)
        if tile is None:
            self.unk_block_read += 1
            return None

        # Placeholder for future aggregated read timing.
        # tile["read_duration"] += read_duration
        tile["pending_blocks"] -= 1

        if tile["pending_blocks"] < 0:
            raise ValueError(
                f"Tile {tile_id} pending_blocks underflow for job '{job_id}'"
            )

        if tile["pending_blocks"] != 0:
            return None

        return RenderPacket(
            job_id=job_id, tile_id=tile_id, window_rect=self.rect_from_window(tile["window"]),
            block_map=tile["block_map"], read_duration=0, )

    def on_tile_written(self, tile_id: int) -> bool:
        """Release all source-block slot references for a completed tile.

        Once the writer has fully committed a tile to output, the dispatcher
        releases all source-block slot references associated with that tile so
        the shared-memory registry can reuse them.

        Args:
            tile_id: Tile that has been fully written.

        Returns:
            ``True`` if the tile existed and resources were released, else
            ``False``.
        """
        finished_tile = self.active_tiles.pop(tile_id, None)
        if finished_tile is None:
            return False

        for source_key, ref in finished_tile["block_map"].items():
            self.runtime.registry.release(source_key, ref.slot_id)

        return True

    def flush_queues(self) -> None:
        """Best-effort drain of worker-input queues before a new job starts.

        This is defensive cleanup to reduce the chance of stale read/render work
        from a previous job remaining in worker input queues.
        """
        for q in [self.runtime.reader_q, self.runtime.worker_q]:
            while not q.empty():
                try:
                    q.get_nowait()
                except Empty:
                    break

    def abort_job(self) -> None:
        """Release all active tile resources and clear dispatcher job state.

        This is used when a job is canceled or aborted after tiles have already
        been dispatched. Every tracked source-block slot reference is released,
        and all current-job dispatcher state is cleared.
        """
        for tile in self.active_tiles.values():
            for source_key, ref in tile["block_map"].items():
                self.runtime.registry.release(source_key, ref.slot_id)

        self.active_tiles.clear()
        self.current_tile_iterator = None
        self.current_job_manifest = None

    @staticmethod
    def rect_from_window(w: Window) -> WindowRect:
        """Convert a Rasterio window into an integer ``WindowRect``.

        Args:
            w: Rasterio window.

        Returns:
            Tuple of ``(col_off, row_off, width, height)`` as integers.
        """
        return int(w.col_off), int(w.row_off), int(w.width), int(w.height)
