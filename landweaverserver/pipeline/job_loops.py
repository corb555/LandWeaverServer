
from __future__ import annotations

from collections.abc import Callable
import cProfile
import multiprocessing
import signal
import traceback
from typing import Any, Optional, TypeVar

import setproctitle

from landweaverserver.common.ipc_packets import (
    BlockLoadedPacket,
    Envelope,
    JobDonePacket,
    Op,
    TileWrittenPacket,
    WriterPacket,
    send_cancel_error,
    send_fatal_error,
)
from landweaverserver.pipeline.job_context import JobContextStore
from landweaverserver.pipeline.worker_context_base import (
    close_worker_ctx,
    sync_ctx_for_packet,
)
from landweaverserver.pipeline.worker_contexts import (
    ReaderContext,
    WorkerContext,
    WriterContext,
)
from landweaverserver.render.task_routines import (
    RenderWorkspace,
    read_task,
    render_task,
    write_task,
)


PROFILE = False

T_CONTEXT = TypeVar("T_CONTEXT")


def process_setup(shm_name: str) -> JobContextStore:
    """Perform common child-process initialization.

    Args:
        shm_name: Shared-memory name for the job-context store.

    Returns:
        Attached job-context store for the current process.
    """
    setproctitle.setproctitle(multiprocessing.current_process().name)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    return JobContextStore(name=shm_name)


def _create_profiler() -> Optional[cProfile.Profile]:
    """Create a profiler when profiling is enabled.

    Returns:
        Profiler instance when profiling is enabled, else ``None``.
    """
    return cProfile.Profile() if PROFILE else None


def _enable_profiler(profiler: Optional[cProfile.Profile]) -> None:
    """Enable profiling if active.

    Args:
        profiler: Optional profiler instance.
    """
    if profiler is not None:
        profiler.enable()


def _disable_profiler(profiler: Optional[cProfile.Profile]) -> None:
    """Disable profiling if active.

    Args:
        profiler: Optional profiler instance.
    """
    if profiler is not None:
        profiler.disable()


def write_profile(
    profiler: Optional[cProfile.Profile],
    worker_id: str,
    job_id: str,
) -> None:
    """Persist profile output for a worker.

    Args:
        profiler: Optional profiler instance.
        worker_id: Identifier for the worker/process.
        job_id: Current job identifier.
    """
    if profiler is None:
        return

    filename = f"profile_w{worker_id}_j{job_id}.pstat"
    profiler.dump_stats(filename)


def _sync_packet_context(
    *,
    ctx: Optional[T_CONTEXT],
    packet: Any,
    shm_store: JobContextStore,
    load_ctx: Callable[[str, JobContextStore], T_CONTEXT],
    err_prefix: str,
) -> Optional[T_CONTEXT]:
    """Synchronize local worker context to the packet job.

    This wrapper centralizes the common stale-packet / job-context refresh
    behavior used by Reader, Worker, and Writer loops.

    Args:
        ctx: Existing worker-local context, if any.
        packet: Packet carrying ``job_id``.
        shm_store: Shared job-context store.
        load_ctx: Stage-specific context loader.
        err_prefix: Human-readable stage label.

    Returns:
        Updated context, or ``None`` when the packet is stale and should be
        ignored.
    """
    return sync_ctx_for_packet(
        ctx=ctx,
        packet_job_id=packet.job_id,
        shm_store=shm_store,
        load_ctx=load_ctx,
        err_prefix=err_prefix,
    )


def _route_stage_error(
    *,
    status_q,
    packet: Any,
    section: str,
    exc: Exception,
    cancel_exceptions: tuple[type[BaseException], ...],
) -> bool:
    """Route a stage exception to cancel or fatal handling.

    Args:
        status_q: Status queue used to emit error envelopes.
        packet: Packet associated with the failure.
        section: Human-readable stage/section label.
        exc: Raised exception.
        cancel_exceptions: Exceptions treated as recoverable job-scoped
            failures.

    Returns:
        ``True`` if the worker loop should continue, ``False`` if the worker
        should terminate.
    """
    if isinstance(exc, cancel_exceptions):
        send_cancel_error(
            status_q=status_q,
            packet=packet,
            section=section,
            message=str(exc),
        )
        return True

    traceback.print_exc()
    send_fatal_error(
        status_q=status_q,
        packet=packet,
        section=section,
        exc=exc,
        include_traceback=False,
    )
    return False


def _handle_shutdown(ctx: Any) -> None:
    """Perform standard shutdown cleanup for a worker-local context.

    Args:
        ctx: Worker-local context, if any.
    """
    if ctx is not None:
        ctx.close_local_resources()


def reader_loop(reader_q, status_q, shm_name: str, pool_map) -> None:
    """Reader worker main loop.

    The Reader consumes ``LOAD_BLOCK`` packets, fills Pipeline-managed shared
    memory blocks, and emits ``BLOCK_LOADED`` when done.

    Control messages:
    - ``SHUTDOWN``: close resources and exit
    - ``JOB_CANCEL``: explicit no-op
    - ``LOAD_BLOCK``: perform stage work

    Args:
        reader_q: Reader input queue.
        status_q: Status queue for completion and error reporting.
        shm_name: Shared-memory name for the job-context store.
        pool_map: Mapping of source id to Pipeline-managed input pools.
    """
    section = f"READER {shm_name}"
    shm_store = process_setup(shm_name)
    profiler = _create_profiler()
    ctx: Optional[ReaderContext] = None
    dbg_job_id = "0"

    while True:
        envelope: Envelope = reader_q.get()
        packet = envelope.payload

        match envelope.op:
            case Op.SHUTDOWN:
                _handle_shutdown(ctx)
                break

            case Op.JOB_CANCEL:
                # Reader has no active cleanup action for cancel; stale packets
                # are rejected through job-id synchronization.
                continue

            case Op.LOAD_BLOCK:
                _enable_profiler(profiler)
                try:
                    ctx = _sync_packet_context(
                        ctx=ctx,
                        packet=packet,
                        shm_store=shm_store,
                        load_ctx=load_reader_job_ctx,
                        err_prefix=section,
                    )
                    if ctx is None:
                        continue

                    dbg_job_id = packet.job_id

                    # Pipeline owns pools and slot lifecycle; the render layer
                    #  receives the views to fill.
                    pool = pool_map[packet.source_id]
                    data_view = pool.data_buf[packet.target_slot_id]
                    mask_view = pool.mask_buf[packet.target_slot_id]

                    duration = read_task(packet, ctx.io, data_view, mask_view)

                    status_q.put(
                        Envelope(
                            op=Op.BLOCK_LOADED,
                            payload=BlockLoadedPacket(
                                job_id=packet.job_id,
                                tile_id=packet.tile_id,
                                source_id=packet.source_id,
                                read_duration=duration,
                            ),
                        )
                    )
                except Exception as exc:
                    should_continue = _route_stage_error(
                        status_q=status_q,
                        packet=packet,
                        section=section,
                        exc=exc,
                        cancel_exceptions=(ValueError, FileNotFoundError, OSError),
                    )
                    if not should_continue:
                        break
                finally:
                    _disable_profiler(profiler)

            case _:
                # Unknown messages are explicit no-ops
                continue

    close_worker_ctx(ctx)
    write_profile(profiler, shm_name, dbg_job_id)

def worker_loop(worker_q, writer_q, status_q, shm_name: str, out_pool, pool_map) -> None:
    """Renderer worker main loop.

    The Worker consumes ``RENDER_TILE`` packets, renders into a Pipeline-managed
    output buffer, and forwards the resulting ``WRITE_TILE`` packet directly to
    the writer queue.

    Control messages:
    - ``SHUTDOWN``: close resources and exit
    - ``JOB_CANCEL``: explicit no-op
    - ``RENDER_TILE``: perform stage work

    Args:
        worker_q: Worker input queue.
        writer_q: Writer queue for downstream output packets.
        status_q: Status queue for error reporting.
        shm_name: Shared-memory name for the job-context store.
        out_pool: Pipeline-managed output shared-memory pool.
        pool_map: Mapping of source id to Pipeline-managed input pools.
    """
    section = "WORKER"
    shm_store = process_setup(shm_name)
    profiler = _create_profiler()
    ctx: Optional[WorkerContext] = None
    workspace = RenderWorkspace()
    dbg_job_id = "0"

    while True:
        envelope: Envelope = worker_q.get()
        packet = envelope.payload

        match envelope.op:
            case Op.SHUTDOWN:
                _handle_shutdown(ctx)
                break

            case Op.JOB_CANCEL:
                # Worker does not own cancel policy. It returns to the queue and
                # stale packets are rejected through the active job context.
                continue

            case Op.RENDER_TILE:
                _enable_profiler(profiler)
                try:
                    ctx = _sync_packet_context(
                        ctx=ctx,
                        packet=packet,
                        shm_store=shm_store,
                        load_ctx=load_worker_job_ctx,
                        err_prefix=section,
                    )
                    if ctx is None:
                        continue

                    dbg_job_id = packet.job_id

                    # Refresh render-side derived state if the job context
                    # changed.
                    workspace.sync_to_context(ctx)

                    result = render_task(
                        packet=packet,
                        ctx=ctx,
                        workspace=workspace,
                        out_pool=out_pool,
                        pool_map=pool_map,
                    )
                    writer_q.put(Envelope(op=Op.WRITE_TILE, payload=result))

                except Exception as exc:
                    should_continue = _route_stage_error(
                        status_q=status_q,
                        packet=packet,
                        section="RENDER",
                        exc=exc,
                        cancel_exceptions=(ValueError, OSError, KeyError),
                    )
                    if not should_continue:
                        break
                finally:
                    _disable_profiler(profiler)

            case _:
                continue

    write_profile(profiler, shm_name, dbg_job_id)


def writer_loop(write_q, status_q, shm_name: str, out_pool) -> None:
    """Writer worker main loop.

    The Writer consumes ``WRITE_TILE`` packets, writes rendered tiles to the
    output file, and emits ``TILE_WRITTEN``. It also handles finalization and
    cancellation.

    Control messages:
    - ``SHUTDOWN``: close resources, unlink active temp output, and exit
    - ``JOB_CANCEL``: close current file, delete partial output, emit
      ``WRITER_ABORTED``
    - ``JOB_DONE``: close current file and emit ``TILES_FINALIZED``
    - ``WRITE_TILE``: perform stage work

    Args:
        write_q: Writer input queue.
        status_q: Status queue for completion and error reporting.
        shm_name: Shared-memory name for the job-context store.
        out_pool: Pipeline-managed output shared-memory pool.
    """
    section = "WRITER"
    shm_store = process_setup(shm_name)
    profiler = _create_profiler()
    ctx: Optional[WriterContext] = None
    dbg_job_id = "0"
    packet: Optional[Any] = None

    while True:
        envelope: Envelope = write_q.get()
        packet = envelope.payload

        match envelope.op:
            case Op.SHUTDOWN:
                if ctx is not None:
                    ctx.close_local_resources()
                    if ctx.output_path.exists():
                        ctx.output_path.unlink()
                break

            case Op.JOB_CANCEL | Op.JOB_DONE:
                done_packet: JobDonePacket = packet
                is_cancel = envelope.op == Op.JOB_CANCEL

                if ctx is not None and ctx.matches_job_id(done_packet.job_id):
                    ctx.close_local_resources()

                    if is_cancel:
                        try:
                            if ctx.output_path.exists():
                                ctx.output_path.unlink()
                        except Exception as exc:
                            print(f"⚠️ [Writer] Failed to delete cancelled file: {exc}")

                        status_q.put(
                            Envelope(op=Op.WRITER_ABORTED, payload=done_packet.job_id)
                        )
                    else:
                        status_q.put(
                            Envelope(op=Op.TILES_FINALIZED, payload=done_packet.job_id)
                        )

                    ctx = None

                continue

            case Op.WRITE_TILE:
                _enable_profiler(profiler)
                try:
                    tile_packet: WriterPacket = packet
                    ctx = _sync_packet_context(
                        ctx=ctx,
                        packet=tile_packet,
                        shm_store=shm_store,
                        load_ctx=load_writer_job_ctx,
                        err_prefix=section,
                    )
                    if ctx is None:
                        continue

                    dbg_job_id = tile_packet.job_id

                    write_task(packet=tile_packet, ctx=ctx, out_pool=out_pool)

                    status_q.put(
                        Envelope(
                            op=Op.TILE_WRITTEN,
                            payload=TileWrittenPacket(
                                job_id=tile_packet.job_id,
                                tile_id=tile_packet.tile_id,
                            ),
                        )
                    )

                except Exception as exc:
                    should_continue = _route_stage_error(
                        status_q=status_q,
                        packet=packet,
                        section=section,
                        exc=exc,
                        cancel_exceptions=(ValueError, FileNotFoundError, OSError),
                    )
                    if not should_continue:
                        break
                finally:
                    _disable_profiler(profiler)

            case _:
                continue

    write_profile(profiler, shm_name, dbg_job_id)


def _load_job_ctx(
    job_id: str,
    shm_store: JobContextStore,
    *,
    role: str,
    loader: Callable[[str], T_CONTEXT],
) -> T_CONTEXT:
    """Load a job-specific context from shared job storage.

    Args:
        job_id: Unique job identifier.
        shm_store: Shared job context store.
        role: Human-readable worker role used in error messages.
        loader: Bound context-loader callable from ``JobContextStore``.

    Returns:
        The requested job context.

    Raises:
        RuntimeError: If the context cannot be loaded.
    """
    try:
        return loader(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"[{role}] Failed to load job context for job '{job_id}': {exc}"
        ) from exc


def load_reader_job_ctx(job_id: str, shm_store: JobContextStore) -> ReaderContext:
    """Load reader context for a job.

    Args:
        job_id: Unique job identifier.
        shm_store: Shared job context store.

    Returns:
        Reader context for the requested job.
    """
    return _load_job_ctx(
        job_id,
        shm_store,
        role="READER",
        loader=shm_store.get_reader_context,
    )


def load_worker_job_ctx(job_id: str, shm_store: JobContextStore) -> WorkerContext:
    """Load worker context for a job.

    Args:
        job_id: Unique job identifier.
        shm_store: Shared job context store.

    Returns:
        Worker context for the requested job.
    """
    return _load_job_ctx(
        job_id,
        shm_store,
        role="WORKER",
        loader=shm_store.get_worker_context,
    )


def load_writer_job_ctx(job_id: str, shm_store: JobContextStore) -> WriterContext:
    """Load writer context for a job.

    Args:
        job_id: Unique job identifier.
        shm_store: Shared job context store.

    Returns:
        Writer context for the requested job.
    """
    return _load_job_ctx(
        job_id,
        shm_store,
        role="WRITER",
        loader=shm_store.get_writer_context,
    )

