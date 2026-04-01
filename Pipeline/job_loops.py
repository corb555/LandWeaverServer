from collections.abc import Callable
import multiprocessing
import signal
import traceback
from typing import Optional, TypeVar

import setproctitle

from Common.ipc_packets import WriterPacket, JobDonePacket, TileWrittenPacket, Op, Envelope, \
    BlockLoadedPacket, send_cancel_error, send_fatal_error
from Pipeline.engine_resources import JobContextStore
from Pipeline.worker_context_base import (close_worker_ctx, sync_ctx_for_packet, )
from Pipeline.worker_contexts import WriterContext, WorkerContext, ReaderContext
from Render.task_routines import write_task, read_task, render_task, RenderWorkspace


# job_loops.py
def process_setup(shm_name) -> JobContextStore:
    # Create a unique process name
    setproctitle.setproctitle(multiprocessing.current_process().name)

    # Ignore keyboard interrupts.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    #  shared memory
    return JobContextStore(name=shm_name)


def reader_loop(reader_q, status_q, shm_name: str, pool_map) -> None:
    section = "READER"
    shm_store = process_setup(shm_name)
    ctx: Optional[ReaderContext] = None

    while True:
        envelope: Envelope = reader_q.get()
        if envelope.op == Op.SHUTDOWN: break

        if envelope.op == Op.LOAD_BLOCK:
            packet = envelope.payload

            # 1. System Sync (Global state check)
            ctx = sync_ctx_for_packet(
                ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                load_ctx=load_reader_job_ctx, err_prefix=section
            )
            if ctx is None: continue

            try:
                # 2. Resource Management
                # The Loop is responsible for the Pool and SHM logic
                pool = pool_map[packet.driver_id]
                data_view = pool.data_buf[packet.target_slot_id]
                mask_view = pool.mask_buf[packet.target_slot_id]

                # 3. Execution
                # We pass the pure NumPy views to the render layer
                duration = read_task(packet, ctx.io, data_view, mask_view)

                # 4. Send DONE
                status_q.put(
                    Envelope(
                        op=Op.BLOCK_LOADED, payload=BlockLoadedPacket(
                            job_id=packet.job_id, tile_id=packet.tile_id,
                            driver_id=packet.driver_id, read_duration=duration
                        )
                    )
                )
            except (ValueError, FileNotFoundError, OSError) as exc:
                send_cancel_error(
                    status_q=status_q, packet=packet, section=section, message=str(exc)
                    )
            except Exception as exc:
                traceback.print_exc()
                send_fatal_error(
                    status_q=status_q, packet=packet, section=section, exc=exc,
                    include_traceback=False
                    )
                break  # Exit loop, process dies
            finally:
                close_worker_ctx(ctx)


def worker_loop(worker_q, writer_q, status_q, shm_name, out_pool, pool_map):
    shm_store = process_setup(shm_name)
    ctx: Optional[WorkerContext] = None
    workspace = RenderWorkspace()
    section = "RENDER"

    while True:
        envelope = worker_q.get()
        packet = envelope.payload
        try:
            match envelope.op:
                case Op.RENDER_TILE:
                    try:
                        ctx = sync_ctx_for_packet(
                            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                            load_ctx=load_worker_job_ctx, err_prefix="WORKER"
                        )
                        if ctx is None: continue

                        # ENGINE TRANSLATION: Rebuild math engines if config changed
                        workspace.sync_to_context(ctx)
                        section = "task"
                        result = render_task(
                            packet=packet, ctx=ctx, workspace=workspace, out_pool=out_pool,
                            pool_map=pool_map
                        )
                        writer_q.put(Envelope(op=Op.WRITE_TILE, payload=result))
                    except (ValueError, OSError, KeyError) as exc:
                        send_cancel_error(
                            status_q=status_q, packet=packet, section=section, message=str(exc)
                            )
                    except Exception as exc:
                        traceback.print_exc()
                        send_fatal_error(
                            status_q=status_q, packet=packet, section=section, exc=exc,
                            include_traceback=False
                            )
                        break
                case Op.SHUTDOWN:
                    if ctx: ctx.close_local_resources()
                    break
                case Op.JOB_CANCEL:
                    # Passive workers just return to get()
                    continue
        except ValueError as exc:
            send_cancel_error(status_q=status_q, packet=packet, section=section, message=str(exc))
        except Exception as exc:
            traceback.print_exc()
            send_fatal_error(
                status_q=status_q, packet=packet, section=section, exc=exc, include_traceback=False
                )
            break


def writer_loop(write_q, status_q, shm_name: str, out_pool) -> None:
    section = "WRITER"
    shm_store = process_setup(shm_name)
    ctx: Optional[WriterContext] = None

    try:
        while True:
            envelope: Envelope = write_q.get()

            match envelope.op:
                case Op.WRITE_TILE:
                    try:
                        packet: WriterPacket = envelope.payload
                        ctx = sync_ctx_for_packet(
                            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                            load_ctx=load_writer_job_ctx, err_prefix=section, )

                        if ctx is None: continue  # ignore stale packet

                        # Write out the tile
                        write_task(packet=packet, ctx=ctx, out_pool=out_pool)

                        payload = TileWrittenPacket(job_id=packet.job_id, tile_id=packet.tile_id)
                        status_q.put(Envelope(op=Op.TILE_WRITTEN, payload=payload))

                    except (ValueError, FileNotFoundError, OSError) as exc:
                        send_cancel_error(
                            status_q=status_q, packet=packet, section=section, message=str(exc)
                            )
                    except Exception as exc:
                        traceback.print_exc()
                        send_fatal_error(
                            status_q=status_q, packet=packet, section=section, exc=exc,
                            include_traceback=False
                            )
                        break
                case Op.JOB_DONE | Op.JOB_CANCEL:
                    is_cancel = (envelope.op == Op.JOB_CANCEL)
                    packet: JobDonePacket = envelope.payload

                    if ctx is not None and ctx.matches_job_id(packet.job_id):
                        # 1. Close the file handle
                        ctx.close_local_resources()

                        # 2. If CANCELLED, delete the partial file
                        if is_cancel:
                            try:
                                if ctx.output_path.exists():
                                    ctx.output_path.unlink()
                            except Exception as exc:
                                print(f"⚠️ [Writer] Failed to delete cancelled file: {exc}")

                            # 3. THE HANDSHAKE: Notify Orch that cleanup is done
                            status_q.put(Envelope(op=Op.WRITER_ABORTED, payload=packet.job_id))

                        elif envelope.op == Op.JOB_DONE:
                            # Notify Success
                            status_q.put(Envelope(op=Op.TILES_FINALIZED, payload=packet.job_id))

                        ctx = None

                case Op.SHUTDOWN:
                    if ctx:
                        ctx.close_local_resources()
                        # unlink on shutdown to prevent artifacts
                        if ctx.output_path.exists(): ctx.output_path.unlink()
                    break
    except Exception as exc:
        traceback.print_exc()
        send_fatal_error(
            status_q=status_q, packet=packet, section=section, exc=exc, include_traceback=False
            )


T_CONTEXT = TypeVar("T_CONTEXT")


def _load_job_ctx(
        job_id: str, shm_store: JobContextStore, *, role: str,
        loader: Callable[[str], T_CONTEXT], ) -> T_CONTEXT:
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
    return _load_job_ctx(
        job_id, shm_store, role="READER", loader=shm_store.get_reader_context, )


def load_worker_job_ctx(job_id: str, shm_store: JobContextStore) -> WorkerContext:
    return _load_job_ctx(
        job_id, shm_store, role="WORKER", loader=shm_store.get_worker_context, )


def load_writer_job_ctx(job_id: str, shm_store: JobContextStore) -> WriterContext:
    return _load_job_ctx(
        job_id, shm_store, role="WRITER", loader=shm_store.get_writer_context, )
