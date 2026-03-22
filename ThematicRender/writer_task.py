import multiprocessing
from multiprocessing import current_process
from typing import Optional

import rasterio
import rasterio.windows
import setproctitle
from ThematicRender.engine_resources import JobContextStore
from ThematicRender.ipc_packets import WriterPacket, Op, Envelope, JobDonePacket, ErrorPacket, \
    send_error, TileWrittenPacket
from ThematicRender.utils import window_from_rect
from ThematicRender.worker_context_base import close_worker_ctx, sync_ctx_for_packet
from ThematicRender.worker_contexts import WriterContext


def load_writer_job_ctx(job_id: str, shm_store: JobContextStore) -> WriterContext:
    """Load the writer context for a specific job from shared job storage."""
    section = "WRITER - load job ctx"
    try:
        return shm_store.get_writer_context(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"{section} Failed to load WriterContext for job '{job_id}': {exc}"
        ) from exc


def writer_loop(write_q, status_q, shm_name: str, out_pool) -> None:
    """
    writer  loop.

    Behavior:
    - Discard stale packets whose packet.job_id does not match the SHM header job_id.
    - Reload local context when packet.job_id matches SHM, but local context (ctx) is stale.
    - write out tile
    - Close and flush on JOB_DONE / JOB_CANCEL.
    """
    section = "WRITER"
    shm_store = JobContextStore(name=shm_name)
    ctx: Optional[WriterContext] = None
    setproctitle.setproctitle(multiprocessing.current_process().name)

    try:
        while True:
            envelope: Envelope = write_q.get()

            match envelope.op:
                case Op.WRITE_TILE:
                    packet: WriterPacket = envelope.payload
                    try:
                        old_ctx_id = id(ctx)
                        ctx = sync_ctx_for_packet(
                            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                            load_ctx=load_writer_job_ctx, err_prefix=section, )

                        if ctx is None: continue  # ignore stale packet

                        # Write out the tile
                        write_task(packet=packet, ctx=ctx, out_pool=out_pool)

                        payload = TileWrittenPacket(job_id=packet.job_id, tile_id=packet.tile_id)
                        status_q.put(Envelope(op=Op.TILE_WRITTEN, payload=payload))

                    except MemoryError as exc:
                        payload = ErrorPacket(packet.job_id, -1, section, str(exc))
                        send_error(status_q, payload)

                case Op.JOB_DONE | Op.JOB_CANCEL:
                    section = "WRITER - DONE/CANCEL"
                    packet: JobDonePacket = envelope.payload
                    try:
                        # Only handle if this is for our current local context
                        if ctx is not None and ctx.matches_job_id(packet.job_id):
                            ctx.close_local_resources()

                            if envelope.op == Op.JOB_DONE:
                                status_q.put(Envelope(op=Op.TILES_FINALIZED, payload=packet.job_id))
                            ctx = None  # Reset state
                    except MemoryError as exc:
                        payload = ErrorPacket(
                            job_id=packet.job_id, tile_id=-1, stage=section,
                            message=f"{section} - Failed to finalize writer output for job '"
                                    f"{packet.job_id}': "
                                    f"{exc}"
                        )
                        send_error(status_q, payload)

                case Op.SHUTDOWN:
                    print(f"[{section}] Shutting down")
                    break

                case _:
                    payload = ErrorPacket("-1", -1, section, f"Unknown OpCode: {envelope.op!r}")
                    send_error(status_q, payload)
    except MemoryError as e:
        print("Writer error")
    #finally:
    #    close_worker_ctx(ctx)


def write_task(*, packet: WriterPacket, ctx: WriterContext, out_pool) -> None:
    """Write a rendered tile to disk and release transient output resources."""
    window = window_from_rect(packet.window_rect)
    local_window = rasterio.windows.Window(
        col_off=int(window.col_off) - int(ctx.write_offset_col),
        row_off=int(window.row_off) - int(ctx.write_offset_row), width=int(window.width),
        height=int(window.height), )

    if packet.img_block is None:
        raise ValueError("Packet img is empty.")

    try:
        ctx.dst.write(packet.img_block, window=local_window)
        out_pool.release(packet.out_ref.slot_id)
    except MemoryError:
        print ("error")
    #finally:
    #    if packet.out_ref is not None:
    #        out_pool.release(packet.out_ref.slot_id)
