from dataclasses import dataclass
from enum import IntEnum
import traceback
from typing import Optional, Tuple, Dict, Any, TypeAlias

import numpy as np

from landweaverserver.common.keys import SourceKey

# ipc_packets.py

WindowRect: TypeAlias = tuple[int, int, int, int]

WKR_TIMEOUT = 7.0
ORCH_TIMEOUT = 10.0

SEV_FATAL = 0  # Fatal  error.  Shutting down.
SEV_CANCEL = 1  # Job Cancellation.
SEV_WARNING = 2  # Job continues with warning text sent to the client

# Shared Mem Special Job Ids
JOB_ID_SHUTTING_DOWN = "-3"
JOB_ID_JOB_CANCELLED = "-2"
JOB_ID_IDLE = "-1"


# Message Operations
class Op(IntEnum):
    JOB_REQUEST = 0  # Client -> Orch: New Job
    JOB_DONE = 1  # Orch -> Client: Job Done
    JOB_CANCEL = 2  # Orch -> Writer: Cancel
    LOAD_BLOCK = 3  # Orch -> Reader: Load Block
    BLOCK_LOADED = 4  # Reader -> Orch: Block loaded
    RENDER_TILE = 5  # Orch -> render: render Tile
    WRITE_TILE = 6  # Orch -> Writer: Write Tile
    TILE_WRITTEN = 7  # Writer -> Orch: Tile Written
    TILES_FINALIZED = 8  # Writer -> Orch:  Output Finalized
    WRITER_ABORTED = 9  # Writer -> Cancel is complete
    TELEMETRY = 10
    ERROR = 11  # Any -> Orch: Error occurred
    SHUTDOWN = 12  # Client -> Orch: SHutdown


@dataclass(frozen=True, slots=True)
class Envelope:
    """The standard container for all Queue communications."""
    op: Op
    payload: Any = None


@dataclass(frozen=True, slots=True)
class SourceBlockRef:
    slot_id: int
    data_h_w: Tuple[int, int]
    inner_slices: Optional[Tuple[slice, slice]] = None


@dataclass(frozen=True, slots=True)
class RenderPacket:
    job_id: str
    tile_id: int
    window_rect: WindowRect
    block_map: Dict[SourceKey, SourceBlockRef]  # All the blocks for this Tile
    read_duration: float = 0.0  # Sum of all  reads for this tile
    queued_at: float = 0.0  # When the coordinator put this in the queue


@dataclass(frozen=True, slots=True)
class WriterPacket:
    job_id: str
    tile_id: int
    window_rect: WindowRect
    refs: Dict[SourceKey, SourceBlockRef]
    img_block: np.ndarray
    out_ref: SourceBlockRef
    read_duration: float = 0.0  # Carried from WorkPacket
    render_duration: float = 0.0  # Time spent in actual math
    worker_idle_time: float = 0.0  # Time worker spent waiting for worker_q
    queued_at: float = 0.0  # When the renderer put this in the result queue


@dataclass(frozen=True, slots=True)
class TileWrittenPacket:
    job_id: str
    tile_id: int


@dataclass(frozen=True, slots=True)
class BlockReadPacket:
    job_id: str
    tile_id: int
    source_id: SourceKey
    window_rect: WindowRect
    target_slot_id: int
    halo: int = 0
    queued_at: float = 0.0  # When the coordinator put this in the queue


@dataclass(frozen=True, slots=True)
class BlockLoadedPacket:
    job_id: str
    tile_id: int
    source_id: SourceKey
    read_duration: float


@dataclass(frozen=True, slots=True)
class JobDonePacket:
    job_id: str


@dataclass(frozen=True, slots=True)
class ShutdownPacket:
    msg: str


@dataclass(frozen=True, slots=True)
class ErrorPacket:
    job_id: str
    tile_id: int
    section: str
    severity: int
    message: str


def send_error(q, payload: ErrorPacket):
    q.put(Envelope(op=Op.ERROR, payload=payload), timeout=1.0)


UNKNOWN_ID = "-1"


@dataclass(frozen=True)
class PacketIds:
    """Safe packet identifiers for error reporting."""

    job_id: str = UNKNOWN_ID
    tile_id: int = UNKNOWN_ID


def packet_ids(packet: Any) -> PacketIds:
    """Extract packet identifiers safely.

    Args:
        packet: IPC packet or payload object.

    Returns:
        Safe job and tile identifiers for logging/error reporting.
    """
    if packet is None:
        return PacketIds()

    return PacketIds(
        job_id=str(getattr(packet, "job_id", UNKNOWN_ID)),
        tile_id=int(getattr(packet, "tile_id", UNKNOWN_ID)), )


def send_cancel_error(
        *, status_q, packet: Any, section: str, message: str, ) -> None:
    """Send a recoverable job-level error.

    Args:
        status_q: Status queue.
        packet: Current packet or payload object.
        section: Logical section name.
        message: Human-readable error message.
    """
    ids = packet_ids(packet)
    payload = ErrorPacket(
        job_id=ids.job_id, tile_id=ids.tile_id, section=section, severity=SEV_CANCEL,
        message=message, )
    send_error(status_q, payload)


def send_fatal_error(
        *, status_q, packet: Any, section: str, exc: BaseException, include_traceback: bool = True,
        prefix: str = "CRITICAL", ) -> None:
    """Send a fatal process-level error.

    Args:
        status_q: Status queue.
        packet: Current packet or payload object.
        section: Logical section name.
        exc: Exception that triggered the failure.
        include_traceback: Whether to append traceback text.
        prefix: Leading message prefix.
    """
    message = f"{prefix}: {type(exc).__name__}: {exc}"
    ids = packet_ids(packet)

    if include_traceback:
        message = f"{message}\n{traceback.format_exc()}"

    payload = ErrorPacket(
        job_id=ids.job_id, tile_id=ids.tile_id, section=section, severity=SEV_FATAL,
        message=message, )
    send_error(status_q, payload)
