from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Tuple, Dict, Any, TypeAlias, List

import numpy as np
from ThematicRender.keys import DriverKey

# ipc_packets.py

WindowRect: TypeAlias = tuple[int, int, int, int]


@dataclass(frozen=True, slots=True)
class DriverBlockRef:
    slot_id: int
    data_h_w: Tuple[int, int]
    inner_slices: Optional[Tuple[slice, slice]] = None


# Message Operations
class Op(IntEnum):
    JOB_REQUEST = 0
    JOB_DONE = 1
    JOB_CANCEL = 2
    LOAD_BLOCK = 3
    BLOCK_LOADED = 4
    RENDER_TILE = 5
    WRITE_TILE = 6
    TILE_WRITTEN = 7
    TILES_FINALIZED = 8
    TELEMETRY = 9
    ERROR = 10
    SHUTDOWN = 11


@dataclass(frozen=True, slots=True)
class Envelope:
    """The standard container for all Queue communications."""
    op: Op
    payload: Any = None


@dataclass(frozen=True, slots=True)
class RenderPacket:
    job_id: str
    tile_id: int
    window_rect: WindowRect
    block_map: Dict[DriverKey, DriverBlockRef]  # All the blocks for this Tile
    read_duration: float = 0.0  # Sum of all driver reads for this tile
    queued_at: float = 0.0  # When the coordinator put this in the queue


@dataclass(frozen=True, slots=True)
class WriterPacket:
    job_id: str
    tile_id: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    img_block: np.ndarray
    out_ref: DriverBlockRef
    read_duration: float = 0.0  # Carried from WorkPacket
    render_duration: float = 0.0  # Time spent in actual math
    worker_idle_time: float = 0.0  # Time worker spent waiting for work_queue
    queued_at: float = 0.0  # When the renderer put this in the result queue


@dataclass(frozen=True, slots=True)
class TileWrittenPacket:
    job_id: str
    tile_id: int


@dataclass(frozen=True, slots=True)
class BlockReadPacket:
    job_id: str
    tile_id: int
    driver_id: DriverKey
    window_rect: WindowRect
    target_slot_id: int
    halo: int = 0
    queued_at: float = 0.0  # When the coordinator put this in the queue


@dataclass(frozen=True, slots=True)
class BlockLoadedPacket:
    job_id: str
    tile_id: int
    driver_id: DriverKey
    read_duration: float


@dataclass(frozen=True, slots=True)
class JobDonePacket:
    job_id: str


@dataclass(slots=True)
class DispatchResult:
    tile_id: Optional[int]
    read_packets: List[Envelope]
    render_packet: Optional[RenderPacket]

@dataclass(frozen=True, slots=True)
class ErrorPacket:
    job_id: str
    tile_id: int
    stage: str
    message: str


def send_error(q, payload: ErrorPacket):
    q.put(
        Envelope(
            op=Op.ERROR, payload=payload, )
    )
