# keys.py

from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Optional, Tuple, Set, FrozenSet, List, Dict, Protocol

import numpy as np

DEFAULT_BUFFER = "canvas"

SourceKey = str
SurfaceKey = str
FactorKey = str

DTYPE_ALIASES = {
    "uint8": np.uint8, "ubyte": np.uint8, "byte": np.uint8, "int16": np.int16, "uint16": np.uint16,
    "int32": np.int32, "uint32": np.uint32, "float32": np.float32, "float": np.float32,
    "float64": np.float64, "double": np.float64,
}


class FileKey(StrEnum):
    """Non-source file keys stored under `cfg['files']`."""

    OUTPUT = "output"
    RAMPS_YML = "ramps_yml"
    THEME_QML = "theme_qml"


@dataclass(frozen=True, slots=True)
class SourceRndrSpec:
    halo_px: int
    dtype: Any = "float32"


@dataclass(frozen=True, slots=True)
class SourceHWSpec:
    halo_px: int
    dtype: Any


@dataclass(frozen=True, slots=True)
class PipelineRequirements:
    factor_names: Set[str]
    surface_inputs: Set[Any]  # SurfaceKey


@dataclass(frozen=True, slots=True)
class SurfaceSpec:
    key: Any
    op: str
    desc: str
    files: FrozenSet[Any] = None
    source: Optional[Any] = None
    required_factors: Tuple[str, ...] = field(default_factory=tuple)
    input_factor: Optional[str] = None
    modifiers: Optional[List] = None


class ConfigView(Protocol):
    def factor_on(self, name: str, default: bool = False) -> bool: ...


@dataclass(frozen=True, slots=True)
class FactorSpec:
    name: str
    op: str
    sources: Tuple[str, ...] = field(default_factory=tuple)
    files: Tuple[str, ...] = field(default_factory=tuple)
    required_factors: Tuple[str, ...] = field(default_factory=tuple)
    noise_id: Optional[str] = None
    desc: str = ""
    categories: Dict[str, Any] = field(default_factory=dict)
    # some other parameters
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _BlendSpec:
    op: str
    desc: str = ""
    enabled: bool = True
    factor: Optional[str] = None
    mask_nm: Optional[str] = None
    input_surfaces: List[SurfaceKey] = field(default_factory=list)
    output_surface: Optional[SurfaceKey] = None
    buffer: str = DEFAULT_BUFFER  # default buffer
    merge_buffer: Optional[str] = None
    scale: float = 1.0
    contrast: float = 0.0
    bias: float = 0.0
    # some other Parameters
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NoiseSpec:
    id: str
    sigmas: Tuple[float, ...]
    weights: Tuple[float, ...]
    stretch: Tuple[float, float] = (1.0, 1.0)
    seed_offset: int = 0
    desc: str = ""


@dataclass(frozen=True, slots=True)
class SurfaceModifierSpec:
    """
    Parameters for applying noise-driven color mottle/perturbation to a surface.

    Attributes:
        intensity: Overall strength of the color shift (0.0 to 255.0).
        shift_vector: RGB weights defining the direction of the hue shift.
            e.g., (1.0, 0.8, -0.5) pushes peaks toward orange/yellow
            and valleys toward blue/cool.
        noise_id: The ID of the noise profile in NoiseRegistry to use
            for the mottle pattern (e.g., "biome" or "fine_mottle").
    """
    intensity: float
    shift_vector: Tuple[float, float, float]
    noise_id: str
    desc: str = ""
    op: str = ""


@dataclass(frozen=False, slots=True)
class RequiredResources:
    """The master manifest produced by scanning the pipeline."""
    #  Sources
    sources: Dict[SourceKey, Path]
    files: Set[FileKey]
    factor_inputs: Set[str]

    # The Geometry Master
    anchor_key: SourceKey

    # Procedural Resources
    noise_profiles: Dict[str, NoiseSpec]

    # Surface Management
    surface_inputs: Set[SurfaceKey]
    primary_surface: Optional[SurfaceKey]

    # --- THE HASHES ---
    # Initialized as empty strings, populated by TaskResolver
    geography_hash: str = ""
    logic_hash: str = ""
    style_hash: str = ""
    topology_hash: str = ""

    def with_hashes(
            self, geography_hash: str, hashes: dict
    ) -> 'RequiredResources':
        """
        Returns a copy of the resources with updated content hashes.
        Uses dataclasses.replace to handle the object update cleanly.
        """
        return replace(
            self, geography_hash=geography_hash, logic_hash=hashes["logic"],
            style_hash=hashes["style"], topology_hash=hashes["topology"]
        )


@dataclass(frozen=True, slots=True)
class ResolvedManifest:
    resources: RequiredResources
    file_map: Dict[str, str]  # Key -> Path string
    factor_details: List[FactorSpec]
    surface_details: List[SurfaceSpec]
    pipeline: List[_BlendSpec]

