from dataclasses import dataclass, field
from typing import Any, TypeAlias, NamedTuple


@dataclass
class DynamicSettingsMetadata:
    enabled: bool
    input_topic: str | None
    settings_type: str | None


@dataclass
class StreamMetadata:
    name: str
    address: str
    msg_type: str


@dataclass
class InputStreamMetadata(StreamMetadata):
    leaky: bool = False
    max_queue: int | None = None


@dataclass
class OutputStreamMetadata(StreamMetadata):
    host: str | None = None
    port: int | None = None
    num_buffers: int | None = None
    buf_size: int | None = None
    force_tcp: bool | None = None


StreamMetadataType: TypeAlias = (
    StreamMetadata | InputStreamMetadata | OutputStreamMetadata
)


@dataclass
class TaskMetadata:
    name: str
    subscribes: str | None = None
    publishes: list[str] = field(default_factory=list)


SettingsReprType: TypeAlias = dict[str, Any] | str
SerializedSettingsType: TypeAlias = bytes | None
InitialSettingsType: TypeAlias = tuple[SerializedSettingsType, SettingsReprType]


@dataclass
class ComponentMetadata:
    address: str
    name: str
    component_type: str
    settings_type: str
    initial_settings: InitialSettingsType
    streams: dict[str, StreamMetadataType]
    dynamic_settings: DynamicSettingsMetadata


@dataclass
class CollectionMetadata(ComponentMetadata):
    children: list[str]


@dataclass
class UnitMetadata(ComponentMetadata):
    tasks: list[TaskMetadata]
    main: str | None
    threads: list[str]


ComponentMetadataType: TypeAlias = (
    ComponentMetadata | CollectionMetadata | UnitMetadata
)


@dataclass
class GraphMetadata:
    schema_version: int
    root_name: str | None
    components: dict[str, ComponentMetadataType]


class Edge(NamedTuple):
    from_topic: str
    to_topic: str


@dataclass
class SnapshotSession:
    edges: list[Edge]
    metadata: GraphMetadata | None


@dataclass
class GraphSnapshot:
    graph: dict[str, list[str]]
    edge_owners: dict[Edge, list[str]]
    sessions: dict[str, SnapshotSession]
