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
class TopicMetadata:
    name: str
    address: str
    msg_type: str


@dataclass
class InputTopicMetadata(TopicMetadata): ...


@dataclass
class OutputTopicMetadata(TopicMetadata): ...


TopicMetadataType: TypeAlias = TopicMetadata | InputTopicMetadata | OutputTopicMetadata


@dataclass
class InputRelayMetadata(InputTopicMetadata):
    leaky: bool = False
    max_queue: int | None = None
    copy_on_forward: bool = True


@dataclass
class OutputRelayMetadata(OutputTopicMetadata):
    host: str | None = None
    port: int | None = None
    num_buffers: int | None = None
    buf_size: int | None = None
    force_tcp: bool | None = None
    copy_on_forward: bool = True


RelayMetadataType: TypeAlias = InputRelayMetadata | OutputRelayMetadata


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
    dynamic_settings: DynamicSettingsMetadata


@dataclass
class CollectionMetadata(ComponentMetadata):
    topics: dict[str, TopicMetadataType]
    relays: dict[str, RelayMetadataType]
    children: list[str]


@dataclass
class UnitMetadata(ComponentMetadata):
    streams: dict[str, StreamMetadataType]
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


@dataclass
class ProcessHello:
    process_id: str
    pid: int
    host: str
    units: list[str]


@dataclass
class ProcessOwnershipUpdate:
    process_id: str
    added_units: list[str] = field(default_factory=list)
    removed_units: list[str] = field(default_factory=list)


class Edge(NamedTuple):
    from_topic: str
    to_topic: str


@dataclass
class SnapshotSession:
    edges: list[Edge]
    metadata: GraphMetadata | None


@dataclass
class SnapshotProcess:
    process_id: str
    pid: int | None
    host: str | None
    units: list[str]


@dataclass
class GraphSnapshot:
    graph: dict[str, list[str]]
    edge_owners: dict[Edge, list[str]]
    sessions: dict[str, SnapshotSession]
    processes: dict[str, SnapshotProcess] = field(default_factory=dict)
