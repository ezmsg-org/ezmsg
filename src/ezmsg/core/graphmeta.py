import enum

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
class ProcessRegistration:
    process_id: str
    pid: int
    host: str
    units: list[str]


@dataclass
class ProcessOwnershipUpdate:
    process_id: str
    added_units: list[str] = field(default_factory=list)
    removed_units: list[str] = field(default_factory=list)


@dataclass
class SettingsSnapshotValue:
    serialized: bytes | None
    repr_value: dict[str, Any] | str


class SettingsEventType(enum.Enum):
    INITIAL_SETTINGS = "INITIAL_SETTINGS"
    SETTINGS_UPDATED = "SETTINGS_UPDATED"


@dataclass
class SettingsChangedEvent:
    seq: int
    event_type: SettingsEventType
    component_address: str
    timestamp: float
    source_session_id: str | None
    source_process_id: str | None
    value: SettingsSnapshotValue


class TopologyEventType(enum.Enum):
    GRAPH_CHANGED = "GRAPH_CHANGED"
    PROCESS_CHANGED = "PROCESS_CHANGED"


@dataclass
class TopologyChangedEvent:
    seq: int
    event_type: TopologyEventType
    timestamp: float
    changed_topics: list[str]
    source_session_id: str | None
    source_process_id: str | None


@dataclass
class ProcessSettingsUpdate:
    process_id: str
    component_address: str
    value: SettingsSnapshotValue
    timestamp: float


@dataclass
class ProcessControlRequest:
    request_id: str
    unit_address: str
    operation: "ProcessControlOperation | str"
    payload: bytes | None = None


class ProcessControlOperation(enum.Enum):
    PING = "PING"
    GET_PROCESS_STATS = "GET_PROCESS_STATS"
    GET_PROFILING_SNAPSHOT = "GET_PROFILING_SNAPSHOT"
    SET_PROFILING_TRACE = "SET_PROFILING_TRACE"
    GET_PROFILING_TRACE_BATCH = "GET_PROFILING_TRACE_BATCH"
    UPDATE_SETTING_FIELD = "UPDATE_SETTING_FIELD"


class ProcessControlErrorCode(enum.Enum):
    UNROUTABLE_UNIT = "UNROUTABLE_UNIT"
    ROUTE_WRITE_FAILED = "ROUTE_WRITE_FAILED"
    TIMEOUT = "TIMEOUT"
    PROCESS_DISCONNECTED = "PROCESS_DISCONNECTED"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    HANDLER_NOT_CONFIGURED = "HANDLER_NOT_CONFIGURED"
    HANDLER_ERROR = "HANDLER_ERROR"
    INVALID_RESPONSE = "INVALID_RESPONSE"


@dataclass
class ProcessControlResponse:
    request_id: str
    ok: bool
    payload: bytes | None = None
    error: str | None = None
    error_code: ProcessControlErrorCode | None = None
    process_id: str | None = None


@dataclass
class SettingsFieldUpdateRequest:
    field_path: str
    value: Any


@dataclass
class ProcessPing:
    process_id: str
    pid: int
    host: str
    timestamp: float


@dataclass
class ProcessStats:
    process_id: str
    pid: int
    host: str
    owned_units: list[str]
    timestamp: float


class ProfileChannelType(enum.Enum):
    LOCAL = "LOCAL"
    SHM = "SHM"
    TCP = "TCP"
    UNKNOWN = "UNKNOWN"


@dataclass
class PublisherProfileSnapshot:
    endpoint_id: str
    topic: str
    messages_published_total: int
    messages_published_window: int
    publish_delta_ns_avg_window: float
    publish_rate_hz_window: float
    inflight_messages_current: int
    inflight_messages_peak_window: int
    backpressure_wait_ns_total: int
    backpressure_wait_ns_window: int
    timestamp: float


@dataclass
class SubscriberProfileSnapshot:
    endpoint_id: str
    topic: str
    messages_received_total: int
    messages_received_window: int
    lease_time_ns_total: int
    lease_time_ns_avg_window: float
    user_span_ns_total: int
    user_span_ns_avg_window: float
    attributable_backpressure_ns_total: int
    attributable_backpressure_ns_window: int
    attributable_backpressure_events_total: int
    channel_kind_last: ProfileChannelType
    timestamp: float


@dataclass
class ProcessProfilingSnapshot:
    process_id: str
    pid: int
    host: str
    window_seconds: float
    timestamp: float
    publishers: dict[str, PublisherProfileSnapshot]
    subscribers: dict[str, SubscriberProfileSnapshot]


@dataclass
class ProfilingTraceControl:
    enabled: bool
    sample_mod: int = 1
    publisher_topics: list[str] | None = None
    subscriber_topics: list[str] | None = None
    publisher_endpoint_ids: list[str] | None = None
    subscriber_endpoint_ids: list[str] | None = None
    metrics: list[str] | None = None
    ttl_seconds: float | None = None


@dataclass
class ProfilingTraceSample:
    timestamp: float
    endpoint_id: str
    topic: str
    metric: str
    value: float
    channel_kind: ProfileChannelType | None = None


@dataclass
class ProcessProfilingTraceBatch:
    process_id: str
    pid: int
    host: str
    timestamp: float
    samples: list[ProfilingTraceSample]


@dataclass
class ProfilingTraceStreamBatch:
    timestamp: float
    batches: dict[str, ProcessProfilingTraceBatch]


@dataclass
class ProfilingStreamControl:
    interval: float = 0.05
    max_samples: int = 1000
    process_ids: list[str] | None = None
    include_empty_batches: bool = False


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
