from __future__ import annotations

import json
import operator
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, overload

import pandas as pd
import pyarrow as pa
from mosaicolabs import Serializable, SessionLevelErrorPolicy

from . import _core
from ._errors import InvalidArgumentError, OntologyMismatchError, SchemaError, StreamClosedError
from ._schema import SPECS, OntologySpec, check_ontology, conform, fill, is_int64, schema_problem, served_schema
from ._views import columns

if TYPE_CHECKING:
    from ._writer import SequenceUpdater

T = TypeVar("T", bound=Serializable)
S = TypeVar("S", bound=Serializable)


@dataclass(frozen=True)
class SessionInfo:
    locator: str
    created_timestamp: int
    completed_timestamp: int | None
    topics: list[str]
    locked: bool


class SequenceHandler:
    def __init__(self, client: _core.Client, core: _core.SequenceHandler) -> None:
        self._client = client
        self._core = core
        raw = core.user_metadata
        self._metadata: dict[str, Any] = json.loads(raw) if raw else {}

    @property
    def name(self) -> str:
        return self._core.name

    @property
    def topics(self) -> list[str]:
        return self._core.topics

    @property
    def sessions(self) -> list[SessionInfo]:
        return [SessionInfo(*s) for s in self._core.sessions]

    @property
    def user_metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def created_timestamp(self) -> int:
        return self._core.created_timestamp

    @property
    def timestamp_ns_min(self) -> int | None:
        return self._core.timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> int | None:
        return self._core.timestamp_ns_max

    def topic_handler(self, topic_name: str, *, ontology_type: type[T]) -> TopicHandler[T]:
        return open_topic(lambda: self._core.topic_handler(topic_name), ontology_type)

    def update(self, on_error: SessionLevelErrorPolicy = SessionLevelErrorPolicy.Report) -> SequenceUpdater:
        from ._writer import SequenceUpdater

        return SequenceUpdater(self._client, self.name, on_error)

    def close(self) -> None:
        self._core.close()


def open_topic(open_core: Callable[[], _core.TopicHandler], ontology_type: type[T]) -> TopicHandler[T]:
    tag = check_ontology(ontology_type).__ontology_tag__
    core = open_core()
    if core.ontology_tag != tag:
        raise OntologyMismatchError(
            f"topic '{core.sequence_name}{core.name}' holds {core.ontology_tag!r} data, not {tag!r}"
        )
    return TopicHandler(core, ontology_type)


class TopicHandler(Generic[T]):
    def __init__(self, core: _core.TopicHandler, ontology: type[T]) -> None:
        self._core = core
        self._ontology = ontology
        self._spec = SPECS.get(ontology)

    @property
    def name(self) -> str:
        return self._core.name

    @property
    def sequence_name(self) -> str:
        return self._core.sequence_name

    @property
    def ontology_tag(self) -> str:
        return self._core.ontology_tag

    @property
    def ontology_type(self) -> type[T]:
        return self._ontology

    @property
    def timestamp_ns_min(self) -> int | None:
        return self._core.timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> int | None:
        return self._core.timestamp_ns_max

    def get_data_streamer(
        self, start_timestamp_ns: int | None = None, end_timestamp_ns: int | None = None
    ) -> TopicDataStreamer[T]:
        spec = self._spec
        if spec is None:
            supported = ", ".join(o.__name__ for o in SPECS)
            raise NotImplementedError(
                f"reading {self._ontology.__name__} as messages is not supported yet, only {supported}: "
                "use get_batch_streamer() or to_arrow()"
            )
        stream, received, schema = self._open(start_timestamp_ns, end_timestamp_ns)
        return TopicDataStreamer(stream, spec, received, schema)

    def get_batch_streamer(
        self, start_timestamp_ns: int | None = None, end_timestamp_ns: int | None = None
    ) -> TopicBatchStreamer:
        stream, received, _ = self._open(start_timestamp_ns, end_timestamp_ns)
        return TopicBatchStreamer(stream, received)

    def to_arrow(self, start_timestamp_ns: int | None = None, end_timestamp_ns: int | None = None) -> pa.Table:
        stream, received, schema = self._open(start_timestamp_ns, end_timestamp_ns)
        with TopicBatchStreamer(stream, received) as batches:
            collected = list(batches)
        if not received.equals(schema):
            collected = [fill(b, schema) for b in collected]
        return pa.Table.from_batches(collected, schema=schema)

    def to_pandas(
        self,
        start_timestamp_ns: int | None = None,
        end_timestamp_ns: int | None = None,
        *,
        dtype_backend: Literal["pyarrow"] | None = None,
    ) -> pd.DataFrame:
        if dtype_backend not in (None, "pyarrow"):
            raise InvalidArgumentError(f"dtype_backend must be None or 'pyarrow', not {dtype_backend!r}")
        table = self.to_arrow(start_timestamp_ns, end_timestamp_ns)
        while any(isinstance(f.type, pa.StructType) for f in table.schema):
            table = table.flatten()
        if dtype_backend == "pyarrow":
            return table.to_pandas(types_mapper=pd.ArrowDtype)
        for i, field in enumerate(table.schema):
            column = table.column(i)
            if _is_string(field.type) and column.num_chunks:
                copy = pa.chunked_array([pa.concat_arrays([chunk]) for chunk in column.chunks], field.type)
                table = table.set_column(i, field, copy)
        return table.to_pandas()

    def close(self) -> None:
        self._core.close()

    def _open(self, start: int | None, end: int | None) -> tuple[_core.BatchStream, pa.Schema, pa.Schema]:
        stream = self._core.get_batch_streamer(_bound("start_timestamp_ns", start), _bound("end_timestamp_ns", end))
        schema = pa.schema(stream.schema)  # pyright: ignore[reportArgumentType]
        if len(schema) == 0:
            expected = served_schema(self._ontology)
            return stream, expected, expected
        if self._spec is None:
            return stream, schema, schema
        problem = schema_problem(self._spec, schema)
        if problem is not None:
            stream.close()
            raise SchemaError(
                f"topic '{self.sequence_name}{self.name}' does not match {self._ontology.__name__}: {problem}"
            )
        return stream, schema, conform(self._spec.schema, schema)


def _bound(label: str, value: Any) -> int | None:
    if value is None:
        return None
    if not is_int64(value):
        raise InvalidArgumentError(f"'{label}' must be an int64 or None, got {value!r}")
    return operator.index(value)


def _is_string(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)


def _import(raw: _core.RawBatch, schema: pa.Schema) -> pa.RecordBatch:
    return pa.RecordBatch._import_from_c(raw.array_address(), schema)  # pyright: ignore


class TopicBatchStreamer:
    def __init__(self, stream: _core.BatchStream, schema: pa.Schema) -> None:
        self._stream = stream
        self._schema = schema

    @property
    def schema(self) -> pa.Schema:
        return self._schema

    def __iter__(self) -> TopicBatchStreamer:
        return self

    def __next__(self) -> pa.RecordBatch:
        while True:
            raw = self._stream.next_batch()
            if raw is None:
                raise StopIteration
            batch = _import(raw, self._schema)
            if batch.num_rows:
                return batch

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        if len(pa.schema(self._stream.schema)) == 0:  # pyright: ignore[reportArgumentType]
            self._stream.close()
            empty = pa.RecordBatchReader.from_batches(self._schema, [])
            return empty.__arrow_c_stream__(requested_schema)  # pyright: ignore[reportArgumentType]
        return self._stream.__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> TopicBatchStreamer:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class TopicDataStreamer(Generic[T]):
    def __init__(self, stream: _core.BatchStream, spec: OntologySpec, received: pa.Schema, schema: pa.Schema) -> None:
        self._stream = stream
        self._spec = spec
        self._received = received
        self._schema = schema
        self._view = spec.view
        self._closed = stream.is_closed
        self._ts: list[int] = []
        self._cols: Any = None
        self._i = 0
        self._n = 0

    def __iter__(self) -> TopicDataStreamer[T]:
        return self

    def __next__(self) -> Message[T]:
        if self._closed():
            raise StreamClosedError("stream closed")
        i = self._i
        if i >= self._n:
            self._load()
            i = 0
        self._i = i + 1
        return Message(self._ts[i], self._view(self._cols, i), self._spec)

    def next_timestamp(self) -> int | None:
        if self._closed():
            raise StreamClosedError("stream closed")
        if self._i >= self._n:
            try:
                self._load()
            except StopIteration:
                return None
        return self._ts[self._i]

    def close(self) -> None:
        self._stream.close()
        self._drop()

    def __enter__(self) -> TopicDataStreamer[T]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _drop(self) -> None:
        self._ts, self._cols, self._i, self._n = [], None, 0, 0

    def _load(self) -> None:
        while True:
            raw = self._stream.next_batch()
            if raw is None:
                self._drop()
                raise StopIteration
            batch = _import(raw, self._received)
            if batch.num_rows:
                break
        timestamps = batch.column("timestamp_ns")
        if timestamps.null_count:
            self.close()
            raise SchemaError("null in required field timestamp_ns")
        self._ts = timestamps.to_numpy().tolist()
        self._cols = columns(fill(batch, self._schema), self._ts)
        self._i = 0
        self._n = batch.num_rows


class Message(Generic[T]):
    __slots__ = ("_spec", "data", "timestamp_ns")

    timestamp_ns: int
    data: Any

    def __init__(self, timestamp_ns: int, data: Any, spec: OntologySpec) -> None:
        self.timestamp_ns = timestamp_ns
        self.data = data
        self._spec = spec

    @overload
    def get_data(self) -> T: ...
    @overload
    def get_data(self, target_type: type[S]) -> S | None: ...
    def get_data(self, target_type: type[Serializable] | None = None) -> Serializable | None:
        if target_type is not None and not issubclass(self._spec.ontology, target_type):
            return None
        return self._spec.build(self.data)

    def ontology_type(self) -> type[Serializable]:
        return self._spec.ontology

    def ontology_tag(self) -> str:
        return self._spec.tag
