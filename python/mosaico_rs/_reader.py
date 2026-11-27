from __future__ import annotations

import functools
import json
import operator
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, overload

import pandas as pd
import pyarrow as pa
from mosaicolabs import SerializationFormat, Serializable, SessionLevelErrorPolicy

from . import _core
from ._errors import InvalidArgumentError, OntologyMismatchError, SchemaError, StreamClosedError
from ._schema import (
    SPECS,
    SPECS_BY_TAG,
    OntologySpec,
    check_ontology,
    conform,
    fill,
    is_int64,
    schema_problem,
    served_schema,
)
from ._views import Usage, columns

if TYPE_CHECKING:
    from ._writer import SequenceUpdater

T = TypeVar("T", bound=Serializable)
S = TypeVar("S", bound=Serializable)
R = TypeVar("R")
B = TypeVar("B")

_CONTENDED_NS = 1_000_000


@dataclass(frozen=True)
class SessionInfo:
    locator: str
    created_timestamp: int
    completed_timestamp: int | None
    topics: list[str]
    locked: bool


def _user_metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    value = json.loads(raw)
    return {} if value is None else value


class SequenceHandler:
    def __init__(self, client: _core.Client, core: _core.SequenceHandler) -> None:
        self._client = client
        self._core = core
        self._metadata: tuple[_core.SequenceHandler, dict[str, Any]] | None = None

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
        core = self._core
        cached = self._metadata
        if cached is not None and cached[0] is core:
            return cached[1]
        value = _user_metadata(core.user_metadata)
        self._metadata = (core, value)
        return value

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

    def get_data_streamer(
        self,
        topics: Sequence[str | TopicHandler[Any]] | None = None,
        start_timestamp_ns: int | None = None,
        end_timestamp_ns: int | None = None,
    ) -> SequenceDataStreamer:
        if isinstance(topics, str):
            raise InvalidArgumentError(f"topics must be a list of names or TopicHandler objects, not {topics!r}")
        start = _bound("start_timestamp_ns", start_timestamp_ns)
        end = _bound("end_timestamp_ns", end_timestamp_ns)
        handlers: list[TopicHandler[Any]] = []
        missing: list[tuple[str, str]] = []
        for topic in topics or self._core.topics:
            if isinstance(topic, TopicHandler):
                handler = topic
            elif isinstance(topic, str):
                core = self._core.topic_handler(topic)
                spec = SPECS_BY_TAG.get(core.ontology_tag)
                if spec is None:
                    missing.append((core.name, core.ontology_tag))
                    continue
                handler = TopicHandler(core, spec.ontology)
            else:
                raise InvalidArgumentError(f"topics must hold names or TopicHandler objects, not {topic!r}")
            if handler._spec is None:
                missing.append((handler.name, handler.ontology_type.__name__))
            handlers.append(handler)
        if missing:
            raise _no_sequence_views(missing)
        stream = self._core.get_window_streamer([h._core for h in handlers], start, end, True)
        try:
            readers: list[_TopicReader] = []
            for handler, holder in zip(handlers, stream.schemas, strict=True):
                spec = handler._spec
                assert spec is not None
                received, schema = handler._schemas(pa.schema(holder))  # pyright: ignore[reportArgumentType]
                readers.append(_TopicReader(spec, received, schema))
        except BaseException:
            stream.close()
            raise
        return SequenceDataStreamer(stream, handlers, readers)

    def update(self, on_error: SessionLevelErrorPolicy = SessionLevelErrorPolicy.Report) -> SequenceUpdater:
        from ._writer import SequenceUpdater

        return SequenceUpdater(self._client, self.name, on_error)

    def reload(self) -> None:
        self._core = self._core.reload()

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

    def __repr__(self) -> str:
        return f"TopicHandler({self.sequence_name!r}, {self.name!r}, {self._ontology.__name__})"

    def __str__(self) -> str:
        return self.name

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

    @functools.cached_property
    def user_metadata(self) -> dict[str, Any]:
        return _user_metadata(self._core.user_metadata)

    @property
    def timestamp_ns_min(self) -> int | None:
        return self._core.timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> int | None:
        return self._core.timestamp_ns_max

    @property
    def msg_count(self) -> int:
        return self._core.msg_count

    @property
    def created_timestamp(self) -> int:
        return self._core.created_timestamp

    @property
    def completed_timestamp(self) -> int | None:
        return self._core.completed_timestamp

    @property
    def locked(self) -> bool:
        return self._core.locked

    @property
    def serialization_format(self) -> SerializationFormat:
        return SerializationFormat(self._core.serialization_format)

    @property
    def chunks_number(self) -> int:
        return self._core.chunks_number

    @property
    def total_size_bytes(self) -> int:
        return self._core.total_size_bytes

    @property
    def ontology_schema(self) -> pa.StructType:
        return pa.struct(pa.schema(self._core.ontology_schema()))  # pyright: ignore[reportArgumentType]

    def get_data_streamer(
        self, start_timestamp_ns: int | None = None, end_timestamp_ns: int | None = None
    ) -> TopicDataStreamer[T]:
        spec = self._spec
        if spec is None:
            raise _no_views(self._ontology.__name__)
        stream, received, schema = self._open(start_timestamp_ns, end_timestamp_ns, join=True)
        return TopicDataStreamer(stream, spec, received, schema)

    def get_batch_streamer(
        self, start_timestamp_ns: int | None = None, end_timestamp_ns: int | None = None
    ) -> TopicBatchStreamer:
        stream, received, _ = self._open(start_timestamp_ns, end_timestamp_ns, strict=False)
        return TopicBatchStreamer(stream, received)

    def to_arrow(self, start_timestamp_ns: int | None = None, end_timestamp_ns: int | None = None) -> pa.Table:
        stream, received, schema = self._open(start_timestamp_ns, end_timestamp_ns, strict=False)
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

    def _open(
        self, start: int | None, end: int | None, strict: bool = True, join: bool = False
    ) -> tuple[_core.BatchStream, pa.Schema, pa.Schema]:
        stream = self._core.get_batch_streamer(
            _bound("start_timestamp_ns", start), _bound("end_timestamp_ns", end), join
        )
        try:
            received, schema = self._schemas(pa.schema(stream.schema), strict)  # pyright: ignore[reportArgumentType]
        except BaseException:
            stream.close()
            raise
        return stream, received, schema

    def _schemas(self, schema: pa.Schema, strict: bool = True) -> tuple[pa.Schema, pa.Schema]:
        if len(schema) == 0:
            expected = served_schema(self._ontology)
            return expected, expected
        if self._spec is None:
            return schema, schema
        problem = schema_problem(self._spec, schema)
        if problem is not None and not strict:
            return schema, schema
        if problem is not None:
            raise SchemaError(
                f"topic '{self.sequence_name}{self.name}' does not match {self._ontology.__name__}: {problem}"
            )
        return schema, conform(self._spec.schema, schema)


def _no_views(name: str) -> NotImplementedError:
    supported = ", ".join(o.__name__ for o in SPECS)
    return NotImplementedError(
        f"reading {name} as messages is not supported yet, only {supported}: use get_batch_streamer() or to_arrow()"
    )


def _no_sequence_views(missing: list[tuple[str, str]]) -> NotImplementedError:
    supported = ", ".join(o.__name__ for o in SPECS)
    listed = ", ".join(f"'{name}' ({tag})" for name, tag in missing)
    return NotImplementedError(
        f"topics {listed} cannot be read as messages yet, only {supported}: pass topics= without them, "
        "or read them with topic_handler(name, ontology_type=...).get_batch_streamer()"
    )


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


def _timed_import(raw: _core.RawBatch, schema: pa.Schema) -> tuple[pa.RecordBatch, bool]:
    started = time.perf_counter_ns()
    batch = _import(raw, schema)
    return batch, time.perf_counter_ns() - started >= _CONTENDED_NS


def _fetch(fetch: Callable[[int], R | None], seq: int, build: Callable[[R], B], close: Callable[[], None]) -> B | None:
    raw = fetch(seq)
    if raw is None:
        return None
    try:
        return build(raw)
    except KeyboardInterrupt:
        raise
    except BaseException:
        close()
        raise


class TopicBatchStreamer:
    def __init__(self, stream: _core.BatchStream, schema: pa.Schema) -> None:
        self._stream = stream
        self._schema = schema
        self._seen = 0

    @property
    def schema(self) -> pa.Schema:
        return self._schema

    def __iter__(self) -> TopicBatchStreamer:
        return self

    def __next__(self) -> pa.RecordBatch:
        seq = self._seen + 1
        batch = _fetch(self._stream.next_batch, seq, lambda raw: _import(raw, self._schema), self.close)
        self._seen = seq
        if batch is None:
            raise StopIteration
        return batch

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        if len(pa.schema(self._stream.schema)) == 0:  # pyright: ignore[reportArgumentType]
            self._stream.close()
            empty = pa.RecordBatchReader.from_batches(self._schema, [])
            return empty.__arrow_c_stream__(requested_schema)  # pyright: ignore[reportArgumentType]
        return self._stream.export(self._seen)

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
        self._seen = 0
        self._contended = False
        self._usage = Usage()

    def __iter__(self) -> TopicDataStreamer[T]:
        return self

    def __next__(self) -> Message[T]:
        self._ensure_open()
        i = self._i
        if i >= self._n:
            self._load()
            i = 0
        msg = Message(self._ts[i], self._view(self._cols, i), self._spec)
        self._i = i + 1
        return msg

    def next_timestamp(self) -> int | None:
        self._ensure_open()
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

    def _ensure_open(self) -> None:
        if self._closed():
            self.close()
            raise StreamClosedError("stream closed")

    def _drop(self) -> None:
        self._ts, self._cols, self._i, self._n = [], None, 0, 0

    def _load(self) -> None:
        seq = self._seen + 1
        loaded = _fetch(lambda s: self._stream.next_batch(s, self._contended), seq, self._build, self.close)
        if loaded is None:
            self._seen = seq
            self._drop()
            raise StopIteration
        ts, cols = loaded
        self._ts, self._cols, self._i, self._n, self._seen = ts, cols, 0, len(ts), seq

    def _build(self, raw: _core.RawBatch) -> tuple[list[int], Any]:
        batch, self._contended = _timed_import(raw, self._received)
        timestamps = batch.column("timestamp_ns")
        if timestamps.null_count:
            raise SchemaError("null in required field timestamp_ns")
        ts = timestamps.to_numpy().tolist()
        return ts, columns(fill(batch, self._schema), ts, self._usage)


@dataclass(frozen=True)
class _TopicReader:
    spec: OntologySpec
    received: pa.Schema
    schema: pa.Schema


class SequenceDataStreamer:
    def __init__(self, stream: _core.WindowStream, handlers: list[TopicHandler[Any]], readers: list[_TopicReader]) -> None:
        self._stream = stream
        self._handlers = handlers
        self._readers = readers
        self._specs = [r.spec for r in readers]
        self._views = [r.spec.view for r in readers]
        self._closed = stream.is_closed
        self._ts: list[list[int]] = [[] for _ in readers]
        self._cols: list[Any] = [None for _ in readers]
        self._rows = [0 for _ in readers]
        self._order: list[int] = []
        self._pos = 0
        self._seen = 0
        self._contended = False
        self._usages = [Usage() for _ in readers]

    @property
    def topics(self) -> list[TopicHandler[Any]]:
        return list(self._handlers)

    @property
    def msg_count(self) -> int:
        return self._stream.msg_count

    @property
    def timestamp_ns_min(self) -> int | None:
        return self._stream.timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> int | None:
        return self._stream.timestamp_ns_max

    def __iter__(self) -> SequenceDataStreamer:
        return self

    def __next__(self) -> tuple[TopicHandler[Any], Message[Any]]:
        self._ensure_open()
        pos = self._pos
        if pos >= len(self._order):
            self._load()
            pos = 0
        k = self._order[pos]
        i = self._rows[k]
        msg = Message(self._ts[k][i], self._views[k](self._cols[k], i), self._specs[k])
        self._pos = pos + 1
        self._rows[k] = i + 1
        return self._handlers[k], msg

    def next_timestamp(self) -> int | None:
        self._ensure_open()
        if self._pos >= len(self._order):
            try:
                self._load()
            except StopIteration:
                return None
        k = self._order[self._pos]
        return self._ts[k][self._rows[k]]

    def close(self) -> None:
        self._stream.close()
        self._drop()

    def __enter__(self) -> SequenceDataStreamer:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed():
            self.close()
            raise StreamClosedError("stream closed")

    def _drop(self) -> None:
        k = len(self._readers)
        self._ts, self._cols, self._rows = [[] for _ in range(k)], [None] * k, [0] * k
        self._order, self._pos = [], 0

    def _load(self) -> None:
        seq = self._seen + 1
        loaded = _fetch(lambda s: self._stream.next_window(s, self._contended), seq, self._build, self.close)
        if loaded is None:
            self._seen = seq
            self._drop()
            raise StopIteration
        updates, order = loaded
        for k, ts, cols in updates:
            self._ts[k], self._cols[k], self._rows[k] = ts, cols, 0
        self._order, self._pos, self._seen = order, 0, seq

    def _build(
        self, window: tuple[list[tuple[int, _core.RawBatch, list[int]]], list[int]]
    ) -> tuple[list[tuple[int, list[int], Any]], list[int]]:
        batches, order = window
        updates: list[tuple[int, list[int], Any]] = []
        contended = False
        for k, raw, ts in batches:
            reader = self._readers[k]
            batch, slow = _timed_import(raw, reader.received)
            contended = contended or slow
            updates.append((k, ts, columns(fill(batch, reader.schema), ts, self._usages[k])))
        self._contended = contended
        return updates, order


class Message(Generic[T]):
    __slots__ = ("_spec", "data", "timestamp_ns")

    timestamp_ns: int
    data: Any

    def __init__(self, timestamp_ns: int, data: Any, spec: OntologySpec) -> None:
        self.timestamp_ns = timestamp_ns
        self.data = data
        self._spec = spec

    def __repr__(self) -> str:
        return f"Message(timestamp_ns={self.timestamp_ns}, data={self.data!r})"

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
