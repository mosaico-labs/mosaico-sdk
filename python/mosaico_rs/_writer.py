from __future__ import annotations

import json
import logging
import threading
import time
from types import TracebackType
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
from mosaicolabs import (
    IMU,
    CompressedImage,
    Image,
    Message,
    Serializable,
    SessionLevelErrorPolicy,
    SessionStatus,
    TopicLevelErrorPolicy,
    TopicWriterStatus,
)

from . import _core
from ._errors import InvalidArgumentError, TopicNotFoundError
from ._schema import check_ontology, wire_schema

_TERMINAL = (TopicWriterStatus.Finalized, TopicWriterStatus.FinalizedWithError)
_RECORD_KINDS: dict[type[Serializable], str] = {
    IMU: "imu",
    Image: "image",
    CompressedImage: "compressed_image",
}
_CHUNK_BYTES = 1 << 20
_log = logging.getLogger(__name__)


def _metadata(metadata: dict[str, Any] | None) -> bytes | None:
    if metadata is None:
        return None
    if not isinstance(metadata, dict):
        raise InvalidArgumentError(f"metadata must be a dict, not {type(metadata).__name__}")
    return json.dumps(metadata).encode("utf-8")


def _topic_key(name: str) -> str:
    return f"/{name.strip('/')}"


def _timestamps(value: Any) -> pa.Array:
    if isinstance(value, pa.Array):
        array = value
    else:
        try:
            array = pa.array(value)
        except Exception as e:
            raise InvalidArgumentError(f"'timestamp_ns' must be an int64 array: {e}") from None
    if array.type != pa.int64():
        raise InvalidArgumentError(f"'timestamp_ns' must be int64, got '{array.type}'")
    return array


class _SessionWriter:
    def __init__(self, client: _core.Client, name: str, on_error: SessionLevelErrorPolicy) -> None:
        self._client = client
        self._name = name
        self._on_error = SessionLevelErrorPolicy(on_error.value)
        self._core: _core.SequenceWriter | None = None
        self._locator = ""
        self._status = SessionStatus.Null
        self._topics: dict[str, TopicWriter] = {}
        self._lock = threading.Lock()

    def _open(self) -> _core.SequenceWriter:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def status(self) -> SessionStatus:
        return self._status

    @property
    def session_status(self) -> SessionStatus:
        self._started()
        return self._status

    @property
    def session_locator(self) -> str:
        self._started()
        return self._locator

    def __enter__(self) -> _SessionWriter:
        if self._status is not SessionStatus.Null:
            raise RuntimeError(f"the write session of '{self._name}' was already started")
        self._core = self._open()
        self._locator = self._core.session_locator
        self._status = SessionStatus.Pending
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        core = self._entered()
        try:
            if exc is None:
                try:
                    self._send_buffered()
                except BaseException as e:
                    self._abort(core, e)
                    raise
                self._finish(core)
                return
            self._drop_buffered()
            self._abort(core, exc)
        finally:
            if core.left_open:
                _log.error("the write session '%s' was left open on the server%s", self._locator, self._cleanup())

    def _abort(self, core: _core.SequenceWriter, exc: BaseException) -> None:
        self._status = SessionStatus.Error
        try:
            core.abort(str(exc) or type(exc).__name__, isinstance(exc, KeyboardInterrupt))
        except Exception as e:
            _log.error("cannot abort the write session of '%s': %s", self._name, e)

    def _send_buffered(self) -> None:
        for topic in self._topic_list():
            topic.flush_buffer()

    def _drop_buffered(self) -> None:
        for topic in self._topic_list():
            topic.drop_buffer()

    def _topic_list(self) -> list[TopicWriter]:
        with self._lock:
            return list(self._topics.values())

    def _finish(self, core: _core.SequenceWriter) -> None:
        try:
            signal = core.finish()
        except BaseException:
            self._status = SessionStatus.Error
            raise
        self._status = SessionStatus.Finalized
        if signal is not None:
            raise signal

    def _cleanup(self) -> str:
        return ""

    def _entered(self) -> _core.SequenceWriter:
        if self._core is None or self._status is not SessionStatus.Pending:
            raise RuntimeError(f"the write session of '{self._name}' must be used inside its 'with' block")
        return self._core

    def _started(self) -> None:
        if self._core is None:
            raise RuntimeError(f"the write session of '{self._name}' must be used inside its 'with' block")

    def topic_create(
        self,
        topic_name: str,
        metadata: dict[str, Any],
        ontology_type: type[Serializable],
        on_error: TopicLevelErrorPolicy = TopicLevelErrorPolicy.Raise,
    ) -> TopicWriter:
        core = self._entered()
        ontology = check_ontology(ontology_type)
        schema = wire_schema(ontology)
        policy = TopicLevelErrorPolicy(on_error.value)
        tag = ontology.__ontology_tag__
        assert tag is not None
        with self._lock:
            writer = core.topic_create(
                topic_name,
                tag,
                ontology.__serialization_format__.value,
                schema,
                _metadata(metadata),
                policy.value,
            )
            topic = TopicWriter(writer, ontology, schema, self._client.max_batch_size_bytes)
            self._topics[writer.name] = topic
        return topic

    def get_topic_writer(self, topic_name: str) -> TopicWriter:
        self._started()
        key = _topic_key(topic_name)
        with self._lock:
            topic = self._topics.get(key)
        if topic is None:
            raise TopicNotFoundError(f"topic '{key}' was not created in this session")
        return topic

    def topic_writer_exists(self, topic_name: str) -> bool:
        self._started()
        with self._lock:
            return _topic_key(topic_name) in self._topics

    def list_topic_writers(self) -> list[str]:
        self._started()
        with self._lock:
            return list(self._topics)


class SequenceWriter(_SessionWriter):
    def __init__(
        self,
        client: _core.Client,
        name: str,
        metadata: dict[str, Any],
        on_error: SessionLevelErrorPolicy,
    ) -> None:
        super().__init__(client, name, on_error)
        self._metadata = _metadata(metadata)

    def __enter__(self) -> SequenceWriter:
        super().__enter__()
        return self

    def _open(self) -> _core.SequenceWriter:
        return self._client.sequence_create(self._name, self._metadata, self._on_error.value)

    def _cleanup(self) -> str:
        return f": delete it with sequence_delete('{self._name}')"


class SequenceUpdater(_SessionWriter):
    def __enter__(self) -> SequenceUpdater:
        super().__enter__()
        return self

    def _open(self) -> _core.SequenceWriter:
        return self._client.sequence_update(self._name, self._on_error.value)


class _Rows:
    def __init__(self) -> None:
        self.batches: list[pa.RecordBatch] = []
        self.bytes = 0
        self.since: float | None = None

    def add(self, row: pa.RecordBatch, size: int) -> None:
        if self.since is None:
            self.since = time.monotonic()
        self.batches.append(row)
        self.bytes += size

    def take(self) -> list[pa.RecordBatch]:
        batches, self.batches = self.batches, []
        self.bytes = 0
        self.since = None
        return batches


class TopicWriter:
    def __init__(
        self,
        core: _core.TopicWriter,
        ontology: type[Serializable],
        schema: pa.Schema,
        record_limit: int,
    ) -> None:
        self._core = core
        self._ontology = ontology
        self._schema = schema
        self._payload = pa.schema(list(ontology.__msco_pyarrow_struct__))
        self._kind = _RECORD_KINDS.get(ontology)
        self._record_limit = record_limit
        self._lock = threading.Lock()
        self._rows = _Rows()
        self._policy: tuple[int | None, int | None, float | None] = (None, None, None)
        self._last_timestamp: int | None = None

    def __enter__(self) -> TopicWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if not isinstance(exc, Exception):
            return False
        try:
            suppress = self._core.handle_error(str(exc) or type(exc).__name__)
        except Exception as e:
            _log.error("cannot apply the error policy of topic '%s': %s", self.name, e)
            return False
        if self.status is TopicWriterStatus.FinalizedWithError:
            self.drop_buffer()
        return suppress

    @property
    def name(self) -> str:
        return self._core.name

    @property
    def ontology_type(self) -> type[Serializable]:
        return self._ontology

    @property
    def status(self) -> TopicWriterStatus:
        return TopicWriterStatus(self._core.status)

    @property
    def last_error(self) -> str | None:
        return self._core.last_error

    @property
    def is_active(self) -> bool:
        return self.status not in _TERMINAL

    @property
    def dropped_record_count(self) -> int:
        return self._core.dropped_records

    def push(self, message: Message) -> None:
        if not isinstance(message, Message):
            kind = type(message)
            raise self._core.reject(f"message must be a mosaicolabs.Message, not {kind.__module__}.{kind.__qualname__}")
        data = message.data
        if not isinstance(data, self._ontology):
            raise self._core.reject(
                f"message data is {type(data).__name__}, but topic '{self.name}' stores {self._ontology.__name__}"
            )
        if self._kind is not None:
            self._core.push(self._kind, message.timestamp_ns, data)
            return
        with self._lock:
            self._push_row(message)

    def _push_row(self, message: Message) -> None:
        if not self.is_active:
            raise self._core.reject("the topic is finalized")
        timestamp = message.timestamp_ns
        last = self._last_timestamp
        if last is not None and timestamp < last:
            raise self._core.reject(
                f"'timestamp_ns' must be non-decreasing across pushes on topic '{self.name}': "
                f"got {timestamp}, lower than the last pushed timestamp {last}"
            )
        try:
            row = pa.RecordBatch.from_pylist([message._encode()], schema=self._schema)
        except Exception as e:
            raise self._core.reject(f"cannot convert the message: {e}") from None
        size = ipc.get_record_batch_size(row)
        if size > self._record_limit:
            self._core.record_too_large(size)
            return
        self._rows.add(row, size)
        self._last_timestamp = timestamp
        self._core.accepted()
        if self._ready():
            self._send_rows(urgent=self._urgent())

    def _ready(self) -> bool:
        max_bytes = self._policy[0]
        limit = _CHUNK_BYTES if max_bytes is None else min(_CHUNK_BYTES, max_bytes)
        return self._rows.bytes >= limit or self._urgent()

    def _urgent(self) -> bool:
        max_bytes, max_rows, max_age = self._policy
        rows = self._rows
        return bool(rows.batches) and (
            (max_bytes is not None and rows.bytes >= max_bytes)
            or (max_rows is not None and len(rows.batches) >= max_rows)
            or (max_age is not None and rows.since is not None and time.monotonic() - rows.since >= max_age)
        )

    def _send_rows(self, urgent: bool = False) -> None:
        batches = self._rows.take()
        if not batches:
            return
        joined = pa.Table.from_batches(batches, schema=self._schema).combine_chunks()
        self._core.push_batch(joined.to_batches()[0])
        if urgent:
            self._core.flush()

    def flush_buffer(self) -> None:
        with self._lock:
            self._send_rows()

    def drop_buffer(self) -> None:
        with self._lock:
            self._rows.take()

    def push_batch(self, timestamp_ns: pa.Array | Any, data: pa.RecordBatch) -> None:
        try:
            batch = self._wire_batch(timestamp_ns, data)
        except InvalidArgumentError as e:
            raise self._core.reject(str(e)) from None
        with self._lock:
            self._send_rows()
            self._core.push_batch(batch)
            if batch.num_rows > 0:
                self._last_timestamp = batch.column(0)[-1].as_py()

    def _wire_batch(self, timestamp_ns: object, data: object) -> pa.RecordBatch:
        if not isinstance(data, pa.RecordBatch):
            raise InvalidArgumentError(f"data must be a pyarrow.RecordBatch, not {type(data).__name__}")
        timestamps = _timestamps(timestamp_ns)
        if len(timestamps) != data.num_rows:
            raise InvalidArgumentError(
                f"'timestamp_ns' has {len(timestamps)} values, but the batch has {data.num_rows} rows"
            )
        columns = [timestamps, *data.columns]
        if data.schema.equals(self._payload, check_metadata=False):
            return pa.RecordBatch.from_arrays(columns, schema=self._schema)
        return pa.RecordBatch.from_arrays(columns, names=[self._schema.names[0], *data.schema.names])

    def push_stream(self, stream: Any) -> None:
        reader = pa.RecordBatchReader.from_stream(stream)
        with self._lock:
            self._send_rows()
            for batch in reader:
                self._core.push_batch(batch)
                if batch.num_rows > 0:
                    self._last_timestamp = batch.column(0)[-1].as_py()

    def flush(self) -> None:
        with self._lock:
            self._send_rows()
            self._core.flush()

    def set_flush_policy(
        self,
        max_bytes: int | None = None,
        max_rows: int | None = None,
        max_age: float | None = None,
    ) -> None:
        with self._lock:
            self._core.set_flush_policy(max_bytes, max_rows, max_age)
            self._policy = (max_bytes, max_rows, max_age)
            if self._urgent():
                self._send_rows(urgent=True)
