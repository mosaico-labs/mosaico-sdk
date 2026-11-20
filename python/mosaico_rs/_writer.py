from __future__ import annotations

import json
import logging
import threading
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
_log = logging.getLogger(__name__)


def _metadata(metadata: dict[str, Any] | None) -> bytes | None:
    if metadata is None:
        return None
    if not isinstance(metadata, dict):
        raise InvalidArgumentError(f"metadata must be a dict, not {type(metadata).__name__}")
    return json.dumps(metadata).encode("utf-8")


def _topic_key(name: str) -> str:
    return "/" + "/".join(part for part in name.split("/") if part not in ("", "."))


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
                self._finish(core)
                return
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
            topic = TopicWriter(writer, ontology, schema)
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


class TopicWriter:
    def __init__(
        self,
        core: _core.TopicWriter,
        ontology: type[Serializable],
        schema: pa.Schema,
    ) -> None:
        self._core = core
        self._ontology = ontology
        self._schema = schema
        self._payload = pa.schema(list(ontology.__msco_pyarrow_struct__))
        self._kind = _RECORD_KINDS.get(ontology)

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
            return self._core.handle_error(str(exc) or type(exc).__name__)
        except Exception as e:
            _log.error("cannot apply the error policy of topic '%s': %s", self.name, e)
            return False

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
        try:
            row = pa.RecordBatch.from_pylist([message._encode()], schema=self._schema)
        except Exception as e:
            raise self._core.reject(f"cannot convert the message: {e}") from None
        self._core.push_row(row, ipc.get_record_batch_size(row))

    def push_batch(self, timestamp_ns: pa.Array | Any, data: pa.RecordBatch) -> None:
        try:
            batch = self._wire_batch(timestamp_ns, data)
        except InvalidArgumentError as e:
            raise self._core.reject(str(e)) from None
        self._core.push_batch(batch)

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
        for batch in pa.RecordBatchReader.from_stream(stream):
            self._core.push_batch(batch)

    def flush(self) -> None:
        self._core.flush()

    def set_flush_policy(
        self,
        max_bytes: int | None = None,
        max_rows: int | None = None,
        max_age: float | None = None,
    ) -> None:
        self._core.set_flush_policy(max_bytes, max_rows, max_age)
