from __future__ import annotations

import datetime
import logging
from typing import Any, TypeVar

from mosaicolabs import GRPCCompression, GRPCCompressionAlgorithm, Serializable, SessionLevelErrorPolicy
from mosaicolabs.comm.notifications import Notification, NotificationType

from . import _core
from ._errors import InvalidArgumentError
from ._reader import SequenceHandler, TopicHandler, open_topic
from ._writer import SequenceUpdater, SequenceWriter

T = TypeVar("T", bound=Serializable)

_log = logging.getLogger(__name__)


def _compression(value: object) -> str | None:
    if isinstance(value, GRPCCompression):
        value = value.algorithm
    if value is None or value == GRPCCompressionAlgorithm.Null:
        return None
    if value == GRPCCompressionAlgorithm.StreamGzip:
        _log.warning("StreamGzip compression is not supported: the connection is not compressed")
        return None
    if value == GRPCCompressionAlgorithm.Gzip:
        return "gzip"
    if value in ("lz4", "zstd", "gzip"):
        return str(value)
    raise InvalidArgumentError(f"compression must be None, 'lz4', 'zstd' or 'gzip', not {value!r}")


def _tls_cert(path: str | None) -> bytes | None:
    if path is None:
        return None
    if not path:
        raise InvalidArgumentError("tls_cert_path cannot be empty")
    try:
        with open(path, "rb") as file:
            return file.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"tls certificate path '{path}' does not exist") from None
    except OSError as e:
        raise InvalidArgumentError(f"cannot read the tls certificate from {path}: {e}") from None


def _datetime(value: str) -> datetime.datetime:
    if value.endswith(" UTC"):
        return datetime.datetime.fromisoformat(value.replace(" UTC", "+00:00"))
    return datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.timezone.utc)


def _notifications(items: list[tuple[str, str | None, str, str, str]]) -> list[Notification]:
    return [
        Notification(
            sequence_name=sequence,
            topic_name=topic,
            type=NotificationType(kind),
            message=message,
            created_datetime=_datetime(created),
        )
        for sequence, topic, kind, message, created in items
    ]


class MosaicoClient:
    def __init__(self, core: _core.Client) -> None:
        self._core = core

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        timeout: float = 5,
        enable_tls: bool = False,
        compression: GRPCCompressionAlgorithm | GRPCCompression | str | None = None,
        tls_cert_path: str | None = None,
        api_key: str | None = None,
    ) -> MosaicoClient:
        core = _core.Client.connect(
            host,
            port,
            float(timeout),
            enable_tls,
            _tls_cert(tls_cert_path),
            api_key,
            _compression(compression),
        )
        return cls(core)

    def __enter__(self) -> MosaicoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def server_version(self) -> str:
        return self._core.server_version

    @property
    def max_batch_size_bytes(self) -> int:
        return self._core.max_batch_size_bytes

    def sequence_handler(self, sequence_name: str) -> SequenceHandler:
        return SequenceHandler(self._core, self._core.sequence_handler(sequence_name))

    def topic_handler(self, sequence_name: str, topic_name: str, *, ontology: type[T]) -> TopicHandler[T]:
        return open_topic(lambda: self._core.topic_handler(sequence_name, topic_name), ontology)

    def sequence_create(
        self,
        sequence_name: str,
        metadata: dict[str, Any],
        on_error: SessionLevelErrorPolicy = SessionLevelErrorPolicy.Report,
    ) -> SequenceWriter:
        return SequenceWriter(self._core, sequence_name, metadata, on_error)

    def sequence_update(
        self,
        sequence_name: str,
        on_error: SessionLevelErrorPolicy = SessionLevelErrorPolicy.Report,
    ) -> SequenceUpdater:
        return SequenceUpdater(self._core, sequence_name, on_error)

    def list_sequences(self) -> list[str]:
        return self._core.list_sequences()

    def sequence_delete(self, sequence_name: str) -> None:
        self._core.sequence_delete(sequence_name)

    def session_delete(self, locator: str) -> None:
        self._core.session_delete(locator)

    def sequence_exists(self, sequence_name: str) -> bool:
        return self._core.sequence_exists(sequence_name)

    def topic_exists(self, sequence_name: str, topic_name: str) -> bool:
        return self._core.topic_exists(sequence_name, topic_name)

    def list_sequence_notifications(self, sequence_name: str) -> list[Notification]:
        return _notifications(self._core.list_sequence_notifications(sequence_name))

    def clear_sequence_notifications(self, sequence_name: str) -> None:
        self._core.clear_sequence_notifications(sequence_name)

    def list_topic_notifications(self, sequence_name: str, topic_name: str) -> list[Notification]:
        return _notifications(self._core.list_topic_notifications(sequence_name, topic_name))

    def clear_topic_notifications(self, sequence_name: str, topic_name: str) -> None:
        self._core.clear_topic_notifications(sequence_name, topic_name)

    def close(self) -> None:
        self._core.close()
