from __future__ import annotations

import datetime
import logging
import operator
import os
from collections.abc import Iterator
from typing import Any, TypeVar

from mosaicolabs import (
    GRPCCompression,
    GRPCCompressionAlgorithm,
    SequenceOrder,
    SequencePage,
    Serializable,
    SessionLevelErrorPolicy,
)
from mosaicolabs.comm.notifications import Notification, NotificationType
from mosaicolabs.platform.server_config import SemVerItem, ServerConfig, ServerInfo

from . import _core
from ._errors import InvalidArgumentError
from ._reader import SequenceHandler, TopicHandler, open_topic
from ._writer import SequenceUpdater, SequenceWriter

T = TypeVar("T", bound=Serializable)

_TLS_CERT_ENV = "MOSAICO_TLS_CERT_FILE"
_API_KEY_ENV = "MOSAICO_API_KEY"

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


def _page_limit(label: str, value: Any) -> int:
    try:
        limit = operator.index(value)
    except TypeError:
        raise InvalidArgumentError(f"{label} must be an int, not {type(value).__name__}") from None
    if limit < 0:
        raise InvalidArgumentError(f"{label} must be >= 0, got {limit}")
    if limit > 2**32 - 1:
        raise InvalidArgumentError(f"{label} must be at most {2**32 - 1}, got {limit}")
    return limit


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
        read_window_size: int | None = None,
        max_streams_per_connection: int | None = None,
    ) -> MosaicoClient:
        core = _core.Client.connect(
            host,
            port,
            float(timeout),
            enable_tls,
            _tls_cert(tls_cert_path),
            api_key,
            _compression(compression),
            read_window_size,
            max_streams_per_connection,
        )
        return cls(core)

    @classmethod
    def from_env(cls, host: str, port: int, timeout: float = 5) -> MosaicoClient:
        tls_cert = os.environ.get(_TLS_CERT_ENV)
        return cls.connect(
            host,
            port,
            timeout=timeout,
            enable_tls=tls_cert is not None,
            tls_cert_path=tls_cert,
            api_key=os.environ.get(_API_KEY_ENV),
        )

    def __enter__(self) -> MosaicoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def info(self) -> ServerInfo:
        version, (major, minor, patch, pre), (decode, encode, target) = self._core.info()
        return ServerInfo(
            version=version,
            semver=SemVerItem(major=major, minor=minor, patch=patch, pre=pre),
            config=ServerConfig(
                grpc_max_decode_message_size=decode,
                grpc_max_encode_message_size=encode,
                grpc_target_encode_message_size=target,
            ),
        )

    @property
    def max_batch_size_bytes(self) -> int:
        return self._core.max_batch_size_bytes

    def sequence_handler(self, sequence_name: str) -> SequenceHandler:
        return SequenceHandler(self._core, self._core.sequence_handler(sequence_name))

    def topic_handler(self, sequence_name: str, topic_name: str, *, ontology_type: type[T]) -> TopicHandler[T]:
        return open_topic(lambda: self._core.topic_handler(sequence_name, topic_name), ontology_type)

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

    def list_sequences_page(
        self,
        limit: int = 50,
        cursor: bytes | bytearray | memoryview | None = None,
        order_by: SequenceOrder | None = None,
    ) -> SequencePage:
        limit = _page_limit("limit", limit)
        if cursor is not None:
            cursor = memoryview(cursor).tobytes()
        if cursor is not None and order_by is not None:
            raise InvalidArgumentError("order_by can't be set with a cursor, which carries its order")
        if order_by is not None and not isinstance(order_by, SequenceOrder):
            raise InvalidArgumentError(f"order_by must be a SequenceOrder or None, not {order_by!r}")
        names, next_cursor = self._core.list_sequences_page(
            limit, cursor, None if order_by is None else order_by.value
        )
        return SequencePage(names=names, next_cursor=next_cursor)

    def iter_sequences(
        self,
        page_size: int = 50,
        order_by: SequenceOrder = SequenceOrder.CreatedAt,
    ) -> Iterator[str]:
        page = self.list_sequences_page(limit=page_size, order_by=order_by)
        yield from page.names
        while page.next_cursor is not None:
            page = self.list_sequences_page(limit=page_size, cursor=page.next_cursor)
            yield from page.names

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
