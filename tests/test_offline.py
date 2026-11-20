import socket
import threading
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from mosaicolabs import CompressedImage, Image, ImageFormat

import mosaico_rs as m
from mosaico_rs._schema import _type_problem, conform, served_schema
from mosaico_rs._writer import _timestamps


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_error_hierarchy() -> None:
    assert issubclass(m.ConnectError, ConnectionError)
    assert issubclass(m.InvalidArgumentError, ValueError)
    assert issubclass(m.StreamClosedError, ValueError)
    assert issubclass(m.SchemaError, ValueError)
    assert issubclass(m.PermissionDeniedError, PermissionError)
    for error in (
        m.ConnectError,
        m.SequenceNotFoundError,
        m.TopicNotFoundError,
        m.InvalidArgumentError,
        m.AlreadyExistsError,
        m.PermissionDeniedError,
        m.OntologyMismatchError,
        m.ServerError,
        m.StreamError,
        m.SchemaError,
        m.StreamClosedError,
    ):
        assert issubclass(error, m.MosaicoError)


def test_connect_errors() -> None:
    with pytest.raises(m.ConnectError):
        m.MosaicoClient.connect("127.0.0.1", free_port(), timeout=0.5)
    with pytest.raises(m.InvalidArgumentError):
        m.MosaicoClient.connect("localhost", 0)
    with pytest.raises(m.InvalidArgumentError):
        m.MosaicoClient.connect("localhost", 6726, timeout=0)
    with pytest.raises(m.InvalidArgumentError):
        m.MosaicoClient.connect("localhost", 6726, compression="brotli")


def test_timestamps_are_never_converted() -> None:
    assert _timestamps([1, 2]).type == pa.int64()
    assert _timestamps(np.array([1, 2], np.int64)).to_pylist() == [1, 2]
    rejected = (
        np.array(["2026-01-01"], "datetime64[us]"),
        [1.5, 2.5],
        np.array([1.0]),
        np.array([1], np.int32),
        pa.array([1], pa.int32()),
        "not timestamps",
    )
    for value in rejected:
        with pytest.raises(m.InvalidArgumentError):
            _timestamps(value)


def test_view_types_match_plain_types() -> None:
    assert _type_problem(pa.string(), pa.string_view(), "f", frozenset()) is None
    assert _type_problem(pa.binary(), pa.binary_view(), "f", frozenset()) is None
    assert _type_problem(pa.string(), pa.binary_view(), "f", frozenset()) is not None


def test_tls_cert_errors_match_sdk(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does not exist"):
        m.MosaicoClient.connect("localhost", free_port(), tls_cert_path=str(tmp_path / "missing.pem"))
    for path in ("", str(tmp_path)):
        with pytest.raises(m.InvalidArgumentError):
            m.MosaicoClient.connect("localhost", free_port(), tls_cert_path=path)


def test_tls_connections_send_the_client_hello() -> None:
    received: list[bytes] = []
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(4)
        server.settimeout(5)
        port = server.getsockname()[1]

        def accept() -> None:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(3)
                try:
                    received.append(conn.recv(5))
                except OSError:
                    received.append(b"")

        thread = threading.Thread(target=accept, daemon=True)
        thread.start()
        with pytest.raises(m.ConnectError):
            m.MosaicoClient.connect("127.0.0.1", port, timeout=1, enable_tls=True)
        thread.join(5)
    assert received and received[0][:1] == b"\x16"


def test_columns_without_data_use_the_server_types() -> None:
    served = served_schema(CompressedImage)
    assert served.field("data").type == pa.binary_view()
    assert served.field("format").type == pa.string_view()
    assert served.field("timestamp_ns").type == pa.int64()
    header = served.field("header").type
    assert isinstance(header, pa.StructType)
    assert header.field("frame_id").type == pa.string()
    expected = pa.schema([pa.field("a", pa.int64()), pa.field("s", pa.string())])
    assert conform(expected, pa.schema([pa.field("a", pa.int64())])).field("s").type == pa.string_view()


def test_png_helpers_match_the_sdk_layout() -> None:
    frame = (np.arange(30 * 40, dtype=np.uint32) % 251).astype(np.uint8).reshape(30, 40)
    image = m.image_from_linear_pixels(frame, stride=40, height=30, width=40, encoding="mono8")
    assert image.format is ImageFormat.PNG
    assert bytes(image.to_linear_pixels()) == frame.tobytes()
    assert m.image_from_linear_pixels(list(frame.tobytes()), 40, 30, 40, "mono8", format=ImageFormat.RAW).data == frame.tobytes()
    with pytest.raises(m.InvalidArgumentError):
        m.encode_png(frame, height=31, stride=40)
    with pytest.raises(m.InvalidArgumentError):
        m.image_from_linear_pixels(frame, 40, 30, 40, "mono8", format=ImageFormat.JPEG)

    payload = pa.schema(list(Image.__msco_pyarrow_struct__))
    rows = [m.image_from_linear_pixels(frame, 40, 30, 40, "mono8", format=ImageFormat.RAW).model_dump() for _ in range(3)]
    rows[1]["format"] = ImageFormat.JPEG
    rows[1]["data"] = b"jpeg"
    encoded = m.encode_png_batch(pa.RecordBatch.from_pylist(rows, schema=payload))
    assert encoded.column("format").to_pylist() == ["png", "jpeg", "png"]
    decoded = Image(**encoded.slice(2, 1).to_pylist()[0])
    assert bytes(decoded.to_linear_pixels()) == frame.tobytes()
    assert encoded.column("data")[1].as_py() == b"jpeg"


def test_png_decoding_matches_the_sdk() -> None:
    import io

    from PIL import Image as PILImage

    rng = np.random.default_rng(9)
    gray = rng.integers(0, 8, (30, 120), dtype=np.uint8)
    rgb = rng.integers(0, 255, (20, 16, 3), dtype=np.uint8)
    deep = rng.integers(0, 65535, (12, 10), dtype=np.uint16)
    small = (np.arange(5 * 13, dtype=np.uint8).reshape(5, 13) % 4) * 60
    images = [PILImage.fromarray(p) for p in (gray, rgb, deep)]
    images += [PILImage.fromarray(small).convert(mode) for mode in ("1", "LA", "RGBA")]
    options: list[dict[str, int]] = [{}] * len(images)
    for mode, bits in (("L", 2), ("L", 4), ("P", 1), ("P", 2), ("P", 4)):
        images.append(PILImage.fromarray(small).convert(mode))
        options.append({"bits": bits})
    for image, extra in zip(images, options):
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", **extra)
        sdk = Image(data=buffer.getvalue(), format=ImageFormat.PNG, width=1, height=1, stride=1, encoding="mono8")
        assert m.decode_png(buffer.getvalue()) == bytes(sdk.to_linear_pixels()), (image.mode, extra)
    with pytest.raises(m.InvalidArgumentError):
        m.decode_png(b"not a png")

    payload = pa.schema(list(Image.__msco_pyarrow_struct__))
    rows = [m.image_from_linear_pixels(gray, 120, 30, 40, "rgb8").model_dump() for _ in range(3)]
    rows[1]["format"] = ImageFormat.JPEG
    rows[1]["data"] = b"jpeg"
    decoded = m.decode_png_batch(pa.RecordBatch.from_pylist(rows, schema=payload))
    assert decoded.column("format").to_pylist() == ["raw", "jpeg", "raw"]
    assert decoded.column("data").to_pylist() == [gray.tobytes(), b"jpeg", gray.tobytes()]
    assert m.decode_png_batch(m.encode_png_batch(decoded)).column("data").to_pylist() == decoded.column("data").to_pylist()
    table = pa.Table.from_batches([pa.RecordBatch.from_pylist(rows, schema=payload)] * 4)
    decoded_table = m.decode_png_table(table)
    assert decoded_table.num_rows == 12
    assert decoded_table.column("data").to_pylist() == [gray.tobytes(), b"jpeg", gray.tobytes()] * 4


def test_png_threads_can_be_limited() -> None:
    import os
    import subprocess
    import sys

    script = (
        "import numpy as np, pyarrow as pa, mosaico_rs as m\n"
        "from mosaicolabs import Image\n"
        "f = np.zeros((4, 6), np.uint8)\n"
        "rows = [m.image_from_linear_pixels(f, 6, 4, 6, 'mono8', format='raw').model_dump()] * 3\n"
        "b = pa.RecordBatch.from_pylist(rows, schema=pa.schema(list(Image.__msco_pyarrow_struct__)))\n"
        "try:\n"
        "    print(m.encode_png_batch(b).column('format').to_pylist())\n"
        "except m.InvalidArgumentError as e:\n"
        "    print('error', e)\n"
    )
    for value, expected in (("2", "['png', 'png', 'png']"), ("zero", "error MOSAICO_PNG_THREADS")):
        env = {**os.environ, "MOSAICO_PNG_THREADS": value}
        out = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True)
        assert out.stdout.startswith(expected), out.stdout
