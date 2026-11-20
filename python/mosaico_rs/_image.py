from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa
from mosaicolabs import Image, ImageFormat
from typing_extensions import Buffer

from . import _core
from ._errors import InvalidArgumentError

_ENCODABLE = (ImageFormat.PNG, ImageFormat.RAW)


def _bytes_view(data: Buffer) -> memoryview:
    try:
        return memoryview(data).cast("B")
    except (TypeError, ValueError) as e:
        raise InvalidArgumentError(f"image data must be a contiguous buffer: {e}") from None


def encode_png(data: Buffer, height: int, stride: int) -> bytes:
    return _core.encode_png(_bytes_view(data), height, stride)


def encode_png_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    return pa.record_batch(_core.encode_png_batch(batch))


def decode_png(data: Buffer) -> bytes:
    return _core.decode_png(_bytes_view(data))


def decode_png_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    return pa.record_batch(_core.decode_png_batch(batch))


def decode_png_table(table: pa.Table) -> pa.Table:
    batches = [pa.record_batch(b) for b in _core.decode_png_batches(list(table.to_batches()))]
    if not batches:
        return table
    return pa.Table.from_batches(batches)


def _image_format(value: ImageFormat | str | None) -> ImageFormat:
    try:
        target = ImageFormat(value or ImageFormat.PNG)
    except ValueError:
        target = None
    if target is None or target not in _ENCODABLE:
        raise InvalidArgumentError(f"invalid image format {value!r}: supported formats are 'png' and 'raw'")
    return target


def image_from_linear_pixels(
    data: Buffer | Iterable[int],
    stride: int,
    height: int,
    width: int,
    encoding: str,
    is_bigendian: bool | None = None,
    format: ImageFormat | str | None = ImageFormat.PNG,
) -> Image:
    target = _image_format(format)
    pixels = data if isinstance(data, Buffer) else bytes(data)
    payload = encode_png(pixels, height, stride) if target is ImageFormat.PNG else bytes(pixels)
    return Image(
        data=payload,
        format=target,
        width=width,
        height=height,
        stride=stride,
        is_bigendian=is_bigendian,
        encoding=encoding,
    )
