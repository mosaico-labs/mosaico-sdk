from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar, TypeVar, final

import pyarrow as pa
from mosaicolabs import IMU, CompressedImage, Header, Image, ImageFormat, Quaternion, Serializable, Time, Vector3d
from pydantic import ValidationError

from ._errors import SchemaError


_SPARSE = 4


class Usage:
    __slots__ = ("calls", "rows", "sparse")

    def __init__(self) -> None:
        self.calls = 0
        self.rows = 0
        self.sparse = False

    def start(self, rows: int) -> None:
        self.sparse = self.rows > 0 and self.calls * _SPARSE < self.rows
        self.calls = 0
        self.rows = rows


class Columns:
    def __init__(self, array: pa.StructArray, path: str, timestamps: list[int], usage: Usage | None = None) -> None:
        self._array = array
        self._path = path
        self._timestamps = timestamps
        self._usage = usage
        self._valid: list[bool | None] | None = (
            None if array.null_count == 0 else array.is_valid().to_pylist()
        )
        self._raw: dict[str, pa.Array] = {}
        self._dicts: list[dict[str, Any]] | None = None
        self._binary: list[str] = []
        self._last: tuple[str, int, Any] | None = None

    def _field(self, name: str) -> pa.Array:
        array = self._raw.get(name)
        if array is None:
            array = self._raw[name] = self._array.field(name)
        return array

    def _scalar(self, name: str, row: int) -> Any:
        last = self._last
        if last is not None and last[0] == name and last[1] == row:
            return last[2]
        value = self._field(name)[row].as_py()
        self._last = (name, row, value)
        return value

    def _size(self, name: str, row: int) -> int | None:
        scalar = self._field(name)[row]
        return scalar.as_buffer().size if scalar.is_valid else None

    def _whole(self) -> bool:
        usage = self._usage
        if usage is None:
            return True
        usage.calls += 1
        return self._dicts is not None or not usage.sparse or usage.calls * _SPARSE >= usage.rows

    def _row(self, row: int, model: pa.StructType) -> dict[str, Any]:
        rows = self._dicts
        if rows is None:
            rows = self._dicts = self._rows(model)
        values = rows[row]
        if self._binary:
            values = {**values, **{name: self._scalar(name, row) for name in self._binary}}
        return values

    def _rows(self, model: pa.StructType) -> list[dict[str, Any]]:
        present = self._array.type
        fields = [f for f in model if present.get_field_index(f.name) >= 0]
        self._binary = [f.name for f in fields if _is_binary(present.field(f.name).type)]
        kept = [f for f in fields if f.name not in self._binary]
        children = [_project(self._array.field(f.name), f.type) for f in kept]
        return pa.RecordBatch.from_arrays(children, names=[f.name for f in kept]).to_pylist()

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        child = self._array.field(name)
        if isinstance(child, pa.StructArray):
            path = f"{self._path}.{name}" if self._path else name
            value: Any = Columns(child, path, self._timestamps)
        elif isinstance(child.type, pa.ListType):
            value = [None if v is None else tuple(v) for v in child.to_pylist()]
        else:
            value = child.to_pylist()
        setattr(self, name, value)
        return value


def _project(array: pa.Array, model: pa.DataType) -> pa.Array:
    if not (isinstance(model, pa.StructType) and isinstance(array, pa.StructArray)):
        return array
    present = array.type
    fields = [f for f in model if present.get_field_index(f.name) >= 0]
    originals = [array.field(f.name) for f in fields]
    children = [_project(child, f.type) for child, f in zip(originals, fields)]
    if len(fields) == present.num_fields and all(a is b for a, b in zip(children, originals)):
        return array
    mask = array.is_null() if array.null_count else None
    return pa.StructArray.from_arrays(children, names=[f.name for f in fields], mask=mask)


def _is_binary(t: pa.DataType) -> bool:
    return pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t)


def columns(batch: pa.RecordBatch, timestamps: list[int], usage: Usage | None = None) -> Columns:
    if usage is not None:
        usage.start(batch.num_rows)
    return Columns(batch.to_struct_array(), "", timestamps, usage)


def _null(columns: Columns, name: str, row: int) -> SchemaError:
    path = f"{columns._path}.{name}" if columns._path else name
    return SchemaError(f"null in required field {path} at timestamp_ns={columns._timestamps[row]}")


def _required(columns: Columns, name: str, row: int) -> Any:
    value = getattr(columns, name)[row]
    if value is None:
        raise _null(columns, name, row)
    return value


def _bytes(columns: Columns, name: str, row: int) -> bytes:
    value = columns._scalar(name, row)
    if value is None:
        raise _null(columns, name, row)
    return value


def _image_format(columns: Columns, row: int) -> ImageFormat:
    value = _required(columns, "format", row)
    try:
        return ImageFormat(value)
    except ValueError:
        path = f"{columns._path}.format" if columns._path else "format"
        raise SchemaError(
            f"unknown image format {value!r} in {path} at timestamp_ns={columns._timestamps[row]}"
        ) from None


def _struct(parent: Columns, name: str, row: int) -> Columns | None:
    child: Columns = getattr(parent, name)
    valid = child._valid
    return child if valid is None or valid[row] else None


class _View:
    __slots__ = ()
    _FIELDS: ClassVar[tuple[str, ...]] = ()
    _BINARY: ClassVar[frozenset[str]] = frozenset()

    def __repr__(self) -> str:
        parts: list[str] = []
        for name in self._FIELDS:
            if name in self._BINARY:
                size = self._binary_size(name)
                parts.append(f"{name}=<SchemaError>" if size is None else f"{name}=<{size} bytes>")
                continue
            try:
                value = getattr(self, name)
            except Exception as e:
                parts.append(f"{name}=<{type(e).__name__}>")
                continue
            parts.append(f"{name}=<{len(value)} bytes>" if isinstance(value, bytes) else f"{name}={value!r}")
        return f"{type(self).__name__}({', '.join(parts)})"

    def _binary_size(self, name: str) -> int | None:
        return None


@final
class TimeView(_View):
    __slots__ = ("_c", "_i")
    _FIELDS = tuple(Time.model_fields)

    def __init__(self, columns: Columns, row: int) -> None:
        self._c = columns
        self._i = row

    @property
    def seconds(self) -> int:
        value = self._c.seconds[self._i]
        if value is None:
            raise _null(self._c, "seconds", self._i)
        return value

    @property
    def nanoseconds(self) -> int:
        value = self._c.nanoseconds[self._i]
        if value is None:
            raise _null(self._c, "nanoseconds", self._i)
        return value


@final
class HeaderView(_View):
    __slots__ = ("_c", "_i")
    _FIELDS = tuple(Header.model_fields)

    def __init__(self, columns: Columns, row: int) -> None:
        self._c = columns
        self._i = row

    @property
    def timestamp(self) -> TimeView | None:
        c = _struct(self._c, "timestamp", self._i)
        return None if c is None else TimeView(c, self._i)

    @property
    def frame_id(self) -> str | None:
        return self._c.frame_id[self._i]

    @property
    def sample_counter(self) -> int | None:
        return self._c.sample_counter[self._i]


class _VectorView(_View):
    __slots__ = ("_c", "_i")

    def __init__(self, columns: Columns, row: int) -> None:
        self._c = columns
        self._i = row

    @property
    def x(self) -> float:
        value = self._c.x[self._i]
        if value is None:
            raise _null(self._c, "x", self._i)
        return value

    @property
    def y(self) -> float:
        value = self._c.y[self._i]
        if value is None:
            raise _null(self._c, "y", self._i)
        return value

    @property
    def z(self) -> float:
        value = self._c.z[self._i]
        if value is None:
            raise _null(self._c, "z", self._i)
        return value

    @property
    def header(self) -> HeaderView | None:
        c = _struct(self._c, "header", self._i)
        return None if c is None else HeaderView(c, self._i)

    @property
    def covariance(self) -> tuple[float, ...] | None:
        value = self._c.covariance[self._i]
        if value is not None and None in value:
            raise _null(self._c, "covariance", self._i)
        return value

    @property
    def covariance_type(self) -> int | None:
        return self._c.covariance_type[self._i]


@final
class Vector3dView(_VectorView):
    __slots__ = ()
    _FIELDS = tuple(Vector3d.model_fields)


@final
class QuaternionView(_VectorView):
    __slots__ = ()
    _FIELDS = tuple(Quaternion.model_fields)

    @property
    def w(self) -> float:
        value = self._c.w[self._i]
        if value is None:
            raise _null(self._c, "w", self._i)
        return value


@final
class IMUView(_View):
    __slots__ = ("_c", "_i")
    _FIELDS = tuple(IMU.model_fields)

    def __init__(self, columns: Columns, row: int) -> None:
        self._c = columns
        self._i = row

    @property
    def acceleration(self) -> Vector3dView:
        c = self._c.acceleration
        if c._valid is not None and not c._valid[self._i]:
            raise _null(self._c, "acceleration", self._i)
        return Vector3dView(c, self._i)

    @property
    def angular_velocity(self) -> Vector3dView:
        c = self._c.angular_velocity
        if c._valid is not None and not c._valid[self._i]:
            raise _null(self._c, "angular_velocity", self._i)
        return Vector3dView(c, self._i)

    @property
    def orientation(self) -> QuaternionView | None:
        c = _struct(self._c, "orientation", self._i)
        return None if c is None else QuaternionView(c, self._i)

    @property
    def header(self) -> HeaderView | None:
        c = _struct(self._c, "header", self._i)
        return None if c is None else HeaderView(c, self._i)


@final
class ImageView(_View):
    __slots__ = ("_c", "_i")
    _FIELDS = tuple(Image.model_fields)
    _BINARY = frozenset({"data"})

    def __init__(self, columns: Columns, row: int) -> None:
        self._c = columns
        self._i = row

    def _binary_size(self, name: str) -> int | None:
        return self._c._size(name, self._i)

    @property
    def header(self) -> HeaderView | None:
        c = _struct(self._c, "header", self._i)
        return None if c is None else HeaderView(c, self._i)

    @property
    def data(self) -> bytes:
        return _bytes(self._c, "data", self._i)

    @property
    def format(self) -> ImageFormat:
        return _image_format(self._c, self._i)

    @property
    def width(self) -> int:
        return _required(self._c, "width", self._i)

    @property
    def height(self) -> int:
        return _required(self._c, "height", self._i)

    @property
    def stride(self) -> int:
        return _required(self._c, "stride", self._i)

    @property
    def encoding(self) -> str:
        return _required(self._c, "encoding", self._i)

    @property
    def is_bigendian(self) -> bool | None:
        return self._c.is_bigendian[self._i]


@final
class CompressedImageView(_View):
    __slots__ = ("_c", "_i")
    _FIELDS = tuple(CompressedImage.model_fields)
    _BINARY = frozenset({"data"})

    def __init__(self, columns: Columns, row: int) -> None:
        self._c = columns
        self._i = row

    def _binary_size(self, name: str) -> int | None:
        return self._c._size(name, self._i)

    @property
    def header(self) -> HeaderView | None:
        c = _struct(self._c, "header", self._i)
        return None if c is None else HeaderView(c, self._i)

    @property
    def data(self) -> bytes:
        return _bytes(self._c, "data", self._i)

    @property
    def format(self) -> ImageFormat:
        return _image_format(self._c, self._i)


def _time(v: TimeView | None) -> Time | None:
    if v is None:
        return None
    return Time(seconds=v.seconds, nanoseconds=v.nanoseconds)


def _header(v: HeaderView | None) -> Header | None:
    if v is None:
        return None
    return Header(timestamp=_time(v.timestamp), frame_id=v.frame_id, sample_counter=v.sample_counter)


def _covariance(v: _VectorView) -> list[float] | None:
    cov = v.covariance
    return None if cov is None else list(cov)


def _vector3d(v: Vector3dView) -> Vector3d:
    return Vector3d(
        x=v.x,
        y=v.y,
        z=v.z,
        header=_header(v.header),
        covariance=_covariance(v),
        covariance_type=v.covariance_type,
    )


def _quaternion(v: QuaternionView | None) -> Quaternion | None:
    if v is None:
        return None
    return Quaternion(
        x=v.x,
        y=v.y,
        z=v.z,
        w=v.w,
        header=_header(v.header),
        covariance=_covariance(v),
        covariance_type=v.covariance_type,
    )


def imu_model(v: IMUView) -> IMU:
    return IMU(
        header=_header(v.header),
        acceleration=_vector3d(v.acceleration),
        angular_velocity=_vector3d(v.angular_velocity),
        orientation=_quaternion(v.orientation),
    )


def image_model(v: ImageView) -> Image:
    return Image(
        header=_header(v.header),
        data=v.data,
        format=v.format,
        width=v.width,
        height=v.height,
        stride=v.stride,
        encoding=v.encoding,
        is_bigendian=v.is_bigendian,
    )


def compressed_image_model(v: CompressedImageView) -> CompressedImage:
    return CompressedImage(header=_header(v.header), data=v.data, format=v.format)


M = TypeVar("M", bound=Serializable)


def validated(model: type[M], build: Callable[[Any], M]) -> Callable[[Any], M]:
    validate = model.__pydantic_validator__.validate_python
    fields = pa.struct(list(model.__msco_pyarrow_struct__))

    def fast(view: Any) -> M:
        c = view._c
        if not c._whole():
            return build(view)
        try:
            return validate(c._row(view._i, fields))
        except ValidationError:
            return build(view)

    return fast
