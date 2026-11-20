from __future__ import annotations

from typing import Any, final

import pyarrow as pa
from mosaicolabs import IMU, Header, Quaternion, Time, Vector3d

from ._errors import SchemaError


class Columns:
    def __init__(self, array: pa.StructArray, path: str, timestamps: list[int]) -> None:
        self._array = array
        self._path = path
        self._timestamps = timestamps
        self._valid: list[bool | None] | None = (
            None if array.null_count == 0 else array.is_valid().to_pylist()
        )

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


def columns(batch: pa.RecordBatch, timestamps: list[int]) -> Columns:
    return Columns(batch.to_struct_array(), "", timestamps)


def _null(columns: Columns, name: str, row: int) -> SchemaError:
    path = f"{columns._path}.{name}" if columns._path else name
    return SchemaError(f"null in required field {path} at timestamp_ns={columns._timestamps[row]}")


def _struct(parent: Columns, name: str, row: int) -> Columns | None:
    child: Columns = getattr(parent, name)
    valid = child._valid
    return child if valid is None or valid[row] else None


@final
class TimeView:
    __slots__ = ("_c", "_i")

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
class HeaderView:
    __slots__ = ("_c", "_i")

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


class _VectorView:
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


@final
class QuaternionView(_VectorView):
    __slots__ = ()

    @property
    def w(self) -> float:
        value = self._c.w[self._i]
        if value is None:
            raise _null(self._c, "w", self._i)
        return value


@final
class IMUView:
    __slots__ = ("_c", "_i")

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
