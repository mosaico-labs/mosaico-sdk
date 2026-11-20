import io
import struct as binary
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
from mosaicolabs import IMU, CompressedImage, Image
from PIL import Image as PILImage

T0 = 1_700_000_000_000_000_000
OUT = Path(sys.argv[1])
FRAMES = Path(sys.argv[2])
ONLY = set(sys.argv[3:])
IMU_ROWS = 1_000_000
IMAGE_ROWS = 200
JPEG_ROWS = 3000
BLOB_ROWS = 300
rng = np.random.default_rng(12345)


def wire(cls: type) -> pa.Schema:
    return pa.schema([pa.field("timestamp_ns", pa.int64(), nullable=False), *cls.__msco_pyarrow_struct__])


def mask(n: int, p: float) -> pa.Array | None:
    m = rng.random(n) < p
    return pa.array(m) if m.any() else None


def struct(type_: pa.StructType, n: int, values: dict, null_p: float = 0.0) -> pa.StructArray:
    arrays = [values.get(f.name, pa.nulls(n, f.type)) for f in type_]
    return pa.StructArray.from_arrays(arrays, fields=list(type_), mask=mask(n, null_p))


def timestamps(n: int, period_ns: int, jitter: float) -> np.ndarray:
    steps = period_ns * rng.uniform(1 - jitter, 1 + jitter, n)
    return T0 + np.cumsum(np.maximum(steps, 1)).astype(np.int64)


def header(type_: pa.StructType, ts: np.ndarray, frame_id: str, null_p: float) -> pa.StructArray:
    n = len(ts)
    stamp = type_.field("timestamp").type
    lag = rng.integers(0, 2_000_000, n)
    sensor = ts - lag
    return struct(
        type_,
        n,
        {
            "timestamp": struct(
                stamp,
                n,
                {
                    "seconds": pa.array((sensor // 1_000_000_000).astype(np.int32)),
                    "nanoseconds": pa.array((sensor % 1_000_000_000).astype(np.uint32)),
                },
            ),
            "frame_id": pa.array([frame_id] * n, pa.string()),
            "sample_counter": pa.array(np.arange(n, dtype=np.uint64), mask=rng.random(n) < 0.05),
        },
        null_p,
    )


def walk(n: int, scale: float, noise: float, bias: float = 0.0) -> np.ndarray:
    return bias + np.cumsum(rng.normal(0, scale, n)) * 0.01 + rng.normal(0, noise, n)


def covariance(n: int, null_p: float) -> pa.ListArray:
    offsets = pa.array(np.arange(0, 9 * n + 1, 9, dtype=np.int32))
    values = pa.array(np.abs(rng.normal(0, 0.01, 9 * n)))
    return pa.ListArray.from_arrays(offsets, values, mask=mask(n, null_p))


def vector(type_: pa.StructType, axes: dict[str, np.ndarray], null_p: float) -> pa.StructArray:
    n = len(next(iter(axes.values())))
    values: dict[str, pa.Array] = {a: pa.array(v) for a, v in axes.items()}
    values["covariance"] = covariance(n, 0.2)
    values["covariance_type"] = pa.array(rng.integers(0, 4, n).astype(np.int16), mask=rng.random(n) < 0.2)
    return struct(type_, n, values, null_p)


def imu(n: int) -> pa.Table:
    s = IMU.__msco_pyarrow_struct__
    ts = timestamps(n, 1_000_000, 0.4)
    q = rng.normal(0, 1, (4,)) + np.cumsum(rng.normal(0, 0.002, (n, 4)), axis=0)
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    columns = [
        pa.array(ts),
        header(s.field("header").type, ts, "imu_link", 0.01),
        vector(
            s.field("acceleration").type,
            {"x": walk(n, 0.5, 0.3), "y": walk(n, 0.5, 0.3), "z": walk(n, 0.2, 0.2, 9.81)},
            0.0,
        ),
        vector(
            s.field("angular_velocity").type,
            {"x": walk(n, 0.3, 0.05), "y": walk(n, 0.3, 0.05), "z": walk(n, 0.3, 0.05)},
            0.0,
        ),
        vector(s.field("orientation").type, {"x": q[:, 0], "y": q[:, 1], "z": q[:, 2], "w": q[:, 3]}, 0.1),
    ]
    return pa.Table.from_arrays(columns, schema=wire(IMU))


def random_strings(n: int, longest: int, null_p: float) -> pa.Array:
    lengths = rng.integers(0, longest + 1, n)
    offsets = np.concatenate([[0], np.cumsum(lengths)]).astype(np.int32)
    values = rng.integers(32, 127, int(offsets[-1]), dtype=np.uint8)
    valid = rng.random(n) >= null_p
    nulls = pa.array(valid).buffers()[1] if not valid.all() else None
    return pa.Array.from_buffers(pa.string(), n, [nulls, pa.py_buffer(offsets), pa.py_buffer(values)])


def random_covariance(n: int, null_p: float) -> pa.ListArray:
    offsets = pa.array(np.arange(0, 9 * n + 1, 9, dtype=np.int32))
    values = pa.array(rng.uniform(-1e6, 1e6, 9 * n))
    return pa.ListArray.from_arrays(offsets, values, mask=mask(n, null_p))


def random_vector(type_: pa.StructType, axes: list[str], n: int, null_p: float) -> pa.StructArray:
    values: dict[str, pa.Array] = {a: pa.array(rng.uniform(-1e6, 1e6, n)) for a in axes}
    values["covariance"] = random_covariance(n, 0.5)
    values["covariance_type"] = pa.array(rng.integers(-32768, 32767, n).astype(np.int16), mask=rng.random(n) < 0.5)
    return struct(type_, n, values, null_p)


def imu_random(n: int) -> pa.Table:
    s = IMU.__msco_pyarrow_struct__
    ts = timestamps(n, 1_000_000, 0.99)
    head = s.field("header").type
    stamp = head.field("timestamp").type
    header_array = struct(
        head,
        n,
        {
            "timestamp": struct(
                stamp,
                n,
                {
                    "seconds": pa.array(rng.integers(-(2**31), 2**31 - 1, n).astype(np.int32)),
                    "nanoseconds": pa.array(rng.integers(0, 1_000_000_000, n).astype(np.uint32)),
                },
            ),
            "frame_id": random_strings(n, 64, 0.3),
            "sample_counter": pa.array(rng.integers(0, 2**63 - 1, n).astype(np.uint64), mask=rng.random(n) < 0.5),
        },
        0.2,
    )
    columns = [
        pa.array(ts),
        header_array,
        random_vector(s.field("acceleration").type, ["x", "y", "z"], n, 0.0),
        random_vector(s.field("angular_velocity").type, ["x", "y", "z"], n, 0.0),
        random_vector(s.field("orientation").type, ["x", "y", "z", "w"], n, 0.5),
    ]
    return pa.Table.from_arrays(columns, schema=wire(IMU))


def noise_images(n: int, width: int, height: int) -> pa.Table:
    s = Image.__msco_pyarrow_struct__
    ts = timestamps(n, 33_333_333, 0.3)
    size = width * height * 3
    data = rng.integers(0, 256, n * size, dtype=np.uint8)
    offsets = np.arange(0, n * size + 1, size, dtype=np.int64)
    columns = [
        pa.array(ts),
        header(s.field("header").type, ts, "camera_noise", 0.0),
        pa.Array.from_buffers(pa.large_binary(), n, [None, pa.py_buffer(offsets), pa.py_buffer(data)]).cast(pa.binary()),
        pa.array(["raw"] * n, pa.string()),
        pa.array(np.full(n, width, dtype=np.int32)),
        pa.array(np.full(n, height, dtype=np.int32)),
        pa.array(np.full(n, width * 3, dtype=np.int32)),
        pa.array(["rgb8"] * n, pa.string()),
        pa.array([False] * n, pa.bool_()),
    ]
    return pa.Table.from_arrays(columns, schema=wire(Image))


def blobs(n: int, smallest: int, largest: int) -> pa.Table:
    s = CompressedImage.__msco_pyarrow_struct__
    ts = timestamps(n, 33_333_333, 0.9)
    sizes = np.exp(rng.uniform(np.log(smallest), np.log(largest), n)).astype(np.int64)
    offsets = np.concatenate([[0], np.cumsum(sizes)]).astype(np.int64)
    data = rng.integers(0, 256, int(offsets[-1]), dtype=np.uint8)
    columns = [
        pa.array(ts),
        header(s.field("header").type, ts, "blob", 0.5),
        pa.Array.from_buffers(pa.large_binary(), n, [None, pa.py_buffer(offsets), pa.py_buffer(data)]).cast(pa.binary()),
        pa.array(rng.choice(["jpeg", "png", "raw"], n).tolist(), pa.string()),
    ]
    return pa.Table.from_arrays(columns, schema=wire(CompressedImage))


def load_frames(path: Path) -> list[tuple[int, int, bytes]]:
    b = path.read_bytes()
    out, at = [], 0
    while at < len(b):
        h, s, n = binary.unpack_from("<III", b, at)
        out.append((h, s, b[at + 12 : at + 12 + n]))
        at += 12 + n
    return out


def order(n: int, k: int) -> list[int]:
    picks: list[int] = []
    while len(picks) < n:
        picks.extend(rng.permutation(k).tolist())
    return picks[:n]


def images(frames: list[tuple[int, int, bytes]], n: int, width: int) -> pa.Table:
    s = Image.__msco_pyarrow_struct__
    ts = timestamps(n, 33_333_333, 0.05)
    pick = order(n, len(frames))
    columns = [
        pa.array(ts),
        header(s.field("header").type, ts, "camera_color_optical_frame", 0.0),
        pa.array([frames[i][2] for i in pick], pa.binary()),
        pa.array(["raw"] * n, pa.string()),
        pa.array(np.full(n, width, dtype=np.int32)),
        pa.array(np.array([frames[i][0] for i in pick], dtype=np.int32)),
        pa.array(np.array([frames[i][1] for i in pick], dtype=np.int32)),
        pa.array(["rgb8"] * n, pa.string()),
        pa.array([False] * n, pa.bool_()),
    ]
    return pa.Table.from_arrays(columns, schema=wire(Image))


def jpegs(frames: list[tuple[int, int, bytes]], n: int, width: int) -> pa.Table:
    encoded = []
    for h, _, data in frames:
        out = io.BytesIO()
        PILImage.frombytes("RGB", (width, h), data).save(out, "JPEG", quality=90)
        encoded.append(out.getvalue())
    s = CompressedImage.__msco_pyarrow_struct__
    ts = timestamps(n, 33_333_333, 0.05)
    pick = order(n, len(encoded))
    columns = [
        pa.array(ts),
        header(s.field("header").type, ts, "camera_color_optical_frame", 0.0),
        pa.array([encoded[i] for i in pick], pa.binary()),
        pa.array(["jpeg"] * n, pa.string()),
    ]
    return pa.Table.from_arrays(columns, schema=wire(CompressedImage))


def save(name: str, table: pa.Table) -> None:
    with pa.ipc.new_file(OUT / f"{name}.arrow", table.schema) as w:
        for batch in table.to_batches(max_chunksize=4096):
            w.write_batch(batch)
    size = table.nbytes
    print(f"{name}: {table.num_rows} rows, {size / 1e6:.0f} MB in memory", flush=True)


def wanted(name: str) -> bool:
    return not ONLY or name in ONLY


OUT.mkdir(parents=True, exist_ok=True)
if wanted("imu"):
    save("imu", imu(IMU_ROWS))
if wanted("image_720p") or wanted("image_480p") or wanted("jpeg_720p"):
    arm = load_frames(FRAMES / "arm_color.frames")
    g2 = load_frames(FRAMES / "g2_color.frames")
    if wanted("image_720p"):
        save("image_720p", images(arm, IMAGE_ROWS, 1280))
    if wanted("image_480p"):
        save("image_480p", images(g2, IMAGE_ROWS, 640))
    if wanted("jpeg_720p"):
        save("jpeg_720p", jpegs(arm, JPEG_ROWS, 1280))
if wanted("imu_random"):
    save("imu_random", imu_random(IMU_ROWS))
if wanted("image_noise_720p"):
    save("image_noise_720p", noise_images(IMAGE_ROWS, 1280, 720))
if wanted("blob_mixed"):
    save("blob_mixed", blobs(BLOB_ROWS, 1 << 10, 16 << 20))
