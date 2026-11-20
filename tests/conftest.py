import os
import uuid
from collections.abc import Iterator

import numpy as np
import pyarrow as pa
import pytest
from mosaicolabs import IMU

import mosaico_rs as m

T0 = 1_700_000_000_000_000_000
STEP_NS = 1_000_000
PAYLOAD = pa.schema(list(IMU.__msco_pyarrow_struct__))
PORT = int(os.environ.get("MOSAICO_PORT", "6726"))


def imu_rows(start: int, n: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for i in range(start, start + n):
        rows.append(
            {
                "header": {"timestamp": {"seconds": i, "nanoseconds": 7}, "frame_id": "imu", "sample_counter": i},
                "acceleration": {"x": float(i), "y": 0.5, "z": 9.81},
                "angular_velocity": {"x": 0.1, "y": 0.2, "z": float(-i)},
                "orientation": None if i % 2 else {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
            }
        )
    return rows


def imu_batch(start: int, n: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(imu_rows(start, n), schema=PAYLOAD)


def timestamps(start: int, n: int) -> pa.Array:
    return pa.array(T0 + np.arange(start, start + n, dtype=np.int64) * STEP_NS)


def write_imu(writer: m.TopicWriter, n: int, batch: int = 1000) -> None:
    for start in range(0, n, batch):
        k = min(batch, n - start)
        writer.push_batch(timestamps(start, k), imu_batch(start, k))


@pytest.fixture(scope="session")
def client() -> Iterator[m.MosaicoClient]:
    try:
        c = m.MosaicoClient.connect("localhost", PORT, timeout=2)
    except m.ConnectError:
        pytest.skip(f"no mosaicod on localhost:{PORT}")
    with c:
        yield c


@pytest.fixture
def name(client: m.MosaicoClient) -> Iterator[str]:
    value = f"pytest_{uuid.uuid4().hex[:12]}"
    yield value
    try:
        client.sequence_delete(value)
    except m.MosaicoError:
        pass
