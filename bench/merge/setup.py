import sys

import numpy as np
import pyarrow as pa
from mosaicolabs import IMU, CompressedImage, Image, SessionLevelErrorPolicy

import mosaico_rs as m

DATA = "target/bench-data"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 6728


def load(kind: str) -> pa.Table:
    return pa.ipc.open_file(f"{DATA}/{kind}.arrow").read_all().combine_chunks()


def shifted(table: pa.Table, rows: int, offset_ns: int, first: int = 0) -> pa.Table:
    part = table.take(pa.array((first + np.arange(rows)) % table.num_rows))
    ts = pa.array(table.column("timestamp_ns").to_numpy()[:rows] + np.int64(offset_ns))
    return part.set_column(0, "timestamp_ns", ts).combine_chunks()


def plans() -> dict[str, list[tuple[str, type, pa.Table, int]]]:
    imu = load("imu")
    jpeg = load("jpeg_720p")
    raw = load("image_720p")
    out: dict[str, list[tuple[str, type, pa.Table, int]]] = {}
    for k in (1, 2, 4, 8, 16):
        n = 1_000_000 // k
        out[f"bm_imu_k{k}"] = [(f"/imu_{j}", IMU, shifted(imu, n, j + 1), 1000) for j in range(k)]
    out["bm_mix_jpeg"] = [("/imu", IMU, shifted(imu, 60_000, 0), 1000)] + [
        (f"/cam_{j}", CompressedImage, shifted(jpeg, 1_800, 1_000_000 * j + 17), 1) for j in range(4)
    ]
    out["bm_mix_raw"] = [("/imu", IMU, shifted(imu, 3_400, 0), 1000)] + [
        (f"/raw_{j}", Image, shifted(raw, 100, 4_000_000 * j + 17, 25 * j), 1) for j in range(8)
    ]
    return out


def exists(client: m.MosaicoClient, name: str, topics: list[str]) -> bool:
    try:
        seq = client.sequence_handler(name)
    except Exception:
        return False
    return sorted(seq.topics) == sorted(topics)


def write(client: m.MosaicoClient, name: str, plan: list[tuple[str, type, pa.Table, int]]) -> None:
    with client.sequence_create(name, {}, on_error=SessionLevelErrorPolicy.Delete) as seq:
        for topic, cls, table, step in plan:
            writer = seq.topic_create(topic, {}, cls)
            payload = pa.schema(list(table.schema)[1:])
            for start in range(0, table.num_rows, step):
                b = table.slice(start, step).to_batches()[0]
                writer.push_batch(b.column(0), pa.RecordBatch.from_arrays(b.columns[1:], schema=payload))


def main() -> None:
    with m.MosaicoClient.connect("localhost", PORT) as client:
        for name, plan in plans().items():
            if exists(client, name, [t for t, _, _, _ in plan]):
                print(name, "exists", flush=True)
                continue
            write(client, name, plan)
            rows = sum(t.num_rows for _, _, t, _ in plan)
            print(name, "written", len(plan), "topics", rows, "rows", flush=True)


if __name__ == "__main__":
    main()
