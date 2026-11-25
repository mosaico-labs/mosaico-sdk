import hashlib
import io
import json
import random
import statistics
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from PIL import Image as PILImage

import mosaico_rs as m

PY = sys.executable
SCRIPT = "bench/cmp/bench_py.py"
RUST = "target/release/examples/bench_cmp"
OUT = Path("target/bench-cmp")
RUNS = 3
CLASSES = {
    "imu": "IMU",
    "imu_random": "IMU",
    "image_720p": "Image",
    "image_480p": "Image",
    "image_noise_720p": "Image",
    "jpeg_720p": "CompressedImage",
    "blob_mixed": "CompressedImage",
}
TOPICS = {
    "imu": "/imu",
    "imu_random": "/imu",
    "image_720p": "/camera/image_raw",
    "image_480p": "/camera/image_raw",
    "image_noise_720p": "/camera/image_raw",
    "jpeg_720p": "/camera/image_compressed",
    "blob_mixed": "/blob",
}
MULTI = [("imu", 200_000), ("image_720p", 200), ("image_noise_720p", 200), ("jpeg_720p", 1_600)]
RANDOM_BATCHES = {"rand_imu": (1, 10_000, 1_000_000), "rand_img": (1, 16, 200), "rand_blob": (1, 8, 300)}

WRITES = [
    ("imu", 1_000_000, "1000", ["sdk", "rs", "rust"]),
    ("imu", 1_000_000, "@rand_imu", ["sdk", "rs", "rust"]),
    ("imu_random", 1_000_000, "@rand_imu", ["sdk", "rs", "rust"]),
    ("imu", 10_000, "1", ["sdk", "sdk-push", "rs", "rust"]),
    ("imu_random", 10_000, "1", ["sdk-push", "rs", "rust"]),
    ("image_720p", 200, "1", ["sdk", "rs", "rust"]),
    ("image_noise_720p", 200, "1", ["sdk", "rs", "rust"]),
    ("image_noise_720p", 200, "@rand_img", ["sdk", "rs", "rust"]),
    ("jpeg_720p", 3_000, "1", ["sdk", "rs", "rust"]),
    ("blob_mixed", 300, "1", ["sdk", "rs", "rust"]),
    ("blob_mixed", 300, "@rand_blob", ["sdk", "rs", "rust"]),
    ("multi", 0, "", ["sdk", "rs", "rs-threads", "rust", "rust-tasks"]),
    ("imu", 100_000, "1", ["sdk-push", "rs-push", "rust-push"]),
    ("imu_random", 100_000, "1", ["sdk-push", "rs-push", "rust-push"]),
    ("jpeg_720p", 3_000, "1", ["sdk-push", "rs-push", "rust-push"]),
    ("image_720p", 200, "1", ["sdk-push", "rs-push", "rust-push"]),
    ("blob_mixed", 300, "1", ["sdk-push", "rs-push", "rust-push"]),
    ("image_720p", 200, "1", ["sdk-png", "rs-png", "rust-png"]),
    ("image_720p", 200, "32", ["rs-png", "rust-png"]),
    ("image_480p", 200, "1", ["sdk-png", "rs-png", "rust-png"]),
    ("image_480p", 200, "32", ["rs-png", "rust-png"]),
]

READS = [
    ("imu", 1_000_000, "rows", None, ["sdk", "rs", "rust"]),
    ("imu", 1_000_000, "batches", None, ["rs", "rust"]),
    ("imu", 1_000_000, "pandas", None, ["rs"]),
    ("imu_random", 1_000_000, "rows", None, ["sdk", "rs", "rust"]),
    ("imu", 1_000_000, "windows", "win_imu", ["sdk", "rs", "rust"]),
    ("image_720p", 200, "rows", None, ["sdk", "rust"]),
    ("image_720p", 200, "batches", None, ["rs", "rust"]),
    ("image_noise_720p", 200, "rows", None, ["sdk", "rust"]),
    ("image_noise_720p", 200, "batches", None, ["rs", "rust"]),
    ("jpeg_720p", 3_000, "rows", None, ["sdk", "rust"]),
    ("jpeg_720p", 3_000, "batches", None, ["rs", "rust"]),
    ("blob_mixed", 300, "rows", None, ["sdk", "rust"]),
    ("blob_mixed", 300, "batches", None, ["rs", "rust"]),
]

DECODES = [
    ("image_720p", 200, "decode-rows", None, ["sdk", "rs", "rust"]),
    ("image_720p", 200, "decode-batches", None, ["rs", "rust"]),
    ("image_720p", 200, "decode-table", None, ["rs", "rust"]),
    ("image_480p", 200, "decode-rows", None, ["sdk", "rs", "rust"]),
    ("image_480p", 200, "decode-batches", None, ["rs", "rust"]),
    ("image_480p", 200, "decode-table", None, ["rs", "rust"]),
]


def call(args: list[str]) -> dict:
    done = subprocess.run(args, capture_output=True, text=True)
    if done.returncode != 0:
        return {"error": done.stderr.strip().splitlines()[-1][-300:] if done.stderr.strip() else "failed"}
    return json.loads(done.stdout.strip().splitlines()[-1])


def spec_arg(spec: str) -> str:
    return f"@{OUT / 'specs' / spec[1:]}.txt" if spec.startswith("@") else spec


def write_cmd(client: str, kind: str, rows: int, spec: str) -> list[str]:
    if kind == "multi":
        if client.startswith("rust"):
            return [RUST, "write-multi", client]
        return [PY, SCRIPT, "write-multi", client]
    if client == "rust":
        return [RUST, "write", kind, str(rows), spec_arg(spec)]
    if client == "rust-png":
        return [RUST, "write", kind, str(rows), spec_arg(spec), "png"]
    if client == "rust-push":
        return [RUST, "write", kind, str(rows), spec_arg(spec), "push"]
    if client in ("sdk-png", "rs-png"):
        return [PY, SCRIPT, "write", client[:-4], kind, str(rows), spec_arg(spec), "png"]
    if client in ("sdk-push", "rs-push"):
        return [PY, SCRIPT, "write", client[:-5], kind, str(rows), spec_arg(spec), "push"]
    return [PY, SCRIPT, "write", client, kind, str(rows), spec_arg(spec)]


def read_cmd(client: str, kind: str, sequence: str, op: str, window: str | None) -> list[str]:
    extra = [f"@{OUT / 'specs' / window}.txt"] if window else []
    if client == "rust":
        return [RUST, "read", kind, sequence, op, *extra]
    return [PY, SCRIPT, "read", client, kind, sequence, op, *extra]


def source(kind: str, rows: int) -> pa.Table:
    return pa.ipc.open_file(f"target/bench-data/{kind}.arrow").read_all().slice(0, rows).combine_chunks()


def values_digest(array: pa.ChunkedArray | pa.Array) -> str:
    return hashlib.sha256(np.ascontiguousarray(array.to_numpy(zero_copy_only=False)).tobytes()).hexdigest()


def fingerprint(table: pa.Table, kind: str) -> tuple:
    ts = table.column("timestamp_ns").to_numpy()
    digest = hashlib.sha256(ts.tobytes())
    if kind.startswith("imu"):
        acc = pc.struct_field(table.column("acceleration"), "z")
        cov = pc.list_flatten(pc.struct_field(table.column("orientation"), "covariance"))
        frame = pc.struct_field(table.column("header"), "frame_id")
        return (
            table.num_rows,
            digest.hexdigest(),
            values_digest(acc),
            values_digest(cov),
            table.column("orientation").null_count,
            table.column("header").null_count,
            pc.sum(pc.utf8_length(frame.cast(pa.string()))).as_py(),
        )
    for chunk in table.column("data").chunks:
        for value in chunk:
            digest.update(value.as_py())
    return (table.num_rows, digest.hexdigest())


def png_pixels(value: bytes) -> bytes:
    return np.asarray(PILImage.open(io.BytesIO(value))).tobytes()


def image_fingerprint(table: pa.Table, decode) -> tuple:
    digest = hashlib.sha256(table.column("timestamp_ns").to_numpy().tobytes())
    for value in table.column("data").to_pylist():
        digest.update(decode(value))
    meta = tuple(tuple(table.column(name).to_pylist()) for name in ("width", "height", "stride", "encoding"))
    return table.num_rows, digest.hexdigest(), meta


def verify(client: m.MosaicoClient, kind: str, sequence: str, rows: int, png: bool = False) -> bool:
    import mosaicolabs

    if png:
        handler = client.topic_handler(sequence, TOPICS[kind], ontology_type=mosaicolabs.Image)
        written = handler.to_arrow()
        return set(written.column("format").to_pylist()) == {"png"} and image_fingerprint(
            written, png_pixels
        ) == image_fingerprint(source(kind, rows), bytes)

    if kind == "multi":
        return all(
            fingerprint(client.topic_handler(sequence, f"/{k}", ontology_type=getattr(mosaicolabs, CLASSES[k])).to_arrow(), k)
            == fingerprint(source(k, n), k)
            for k, n in MULTI
        )
    handler = client.topic_handler(sequence, TOPICS[kind], ontology_type=getattr(mosaicolabs, CLASSES[kind]))
    return fingerprint(handler.to_arrow(), kind) == fingerprint(source(kind, rows), kind)


def write_specs() -> dict[str, int]:
    rng = np.random.default_rng(2026_11_20)
    folder = OUT / "specs"
    folder.mkdir(parents=True, exist_ok=True)
    for name, (lo, hi, total) in RANDOM_BATCHES.items():
        values: list[int] = []
        while sum(values) < total:
            values.append(int(np.exp(rng.uniform(np.log(lo), np.log(hi + 1)))))
        (folder / f"{name}.txt").write_text("\n".join(map(str, values)))
    ts = source("imu", 1_000_000).column("timestamp_ns").to_numpy()
    expected = 0
    pairs = []
    for _ in range(50):
        length = int(np.exp(rng.uniform(0, np.log(10_000))))
        start = int(rng.integers(0, len(ts) - length - 1))
        pairs.append(f"{ts[start]} {ts[start + length]}")
        expected += length
    (folder / "win_imu.txt").write_text("\n".join(pairs))
    return {"win_imu": expected}


def summarize(times: list[float]) -> dict:
    return {"median": statistics.median(times), "min": min(times), "max": max(times), "runs": times}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    only = sys.argv[1] if len(sys.argv) > 1 else "all"
    expected_windows = write_specs()
    order = random.Random(7)
    results = []
    with m.MosaicoClient.connect("localhost", 6728) as client:
        if only in ("all", "write"):
            for kind, rows, spec, clients in WRITES:
                times: dict[str, list[float]] = {c: [] for c in clients}
                cpus: dict[str, list[float]] = {c: [] for c in clients}
                checked: dict[str, bool] = {}
                errors: dict[str, str] = {}
                pushes: dict[str, int] = {}
                data_bytes: dict[str, int] = {}
                for run in range(RUNS):
                    for c in order.sample(clients, len(clients)):
                        if c in errors:
                            continue
                        out = call(write_cmd(c, kind, rows, spec))
                        if "error" in out:
                            errors[c] = out["error"]
                            continue
                        times[c].append(out["secs"])
                        cpus[c].append(out["cpu"])
                        pushes[c] = out.get("pushes", 0)
                        data_bytes[c] = out.get("data_bytes", 0)
                        if run == 0:
                            checked[c] = verify(client, kind, out["sequence"], rows, c.endswith("-png"))
                for c in clients:
                    row = {"op": "write", "kind": kind, "rows": rows, "batch": spec, "client": c}
                    if c in errors:
                        row["error"] = errors[c]
                    else:
                        row |= summarize(times[c]) | {
                            "cpu": statistics.median(cpus[c]),
                            "cpu_runs": cpus[c],
                            "pushes": pushes[c],
                            "data_bytes": data_bytes[c],
                            "data_ok": checked[c],
                        }
                    results.append(row)
                    print(json.dumps(row), flush=True)
        if only in ("all", "read", "decode"):
            sequences: dict[str, str] = {}
            png = only == "decode"
            for kind, rows, op, window, clients in DECODES if png else READS:
                if kind not in sequences:
                    if png:
                        out = call(write_cmd("rust-png", kind, rows, "32"))
                    else:
                        out = call(write_cmd("rust", kind, rows, "1000" if kind.startswith("imu") else "1"))
                    sequences[kind] = out["sequence"]
                    assert verify(client, kind, sequences[kind], rows, png), kind
                sequence = sequences[kind]
                times = {c: [] for c in clients}
                counts: dict[str, set] = {c: set() for c in clients}
                errors = {}
                for c in clients:
                    warm = call(read_cmd(c, kind, sequence, op, window))
                    if "error" in warm:
                        errors[c] = warm["error"]
                for _ in range(RUNS):
                    for c in order.sample(clients, len(clients)):
                        if c in errors:
                            continue
                        out = call(read_cmd(c, kind, sequence, op, window))
                        times[c].append(out["secs"])
                        counts[c].add((out["rows"], round(float(out["check"]), 3)))
                want = expected_windows[window] if window else rows
                for c in clients:
                    row = {"op": f"read-{op}", "kind": kind, "rows": want, "client": c}
                    if c in errors:
                        row["error"] = errors[c]
                    else:
                        row |= summarize(times[c]) | {
                            "results": sorted(counts[c]),
                            "rows_ok": all(r == want for r, _ in counts[c]),
                        }
                    results.append(row)
                    print(json.dumps(row), flush=True)
    (OUT / f"results-{only}.json").write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
