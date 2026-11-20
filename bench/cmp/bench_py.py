import json
import os
import sys
import threading
import time
import uuid

import pyarrow as pa

KINDS = {
    "imu": ("IMU", "/imu"),
    "imu_random": ("IMU", "/imu"),
    "image_720p": ("Image", "/camera/image_raw"),
    "image_480p": ("Image", "/camera/image_raw"),
    "image_noise_720p": ("Image", "/camera/image_raw"),
    "jpeg_720p": ("CompressedImage", "/camera/image_compressed"),
    "blob_mixed": ("CompressedImage", "/blob"),
}
MULTI = [("imu", 200_000, "1000"), ("image_720p", 200, "1"), ("image_noise_720p", 200, "1"), ("jpeg_720p", 1_600, "8")]
PORT = int(os.environ.get("MOSAICO_PORT", "6728"))
DATA = os.environ.get("BENCH_DATA", "target/bench-data")


def load(kind: str, rows: int) -> pa.Table:
    table = pa.ipc.open_file(f"{DATA}/{kind}.arrow").read_all().slice(0, rows)
    return table.combine_chunks()


def ontology(name: str):
    import mosaicolabs

    return getattr(mosaicolabs, name)


def connect(client_name: str):
    if client_name == "sdk":
        from mosaicolabs import MosaicoClient
    else:
        from mosaico_rs import MosaicoClient
    return MosaicoClient.connect("localhost", PORT)


def sizes(spec: str, total: int) -> list[int]:
    if spec.startswith("@"):
        with open(spec[1:]) as f:
            pattern = [int(v) for v in f.read().split()]
    else:
        pattern = [int(spec)]
    out: list[int] = []
    covered = 0
    while covered < total:
        k = min(pattern[len(out) % len(pattern)], total - covered)
        out.append(k)
        covered += k
    return out


def pushes(table: pa.Table, spec: str) -> list[tuple[pa.Array, pa.RecordBatch]]:
    payload = pa.schema(list(table.schema)[1:])
    out = []
    start = 0
    for k in sizes(spec, table.num_rows):
        b = table.slice(start, k).to_batches()[0]
        out.append((b.column(0), pa.RecordBatch.from_arrays(b.columns[1:], schema=payload)))
        start += k
    return out


def touch(kind: str, data) -> float:
    if kind.startswith("imu"):
        return data.acceleration.z
    return float(len(data.data))


def write(client_name: str, kind: str, rows: int, spec: str, mode: str) -> dict:
    from mosaicolabs import SessionLevelErrorPolicy

    tag, topic = KINDS[kind]
    cls = ontology(tag)
    table = load(kind, rows)
    batches = pushes(table, spec)
    messages = []
    frames = []
    if mode == "push":
        from mosaicolabs import Message

        for row in table.to_pylist():
            ts = row.pop("timestamp_ns")
            messages.append(Message(timestamp_ns=ts, data=cls(**row)))
    elif mode == "png":
        from mosaicolabs import Message
        from mosaicolabs.models.sensors.image import ImageFormat

        frames = table.select(["timestamp_ns", "data", "width", "height", "stride", "encoding"]).to_pylist()
        if client_name != "sdk":
            import mosaico_rs

            encode = mosaico_rs.image_from_linear_pixels if spec == "1" else None
    sequence = f"cmp_{client_name}_{kind}_{uuid.uuid4().hex[:8]}"
    pushes_done = len(batches)
    data_bytes = 0
    with connect(client_name) as client:
        started = time.perf_counter()
        cpu = time.process_time()
        with client.sequence_create(sequence, {}, on_error=SessionLevelErrorPolicy.Delete) as seq:
            writer = seq.topic_create(topic, {}, cls)
            assert writer is not None
            if mode == "push":
                for message in messages:
                    writer.push(message)
            elif mode == "png" and client_name != "sdk" and encode is None:
                for ts, data in batches:
                    encoded = mosaico_rs.encode_png_batch(data)
                    data_bytes += sum(len(v) for v in encoded.column("data").to_pylist())
                    writer.push_batch(ts, encoded)
            elif mode == "png" and client_name != "sdk":
                for frame in frames:
                    image = encode(
                        frame["data"],
                        frame["stride"],
                        frame["height"],
                        frame["width"],
                        frame["encoding"],
                        is_bigendian=False,
                    )
                    data_bytes += len(image.data)
                    writer.push(Message(timestamp_ns=frame["timestamp_ns"], data=image))
                pushes_done = len(frames)
            elif mode == "png":
                for frame in frames:
                    image = cls.from_linear_pixels(
                        data=frame["data"],
                        stride=frame["stride"],
                        height=frame["height"],
                        width=frame["width"],
                        encoding=frame["encoding"],
                        is_bigendian=False,
                        format=ImageFormat.PNG,
                    )
                    data_bytes += len(image.data)
                    writer.push(Message(timestamp_ns=frame["timestamp_ns"], data=image))
                pushes_done = len(frames)
            else:
                for ts, data in batches:
                    writer.push_batch(ts, data)
        elapsed = time.perf_counter() - started
        cpu = time.process_time() - cpu
    return {
        "sequence": sequence,
        "secs": elapsed,
        "cpu": cpu,
        "rows": table.num_rows,
        "pushes": pushes_done,
        "data_bytes": data_bytes,
    }


def push_all(writer, batches, errors: list[BaseException]) -> None:
    try:
        for ts, data in batches:
            writer.push_batch(ts, data)
    except BaseException as e:
        errors.append(e)


def write_multi(client_name: str) -> dict:
    from mosaicolabs import SessionLevelErrorPolicy

    plans = [(f"/{kind}", ontology(KINDS[kind][0]), pushes(load(kind, rows), spec)) for kind, rows, spec in MULTI]
    sequence = f"cmp_{client_name.replace('-', '_')}_multi_{uuid.uuid4().hex[:8]}"
    errors: list[BaseException] = []
    with connect("sdk" if client_name == "sdk" else "rs") as client:
        started = time.perf_counter()
        cpu = time.process_time()
        with client.sequence_create(sequence, {}, on_error=SessionLevelErrorPolicy.Delete) as seq:
            writers = [seq.topic_create(name, {}, cls) for name, cls, _ in plans]
            if client_name == "rs-threads":
                threads = [
                    threading.Thread(target=push_all, args=(w, batches, errors))
                    for w, (_, _, batches) in zip(writers, plans)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                if errors:
                    raise errors[0]
            else:
                for step in range(max(len(batches) for _, _, batches in plans)):
                    for w, (_, _, batches) in zip(writers, plans):
                        if step < len(batches):
                            w.push_batch(*batches[step])
        elapsed = time.perf_counter() - started
        cpu = time.process_time() - cpu
    rows = sum(rows for _, rows, _ in MULTI)
    return {"sequence": sequence, "secs": elapsed, "cpu": cpu, "rows": rows, "pushes": sum(len(p) for _, _, p in plans)}


def windows(path: str) -> list[tuple[int, int]]:
    with open(path) as f:
        values = [int(v) for v in f.read().split()]
    return list(zip(values[::2], values[1::2]))


def read(client_name: str, kind: str, sequence: str, op: str, bounds: list[str]) -> dict:
    tag, topic = KINDS[kind]
    cls = ontology(tag)
    if bounds and bounds[0].startswith("@"):
        ranges = windows(bounds[0][1:])
    elif bounds:
        ranges = [(int(bounds[0]), int(bounds[1]))]
    else:
        ranges = [(None, None)]
    count, acc = 0, 0.0
    with connect(client_name) as client:
        started = time.perf_counter()
        if client_name == "sdk":
            handler = client.topic_handler(sequence, topic)
            assert handler is not None
            if op not in ("rows", "windows", "decode-rows"):
                raise SystemExit("the current SDK has no batch read")
            for start, end in ranges:
                kwargs = {} if start is None else {"start_timestamp_ns": start, "end_timestamp_ns": end}
                for msg in handler.get_data_streamer(**kwargs):
                    if op == "decode-rows":
                        acc += len(msg.data.to_linear_pixels())
                    else:
                        acc += touch(kind, msg.data)
                    count += 1
        else:
            handler = client.topic_handler(sequence, topic, ontology=cls)
            for start, end in ranges:
                if op in ("rows", "windows"):
                    for msg in handler.get_data_streamer(start, end):
                        acc += touch(kind, msg.data)
                        count += 1
                elif op == "batches":
                    for b in handler.get_batch_streamer(start, end):
                        count += b.num_rows
                elif op == "decode-rows":
                    import mosaico_rs

                    for b in handler.get_batch_streamer(start, end):
                        for value in b.column("data"):
                            acc += len(mosaico_rs.decode_png(value.as_buffer()))
                        count += b.num_rows
                elif op == "decode-batches":
                    import pyarrow.compute as pc
                    import mosaico_rs

                    for b in handler.get_batch_streamer(start, end):
                        decoded = mosaico_rs.decode_png_batch(b)
                        acc += pc.sum(pc.binary_length(decoded.column("data"))).as_py()
                        count += b.num_rows
                elif op == "decode-table":
                    import pyarrow.compute as pc
                    import mosaico_rs

                    decoded = mosaico_rs.decode_png_table(handler.to_arrow(start, end))
                    acc += pc.sum(pc.binary_length(decoded.column("data"))).as_py()
                    count += decoded.num_rows
                elif op == "arrow":
                    count += handler.to_arrow(start, end).num_rows
                elif op == "pandas":
                    count += len(handler.to_pandas(start, end))
                else:
                    raise SystemExit(f"unknown op {op}")
        elapsed = time.perf_counter() - started
    return {"secs": elapsed, "rows": count, "check": acc}


def main() -> None:
    command, client_name = sys.argv[1:3]
    if command == "write":
        kind, rows, spec = sys.argv[3], int(sys.argv[4]), sys.argv[5]
        mode = sys.argv[6] if len(sys.argv) > 6 else "batch"
        out = write(client_name, kind, rows, spec, mode)
    elif command == "write-multi":
        out = write_multi(client_name)
    else:
        kind, sequence, op = sys.argv[3:6]
        out = read(client_name, kind, sequence, op, sys.argv[6:])
    print(json.dumps(out))


if __name__ == "__main__":
    main()
