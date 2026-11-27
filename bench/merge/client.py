import ctypes
import json
import os
import resource
import sys
import threading
import time

import pyarrow as pa

HASH_MOD = (1 << 61) - 1
LOCK = threading.Lock()


class ZoneStats(ctypes.Structure):
    _fields_ = [
        ("blocks", ctypes.c_uint),
        ("in_use", ctypes.c_size_t),
        ("max_in_use", ctypes.c_size_t),
        ("allocated", ctypes.c_size_t),
    ]


libc = ctypes.CDLL(None)
libc.malloc_zone_statistics.argtypes = [ctypes.c_void_p, ctypes.POINTER(ZoneStats)]


def heap_bytes() -> int:
    stats = ZoneStats()
    libc.malloc_zone_statistics(None, ctypes.byref(stats))
    return stats.in_use + pa.total_allocated_bytes()


class Sampler(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.base = heap_bytes()
        self.peak = self.base
        self.done = threading.Event()

    def run(self) -> None:
        while not self.done.wait(0.001):
            self.peak = max(self.peak, heap_bytes())

    def stop(self) -> float:
        self.done.set()
        self.join()
        self.peak = max(self.peak, heap_bytes())
        return (self.peak - self.base) / 2**20


def busy() -> None:
    x = 0
    while True:
        x += 1


def rss_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20


def ranges(seq, spec: str) -> list[tuple[int | None, int | None]]:
    if spec == "-":
        return [(None, None)]
    if spec.startswith("mid:"):
        width = int(spec[4:])
        lo, hi = seq.timestamp_ns_min, seq.timestamp_ns_max
        mid = lo + (hi - lo) // 2
        return [(mid - width // 2, mid + width // 2)]
    with open(spec[1:]) as f:
        values = [int(v) for v in f.read().split()]
    return list(zip(values[::2], values[1::2]))


def touch(data, times: int) -> float:
    acc = getattr(data, "acceleration", None)
    if acc is not None:
        return acc.z
    total = 0
    for _ in range(times):
        total += len(data.data)
    return float(total)


def read(client_name: str, seq, topics: list[str], bounds, mode: str, check: bool, started: float, out: dict) -> None:
    index = {name: i for i, name in enumerate(topics)}
    rows = windows = 0
    touched = 0.0
    h = 0
    every = int(mode[6:]) if mode.startswith("sparse") else 0
    times = int(mode[4:]) if mode.startswith("data") else 1
    for start, end in bounds:
        if client_name == "sdk":
            stream = seq.get_data_streamer(topics=topics, start_timestamp_ns=start, end_timestamp_ns=end)
        else:
            stream = seq.get_data_streamer(topics, start, end)
        out.setdefault("open", time.perf_counter() - started)
        for topic, msg in stream:
            if "first" not in out:
                out["first"] = time.perf_counter() - started
            if check:
                name = topic if client_name == "sdk" else topic.name
                h = (h * 31 + msg.timestamp_ns + index[name]) % HASH_MOD
            if mode == "iter":
                pass
            elif every:
                if rows % every == 0:
                    msg.get_data()
            elif mode == "heavy" and client_name != "sdk":
                touched += touch(msg.get_data(), 1)
            else:
                touched += touch(msg.data, times)
            rows += 1
        if client_name != "sdk":
            windows += stream._seen - 1
    with LOCK:
        out["rows"] = out.get("rows", 0) + rows
        out["windows"] = out.get("windows", 0) + windows
        out["touched"] = out.get("touched", 0.0) + touched
        out["hash"] = str(h)


def main() -> None:
    client_name, port, sequence, topics, range_spec, mode, threads, check = sys.argv[1:9]
    topics_list = topics.split(",")
    check_run = check == "1"
    if client_name == "sdk":
        from mosaicolabs import MosaicoClient
    else:
        from mosaico_rs import MosaicoClient
    if os.environ.get("BENCH_BUSY") == "1":
        threading.Thread(target=busy, daemon=True).start()
    client = MosaicoClient.connect("localhost", int(port))
    sampler = Sampler() if check_run else None
    rss_before = rss_mib()
    if sampler:
        sampler.start()
    started = time.perf_counter()
    cpu = time.process_time()
    seq = client.sequence_handler(sequence)
    bounds = ranges(seq, range_spec)
    out: dict = {}
    errors: list[BaseException] = []

    def work() -> None:
        try:
            read(client_name, seq, topics_list, bounds, mode, check_run, started, out)
        except BaseException as e:
            errors.append(e)

    if int(threads) == 1:
        work()
    else:
        workers = [threading.Thread(target=work) for _ in range(int(threads))]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
    secs = time.perf_counter() - started
    cpu = time.process_time() - cpu
    peak = sampler.stop() if sampler else 0.0
    if errors:
        raise errors[0]
    print(
        json.dumps(
            {
                "secs": secs,
                "cpu": cpu,
                "rows": out["rows"],
                "windows": out["windows"],
                "touched": out["touched"],
                "hash": out["hash"],
                "open_ms": out.get("open", 0.0) * 1e3,
                "first_ms": out.get("first", 0.0) * 1e3,
                "peak_mib": peak,
                "rss_mib": rss_mib() - rss_before,
            }
        )
    )
    client.close()


if __name__ == "__main__":
    main()
