import os
import statistics
import sys
import time
import uuid

sys.path.insert(0, "bench/cmp")
from bench_py import load, pushes
from mosaicolabs import IMU, SessionLevelErrorPolicy

from mosaico_rs import MosaicoClient

N = int(os.environ.get("N", "10000"))
LOOPS = int(os.environ.get("LOOPS", "5"))
PORT = int(os.environ.get("SINK_PORT", "6799"))

batches = pushes(load("imu", N), "1")
with MosaicoClient.connect("127.0.0.1", PORT) as client:
    results = []
    for _ in range(LOOPS):
        name = f"sink_{uuid.uuid4().hex[:8]}"
        with client.sequence_create(name, {}, on_error=SessionLevelErrorPolicy.Delete) as seq:
            writer = seq.topic_create("/imu", {}, IMU)
            started = time.perf_counter()
            for ts, data in batches:
                writer.push_batch(ts, data)
            results.append((time.perf_counter() - started) / len(batches) * 1e6)
    runs = ", ".join(f"{r:.1f}" for r in results)
    print(f"binding push_batch, 1 row, against the sink: {statistics.median(results):.1f} us/push (runs: {runs})")
