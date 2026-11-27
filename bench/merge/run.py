import json
import os
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

PY = sys.executable
CLIENT = "bench/merge/client.py"
RUST = "target/release/examples/bench_merge"
OUT = Path("target/bench-merge")
RUNS = 3
PORT = int(os.environ.get("BENCH_PORT", "6728"))
R2B_PORT = int(os.environ.get("BENCH_R2B_PORT", "6730"))
PROXY_PORT = 6740
MID_5S = "mid:5000000000"
IMU_K = {k: ",".join(f"/imu_{j}" for j in range(k)) for k in (1, 2, 4, 8, 16)}
JPEG = {n: ",".join(["/imu"] + [f"/cam_{j}" for j in range(n)]) for n in (2, 4)}
RAW = {n: ",".join(["/imu"] + [f"/raw_{j}" for j in range(n)]) for n in (2, 4)}
RAW_ONLY = {k: ",".join(f"/raw_{j}" for j in range(k)) for k in (2, 4, 8)}
R2B = ["r2b_galileo", "r2b_galileo2", "r2b_robotarm", "r2b_whitetunnel"]


def case(label, seq, topics, clients, ranges="-", threads=1, port=PORT, env=None):
    return dict(label=label, seq=seq, topics=topics, clients=clients, ranges=ranges, threads=threads, port=port, env=env or {})


def windows_file() -> str:
    rng = np.random.default_rng(2026_11_26)
    path = OUT / "win_mix.txt"
    t0 = 1_700_000_000_000_000_000
    pairs = []
    for _ in range(50):
        length = int(np.exp(rng.uniform(np.log(100_000_000), np.log(5_000_000_000))))
        start = t0 + int(rng.integers(0, 60_000_000_000 - length))
        pairs.append(f"{start} {start + length}")
    path.write_text("\n".join(pairs))
    return f"@{path}"


def block_b() -> list[dict]:
    cases = []
    for k, topics in IMU_K.items():
        clients = ["rs:light", "rs:iter", "rust:light", "rust:raw", "rust:windows"]
        if k in (1, 4, 16):
            clients.insert(0, "sdk:light")
        if k in (2, 16):
            clients.append("rs-busy:light")
        cases.append(case(f"B1 IMU K={k}", f"bm_imu_k{k}", topics, clients))
    mixed = ["sdk:light", "rs:light", "rs:heavy", "rust:light", "rust:heavy", "rust:windows"]
    for n, topics in JPEG.items():
        cases.append(case(f"B2 IMU+{n} JPEG", "bm_mix_jpeg", topics, mixed))
    for n, topics in RAW.items():
        cases.append(case(f"B2 IMU+{n} raw", "bm_mix_raw", topics, mixed))
    cases.append(case("B3 IMU+2 JPEG, 50 windows", "bm_mix_jpeg", JPEG[2], ["sdk:light", "rs:light", "rust:light"], windows_file()))
    cases.append(case("B4 IMU+2 JPEG, 2 readers", "bm_mix_jpeg", JPEG[2], ["rs:light", "rust:light"], threads=2))
    cases.append(case("B4 IMU+2 raw, 2 readers", "bm_mix_raw", RAW[2], ["rs:light", "rust:light"], threads=2))
    return cases


def block_c() -> list[dict]:
    imu = ["rs:iter", "rs:light", "rs:heavy", "rs:sparse33", "rs-busy:light", "rust:light", "rust:heavy"]
    mix = ["rs:iter", "rs:data1", "rs:data2", "rs:data3", "rs:heavy", "rs:sparse33", "rust:light", "rust:heavy"]
    return [
        case("C IMU K=1", "bm_imu_k1", IMU_K[1], imu),
        case("C IMU+2 JPEG", "bm_mix_jpeg", JPEG[2], mix),
        case("C IMU+2 raw", "bm_mix_raw", RAW[2], mix),
    ]


def r2b_topics(seq_name: str) -> str:
    import mosaico_rs as m

    with m.MosaicoClient.connect("localhost", R2B_PORT) as client:
        seq = client.sequence_handler(seq_name)
        tags = {t: seq._core.topic_handler(t).ontology_tag for t in seq.topics}
    imu = [t for t in seq.topics if tags[t] == "IMU"]
    images = [t for t in seq.topics if tags[t] in ("Image", "CompressedImage")]
    return ",".join(imu + images)


def block_d() -> list[dict]:
    clients = ["sdk:light", "rs:light", "rs:heavy", "rust:light", "rust:heavy"]
    cases = []
    for seq in R2B:
        topics = r2b_topics(seq)
        cases.append(case(f"D {seq} full", seq, topics, clients, port=R2B_PORT))
        cases.append(case(f"D {seq} 5 s", seq, topics, clients, MID_5S, port=R2B_PORT))
    return cases


def block_e() -> list[dict]:
    cases = []
    for rtt in (0, 5, 20, 50):
        for k, topics in RAW_ONLY.items():
            port = PORT if rtt == 0 else PROXY_PORT
            env = {"BENCH_RTT_MS": str(rtt)}
            cases.append(case(f"E RTT {rtt} ms K={k}", "bm_mix_raw", topics, ["rust:raw"], port=port, env=env))
    proxy0 = {"BENCH_RTT_MS": "proxy0"}
    cases.append(case("E proxy 0 ms K=4", "bm_mix_raw", RAW_ONLY[4], ["rust:raw"], port=PROXY_PORT, env=proxy0))
    return cases


def command(c: dict, client: str, check: bool) -> tuple[list[str], dict]:
    name, mode = client.split(":")
    env = dict(os.environ, MallocLargeCache="0", **c["env"])
    tail = [str(c["port"]), c["seq"], c["topics"], c["ranges"], mode, str(c["threads"]), "1" if check else "0"]
    if name == "rust":
        return [RUST, *tail], env
    if name == "rs-busy":
        env["BENCH_BUSY"] = "1"
        name = "rs"
    return [PY, CLIENT, name, *tail], env


def call(c: dict, client: str, check: bool) -> dict:
    args, env = command(c, client, check)
    done = subprocess.run(args, capture_output=True, text=True, env=env)
    if done.returncode != 0:
        tail = done.stderr.strip().splitlines()
        return {"error": tail[-1][-300:] if tail else "failed"}
    return json.loads(done.stdout.strip().splitlines()[-1])


class Proxy:
    def __init__(self) -> None:
        self.rtt: str | None = None
        self.process: subprocess.Popen | None = None

    def ensure(self, c: dict) -> None:
        rtt = c["env"].get("BENCH_RTT_MS")
        if c["port"] != PROXY_PORT or rtt == self.rtt:
            return
        self.stop()
        delay = "0" if rtt == "proxy0" else str(rtt)
        self.process = subprocess.Popen([PY, "bench/merge/delay_proxy.py", str(PROXY_PORT), str(PORT), delay], stdout=subprocess.PIPE, text=True)
        assert self.process.stdout is not None and self.process.stdout.readline().strip() == "ready"
        self.rtt = rtt

    def stop(self) -> None:
        if self.process:
            self.process.terminate()
            self.process.wait()
        self.process, self.rtt = None, None


def med(values: list[float]) -> float:
    return statistics.median(values)


def run_case(c: dict, order: random.Random) -> list[dict]:
    clients = c["clients"]
    if os.environ.get("BENCH_NO_SDK") == "1":
        clients = [x for x in clients if not x.startswith("sdk")]
    call(c, next((x for x in clients if x.startswith("rust")), clients[0]), False)
    checks = {x: call(c, x, True) for x in clients}
    runs: dict[str, list[dict]] = {x: [] for x in clients if "error" not in checks[x]}
    for _ in range(RUNS):
        for x in order.sample(list(runs), len(runs)):
            out = call(c, x, False)
            if "error" in out:
                checks[x] = out
                runs.pop(x)
                break
            runs[x].append(out)
    ok = {x: checks[x] for x in runs if "error" not in checks[x] and not x.endswith(":windows")}
    reference = next(iter(ok.values()), {})
    rows = []
    for x in clients:
        row = {"case": c["label"], "client": x, "seq": c["seq"], "topics": c["topics"].count(",") + 1}
        if x not in runs or not runs[x]:
            row["error"] = checks[x].get("error", "failed")
        else:
            r, k = runs[x], checks[x]
            row |= {
                "secs": med([v["secs"] for v in r]),
                "secs_runs": [v["secs"] for v in r],
                "cpu": med([v["cpu"] for v in r]),
                "rows": k["rows"],
                "windows": k["windows"],
                "open_ms": med([v["open_ms"] for v in r]),
                "first_ms": med([v["first_ms"] for v in r]),
                "peak_mib": k["peak_mib"],
                "rss_mib": k["rss_mib"],
                "same_rows": k["rows"] == reference.get("rows"),
                "same_order": k["hash"] == reference.get("hash") or x.endswith(":windows"),
            }
        rows.append(row)
    return rows


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    blocks = {"B": block_b, "C": block_c, "D": block_d, "E": block_e}
    wanted = sys.argv[1:] or list(blocks)
    order = random.Random(11)
    proxy = Proxy()
    try:
        for name in wanted:
            started = time.perf_counter()
            results = []
            only = os.environ.get("BENCH_ONLY", "")
            for c in blocks[name]():
                if only not in c["label"]:
                    continue
                proxy.ensure(c)
                for row in run_case(c, order):
                    results.append(row)
                    print(json.dumps(row), flush=True)
            tag = os.environ.get("BENCH_TAG", "")
            (OUT / f"results-{name}{tag}.json").write_text(json.dumps(results, indent=1))
            print(f"block {name} done in {time.perf_counter() - started:.0f} s", flush=True)
    finally:
        proxy.stop()


if __name__ == "__main__":
    main()
