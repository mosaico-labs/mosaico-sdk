# Benchmark

This compares the new SDK with the current one, writing to and reading from the same local mosaicod. There are three clients:

- **Current SDK**: the current Python SDK (`mosaicolabs`), pure Python.
- **New SDK, Python**: the Python side of the new SDK (`mosaico_rs`), backed by the Rust core.
- **New SDK, Rust**: the Rust core called directly.

How the numbers are taken:

- **Same conditions.** All three clients write the same data to the same mosaicod, so every difference comes from the client. Everything runs locally on a MacBook with Apple M5 (10 cores), with mosaicod in release and its default settings.
- **Time of a write**: from creating the sequence to the end of `finish`, which is how long the user waits.
- **Time of a read**: from opening the topic to the last row.
- **Runs.** Each case runs each client as a separate process, 3 times in random order. The tables show the median. Measured on 2026-11-20.
- **Check.** For each case, the first write of each client is read back and compared with the source data, so a fast but wrong write cannot pass. Every read checks the row count and a sum over the data.
- **Speedup** is the time of the current SDK divided by the time of the new SDK. Lower times are better.

## At a glance: writes

| Write | Current SDK | New SDK, Python | New SDK, Rust | Speedup (Python · Rust) |
|---|---:|---:|---:|---:|
| IMU, one message at a time (100 000 messages) | 4.71 s | 0.22 s | 0.14 s | **21× · 34×** |
| JPEG frames, one per write (3 000 frames) | 9.18 s | 0.70 s | 0.68 s | **13× · 13×** |
| IMU in blocks of 1 000 rows (1 M rows) | 8.04 s | 1.05 s | 1.04 s | **7.7× · 7.7×** |
| Raw camera frames encoded to PNG (200 × 1280×720) | 3.26 s | 1.33 s | 1.29 s | **2.5× · 2.5×** |
| Large images and blobs (2–13 MB per write) | | | | 1.1–2.3× |

- **One message at a time:** the client converts each message. The new SDK does it in Rust, about 1 µs per message instead of about 47 µs.
- **Small writes:** the current SDK sends each write to the server as its own message, and mosaicod has a fixed cost per message. The new SDK joins consecutive writes into large messages, up to about 45 MB.
- **PNG:** the current SDK encodes with Pillow, the new SDK in Rust. It is faster, and the files are about a third smaller.
- **Large images:** with several megabytes per write, most of the time is the server compressing and storing the data, the same for every client.
- **Same API from Python** as the current SDK.

## At a glance: reads

| Read | Current SDK | New SDK, Python | New SDK, Rust | Speedup (Python · Rust) |
|---|---:|---:|---:|---:|
| IMU, row by row (1 M rows) | 26.22 s | 0.33 s | 0.33 s | **79× · 79×** |
| PNG camera frames, read and decoded to pixels (200 × 1280×720) | 4.07 s | 0.48 s | 0.46 s | **8.5× · 8.8×** |
| IMU, 50 short time windows, row by row | 1.84 s | 0.42 s | 0.38 s | **4.4× · 4.8×** |
| Raw images, JPEG and blobs, as stored | | | | 1.0× |

- **Row by row:** the current SDK builds a Python object for every row. The new SDK reads the rows in place, over the data as it arrives.
- **PNG frames:** decoding is most of the time (reading the 200 frames alone takes 0.24 s). The new SDK decodes in Rust, on all cores.
- **Images, JPEG and blobs as stored:** every client reads them in the same time, because the server is the limit.

## Writes in detail

### Messages: `push(msg)`

One `push(msg)` per row, as most users write today. The messages are built before the timer starts, so the timer covers only the push and the upload. The current SDK and the new SDK from Python push the same `mosaicolabs` objects; Rust pushes the ontology structs of the Rust crate.

| Data | Messages | Current SDK | New SDK, Python | New SDK, Rust | Speedup (Python · Rust) |
|---|---:|---:|---:|---:|---:|
| IMU | 100 000 | 4.71 s | 0.22 s | 0.14 s | **21× · 34×** |
| IMU, random values | 100 000 | 4.62 s | 0.22 s | 0.13 s | **21× · 36×** |
| JPEG 1280×720 | 3 000 | 0.70 s | 0.72 s | 0.71 s | 1.0× · 1.0× |
| Raw image 1280×720 | 200 | 3.31 s | 3.34 s | 3.36 s | 1.0× · 1.0× |
| Blobs 1 KB–16 MB | 300 | 0.67 s | 0.70 s | 0.71 s | 1.0× · 0.9× |

All three clients buffer the messages of a topic and send them in large batches, so what differs is the cost of each message on the client. With IMU, that cost is the whole time for the current SDK: its client CPU is 4.66 s, against 0.11 s for the new SDK from Python and 0.03 s for Rust. With images, JPEG and blobs each message carries kilobytes or megabytes, so the cost per message does not matter and all three wait for the server.

From Python, `push(msg)` is fast for the ontologies the Rust crate knows: IMU, Image and CompressedImage. Every other ontology goes through the same Python conversion as the current SDK, at the same speed.

### Batches: `push_batch`

Each push is a `push_batch` of Arrow data. The rows are sorted by the size of a push.

| Data | Rows per push | MB per push | Current SDK | New SDK, Python | New SDK, Rust | Speedup (Python · Rust) |
|---|---|---:|---:|---:|---:|---:|
| JPEG 1280×720, 3 000 frames | 1 | 0.15 | 9.18 s | 0.70 s | 0.68 s | **13× · 13×** |
| IMU, 1 M rows | 1 000 | 0.40 | 8.04 s | 1.05 s | 1.04 s | **7.7× · 7.7×** |
| IMU, 1 M rows | random 1–10 000 | 0.49 | 6.98 s | 1.07 s | 1.03 s | **6.5× · 6.8×** |
| IMU, random values, 1 M rows | random 1–10 000 | 0.52 | 6.84 s | 1.11 s | 1.09 s | **6.2× · 6.3×** |
| Blobs 1 KB–16 MB, 300 | 1 | 2.1 | 1.43 s | 0.69 s | 0.62 s | 2.1× · 2.3× |
| Raw image 1280×720, 200 | 1 | 2.8 | 4.44 s | 3.30 s | 3.28 s | 1.3× · 1.4× |
| Noise image 1280×720, 200 | 1 | 2.8 | 1.25 s | 0.62 s | 0.58 s | 2.0× · 2.2× |
| Blobs 1 KB–16 MB, 300 | random 1–8 | 6.8 | 0.83 s | 0.66 s | 0.64 s | 1.3× · 1.3× |
| Noise image 1280×720, 200 | random 1–16 | 12.6 | 0.66 s | 0.62 s | 0.57 s | 1.1× · 1.2× |
| 4 topics in one sequence | mixed | 0.4–2.8 | 4.29 s | 3.57 s | 3.52 s | 1.2× · 1.2× |

The gain depends on the size of the pushes:
- **Small pushes:** mosaicod has a fixed cost of a few milliseconds for each message it stores. Thousands of small pushes are slow when each is its own message; the new SDK sends a few large ones.
- **Pushes of megabytes:** the messages are large for both clients, so the time is the server compressing and storing the data.
- **Raw frames vs noise of the same size:** raw frames gain less, because the server takes longer to compress them.

The 4 topics are IMU (200 000 rows, 1 000 per push), raw and noise images (200 each, 1 per push) and JPEG (1 600, 8 per push), written in round robin from one thread. Writing each topic from its own thread (Python) or task (Rust) takes the same time, 3.63 and 3.51 s. That is close to the time of the raw image topic alone, which is the limit.

<details>
<summary>Client CPU of the batch writes</summary>

CPU time of the client process (user + system, all threads) for the same writes. Below 1× the new SDK uses less CPU than the current one.

| Data | Rows per push | Current SDK | New SDK, Python | New SDK, Rust | New vs current (Python · Rust) |
|---|---|---:|---:|---:|---:|
| JPEG 1280×720 | 1 | 0.38 s | 0.22 s | 0.14 s | 0.6× · 0.4× |
| IMU | 1 000 | 0.20 s | 0.24 s | 0.13 s | 1.2× · 0.7× |
| IMU | random 1–10 000 | 0.18 s | 0.20 s | 0.13 s | 1.1× · 0.7× |
| IMU, random values | random 1–10 000 | 0.19 s | 0.24 s | 0.13 s | 1.3× · 0.7× |
| Blobs 1 KB–16 MB | 1 | 0.13 s | 0.30 s | 0.19 s | 2.3× · 1.5× |
| Raw image 1280×720 | 1 | 0.12 s | 0.24 s | 0.17 s | 2.0× · 1.4× |
| Noise image 1280×720 | 1 | 0.11 s | 0.25 s | 0.16 s | 2.3× · 1.5× |
| Blobs 1 KB–16 MB | random 1–8 | 0.10 s | 0.30 s | 0.18 s | 3.0× · 1.8× |
| Noise image 1280×720 | random 1–16 | 0.09 s | 0.22 s | 0.17 s | 2.4× · 1.9× |
| 4 topics in one sequence | mixed | 0.37 s | 0.80 s | 0.70 s | 2.2× · 1.9× |

With images and blobs the new SDK uses more CPU, because it copies data that the current SDK sends without copies. pyarrow's Flight client sends the buffers of each `push_batch` as they are. The new SDK copies the data in three places: when it joins the batches, when it encodes them as Arrow IPC, and when it builds the gRPC message. From Python it also copies each batch at push time, so the caller can reuse its buffers right away. It is still as fast as the current SDK or faster.

</details>

### Raw frames to PNG

The client gets raw frames and encodes them as PNG before writing, as the current SDK does by default for `Image`. The timer includes the encoding.

| Data | Current SDK | New SDK, Python (1 · 32 per push) | New SDK, Rust (1 · 32 per push) | Speedup, 1 per push (Python · Rust) |
|---|---:|---:|---:|---:|
| 200 frames 1280×720 | 3.26 s | 1.33 s · 1.40 s | 1.29 s · 1.24 s | **2.5× · 2.5×** |
| 200 frames 640×480 | 1.16 s | 0.48 s · 0.37 s | 0.47 s · 0.37 s | **2.4× · 2.5×** |

- **Current SDK:** `Image.from_linear_pixels(..., format=PNG)` (Pillow, zlib level 1) and `push(msg)` for each frame.
- **New SDK, 1 per push:** `image_from_linear_pixels(...)` and `push(msg)`, with the same signature as the current SDK.
- **New SDK, 32 per push:** `encode_png_batch` and `push_batch`; the frames of a batch are encoded on all cores.

With 32 frames per push, 640×480 finishes sooner (3.1× faster than the current SDK). 1280×720 gains nothing, because there the server is already the limit.

Every client writes the same layout (8-bit grayscale, height × stride, the "wide grayscale" of the current SDK), so each can read back what the others wrote.

<details>
<summary>Client CPU and PNG size</summary>

| Data | Current SDK | New SDK, Python (1 · 32 per push) | New SDK, Rust (1 · 32 per push) | PNG size, current · new SDK (% of raw) |
|---|---:|---:|---:|---:|
| 200 frames 1280×720 | 3.11 s | 0.89 s · 1.18 s | 0.84 s · 1.14 s | 67% · 43% |
| 200 frames 640×480 | 1.07 s | 0.27 s · 0.39 s | 0.27 s · 0.38 s | 69% · 47% |

With one frame per push the new SDK uses 3.5–4× less CPU than the current one. Its PNG files are also smaller, so the server receives about a third less data.

</details>

## Reads in detail

### Rows and batches

Each topic is written once and then read 3 times by each client, after one read to warm up. Every read checks the row count and a sum over one field of every row, so all clients read the same data.

| Data | How | Current SDK | New SDK, Python | New SDK, Rust | Speedup (Python · Rust) |
|---|---|---:|---:|---:|---:|
| IMU, 1 M rows | row by row | 26.22 s | 0.33 s | 0.33 s | **79× · 79×** |
| IMU, random values, 1 M rows | row by row | 23.83 s | 0.33 s | 0.32 s | **72× · 74×** |
| IMU, 50 windows, 50 439 rows | row by row | 1.84 s | 0.42 s | 0.38 s | **4.4× · 4.8×** |
| IMU, 1 M rows | Arrow batches | | 0.33 s | 0.33 s | |
| IMU, 1 M rows | pandas DataFrame | | 0.60 s | | |
| Raw image 1280×720, 200 | row by row | 1.19 s | 1.20 s | 1.20 s | 1.0× · 1.0× |
| Noise image 1280×720, 200 | row by row | 0.20 s | 0.19 s | 0.19 s | 1.0× · 1.0× |
| JPEG 1280×720, 3 000 | row by row | 0.20 s | 0.20 s | 0.20 s | 1.0× · 1.0× |
| Blobs 1 KB–16 MB, 300 | row by row | 0.30 s | 0.31 s | 0.30 s | 1.0× · 1.0× |

- **Windows:** 50 ranges of 1–10 000 rows at random positions. Each window is a separate request with a fixed cost on the server, so the gain is smaller.
- **Images, JPEG and blobs:** these take the same time with every client, because the time is the server reading and sending the data. From Python they are read as Arrow batches, since the new SDK has no row views for images yet.

### PNG frames: read and decode

Images are usually stored as PNG, so whoever reads them also decodes them back to raw pixels. The time covers reading 200 PNG frames and decoding every frame.

| Data | Current SDK | New SDK, Python (1 at a time · per batch · table) | New SDK, Rust (1 at a time · per batch · table) | Speedup, Python (1 at a time · table) |
|---|---:|---:|---:|---:|
| 200 frames 1280×720 | 4.07 s | 1.55 s · 1.59 s · 0.48 s | 1.52 s · 1.55 s · 0.46 s | **2.6× · 8.5×** |
| 200 frames 640×480 | 1.36 s | 0.52 s · 0.29 s · 0.17 s | 0.51 s · 0.29 s · 0.17 s | **2.6× · 7.9×** |

- **Current SDK:** `msg.data.to_linear_pixels()` for each frame (Pillow; it returns a list of ints).
- **1 at a time:** `decode_png` on each frame.
- **Per batch:** `decode_png_batch` on each batch of `get_batch_streamer`.
- **Table:** `decode_png_table(handler.to_arrow())` in Python, `decode_png_batches` in Rust. Every frame of the topic is decoded on all cores, so the whole topic is held in memory.

mosaicod sends images one or two rows per batch. That leaves little to decode in parallel per batch, so only the table form uses all the cores.

All clients return the same bytes. Each read checks the total decoded size, and the offline tests compare `decode_png` with `to_linear_pixels` on every PNG type.

## Data

All data is generated by `bench/cmp/gen.py` with fixed seeds and saved as Arrow files in `target/bench-data`, so every client writes the same bytes:

- **IMU**: 1 M synthetic rows of the `IMU` ontology (header, acceleration, angular velocity, orientation, each with a 3×3 covariance). The values look like a real sensor at about 1 kHz: smooth random walks, gravity on z, normalized quaternions, few nulls.
- **IMU, random values**: the same schema and size, with uniform random values, random frame ids up to 64 characters, and many nulls. This is the worst case for compression and for null handling.
- **Raw image 1280×720**: 200 `Image` rows of raw RGB frames from the camera of the r2b `robotarm` bag (example data of the Mosaico docs).
- **Raw image 640×480**: 200 `Image` rows of raw RGB frames from the r2b `galileo2` bag.
- **Noise image 1280×720**: 200 `Image` rows of random bytes, which cannot be compressed.
- **JPEG 1280×720**: 3 000 `CompressedImage` rows, the `robotarm` frames as JPEG at quality 90.
- **Blobs 1 KB–16 MB**: 300 `CompressedImage` rows of random bytes, with sizes spread log-uniform.

"Random" rows per push are batch sizes drawn log-uniform with a fixed seed, saved in `target/bench-cmp/specs`, so every client pushes the same sequence.

## Method

- **Time:**
  - write: from sequence creation to the end of `finish`;
  - read: from opening the topic to the last row.
- **Client CPU:** user + system time of the client process, all threads, in the same interval.
- **Runs:** each case starts each client as a separate process, 3 times in random order, and keeps the medians.
- **Check:** the first write of each client is read back and compared with the source data, decoding the PNG frames, so a fast but wrong write cannot pass.
- **Scripts** (in `bench/cmp`):
  - `run.py` runs the cases;
  - `bench_py.py` does one write or read with the current SDK or the new SDK from Python;
  - `mosaico-ontology/examples/bench_cmp.rs` does the same in Rust;
  - `gen.py` and `extract_frames.py` build the data.

## Machine

- MacBook with Apple M5, 10 cores, 24 GB RAM, macOS 27.2.1.
- Everything local: mosaicod in release with its default settings, Postgres 16 in Docker, store on local disk (`file://`).

These are local numbers, with the server on the same machine. Over a real network the cost of each message is higher, so the gains on small pushes are likely to grow; the cases limited by the server should stay about the same.

## Reproduce

From the repo root, with `uv` and Docker:

```sh
# 1. Python environment and the new SDK
uv venv && source .venv/bin/activate
uv pip install maturin mosaicolabs pyarrow numpy pillow rosbags
maturin develop --release -m mosaico-py/Cargo.toml

# 2. Rust client, with PNG support
cargo build --release --example bench_cmp -p mosaico-ontology --features png

# 3. mosaicod, built into this repo's target
(cd ~/mosaico/mosaico/mosaicod && CARGO_TARGET_DIR=~/mosaico/mosaico-sdk/target/mosaicod \
  SQLX_OFFLINE=true cargo build --release --bin mosaicod)

# 4. Postgres: container db-devel from docker/devel/compose.yml of the mosaico repo,
#    plus a dedicated database
docker start db-devel || (cd ~/mosaico/mosaico/docker/devel && docker compose up -d database)
docker exec db-devel createdb -U postgres mosaico_bench

# 5. Start mosaicod on port 6728, store under target/bench-store
mkdir -p target/bench-store
(set -a; source ~/mosaico/mosaico/mosaicod/env.devel; set +a
 export MOSAICOD_DB_URL=postgresql://localhost:5432/mosaico_bench
 export MOSAICOD_STORE_ENDPOINT=file://$PWD/target/bench-store
 export MOSAICOD_PORT=6728
 cd target && ../target/mosaicod/release/mosaicod server) &

# 6. Data: frames from the r2b example bags, then the Arrow files
python bench/cmp/extract_frames.py <folder with r2b_*.mcap> target/frames
python bench/cmp/gen.py target/bench-data target/frames

# 7. Run (one JSON line per case, results in target/bench-cmp/results-<mode>.json)
python bench/cmp/run.py all      # writes and reads, about 30 minutes
python bench/cmp/run.py decode   # PNG read and decode
```

A single case on its own:

```sh
python bench/cmp/bench_py.py write sdk imu 1000000 1000        # current SDK, push_batch
python bench/cmp/bench_py.py write rs imu 1000000 1000         # new SDK from Python, push_batch
target/release/examples/bench_cmp write imu 1000000 1000       # new SDK in Rust, push_batch
python bench/cmp/bench_py.py write rs imu 100000 1 push        # new SDK from Python, push(msg)
target/release/examples/bench_cmp write imu 100000 1 push      # new SDK in Rust, push
python bench/cmp/bench_py.py write rs image_720p 200 1 png     # new SDK from Python, raw → PNG
target/release/examples/bench_cmp write image_720p 200 32 png  # new SDK in Rust, raw → PNG
```

`MOSAICO_PORT` changes the port of every client (default 6728).
