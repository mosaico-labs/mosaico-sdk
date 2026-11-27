use std::alloc::{GlobalAlloc, Layout, System};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use mosaico_core::{ConnectOptions, MosaicoClient, RowRef, SequenceHandler, TimestampRange};
use mosaico_ontology::{CompressedImage, Image, Imu};

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

fn grew(by: usize) {
    let now = CURRENT.fetch_add(by, Ordering::Relaxed) + by;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            if new_size > layout.size() {
                grew(new_size - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const HASH_MOD: u128 = (1 << 61) - 1;

#[derive(Default)]
struct Tally {
    rows: u64,
    windows: u64,
    touched: f64,
    hash: u128,
    open: Option<f64>,
    first: Option<f64>,
}

fn usage() -> (f64, f64) {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (
        secs(usage.ru_utime) + secs(usage.ru_stime),
        usage.ru_maxrss as f64 / f64::from(1 << 20),
    )
}

fn ranges(seq: &SequenceHandler, spec: &str) -> Result<Vec<TimestampRange>, Box<dyn Error>> {
    if spec == "-" {
        return Ok(vec![TimestampRange::ALL]);
    }
    if let Some(width) = spec.strip_prefix("mid:") {
        let width: i64 = width.parse()?;
        let lo = seq.timestamp_ns_min().ok_or("no time range")?.as_ns();
        let hi = seq.timestamp_ns_max().ok_or("no time range")?.as_ns();
        let mid = lo + (hi - lo) / 2;
        return Ok(vec![(mid - width / 2..mid + width / 2).into()]);
    }
    let path = spec
        .strip_prefix('@')
        .ok_or("ranges must be -, mid:<ns> or @file")?;
    let values = std::fs::read_to_string(path)?
        .split_whitespace()
        .map(str::parse)
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(values
        .chunks(2)
        .map(|pair| (pair[0]..pair[1]).into())
        .collect())
}

fn touch(row: &RowRef<'_>, heavy: bool) -> mosaico_core::Result<f64> {
    if let Some(v) = row.view::<Imu>()? {
        return match heavy {
            true => Ok(v.to_model()?.acceleration.z),
            false => v.acceleration()?.z(),
        };
    }
    if let Some(v) = row.view::<Image>()? {
        return match heavy {
            true => Ok(v.to_model()?.data.len() as f64),
            false => Ok(v.data()?.len() as f64),
        };
    }
    if let Some(v) = row.view::<CompressedImage>()? {
        return match heavy {
            true => Ok(v.to_model()?.data.len() as f64),
            false => Ok(v.data()?.len() as f64),
        };
    }
    Err(mosaico_core::Error::invalid_argument(
        "topic without a view",
    ))
}

async fn read(
    seq: SequenceHandler,
    topics: Vec<String>,
    ranges: Vec<TimestampRange>,
    mode: String,
    check: bool,
    started: Instant,
) -> Result<Tally, mosaico_core::Error> {
    let names: Vec<&str> = topics.iter().map(String::as_str).collect();
    let mut tally = Tally::default();
    for range in ranges {
        if mode == "windows" {
            let mut stream = seq.get_window_streamer(&names, range).await?;
            tally.open.get_or_insert(started.elapsed().as_secs_f64());
            while let Some(window) = stream.next_window().await? {
                tally.first.get_or_insert(started.elapsed().as_secs_f64());
                tally.windows += 1;
                tally.rows += window.order.len() as u64;
                tally.touched += window.batches.len() as f64;
            }
            continue;
        }
        let mut stream = seq.get_data_streamer(&names, range).await?;
        tally.open.get_or_insert(started.elapsed().as_secs_f64());
        while let Some(row) = stream.next().await? {
            tally.first.get_or_insert(started.elapsed().as_secs_f64());
            if check {
                let topic = names
                    .iter()
                    .position(|n| *n == row.topic().name())
                    .unwrap_or(usize::MAX);
                tally.hash =
                    (tally.hash * 31 + row.timestamp_ns.as_ns() as u128 + topic as u128) % HASH_MOD;
            }
            tally.touched += match mode.as_str() {
                "raw" => 0.0,
                "heavy" => touch(&row, true)?,
                _ => touch(&row, false)?,
            };
            tally.rows += 1;
        }
    }
    Ok(tally)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [port, sequence, topics, range_spec, mode, threads, check] = args.as_slice() else {
        return Err("usage: bench_merge PORT SEQUENCE TOPICS RANGES MODE THREADS CHECK".into());
    };
    let topics: Vec<String> = topics.split(',').map(str::to_owned).collect();
    let threads: usize = threads.parse()?;
    let check = check == "1";
    let client = MosaicoClient::connect(ConnectOptions::new("localhost", port.parse()?)).await?;
    let (cpu_before, rss_before) = usage();
    let heap_before = CURRENT.load(Ordering::Relaxed);
    PEAK.store(heap_before, Ordering::Relaxed);
    let started = Instant::now();
    let seq = client.sequence_handler(sequence).await?;
    let ranges = ranges(&seq, range_spec)?;
    let tasks: Vec<_> = (0..threads)
        .map(|_| {
            tokio::spawn(read(
                seq.clone(),
                topics.clone(),
                ranges.clone(),
                mode.clone(),
                check,
                started,
            ))
        })
        .collect();
    let mut total = Tally::default();
    for task in tasks {
        let tally = task.await??;
        total.rows += tally.rows;
        total.windows += tally.windows;
        total.touched += tally.touched;
        total.hash = tally.hash;
        total.open = total.open.or(tally.open);
        total.first = total.first.or(tally.first);
    }
    let secs = started.elapsed().as_secs_f64();
    let (cpu_after, rss_after) = usage();
    let heap = (PEAK.load(Ordering::Relaxed) - heap_before) as f64 / f64::from(1 << 20);
    println!(
        "{{\"secs\": {secs}, \"cpu\": {}, \"rows\": {}, \"windows\": {}, \"touched\": {}, \"hash\": \"{}\", \"open_ms\": {}, \"first_ms\": {}, \"peak_mib\": {heap}, \"rss_mib\": {}}}",
        cpu_after - cpu_before,
        total.rows,
        total.windows,
        total.touched,
        total.hash,
        total.open.unwrap_or(0.0) * 1e3,
        total.first.unwrap_or(0.0) * 1e3,
        rss_after - rss_before,
    );
    Ok(())
}
