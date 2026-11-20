use std::error::Error;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use mosaico_core::{ConnectOptions, MosaicoClient};
use mosaico_ontology::Imu;

const TOPIC: &str = "/imu";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let sequence = args.next().unwrap_or_else(|| "bench_imu".to_owned());
    let runs: usize = args.next().map(|r| r.parse()).transpose()?.unwrap_or(5);

    let client = MosaicoClient::connect(ConnectOptions::new("localhost", 6726)).await?;

    let (count, total, elapsed) = read_once(&client, &sequence).await?;
    println!(
        "warmup: {count} rows in {:.2}s (sum z = {total:.6})",
        elapsed.as_secs_f64()
    );
    let mut times = Vec::with_capacity(runs);
    for i in 0..runs {
        let (count, _, elapsed) = read_once(&client, &sequence).await?;
        let secs = elapsed.as_secs_f64();
        println!(
            "run {}: {secs:.2}s, {:.0} msg/s",
            i + 1,
            count as f64 / secs
        );
        times.push(secs);
    }
    times.sort_by(f64::total_cmp);
    let median = times[times.len() / 2];
    println!(
        "rust median: {median:.2}s, {:.0} msg/s",
        count as f64 / median
    );
    Ok(())
}

async fn read_once(
    client: &MosaicoClient,
    sequence: &str,
) -> Result<(u64, f64, Duration), Box<dyn Error>> {
    let started = Instant::now();
    let seq = client.sequence_handler(sequence).await?;
    let topic = seq.topic_handler(TOPIC)?;
    let mut streamer = topic.get_data_streamer::<Imu>(None, None).await?;
    let mut count = 0u64;
    let mut total = 0.0;
    while let Some(msg) = streamer.next().await? {
        total += msg.view.acceleration()?.z()?;
        count += 1;
    }
    Ok((count, total, started.elapsed()))
}
