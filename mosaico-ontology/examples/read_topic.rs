use std::error::Error;
use std::process::ExitCode;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use mosaico_core::{ConnectOptions, MosaicoClient};
use mosaico_ontology::Imu;

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
    let client = MosaicoClient::connect(ConnectOptions::new("localhost", 6726)).await?;

    let seq = client.sequence_handler("r2b_galileo_0").await?;
    let imu = seq.topic_handler("/chassis/imu")?;
    let Some(t0) = imu.timestamp_ns_min() else {
        return Err("no time range: topic empty or not finalized".into());
    };
    let t1 = t0 + 60 * 1_000_000_000;

    let mut kept = Vec::new();
    let mut streamer = imu.get_data_streamer::<Imu>(Some(t0), Some(t1)).await?;
    while let Some(msg) = streamer.next().await? {
        if msg.data.acceleration()?.z()? > 15.0 {
            kept.push(msg.get_data()?);
        }
    }

    let mut streamer = imu.get_batch_streamer(Some(t0), Some(t1)).await?;
    while let Some(rb) = streamer.next_batch().await? {
        let ts = rb
            .column_by_name("timestamp_ns")
            .and_then(|c| c.as_primitive_opt::<Int64Type>())
            .ok_or("missing timestamp_ns")?;
        let z = rb
            .column_by_name("acceleration")
            .and_then(|c| c.as_struct_opt())
            .and_then(|s| s.column_by_name("z"))
            .and_then(|c| c.as_primitive_opt::<Float64Type>())
            .ok_or("missing acceleration.z")?;
        let max = z
            .iter()
            .zip(ts.iter())
            .filter_map(|(z, t)| Some((z?, t?)))
            .reduce(|best, row| {
                if row.0.total_cmp(&best.0).is_gt() {
                    row
                } else {
                    best
                }
            });
        if let Some((z, t)) = max {
            println!("{z} {t}");
        }
    }

    let _table = imu.to_arrow(Some(t0), Some(t1)).await?;
    Ok(())
}
