use std::error::Error;
use std::fs::File;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray, new_null_array,
};
use arrow_ipc::reader::FileReader;
use arrow_select::concat::concat_batches;
use mosaico_core::{
    ConnectOptions, Encode, Format, MosaicoClient, Ontology, SessionErrorPolicy, TopicErrorPolicy,
    TopicHandler,
};
use mosaico_ontology::{
    CompressedImage, Image, Imu, Pixels, decode_png, decode_png_batch, decode_png_batches,
    encode_png_column,
};

const MULTI: [(&str, usize, &str); 4] = [
    ("imu", 200_000, "1000"),
    ("image_720p", 200, "1"),
    ("image_noise_720p", 200, "1"),
    ("jpeg_720p", 1_600, "8"),
];

type Range = (Option<i64>, Option<i64>);

fn kind(name: &str) -> Result<(&'static str, &'static str, Format), Box<dyn Error>> {
    Ok(match name {
        "imu" | "imu_random" => ("IMU", "/imu", Format::Default),
        "image_720p" | "image_480p" | "image_noise_720p" => {
            ("Image", "/camera/image_raw", Format::Image)
        }
        "jpeg_720p" => ("CompressedImage", "/camera/image_compressed", Format::Image),
        "blob_mixed" => ("CompressedImage", "/blob", Format::Image),
        other => return Err(format!("unknown kind {other}").into()),
    })
}

fn port() -> u16 {
    std::env::var("MOSAICO_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6728)
}

fn load(kind: &str, rows: usize) -> Result<RecordBatch, Box<dyn Error>> {
    let dir = std::env::var("BENCH_DATA").unwrap_or_else(|_| "target/bench-data".to_owned());
    let reader = FileReader::try_new(File::open(format!("{dir}/{kind}.arrow"))?, None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    let all = concat_batches(&schema, &batches)?;
    Ok(all.slice(0, rows.min(all.num_rows())))
}

fn sizes(spec: &str, total: usize) -> Result<Vec<usize>, Box<dyn Error>> {
    let pattern: Vec<usize> = match spec.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path)?
            .split_whitespace()
            .map(str::parse)
            .collect::<Result<_, _>>()?,
        None => vec![spec.parse()?],
    };
    let mut out = Vec::new();
    let mut covered = 0;
    while covered < total {
        let k = pattern[out.len() % pattern.len()].min(total - covered);
        out.push(k);
        covered += k;
    }
    Ok(out)
}

fn slices(table: &RecordBatch, spec: &str) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let mut start = 0;
    let mut out = Vec::new();
    for k in sizes(spec, table.num_rows())? {
        out.push(table.slice(start, k));
        start += k;
    }
    Ok(out)
}

fn windows(path: &str) -> Result<Vec<Range>, Box<dyn Error>> {
    let values = std::fs::read_to_string(path)?
        .split_whitespace()
        .map(str::parse)
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(values
        .chunks(2)
        .map(|pair| (Some(pair[0]), Some(pair[1])))
        .collect())
}

fn cpu_secs() -> f64 {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    secs(usage.ru_utime) + secs(usage.ru_stime)
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, Box<dyn Error>> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| format!("column '{name}' is missing or has another type").into())
}

fn png_batch(batch: &RecordBatch) -> Result<(RecordBatch, usize), Box<dyn Error>> {
    let data = column::<BinaryArray>(batch, "data")?;
    let heights = column::<Int32Array>(batch, "height")?;
    let strides = column::<Int32Array>(batch, "stride")?;
    let pixels: Vec<Pixels> = (0..batch.num_rows())
        .map(|row| Pixels {
            data: data.value(row),
            height: heights.value(row) as u32,
            stride: strides.value(row) as u32,
        })
        .collect();
    let encoded = encode_png_column(&pixels)?;
    let bytes = encoded.value_data().len();
    let encoded: ArrayRef = Arc::new(encoded);
    let rows = batch.num_rows();
    let schema = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| match field.name().as_str() {
            "header" => new_null_array(field.data_type(), rows),
            "data" => encoded.clone(),
            "format" => Arc::new(StringArray::from(vec!["png"; rows])) as ArrayRef,
            _ => column.clone(),
        })
        .collect();
    Ok((RecordBatch::try_new(schema, columns)?, bytes))
}

fn sequence_name(label: &str) -> Result<String, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("cmp_rust_{label}_{nanos}"))
}

async fn connect() -> Result<MosaicoClient, Box<dyn Error>> {
    Ok(MosaicoClient::connect(ConnectOptions::new("localhost", port())).await?)
}

async fn write(
    kind_name: &str,
    rows: usize,
    spec: &str,
    png: bool,
) -> Result<String, Box<dyn Error>> {
    let (tag, topic, format) = kind(kind_name)?;
    let table = load(kind_name, rows)?;
    let batches = slices(&table, spec)?;
    let pushes = batches.len();
    let sequence = sequence_name(kind_name)?;
    let client = connect().await?;
    let started = Instant::now();
    let cpu = cpu_secs();
    let mut seq = client
        .sequence_create(&sequence, None, SessionErrorPolicy::Delete)
        .await?;
    let result = async {
        let writer = seq
            .topic_create(
                topic,
                tag,
                format,
                table.schema(),
                None,
                TopicErrorPolicy::Raise,
            )
            .await?;
        let mut data_bytes = 0;
        for batch in batches {
            let batch = match png {
                true => {
                    let (batch, bytes) = png_batch(&batch)
                        .map_err(|e| mosaico_core::Error::invalid_argument(e.to_string()))?;
                    data_bytes += bytes;
                    batch
                }
                false => batch,
            };
            writer.push_batch(batch).await?;
        }
        Ok(data_bytes)
    }
    .await;
    let data_bytes = seq.complete(result).await?;
    let secs = started.elapsed().as_secs_f64();
    let cpu = cpu_secs() - cpu;
    Ok(format!(
        "{{\"sequence\": \"{sequence}\", \"secs\": {secs}, \"cpu\": {cpu}, \"rows\": {}, \"pushes\": {pushes}, \"data_bytes\": {data_bytes}}}",
        table.num_rows()
    ))
}

fn models<T: Ontology>(table: &RecordBatch) -> Result<Vec<(i64, T)>, Box<dyn Error>> {
    let columns = T::columns(table)?;
    let timestamps = table
        .column_by_name("timestamp_ns")
        .ok_or("no timestamp_ns column")?
        .as_primitive::<Int64Type>();
    (0..table.num_rows())
        .map(|row| {
            Ok((
                timestamps.value(row),
                T::from_view(&T::view(&columns, row))?,
            ))
        })
        .collect()
}

async fn write_push<T: Encode>(kind_name: &str, rows: usize) -> Result<String, Box<dyn Error>> {
    let (_, topic, _) = kind(kind_name)?;
    let table = load(kind_name, rows)?;
    let messages = models::<T>(&table)?;
    let sequence = sequence_name(kind_name)?;
    let client = connect().await?;
    let started = Instant::now();
    let cpu = cpu_secs();
    let mut seq = client
        .sequence_create(&sequence, None, SessionErrorPolicy::Delete)
        .await?;
    let result = async {
        let writer = seq
            .topic_create_for::<T>(topic, None, TopicErrorPolicy::Raise)
            .await?;
        for (timestamp_ns, value) in &messages {
            writer.push(*timestamp_ns, value).await?;
        }
        Ok(())
    }
    .await;
    seq.complete(result).await?;
    let secs = started.elapsed().as_secs_f64();
    let cpu = cpu_secs() - cpu;
    Ok(format!(
        "{{\"sequence\": \"{sequence}\", \"secs\": {secs}, \"cpu\": {cpu}, \"rows\": {}, \"pushes\": {}, \"data_bytes\": 0}}",
        table.num_rows(),
        messages.len()
    ))
}

async fn write_multi(variant: &str) -> Result<String, Box<dyn Error>> {
    let mut plans = Vec::new();
    for (name, rows, spec) in MULTI {
        let (tag, _, format) = kind(name)?;
        let table = load(name, rows)?;
        plans.push((
            format!("/{name}"),
            tag,
            format,
            table.schema(),
            slices(&table, spec)?,
        ));
    }
    let pushes: usize = plans.iter().map(|plan| plan.4.len()).sum();
    let rows: usize = MULTI.iter().map(|(_, rows, _)| rows).sum();
    let sequence = sequence_name(&format!("multi_{}", variant.replace('-', "_")))?;
    let client = connect().await?;
    let started = Instant::now();
    let cpu = cpu_secs();
    let mut seq = client
        .sequence_create(&sequence, None, SessionErrorPolicy::Delete)
        .await?;
    let result = async {
        let mut work = Vec::new();
        for (topic, tag, format, schema, batches) in plans {
            let writer = seq
                .topic_create(&topic, tag, format, schema, None, TopicErrorPolicy::Raise)
                .await?;
            work.push((writer, batches));
        }
        if variant == "rust-tasks" {
            let tasks: Vec<_> = work
                .into_iter()
                .map(|(writer, batches)| {
                    tokio::spawn(async move {
                        for batch in batches {
                            writer.push_batch(batch).await?;
                        }
                        Ok::<_, mosaico_core::Error>(())
                    })
                })
                .collect();
            for task in tasks {
                task.await
                    .map_err(|e| mosaico_core::Error::invalid_argument(e.to_string()))??;
            }
        } else {
            let steps = work.iter().map(|(_, b)| b.len()).max().unwrap_or(0);
            let mut queues: Vec<_> = work
                .into_iter()
                .map(|(writer, batches)| (writer, batches.into_iter()))
                .collect();
            for _ in 0..steps {
                for (writer, batches) in queues.iter_mut() {
                    if let Some(batch) = batches.next() {
                        writer.push_batch(batch).await?;
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    seq.complete(result).await?;
    let secs = started.elapsed().as_secs_f64();
    let cpu = cpu_secs() - cpu;
    Ok(format!(
        "{{\"sequence\": \"{sequence}\", \"secs\": {secs}, \"cpu\": {cpu}, \"rows\": {rows}, \"pushes\": {pushes}}}"
    ))
}

async fn rows<T: Ontology>(
    handler: &TopicHandler,
    (start, end): Range,
    touch: impl Fn(&T::View<'_>) -> mosaico_core::Result<f64>,
) -> Result<(u64, f64), Box<dyn Error>> {
    let mut streamer = handler.get_data_streamer::<T>(start, end).await?;
    let (mut count, mut acc) = (0u64, 0.0);
    while let Some(msg) = streamer.next().await? {
        acc += touch(&msg.view)?;
        count += 1;
    }
    Ok((count, acc))
}

async fn batches(
    handler: &TopicHandler,
    (start, end): Range,
) -> Result<(u64, f64), Box<dyn Error>> {
    let mut streamer = handler.get_batch_streamer(start, end).await?;
    let mut count = 0u64;
    while let Some(batch) = streamer.next_batch().await? {
        count += batch.num_rows() as u64;
    }
    Ok((count, 0.0))
}

async fn decoded(
    handler: &TopicHandler,
    (start, end): Range,
    per_batch: bool,
) -> Result<(u64, f64), Box<dyn Error>> {
    let mut streamer = handler.get_batch_streamer(start, end).await?;
    let (mut count, mut bytes) = (0u64, 0usize);
    while let Some(batch) = streamer.next_batch().await? {
        count += batch.num_rows() as u64;
        if per_batch {
            let decoded = decode_png_batch(&batch)?;
            bytes += column::<BinaryArray>(&decoded, "data")?.value_data().len();
            continue;
        }
        let data = batch.column_by_name("data").ok_or("no data column")?;
        let values: Vec<&[u8]> = match data.data_type() {
            arrow_schema::DataType::BinaryView => data.as_binary_view().iter().flatten().collect(),
            _ => data.as_binary::<i32>().iter().flatten().collect(),
        };
        for value in values {
            bytes += decode_png(value)?.len();
        }
    }
    Ok((count, bytes as f64))
}

async fn read(
    kind_name: &str,
    sequence: &str,
    op: &str,
    bounds: &[String],
) -> Result<String, Box<dyn Error>> {
    let ranges = match bounds.first() {
        Some(first) if first.starts_with('@') => windows(&first[1..])?,
        Some(first) => vec![(Some(first.parse()?), Some(bounds[1].parse()?))],
        None => vec![(None, None)],
    };
    let (_, topic, _) = kind(kind_name)?;
    let client = connect().await?;
    let started = Instant::now();
    let handler = client.topic_handler(sequence, topic).await?;
    let (mut count, mut acc) = (0u64, 0.0);
    for range in ranges {
        let (c, a) = match (op, kind_name) {
            ("rows" | "windows", "imu" | "imu_random") => {
                rows::<Imu>(&handler, range, |v| v.acceleration()?.z()).await?
            }
            ("rows" | "windows", "jpeg_720p" | "blob_mixed") => {
                rows::<CompressedImage>(&handler, range, |v| Ok(v.data()?.len() as f64)).await?
            }
            ("rows" | "windows", _) => {
                rows::<Image>(&handler, range, |v| Ok(v.data()?.len() as f64)).await?
            }
            ("batches", _) => batches(&handler, range).await?,
            ("decode-rows", _) => decoded(&handler, range, false).await?,
            ("decode-batches", _) => decoded(&handler, range, true).await?,
            ("decode-table", _) => {
                let mut streamer = handler.get_batch_streamer(range.0, range.1).await?;
                let mut all = Vec::new();
                while let Some(batch) = streamer.next_batch().await? {
                    all.push(batch);
                }
                let rows: u64 = all.iter().map(|b| b.num_rows() as u64).sum();
                let bytes: usize = decode_png_batches(&all)?
                    .iter()
                    .map(|b| column::<BinaryArray>(b, "data").map(|c| c.value_data().len()))
                    .sum::<Result<_, _>>()?;
                (rows, bytes as f64)
            }
            _ => return Err(format!("unknown op {op}").into()),
        };
        count += c;
        acc += a;
    }
    let secs = started.elapsed().as_secs_f64();
    Ok(format!(
        "{{\"secs\": {secs}, \"rows\": {count}, \"check\": {acc}}}"
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).map(String::as_str).unwrap_or("");
    let out = match arg(0) {
        "write" if arg(4) == "push" => match kind(arg(1))?.0 {
            "IMU" => write_push::<Imu>(arg(1), arg(2).parse()?).await?,
            "Image" => write_push::<Image>(arg(1), arg(2).parse()?).await?,
            _ => write_push::<CompressedImage>(arg(1), arg(2).parse()?).await?,
        },
        "write" => write(arg(1), arg(2).parse()?, arg(3), arg(4) == "png").await?,
        "write-multi" => write_multi(arg(1)).await?,
        "read" => read(arg(1), arg(2), arg(3), args.get(4..).unwrap_or(&[])).await?,
        other => return Err(format!("unknown command {other}").into()),
    };
    println!("{out}");
    Ok(())
}
