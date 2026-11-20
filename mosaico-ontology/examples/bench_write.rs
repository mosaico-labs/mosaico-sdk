use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array,
    ListArray, RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array, new_null_array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef};
use mosaico_core::{
    Compression, ConnectOptions, Format, MosaicoClient, SessionErrorPolicy, TopicErrorPolicy,
};

const T0: i64 = 1_700_000_000_000_000_000;
const WIDTH: usize = 640;
const HEIGHT: usize = 480;
const CHANNELS: usize = 3;
const COMPRESSED_BYTES: usize = 100_000;
const POOL: usize = 8;
const CAMERA_PERIOD_NS: i64 = 33_333_333;
const IMU_PERIOD_NS: i64 = 1_000_000;
const FRAMES_PER_BATCH: usize = 32;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Imu,
    Image,
    Compressed,
}

impl Kind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "imu" => Some(Self::Imu),
            "image" => Some(Self::Image),
            "compressed" => Some(Self::Compressed),
            _ => None,
        }
    }

    fn defaults(self) -> (usize, usize, i64) {
        match self {
            Self::Imu => (10_000_000, 50_000, IMU_PERIOD_NS),
            Self::Image => (2_000, FRAMES_PER_BATCH, CAMERA_PERIOD_NS),
            Self::Compressed => (10_000, 256, CAMERA_PERIOD_NS),
        }
    }

    fn topic(self) -> &'static str {
        match self {
            Self::Imu => "/imu",
            Self::Image => "/camera/image_raw",
            Self::Compressed => "/camera/image_compressed",
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Imu => "IMU",
            Self::Image => "Image",
            Self::Compressed => "CompressedImage",
        }
    }

    fn format(self) -> Format {
        match self {
            Self::Imu => Format::Default,
            Self::Image | Self::Compressed => Format::Image,
        }
    }

    fn row_bytes(self) -> usize {
        match self {
            Self::Imu => 0,
            Self::Image => WIDTH * HEIGHT * CHANNELS,
            Self::Compressed => COMPRESSED_BYTES,
        }
    }

    fn schema(self) -> SchemaRef {
        let mut fields = vec![
            Field::new("timestamp_ns", DataType::Int64, false),
            Field::new("header", header_type(), true),
        ];
        match self {
            Self::Imu => fields.extend([
                Field::new("acceleration", vector_type("xyz"), false),
                Field::new("angular_velocity", vector_type("xyz"), false),
                Field::new("orientation", vector_type("xyzw"), true),
            ]),
            Self::Image => fields.extend([
                Field::new("data", DataType::Binary, false),
                Field::new("format", DataType::Utf8, false),
                Field::new("width", DataType::Int32, false),
                Field::new("height", DataType::Int32, false),
                Field::new("stride", DataType::Int32, false),
                Field::new("encoding", DataType::Utf8, false),
                Field::new("is_bigendian", DataType::Boolean, true),
            ]),
            Self::Compressed => fields.extend([
                Field::new("data", DataType::Binary, false),
                Field::new("format", DataType::Utf8, false),
            ]),
        }
        Arc::new(Schema::new(fields))
    }
}

fn time_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("seconds", DataType::Int32, false),
        Field::new("nanoseconds", DataType::UInt32, false),
    ]))
}

fn header_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("timestamp", time_type(), true),
        Field::new("frame_id", DataType::Utf8, true),
        Field::new("sample_counter", DataType::UInt64, true),
    ]))
}

fn covariance_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Float64, true))
}

fn vector_type(axes: &str) -> DataType {
    let mut fields = vec![
        Field::new("header", header_type(), true),
        Field::new("covariance", DataType::List(covariance_field()), true),
        Field::new("covariance_type", DataType::Int16, true),
    ];
    fields.extend(
        axes.chars()
            .map(|axis| Field::new(axis.to_string(), DataType::Float64, true)),
    );
    DataType::Struct(Fields::from(fields))
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn float(&mut self, scale: f64) -> f64 {
        ((self.next() >> 11) as f64 / (1u64 << 53) as f64 - 0.5) * 2.0 * scale
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            out.extend_from_slice(&self.next().to_le_bytes());
        }
        out.truncate(len);
        out
    }
}

fn struct_array(data_type: &DataType, columns: Vec<ArrayRef>) -> ArrayRef {
    let DataType::Struct(fields) = data_type else {
        unreachable!()
    };
    Arc::new(StructArray::new(fields.clone(), columns, None))
}

fn header(timestamps: &[i64], start: usize, frame_id: &str) -> ArrayRef {
    let n = timestamps.len();
    let seconds: Int32Array = timestamps
        .iter()
        .map(|t| (t / 1_000_000_000) as i32)
        .collect();
    let nanoseconds: UInt32Array = timestamps
        .iter()
        .map(|t| (t % 1_000_000_000) as u32)
        .collect();
    let time = struct_array(&time_type(), vec![Arc::new(seconds), Arc::new(nanoseconds)]);
    let frame_id = StringArray::from_iter_values(std::iter::repeat_n(frame_id, n));
    let counter: UInt64Array = (start as u64..(start + n) as u64).collect();
    struct_array(
        &header_type(),
        vec![time, Arc::new(frame_id), Arc::new(counter)],
    )
}

fn vector(rng: &mut Rng, n: usize, axes: &str, scale: f64) -> ArrayRef {
    let values: Float64Array = (0..9 * n).map(|_| rng.float(1.0)).collect();
    let covariance = ListArray::new(
        covariance_field(),
        OffsetBuffer::from_lengths(std::iter::repeat_n(9, n)),
        Arc::new(values),
        None,
    );
    let mut columns: Vec<ArrayRef> = vec![
        new_null_array(&header_type(), n),
        Arc::new(covariance),
        Arc::new(Int16Array::from(vec![2; n])),
    ];
    for _ in axes.chars() {
        let axis: Float64Array = (0..n).map(|_| rng.float(scale)).collect();
        columns.push(Arc::new(axis));
    }
    struct_array(&vector_type(axes), columns)
}

struct Generator {
    kind: Kind,
    schema: SchemaRef,
    period_ns: i64,
    rng: Rng,
    pool: Vec<Vec<u8>>,
}

impl Generator {
    fn new(kind: Kind, period_ns: i64) -> Self {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let pool = (0..POOL).map(|_| rng.bytes(kind.row_bytes())).collect();
        Self {
            kind,
            schema: kind.schema(),
            period_ns,
            rng,
            pool,
        }
    }

    fn batch(&mut self, start: usize, n: usize) -> Result<RecordBatch, ArrowError> {
        let timestamps: Vec<i64> = (start..start + n)
            .map(|i| T0 + i as i64 * self.period_ns)
            .collect();
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(timestamps.clone()))];
        match self.kind {
            Kind::Imu => {
                columns.push(header(&timestamps, start, "imu_link"));
                columns.push(vector(&mut self.rng, n, "xyz", 5.0));
                columns.push(vector(&mut self.rng, n, "xyz", 1.0));
                columns.push(vector(&mut self.rng, n, "xyzw", 0.5));
            }
            Kind::Image => {
                columns.push(header(&timestamps, start, "camera"));
                columns.push(Arc::new(self.frames(start, n)));
                columns.push(Arc::new(StringArray::from(vec!["raw"; n])));
                columns.push(Arc::new(Int32Array::from(vec![WIDTH as i32; n])));
                columns.push(Arc::new(Int32Array::from(vec![HEIGHT as i32; n])));
                columns.push(Arc::new(Int32Array::from(vec![
                    (WIDTH * CHANNELS) as i32;
                    n
                ])));
                columns.push(Arc::new(StringArray::from(vec!["rgb8"; n])));
                columns.push(Arc::new(BooleanArray::from(vec![false; n])));
            }
            Kind::Compressed => {
                columns.push(header(&timestamps, start, "camera"));
                columns.push(Arc::new(self.frames(start, n)));
                columns.push(Arc::new(StringArray::from(vec!["jpeg"; n])));
            }
        }
        RecordBatch::try_new(self.schema.clone(), columns)
    }

    fn frames(&self, start: usize, n: usize) -> BinaryArray {
        BinaryArray::from_iter_values((start..start + n).map(|i| self.pool[i % POOL].as_slice()))
    }
}

fn usage() -> String {
    [
        "usage: bench_write <imu|image|compressed> <sequence> [rows] [batch] [none|lz4|zstd]",
        "       bench_write multi <sequence> [frames per camera] [cameras] [none|lz4|zstd]",
        "       bench_write delete <sequence>",
        "server port from MOSAICO_PORT, default 6726",
    ]
    .join("\n")
}

fn arg<T: std::str::FromStr>(args: &[String], index: usize, default: T) -> Result<T, String> {
    match args.get(index) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .map_err(|_| format!("invalid argument '{value}'\n{}", usage())),
    }
}

fn compression(args: &[String]) -> Result<Compression, String> {
    match args.get(4).map(String::as_str) {
        None | Some("none") => Ok(Compression::None),
        Some("lz4") => Ok(Compression::Lz4),
        Some("zstd") => Ok(Compression::Zstd),
        Some(_) => Err(usage()),
    }
}

fn options(compression: Compression) -> Result<ConnectOptions, String> {
    let port = match std::env::var("MOSAICO_PORT") {
        Ok(port) => port
            .parse()
            .map_err(|_| format!("invalid MOSAICO_PORT '{port}'"))?,
        Err(_) => 6726,
    };
    let mut options = ConnectOptions::new("localhost", port);
    options.compression = compression;
    Ok(options)
}

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
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(command), Some(sequence)) = (args.first(), args.get(1)) else {
        return Err(usage().into());
    };

    if command == "delete" {
        let client = MosaicoClient::connect(options(Compression::None)?).await?;
        if !client.list_sequences().await?.contains(sequence) {
            return Err(format!("sequence '{sequence}' not found").into());
        }
        client.sequence_delete(sequence).await?;
        println!("deleted {sequence}");
        return Ok(());
    }

    let compression = compression(&args)?;
    let started = Instant::now();
    let client = MosaicoClient::connect(options(compression)?).await?;
    if client.list_sequences().await?.contains(sequence) {
        return Err(format!(
            "sequence '{sequence}' already exists: run `bench_write delete {sequence}` first"
        )
        .into());
    }
    let label = args.get(4).map_or("none", String::as_str);
    if command == "multi" {
        let frames: usize = arg(&args, 2, 500)?;
        let cameras: usize = arg(&args, 3, 4)?;
        run_multi(&client, sequence, frames, cameras).await?;
        let total = started.elapsed().as_secs_f64();
        let megabytes = (cameras * frames * Kind::Image.row_bytes()) as f64 / 1e6;
        println!(
            "rust multi {cameras} cameras x {frames} frames + imu, compression {label}: total {total:.2}s, {:.0} MB/s",
            megabytes / total
        );
        return Ok(());
    }

    let kind = Kind::parse(command).ok_or_else(usage)?;
    let (default_rows, default_batch, period_ns) = kind.defaults();
    let rows: usize = arg(&args, 2, default_rows)?;
    let batch: usize = arg(&args, 3, default_batch)?;
    let mut generator = Generator::new(kind, period_ns);

    let mut pushing = Duration::ZERO;
    let mut writer = client
        .sequence_create(sequence, None, SessionErrorPolicy::Delete)
        .await?;
    let topic = writer
        .topic_create(
            kind.topic(),
            kind.tag(),
            kind.format(),
            kind.schema(),
            None,
            TopicErrorPolicy::Raise,
        )
        .await?;

    let mut start = 0;
    while start < rows {
        let n = batch.min(rows - start);
        let data = generator.batch(start, n)?;
        let pushed = Instant::now();
        if let Err(e) = topic.push_batch(data).await {
            let _ = writer.abort(&e.to_string()).await;
            return Err(e.into());
        }
        pushing += pushed.elapsed();
        start += n;
        eprint!("\r{start}/{rows}");
    }
    let dropped = topic.dropped_records();
    let finishing = Instant::now();
    writer.finish().await?;
    pushing += finishing.elapsed();

    let total = started.elapsed().as_secs_f64();
    let push = pushing.as_secs_f64();
    eprintln!();
    println!(
        "rust {command} {rows} rows, batch {batch}, compression {label}: total {total:.2}s, push+finish {push:.2}s, {:.0} rows/s",
        rows as f64 / total,
    );
    if kind.row_bytes() > 0 {
        let megabytes = (rows * kind.row_bytes()) as f64 / 1e6;
        println!(
            "payload {megabytes:.0} MB, {:.0} MB/s total, {:.0} MB/s push+finish",
            megabytes / total,
            megabytes / push
        );
    }
    if dropped > 0 {
        println!("dropped {dropped} rows");
    }
    Ok(())
}

async fn run_multi(
    client: &MosaicoClient,
    sequence: &str,
    frames: usize,
    cameras: usize,
) -> Result<(), Box<dyn Error>> {
    let imu_rows = (frames as i64 * CAMERA_PERIOD_NS / IMU_PERIOD_NS) as usize;
    let imu_batch = (FRAMES_PER_BATCH as i64 * CAMERA_PERIOD_NS / IMU_PERIOD_NS) as usize;
    let mut plan: Vec<(Kind, String, usize, usize)> = (0..cameras)
        .map(|c| {
            (
                Kind::Image,
                format!("/cam{c}/image_raw"),
                frames,
                FRAMES_PER_BATCH,
            )
        })
        .collect();
    plan.push((Kind::Imu, "/imu".to_owned(), imu_rows, imu_batch));

    let mut writer = client
        .sequence_create(sequence, None, SessionErrorPolicy::Delete)
        .await?;
    let mut tasks = Vec::new();
    for (kind, name, rows, batch) in plan {
        let topic = writer
            .topic_create(
                &name,
                kind.tag(),
                kind.format(),
                kind.schema(),
                None,
                TopicErrorPolicy::Raise,
            )
            .await?;
        tasks.push(tokio::spawn(async move {
            let mut generator = Generator::new(kind, kind.defaults().2);
            let mut start = 0;
            while start < rows {
                let n = batch.min(rows - start);
                let data = generator.batch(start, n).map_err(|e| e.to_string())?;
                topic.push_batch(data).await.map_err(|e| e.to_string())?;
                start += n;
            }
            Ok::<(), String>(())
        }));
    }
    for task in tasks {
        let result = match task.await {
            Ok(result) => result,
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = result {
            let _ = writer.abort(&e).await;
            return Err(e.into());
        }
    }
    writer.finish().await?;
    Ok(())
}
