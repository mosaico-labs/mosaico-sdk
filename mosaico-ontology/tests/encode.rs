use std::sync::Arc;

use arrow_array::RecordBatch;
use mosaico_core::{Encode, Format, RecordBuilder, Result, record_schema};
use mosaico_ontology::{
    CompressedImage, Header, Image, ImageFormat, Imu, Quaternion, Time, Vector3d,
};

fn round_trip<T: Encode + std::fmt::Debug + PartialEq>(values: &[T]) -> Result<()> {
    let mut builder = T::Builder::default();
    for value in values {
        builder.append(value);
    }
    let schema = record_schema::<T>();
    let payload: Vec<_> = (1..schema.fields().len()).collect();
    let payload = Arc::new(schema.project(&payload).unwrap());
    let batch = RecordBatch::try_new(payload, builder.finish()).unwrap();
    assert_eq!(batch.num_rows(), values.len());
    let read = T::columns(&batch)?;
    for (row, value) in values.iter().enumerate() {
        assert_eq!(&T::from_view(&T::view(&read, row))?, value);
    }
    Ok(())
}

fn header(n: u32) -> Header {
    Header {
        timestamp: Some(Time {
            seconds: n as i32,
            nanoseconds: 7,
        }),
        frame_id: Some(format!("frame{n}")),
        sample_counter: Some(n.into()),
    }
}

fn vector(n: f64) -> Vector3d {
    Vector3d {
        x: n,
        y: n + 1.0,
        z: n + 2.0,
        covariance: None,
        covariance_type: None,
        header: None,
    }
}

#[test]
fn imu_round_trip() -> Result<()> {
    let full = Imu {
        header: Some(header(3)),
        acceleration: Vector3d {
            covariance: Some(vec![1.0; 9]),
            covariance_type: Some(2),
            header: Some(Header {
                timestamp: None,
                frame_id: None,
                sample_counter: Some(1),
            }),
            ..vector(1.0)
        },
        angular_velocity: vector(4.0),
        orientation: Some(Quaternion {
            x: 0.0,
            y: 0.0,
            z: 0.0,
            w: 1.0,
            covariance: Some(vec![]),
            covariance_type: None,
            header: None,
        }),
    };
    let bare = Imu {
        header: None,
        acceleration: vector(-1.0),
        angular_velocity: vector(-2.0),
        orientation: None,
    };
    round_trip(&[full.clone(), bare.clone(), full, bare])?;
    round_trip::<Imu>(&[])
}

#[test]
fn image_round_trip() -> Result<()> {
    let image = |n: u8, header| Image {
        header,
        data: vec![n; n as usize],
        format: ImageFormat::Raw,
        width: n.into(),
        height: 1,
        stride: n.into(),
        encoding: "mono8".into(),
        is_bigendian: n.is_multiple_of(2).then_some(true),
    };
    round_trip(&[image(1, None), image(2, Some(header(2))), image(0, None)])?;
    let compressed = |n: u8| CompressedImage {
        header: (n > 1).then(|| header(n.into())),
        data: vec![n; 3],
        format: ImageFormat::Jpeg,
    };
    round_trip(&[compressed(1), compressed(2)])
}

#[test]
fn record_schema_starts_with_the_timestamp() {
    let schema = record_schema::<Imu>();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "timestamp_ns",
            "header",
            "acceleration",
            "angular_velocity",
            "orientation"
        ]
    );
    assert!(!schema.field(0).is_nullable());
    assert_eq!(Image::FORMAT, Format::Image);
    assert_eq!(Imu::FORMAT, Format::Default);
}
