use std::time::{SystemTime, UNIX_EPOCH};

use mosaico_core::{ConnectOptions, MosaicoClient, SessionLevelErrorPolicy, TopicLevelErrorPolicy};
use mosaico_ontology::{CompressedImage, ImageFormat, Imu, Vector3d};

const T0: i64 = 1_700_000_000_000_000_000;
const IMU_ROWS: i64 = 2000;
const FRAMES: i64 = 40;
const FRAME_PERIOD: i64 = 50;

async fn client() -> Option<MosaicoClient> {
    let port = std::env::var("MOSAICO_PORT").ok()?.parse().ok()?;
    Some(
        MosaicoClient::connect(ConnectOptions::new("localhost", port))
            .await
            .expect("mosaicod reachable on MOSAICO_PORT"),
    )
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{nanos}")
}

fn vector(x: f64) -> Vector3d {
    Vector3d {
        x,
        y: 0.5,
        z: 9.81,
        covariance: None,
        covariance_type: None,
        header: None,
    }
}

fn imu(i: i64) -> Imu {
    Imu {
        header: None,
        acceleration: vector(i as f64),
        angular_velocity: vector(-(i as f64)),
        orientation: None,
    }
}

fn frame(i: i64) -> CompressedImage {
    CompressedImage {
        header: None,
        data: vec![i as u8; 30_000],
        format: ImageFormat::Jpeg,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_typed_views() {
    let Some(client) = client().await else {
        return;
    };
    let name = unique("rust_e2e_merge_typed");
    let mut writer = client
        .sequence_create(&name, None, SessionLevelErrorPolicy::Report)
        .await
        .unwrap();
    let imu_topic = writer
        .topic_create_for::<Imu>("/imu", None, TopicLevelErrorPolicy::Raise)
        .await
        .unwrap();
    let cam_topic = writer
        .topic_create_for::<CompressedImage>("/cam", None, TopicLevelErrorPolicy::Raise)
        .await
        .unwrap();
    for i in 0..IMU_ROWS {
        imu_topic.push(T0 + i, &imu(i)).await.unwrap();
    }
    for i in 0..FRAMES {
        cam_topic
            .push(T0 + i * FRAME_PERIOD, &frame(i))
            .await
            .unwrap();
    }
    writer.finish().await.unwrap();

    let seq = client.sequence_handler(&name).await.unwrap();
    let mut stream = seq.get_data_streamer(&["/cam", "/imu"], ..).await.unwrap();
    assert_eq!(stream.msg_count(), (IMU_ROWS + FRAMES) as u64);
    let (mut imus, mut frames, mut last) = (0, 0, i64::MIN);
    let mut previous_topic = String::new();
    while let Some(row) = stream.next().await.unwrap() {
        let timestamp_ns = row.timestamp_ns.as_ns();
        assert!(timestamp_ns >= last);
        if timestamp_ns == last {
            assert!(!(previous_topic == "/imu" && row.topic().name() == "/cam"));
        }
        last = timestamp_ns;
        previous_topic = row.topic().name().to_owned();
        let i = timestamp_ns - T0;
        match row.topic().name() {
            "/imu" => {
                assert!(row.view::<CompressedImage>().unwrap().is_none());
                let view = row.view::<Imu>().unwrap().unwrap();
                assert_eq!(view.acceleration().unwrap().x().unwrap(), i as f64);
                assert_eq!(view.to_model().unwrap(), imu(i));
                imus += 1;
            }
            "/cam" => {
                assert!(row.view::<Imu>().unwrap().is_none());
                let view = row.view::<CompressedImage>().unwrap().unwrap();
                let index = i / FRAME_PERIOD;
                assert_eq!(view.data().unwrap(), frame(index).data.as_slice());
                assert_eq!(view.format().unwrap(), ImageFormat::Jpeg);
                frames += 1;
            }
            other => panic!("unexpected topic {other}"),
        }
    }
    assert_eq!((imus, frames), (IMU_ROWS, FRAMES));
    client.sequence_delete(&name).await.unwrap();
}
