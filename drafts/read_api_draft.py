from mosaicolabs import IMU, CompressedImage, MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    seq = client.sequence_handler("mission_alpha")
    imu = seq.topic_handler("/front/imu", ontology_type=IMU)
    t0 = imu.timestamp_ns_min
    if t0 is None:
        raise RuntimeError("no time range: topic empty or not finalized")
    t1 = t0 + 60 * 10**9

    kept = []
    for msg in imu.get_data_streamer(start_timestamp_ns=t0, end_timestamp_ns=t1):
        if msg.data.acceleration.z > 15.0:
            kept.append(msg.get_data())

    for rb in imu.get_batch_streamer(start_timestamp_ns=t0, end_timestamp_ns=t1):
        ts = rb["timestamp_ns"].to_numpy()
        z = rb["acceleration"].field("z").to_numpy()
        print(z.max(), ts[z.argmax()])

    df = imu.to_pandas(start_timestamp_ns=t0, end_timestamp_ns=t1)

    cam = seq.topic_handler("/front/camera", ontology_type=CompressedImage)
    frames = []
    last_z = None
    with seq.get_data_streamer(topics=[imu, cam, "/rear/imu"], start_timestamp_ns=t0, end_timestamp_ns=t1) as stream:
        print(stream.msg_count, stream.next_timestamp())
        for topic, msg in stream:
            if topic is imu:
                last_z = msg.data.acceleration.z
            elif topic is cam:
                frames.append((msg.timestamp_ns, last_z, msg.data.format))
            else:
                print(f"[{topic}] {msg.timestamp_ns}")
