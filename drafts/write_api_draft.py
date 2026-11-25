from mosaicolabs import IMU, Message, MosaicoClient, Vector3d

with MosaicoClient.connect("localhost", 6726) as client:
    src = client.topic_handler("mission_alpha", "/front/imu", ontology=IMU)

    with client.sequence_create("mission_beta", {"vehicle": "rover_1"}) as seq:
        imu = seq.topic_create("/front/imu", {"rate_hz": 100}, IMU)
        for i in range(1000):
            data = IMU(acceleration=Vector3d(x=0.0, y=0.0, z=9.81), angular_velocity=Vector3d(x=0.0, y=0.0, z=0.1))
            imu.push(Message(timestamp_ns=1_700_000_000_000_000_000 + i * 10**7, data=data))

        rear = seq.topic_create("/rear/imu", {}, IMU)
        for rb in src.get_batch_streamer():
            rear.push_batch(rb["timestamp_ns"], rb.drop_columns(["timestamp_ns"]))

        copy = seq.topic_create("/copy/imu", {}, IMU)
        copy.push_stream(src.get_batch_streamer())
