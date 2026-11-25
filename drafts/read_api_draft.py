from mosaicolabs import MosaicoClient, QueryOntologyCatalog, IMU

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

   # -------------------------------------------------------------

#   for topic, msg in seq.get_data_streamer(topics=["/gps", "/front/imu"]):
#       ...
#
#   """
#   4. query
#   """
#   found = client.query(QueryOntologyCatalog(IMU.Q.acceleration.z.gt(15.0)))
#   if found is None:
#       raise RuntimeError("query failed")
#   for item in found:
#       for topic in item.topics:
#           for cluster in topic.clusterize(clustering_dt_ns=int(1e9)):
#               for msg in topic.get_data_streamer(timestamp_range=cluster.timerange):
#                   ...
