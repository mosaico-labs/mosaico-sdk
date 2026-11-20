import os
import signal
import threading
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from mosaicolabs import (
    GPS,
    IMU,
    CompressedImage,
    Floating64,
    Header,
    Image,
    ImageFormat,
    Message,
    Point3d,
    Quaternion,
    SessionLevelErrorPolicy,
    SessionStatus,
    TopicLevelErrorPolicy,
    Time,
    TopicWriterStatus,
    Vector3d,
)

import mosaico_rs as m

from .conftest import PAYLOAD, PORT, STEP_NS, T0, imu_batch, timestamps, write_imu

N = 2500


def test_roundtrip(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {"driver": "a", "n": 1}) as seq:
        topic = seq.topic_create("imu", {"rate": 1000}, IMU)
        write_imu(topic, N, batch=700)
    assert seq.status is SessionStatus.Finalized
    assert topic.status is TopicWriterStatus.Finalized

    handler = client.sequence_handler(name)
    assert handler.topics == ["/imu"]
    assert handler.user_metadata == {"driver": "a", "n": 1}
    assert handler.timestamp_ns_min == T0
    assert handler.timestamp_ns_max == T0 + (N - 1) * STEP_NS

    imu = handler.get_topic_handler("/imu", ontology=IMU)
    assert imu.ontology_tag == "IMU"
    count = 0
    for i, msg in enumerate(imu.get_data_streamer()):
        assert msg.timestamp_ns == T0 + i * STEP_NS
        assert msg.data.acceleration.x == float(i)
        assert msg.data.angular_velocity.z == float(-i)
        assert msg.data.header.frame_id == "imu"
        assert msg.data.header.timestamp.seconds == i
        assert (msg.data.orientation is None) == bool(i % 2)
        count += 1
    assert count == N

    first = next(iter(imu.get_data_streamer()))
    model = first.get_data()
    assert isinstance(model, IMU)
    assert model.acceleration.z == 9.81
    assert first.get_data(Image) is None

    window = imu.get_data_streamer(T0 + 100 * STEP_NS, T0 + 200 * STEP_NS)
    assert [msg.data.acceleration.x for msg in window] == [float(i) for i in range(100, 200)]

    assert sum(b.num_rows for b in imu.get_batch_streamer()) == N
    assert pa.RecordBatchReader.from_stream(imu.get_batch_streamer()).read_all().num_rows == N
    table = imu.to_arrow()
    assert table.num_rows == N
    assert pc.struct_field(table.column("acceleration"), "x").to_pylist() == [float(i) for i in range(N)]
    df = imu.to_pandas(T0, T0 + 10 * STEP_NS)
    assert len(df) == 10
    assert "acceleration.x" in df.columns
    assert df["timestamp_ns"].dtype == "int64"


def test_errors(client: m.MosaicoClient, name: str) -> None:
    with pytest.raises(m.SequenceNotFoundError):
        client.sequence_handler(name)
    with client.sequence_create(name, {}) as seq:
        write_imu(seq.topic_create("/imu", {}, IMU), 10)
    with pytest.raises(m.TopicNotFoundError):
        client.topic_handler(name, "/nope", ontology=IMU)
    with pytest.raises(m.TopicNotFoundError):
        client.sequence_handler(name).get_topic_handler("/nope", ontology=IMU)
    with pytest.raises(m.OntologyMismatchError):
        client.topic_handler(name, "/imu", ontology=Image)
    with pytest.raises(m.InvalidArgumentError):
        client.topic_handler(name, "/imu", ontology=dict)  # pyright: ignore[reportArgumentType]
    with pytest.raises(m.InvalidArgumentError):
        client.sequence_handler("a/b")


def test_close(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        write_imu(seq.topic_create("/imu", {}, IMU), N, batch=100)
    imu = client.topic_handler(name, "/imu", ontology=IMU)

    stream = imu.get_data_streamer()
    next(stream)
    stream.close()
    with pytest.raises(m.StreamClosedError):
        next(stream)

    batches = imu.get_batch_streamer()
    other = imu.get_data_streamer()
    next(batches)
    next(other)
    imu.close()
    with pytest.raises(m.StreamClosedError):
        next(batches)
    with pytest.raises(m.StreamClosedError):
        next(other)
    assert sum(b.num_rows for b in imu.get_batch_streamer()) == N

    with imu.get_batch_streamer() as scoped:
        next(scoped)
    with pytest.raises(m.StreamClosedError):
        next(scoped)

    a = imu.get_data_streamer()
    b = imu.get_data_streamer()
    assert next(a).timestamp_ns == next(b).timestamp_ns == T0


def test_client_close(name: str) -> None:
    c = m.MosaicoClient.connect("localhost", PORT)
    with c:
        c.list_sequences()
    with pytest.raises(m.MosaicoError):
        c.list_sequences()
    with pytest.raises(m.MosaicoError):
        c.sequence_handler(name)


def test_policy_delete(client: m.MosaicoClient, name: str) -> None:
    seq = client.sequence_create(name, {}, on_error=SessionLevelErrorPolicy.Delete)
    with pytest.raises(RuntimeError, match="boom"):
        with seq:
            write_imu(seq.topic_create("/imu", {}, IMU), 100)
            raise RuntimeError("boom")
    assert seq.status is SessionStatus.Error
    with pytest.raises(m.SequenceNotFoundError):
        client.sequence_handler(name)


def test_policy_report(client: m.MosaicoClient, name: str) -> None:
    seq = client.sequence_create(name, {}, on_error=SessionLevelErrorPolicy.Report)
    topics: list[m.TopicWriter] = []
    with pytest.raises(RuntimeError, match="boom"):
        with seq:
            topics.append(seq.topic_create("/imu", {}, IMU))
            write_imu(topics[0], 300)
            raise RuntimeError("boom")
    topic = topics[0]
    assert seq.status is SessionStatus.Error
    assert topic.status is TopicWriterStatus.FinalizedWithError
    assert topic.last_error == "boom"
    imu = client.topic_handler(name, "/imu", ontology=IMU)
    assert imu.to_arrow().num_rows == 300


def test_topic_status(client: m.MosaicoClient, name: str) -> None:
    seq = client.sequence_create(name, {})
    with pytest.raises(RuntimeError):
        seq.topic_create("/imu", {}, IMU)
    with seq:
        topic = seq.topic_create("/imu", {}, IMU, on_error=TopicLevelErrorPolicy.Raise)
        assert topic.status is TopicWriterStatus.Active
        assert seq.get_topic_writer("imu") is topic
        assert seq.topic_writer_exists("imu")
        assert seq.list_topic_writers() == ["/imu"]
        with pytest.raises(m.TopicNotFoundError):
            seq.get_topic_writer("/other")
        with pytest.raises(m.ServerError):
            seq.topic_create("/imu", {}, IMU)

        backwards = pa.array([T0 + 2, T0 + 1], pa.int64())
        with pytest.raises(m.InvalidArgumentError):
            topic.push_batch(backwards, imu_batch(0, 2))
        assert topic.status is TopicWriterStatus.RaisedException
        assert topic.last_error is not None
        assert topic.is_active

        with pytest.raises(m.SchemaError):
            topic.push_batch(timestamps(0, 2), pa.RecordBatch.from_pydict({"x": [1.0, 2.0]}))
        assert topic.status is TopicWriterStatus.RaisedException
        assert topic.last_error is not None and "columns" in topic.last_error
        with pytest.raises(m.InvalidArgumentError):
            topic.push_batch(timestamps(0, 3), imu_batch(0, 2))
        assert topic.last_error is not None and "rows" in topic.last_error
        with pytest.raises(m.InvalidArgumentError):
            topic.push(None)  # pyright: ignore[reportArgumentType]

        write_imu(topic, 50)
        assert topic.status is TopicWriterStatus.Active
        assert topic.last_error is None
        assert topic.dropped_record_count == 0
    assert topic.status is TopicWriterStatus.Finalized
    assert not topic.is_active
    with pytest.raises(m.StreamClosedError):
        write_imu(topic, 1)
    assert client.topic_handler(name, "/imu", ontology=IMU).to_arrow().num_rows == 50


def test_topic_policy_applies_when_its_with_block_fails_as_sdk(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        finalize = seq.topic_create("/finalize", {}, IMU, on_error=TopicLevelErrorPolicy.Finalize)
        with finalize:
            write_imu(finalize, 20)
            with pytest.raises(m.InvalidArgumentError):
                finalize.push_batch(pa.array([T0 + 1, T0], pa.int64()), imu_batch(0, 2))
            assert finalize.status is TopicWriterStatus.RaisedException
            assert finalize.is_active
            raise RuntimeError("sensor lost")
        assert finalize.status is TopicWriterStatus.FinalizedWithError
        assert finalize.last_error == "sensor lost"
        with pytest.raises(m.StreamClosedError):
            write_imu(finalize, 1)

        ignore = seq.topic_create("/ignore", {}, IMU, on_error=TopicLevelErrorPolicy.Ignore)
        with ignore:
            write_imu(ignore, 5)
            raise RuntimeError("glitch")
        assert ignore.status is TopicWriterStatus.IgnoredLastError
        ignore.push_batch(timestamps(5, 5), imu_batch(5, 5))
        assert ignore.status is TopicWriterStatus.Active

        raise_policy = seq.topic_create("/raise", {}, IMU)
        with pytest.raises(RuntimeError, match="boom"):
            with raise_policy:
                raise RuntimeError("boom")
        assert raise_policy.status is TopicWriterStatus.RaisedException
    assert client.topic_handler(name, "/finalize", ontology=IMU).to_arrow().num_rows == 20
    assert client.topic_handler(name, "/ignore", ontology=IMU).to_arrow().num_rows == 10
    assert any("sensor lost" in n.message for n in client.list_topic_notifications(name, "/finalize"))
    assert any("glitch" in n.message for n in client.list_topic_notifications(name, "/ignore"))
    assert client.list_topic_notifications(name, "/raise") == []


def test_timestamp_order_matches_sdk(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        topic = seq.topic_create("/imu", {}, IMU)
        topic.push_batch([T0, T0, T0 + 1], imu_batch(0, 3))
        topic.push_batch([T0 + 1, T0 + 2], imu_batch(3, 2))
        with pytest.raises(m.InvalidArgumentError, match="across pushes"):
            topic.push_batch([T0 + 1, T0 + 5], imu_batch(5, 2))
        with pytest.raises(m.InvalidArgumentError, match="non-decreasing"):
            topic.push_batch([T0 + 4, T0 + 3], imu_batch(5, 2))
        with pytest.raises(m.SchemaError):
            topic.push_batch([T0], pa.RecordBatch.from_pydict({"x": [1.0]}))
        topic.push_batch([T0 + 2], imu_batch(5, 1))
        assert topic.status is TopicWriterStatus.Active
    table = client.topic_handler(name, "/imu", ontology=IMU).to_arrow()
    assert table.column("timestamp_ns").to_pylist() == [T0, T0, T0 + 1, T0 + 1, T0 + 2, T0 + 2]


def test_push_stream_and_flush_policy(client: m.MosaicoClient, name: str) -> None:
    wire = pa.schema([pa.field("timestamp_ns", pa.int64(), nullable=False), *PAYLOAD])
    batches = [
        pa.RecordBatch.from_arrays([timestamps(s, 100), *imu_batch(s, 100).columns], schema=wire)
        for s in range(0, 1000, 100)
    ]
    with client.sequence_create(name, {}) as seq:
        topic = seq.topic_create("/imu", {}, IMU)
        topic.set_flush_policy(max_rows=250, max_age=1.0)
        topic.push_stream(pa.Table.from_batches(batches))
        topic.flush()
        with pytest.raises(m.InvalidArgumentError):
            topic.set_flush_policy(max_age=0)
        with pytest.raises(m.InvalidArgumentError):
            topic.set_flush_policy(max_bytes=-1)
    imu = client.topic_handler(name, "/imu", ontology=IMU)
    assert imu.to_arrow().num_rows == 1000


def test_empty_topic(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        seq.topic_create("/imu", {}, IMU)
    imu = client.topic_handler(name, "/imu", ontology=IMU)
    assert imu.timestamp_ns_min is None
    assert list(imu.get_data_streamer()) == []
    table = imu.to_arrow()
    assert table.num_rows == 0
    assert table.schema.names == ["timestamp_ns", *PAYLOAD.names]
    assert pa.RecordBatchReader.from_stream(imu.get_batch_streamer()).read_all().num_rows == 0


def test_update(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        write_imu(seq.topic_create("/a", {}, IMU), 10)
    with client.sequence_handler(name).update() as upd:
        write_imu(upd.topic_create("/b", {}, IMU), 20)
    assert upd.status is SessionStatus.Finalized
    handler = client.sequence_handler(name)
    assert handler.topics == ["/a", "/b"]
    assert len(handler.sessions) == 2
    with pytest.raises(m.SequenceNotFoundError):
        with client.sequence_update(name + "_missing"):
            pass


def test_threads(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        topics = [seq.topic_create(f"/imu{i}", {}, IMU) for i in range(4)]
        threads = [threading.Thread(target=write_imu, args=(t, 20_000, 500)) for t in topics]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    handler = client.sequence_handler(name)
    for i in range(4):
        assert handler.get_topic_handler(f"/imu{i}", ontology=IMU).to_arrow().num_rows == 20_000


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_fork(client: m.MosaicoClient) -> None:
    client.list_sequences()
    pid = os.fork()
    if pid == 0:
        try:
            client.list_sequences()
        except m.MosaicoError as e:
            os._exit(0 if "fork" in str(e) else 2)
        except BaseException:
            os._exit(3)
        os._exit(1)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    client.list_sequences()


def test_sliced_nested_batches(client: m.MosaicoClient, name: str) -> None:
    n = 3000
    rows = imu_batch(0, n).to_pylist()
    for i, row in enumerate(rows):
        if i % 7 == 0:
            row["header"] = None
        if i % 5 == 0:
            row["acceleration"]["covariance"] = [float(i)] * 9
            row["acceleration"]["header"] = {"timestamp": {"seconds": i, "nanoseconds": 1}, "frame_id": None}
    payload = pa.RecordBatch.from_pylist(rows, schema=PAYLOAD)
    ts = timestamps(0, n)
    with client.sequence_create(name, {}) as seq:
        topic = seq.topic_create("/imu", {}, IMU)
        for start in range(0, n, 333):
            k = min(333, n - start)
            topic.push_batch(ts.slice(start, k), payload.slice(start, k))
    table = client.topic_handler(name, "/imu", ontology=IMU).to_arrow()
    assert table.column("timestamp_ns").to_pylist() == ts.to_pylist()
    assert table.select(PAYLOAD.names).to_pylist() == payload.to_pylist()


def test_push_errors_raise_and_leave_the_topic_open(client: m.MosaicoClient, name: str) -> None:
    rows = imu_batch(0, 2).to_pylist()
    rows[1]["acceleration"] = None
    failures = [
        (m.InvalidArgumentError, timestamps(0, 3), imu_batch(0, 2)),
        (m.InvalidArgumentError, np.array([T0, T0 + 1], "datetime64[ns]"), imu_batch(0, 2)),
        (m.InvalidArgumentError, [1.5, 2.5], imu_batch(0, 2)),
        (m.InvalidArgumentError, pa.array([T0, None], pa.int64()), imu_batch(0, 2)),
        (m.InvalidArgumentError, timestamps(0, 2), "not a batch"),
        (m.SchemaError, timestamps(0, 2), pa.RecordBatch.from_pydict({"x": [1.0, 2.0]})),
        (m.SchemaError, timestamps(0, 2), pa.RecordBatch.from_pylist(rows, schema=PAYLOAD)),
    ]
    with client.sequence_create(name, {}) as seq:
        topic = seq.topic_create("/ignore", {}, IMU, on_error=TopicLevelErrorPolicy.Finalize)
        for i, (error, ts, data) in enumerate(failures):
            with pytest.raises(error):
                topic.push_batch(ts, data)
            assert topic.status is TopicWriterStatus.RaisedException
            assert topic.last_error
            topic.push_batch([T0 + 2 * i, T0 + 2 * i + 1], imu_batch(0, 2))
            assert topic.status is TopicWriterStatus.Active
    table = client.topic_handler(name, "/ignore", ontology=IMU).to_arrow()
    assert table.column("timestamp_ns").to_pylist() == [T0 + i for i in range(2 * len(failures))]


def test_max_batch_size_bytes_matches_sdk(client: m.MosaicoClient) -> None:
    import mosaicolabs

    with mosaicolabs.MosaicoClient.connect("localhost", PORT) as sdk:
        assert client.max_batch_size_bytes == sdk.max_batch_size_bytes


def test_exported_stream(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        write_imu(seq.topic_create("/imu", {}, IMU), N, batch=100)
    streamer = client.topic_handler(name, "/imu", ontology=IMU).get_batch_streamer()
    reader = pa.RecordBatchReader.from_stream(streamer)
    with pytest.raises(m.StreamClosedError):
        next(streamer)
    with pytest.raises(m.StreamClosedError):
        pa.RecordBatchReader.from_stream(streamer)
    assert reader.read_all().num_rows == N


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_exported_stream_after_fork(client: m.MosaicoClient, name: str) -> None:
    with client.sequence_create(name, {}) as seq:
        write_imu(seq.topic_create("/imu", {}, IMU), N, batch=100)
    imu = client.topic_handler(name, "/imu", ontology=IMU)
    reader = pa.RecordBatchReader.from_stream(imu.get_batch_streamer())
    first = reader.read_next_batch().num_rows
    pid = os.fork()
    if pid == 0:
        signal.alarm(10)
        try:
            reader.read_next_batch()
        except pa.ArrowInvalid as e:
            os._exit(0 if "fork" in str(e) else 2)
        except BaseException:
            os._exit(3)
        os._exit(1)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert first + reader.read_all().num_rows == N


class _Exported:
    def __init__(self, batch: pa.RecordBatch) -> None:
        self.pair = batch.__arrow_c_array__()

    def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
        return self.pair


def test_import_of_unusual_batches(client: m.MosaicoClient, name: str) -> None:
    inner = pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.struct([pa.field("c", pa.int32())]))])
    schema = pa.schema([pa.field("timestamp_ns", pa.int64(), nullable=False), pa.field("v", pa.list_(inner, 2))])
    values = pa.StructArray.from_arrays(
        [pa.array(range(20), pa.int32()), pa.StructArray.from_arrays([pa.array(range(100, 120), pa.int32())], names=["c"])],
        names=["a", "b"],
    )
    batch = pa.RecordBatch.from_arrays([timestamps(0, 10), pa.FixedSizeListArray.from_arrays(values, 2)], schema=schema)
    exported = _Exported(batch.slice(7, 3))
    with client.sequence_create(name, {}) as seq:
        assert seq._core is not None
        topic = seq._core.topic_create("/v", "Custom", "default", schema, None, "raise")
        topic.push_batch(batch.slice(3, 4))
        topic.push_batch(exported)
        with pytest.raises(m.InvalidArgumentError, match="consumed"):
            topic.push_batch(exported)
        with pytest.raises(m.InvalidArgumentError):
            topic.push_batch([1, 2])
    stored = pa.RecordBatchReader.from_stream(client._core.topic_handler(name, "/v").get_batch_streamer()).read_all()
    assert stored.column("v").to_pylist() == batch.slice(3, 7).column("v").to_pylist()


def test_session_api(client: m.MosaicoClient, name: str) -> None:
    seq = client.sequence_create(name, {})
    for call in (
        lambda: seq.session_status,
        lambda: seq.session_locator,
        lambda: seq.topic_writer_exists("/imu"),
        seq.list_topic_writers,
        lambda: seq.get_topic_writer("/imu"),
    ):
        with pytest.raises(RuntimeError):
            call()
    with seq:
        assert seq.session_status is SessionStatus.Pending
        assert seq.session_locator.startswith(f"{name}:")
        topic = seq.topic_create("/imu", {}, IMU)
        core = seq._core
    assert seq.session_status is SessionStatus.Finalized
    assert seq.get_topic_writer("imu") is topic
    assert seq.topic_writer_exists("/imu")
    assert client.sequence_handler(name).sessions[0].locator == seq.session_locator
    assert core is not None
    with pytest.raises(m.StreamClosedError):
        core.get_topic_writer("/imu")


def test_topic_writers_from_threads(client: m.MosaicoClient, name: str) -> None:
    errors: list[BaseException] = []
    done = threading.Event()
    with client.sequence_create(name, {}) as seq:

        def lookup() -> None:
            i = 0
            while not done.is_set():
                try:
                    seq.get_topic_writer(f"/t{i}")
                    i += 1
                except m.TopicNotFoundError:
                    pass
                except BaseException as e:
                    errors.append(e)
                    i += 1

        thread = threading.Thread(target=lookup)
        thread.start()
        try:
            for i in range(50):
                seq.topic_create(f"/t{i}", {}, IMU)
        finally:
            done.set()
            thread.join()
    assert errors == []


def test_push_copies_the_caller_buffers(client: m.MosaicoClient, name: str) -> None:
    n = 50
    ts = T0 + np.arange(n, dtype=np.int64)
    values = np.arange(n, dtype=np.float64)
    frames = np.arange(n * 8, dtype=np.uint8)
    offsets = np.arange(0, n * 8 + 1, 8, dtype=np.int32)
    data = pa.Array.from_buffers(pa.binary(), n, [None, pa.py_buffer(offsets), pa.py_buffer(frames)])
    floats = pa.schema(list(Floating64.__msco_pyarrow_struct__))
    images = pa.schema(list(CompressedImage.__msco_pyarrow_struct__))
    expected = [frames[8 * i : 8 * i + 8].tobytes() for i in range(n)]
    with client.sequence_create(name, {}) as seq:
        seq.topic_create("/floats", {}, Floating64).push_batch(
            pa.array(ts), pa.RecordBatch.from_arrays([pa.array(values)], schema=floats)
        )
        seq.topic_create("/images", {}, CompressedImage).push_batch(
            pa.array(ts),
            pa.RecordBatch.from_arrays(
                [pa.nulls(n, images.field("header").type), data, pa.array(["raw"] * n)], schema=images
            ),
        )
        ts[:] = 0
        values[:] = -1
        frames[:] = 0
    stored = client.topic_handler(name, "/floats", ontology=Floating64).to_arrow()
    assert stored.column("timestamp_ns").to_pylist() == [T0 + i for i in range(n)]
    assert stored.column("data").to_pylist() == [float(i) for i in range(n)]
    stored = client.topic_handler(name, "/images", ontology=CompressedImage).to_arrow()
    assert stored.column("data").to_pylist() == expected


BACKLOG = 2000


def push_backlog(seq: m.SequenceWriter) -> m.TopicWriter:
    topic = seq.topic_create("/imu", {}, IMU)
    topic.set_flush_policy(max_bytes=0)
    payload = imu_batch(0, 10)
    for i in range(BACKLOG):
        topic.push_batch(timestamps(10 * i, 10), payload)
    return topic


def test_ctrl_c_inside_the_with_does_not_send_the_queue(client: m.MosaicoClient, name: str) -> None:
    import mosaicolabs

    seq = client.sequence_create(name, {})
    topics: list[m.TopicWriter] = []
    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        with seq:
            topics.append(push_backlog(seq))
            started = time.monotonic()
            raise KeyboardInterrupt
    assert time.monotonic() - started < 5
    assert seq.status is SessionStatus.Error
    assert topics[0].status is TopicWriterStatus.FinalizedWithError
    handler = client.sequence_handler(name)
    assert all(session.locked for session in handler.sessions)
    assert handler.get_topic_handler("/imu", ontology=IMU).to_arrow().num_rows < BACKLOG * 10
    with mosaicolabs.MosaicoClient.connect("localhost", PORT) as sdk:
        messages = [n.message for n in sdk.list_sequence_notifications(name)]
    assert any("KeyboardInterrupt" in message for message in messages)


@pytest.mark.parametrize("policy", [SessionLevelErrorPolicy.Report, SessionLevelErrorPolicy.Delete])
def test_ctrl_c_during_finish_closes_by_policy(
    client: m.MosaicoClient, name: str, policy: SessionLevelErrorPolicy
) -> None:
    seq = client.sequence_create(name, {}, on_error=policy)
    topics: list[m.TopicWriter] = []
    timer = threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGINT))
    started = time.monotonic()
    try:
        with pytest.raises(KeyboardInterrupt):
            with seq:
                topics.append(push_backlog(seq))
                started = time.monotonic()
                timer.start()
    finally:
        timer.cancel()
    assert time.monotonic() - started < 5
    assert seq.status is SessionStatus.Error
    assert topics[0].status is TopicWriterStatus.FinalizedWithError
    assert topics[0].last_error == "interrupted"
    if policy is SessionLevelErrorPolicy.Delete:
        with pytest.raises(m.SequenceNotFoundError):
            client.sequence_handler(name)
        return
    handler = client.sequence_handler(name)
    assert all(session.locked for session in handler.sessions)
    assert handler.get_topic_handler("/imu", ontology=IMU).to_arrow().num_rows < BACKLOG * 10


def test_reads_without_data_have_the_schema_of_reads_with_data(client: m.MosaicoClient, name: str) -> None:
    images = pa.schema(list(CompressedImage.__msco_pyarrow_struct__))
    n = 3
    with client.sequence_create(name, {}) as seq:
        seq.topic_create("/empty", {}, CompressedImage)
        seq.topic_create("/full", {}, CompressedImage).push_batch(
            timestamps(0, n),
            pa.RecordBatch.from_arrays(
                [pa.nulls(n, images.field("header").type), pa.array([b"a", b"b", b"c"]), pa.array(["raw"] * n)],
                schema=images,
            ),
        )
    handler = client.sequence_handler(name)
    full = handler.get_topic_handler("/full", ontology=CompressedImage)
    with_data = full.to_arrow()
    assert with_data.num_rows == n
    never_written = handler.get_topic_handler("/empty", ontology=CompressedImage).to_arrow()
    empty_window = full.to_arrow(T0 + 10 * STEP_NS, T0 + 20 * STEP_NS)
    for empty in (never_written, empty_window):
        assert empty.num_rows == 0
        assert empty.schema == with_data.schema
        assert pa.concat_tables([empty, with_data]).num_rows == n


def sample_messages(n: int) -> dict[str, list[Message]]:
    def header(i: int) -> Header | None:
        if i % 3 == 0:
            return None
        return Header(timestamp=Time(seconds=i, nanoseconds=7), frame_id=None if i % 2 else "f", sample_counter=i)

    def vector(i: int) -> Vector3d:
        covariance = [float(i)] * 9 if i % 4 == 0 else None
        return Vector3d(x=float(i), y=0.5, z=-1.0, covariance=covariance, covariance_type=1 if covariance else None)

    def imu(i: int) -> IMU:
        orientation = None if i % 2 else Quaternion(x=0, y=0, z=0, w=1, header=header(i + 1))
        return IMU(header=header(i), acceleration=vector(i), angular_velocity=vector(i + 1), orientation=orientation)

    def image(i: int) -> Image:
        data = bytes([i % 256]) * (16 + i)
        bigendian = None if i % 2 else True
        return Image(
            header=header(i), data=data, format=ImageFormat.RAW, width=4, height=4, stride=4, encoding="mono8", is_bigendian=bigendian
        )

    def compressed(i: int) -> CompressedImage:
        return CompressedImage(header=header(i), data=bytes([i % 256]) * (8 + i), format=ImageFormat.JPEG)

    def gps(i: int) -> GPS:
        return GPS(header=header(i), position=Point3d(x=float(i), y=1.0, z=2.0))

    makers = {"imu": imu, "image": image, "compressed": compressed, "gps": gps}
    return {key: [Message(timestamp_ns=T0 + i // 2, data=make(i)) for i in range(n)] for key, make in makers.items()}


def test_push_stores_what_the_sdk_stores(client: m.MosaicoClient, name: str) -> None:
    messages = sample_messages(700)
    types = {"imu": IMU, "image": Image, "compressed": CompressedImage, "gps": GPS}
    with client.sequence_create(name, {}) as seq:
        writers = {key: seq.topic_create(key, {}, types[key]) for key in messages}
        for i in range(700):
            for key, writer in writers.items():
                writer.push(messages[key][i])
                if i == 350:
                    writer.flush()
    for key, ontology in types.items():
        table = client.topic_handler(name, key, ontology=ontology).to_arrow()
        expected = pa.RecordBatch.from_pylist([msg._encode() for msg in messages[key]], schema=Message._get_schema(ontology))
        assert sorted(map(repr, table.to_pylist())) == sorted(map(repr, expected.to_pylist())), key
        assert table.column("timestamp_ns").to_pylist() == expected.column("timestamp_ns").to_pylist(), key


def test_push_order_and_errors_match_sdk(client: m.MosaicoClient, name: str) -> None:
    def imu(i: int) -> IMU:
        return IMU(acceleration=Vector3d(x=i, y=0, z=0), angular_velocity=Vector3d(x=0, y=0, z=i))

    with client.sequence_create(name, {}) as seq:
        topic = seq.topic_create("/imu", {}, IMU, on_error=TopicLevelErrorPolicy.Ignore)
        topic.push(Message(timestamp_ns=T0 + 10, data=imu(0)))
        with pytest.raises(m.InvalidArgumentError, match="non-decreasing"):
            topic.push(Message(timestamp_ns=T0 + 9, data=imu(1)))
        assert topic.status is TopicWriterStatus.RaisedException
        with pytest.raises(m.InvalidArgumentError, match="stores IMU"):
            topic.push(Message(timestamp_ns=T0 + 11, data=GPS(position=Point3d(x=0, y=0, z=0))))
        topic.push(Message(timestamp_ns=T0 + 10, data=imu(2)))
        assert topic.status is TopicWriterStatus.Active
        with pytest.raises(m.InvalidArgumentError, match="non-decreasing"):
            topic.push_batch([T0 + 5], imu_batch(0, 1))
        topic.push_batch([T0 + 12], imu_batch(3, 1))
        with pytest.raises(m.InvalidArgumentError, match="non-decreasing"):
            topic.push(Message(timestamp_ns=T0 + 11, data=imu(4)))
        topic.push(Message(timestamp_ns=T0 + 12, data=imu(5)))

        too_large = Image(
            data=b"\0" * (client.max_batch_size_bytes + 1), format=ImageFormat.RAW, width=1, height=1, stride=1, encoding="mono8"
        )
        images = seq.topic_create("/images", {}, Image)
        images.push(Message(timestamp_ns=T0, data=too_large))
        assert images.status is TopicWriterStatus.RecordTooLarge
        assert images.dropped_record_count == 1
        assert images.last_error is not None and "record dropped" in images.last_error
        small = Image(data=b"\1", format=ImageFormat.RAW, width=1, height=1, stride=1, encoding="mono8")
        images.push(Message(timestamp_ns=T0, data=small))
        assert images.status is TopicWriterStatus.Active
    table = client.topic_handler(name, "/imu", ontology=IMU).to_arrow()
    assert table.column("timestamp_ns").to_pylist() == [T0 + 10, T0 + 10, T0 + 12, T0 + 12]
    assert pc.struct_field(table.column("acceleration"), "x").to_pylist() == [0.0, 2.0, 3.0, 5.0]
    assert client.topic_handler(name, "/images", ontology=Image).to_arrow().num_rows == 1
    notes = client.list_topic_notifications(name, "/images")
    assert any("record dropped" in note.message for note in notes)


def test_generic_push_order_and_errors_match_sdk(client: m.MosaicoClient, name: str) -> None:
    def gps(i: int) -> GPS:
        return GPS(position=Point3d(x=float(i), y=0, z=0))

    with client.sequence_create(name, {}) as seq:
        topic = seq.topic_create("/gps", {}, GPS)
        topic.push(Message(timestamp_ns=T0 + 10, data=gps(0)))
        with pytest.raises(m.InvalidArgumentError, match="non-decreasing"):
            topic.push(Message(timestamp_ns=T0 + 9, data=gps(1)))
        assert topic.status is TopicWriterStatus.RaisedException
        with pytest.raises(m.InvalidArgumentError, match="stores GPS"):
            topic.push(Message(timestamp_ns=T0 + 11, data=IMU(acceleration=Vector3d(x=0, y=0, z=0), angular_velocity=Vector3d(x=0, y=0, z=0))))
        topic.push(Message(timestamp_ns=T0 + 10, data=gps(2)))
        assert topic.status is TopicWriterStatus.Active
        topic.flush()
        with pytest.raises(m.InvalidArgumentError, match="non-decreasing"):
            topic.push(Message(timestamp_ns=T0 + 9, data=gps(3)))
        topic.push(Message(timestamp_ns=T0 + 12, data=gps(4)))
    table = client.topic_handler(name, "/gps", ontology=GPS).to_arrow()
    assert table.column("timestamp_ns").to_pylist() == [T0 + 10, T0 + 10, T0 + 12]
    assert sorted(pc.struct_field(table.column("position"), "x").to_pylist()) == [0.0, 2.0, 4.0]


def test_gzip_compression_round_trips(name: str) -> None:
    gzip_port = os.environ.get("MOSAICO_GZIP_PORT")
    if gzip_port is None:
        pytest.skip("set MOSAICO_GZIP_PORT to a mosaicod started with MOSAICOD_GZIP_ENABLED=true")
    with m.MosaicoClient.connect("localhost", int(gzip_port), compression="gzip") as gz:
        with gz.sequence_create(name, {}) as seq:
            write_imu(seq.topic_create("/imu", {}, IMU), N, batch=700)
        table = gz.topic_handler(name, "/imu", ontology=IMU).to_arrow()
    assert table.column("timestamp_ns").to_pylist() == timestamps(0, N).to_pylist()
    assert table.select(PAYLOAD.names).to_pylist() == imu_batch(0, N).to_pylist()


def test_png_images_round_trip_through_the_sdk(client: m.MosaicoClient, name: str) -> None:
    rng = np.random.default_rng(5)
    frames = [rng.integers(0, 8, (48, 64 * 3), dtype=np.uint8) for _ in range(6)]
    payload = pa.schema(list(Image.__msco_pyarrow_struct__))
    raw = [m.image_from_linear_pixels(f, 64 * 3, 48, 64, "rgb8", format=ImageFormat.RAW).model_dump() for f in frames]
    with client.sequence_create(name, {}) as seq:
        single = seq.topic_create("/single", {}, Image)
        for i, frame in enumerate(frames):
            single.push(Message(timestamp_ns=T0 + i, data=m.image_from_linear_pixels(frame, 64 * 3, 48, 64, "rgb8")))
        batched = seq.topic_create("/batched", {}, Image)
        batched.push_batch(timestamps(0, 6), m.encode_png_batch(pa.RecordBatch.from_pylist(raw, schema=payload)))
    for topic in ("/single", "/batched"):
        rows = client.topic_handler(name, topic, ontology=Image).to_arrow().select(payload.names).to_pylist()
        assert len(rows) == len(frames)
        for row, frame in zip(rows, frames):
            image = Image(**row)
            assert image.format is ImageFormat.PNG
            assert bytes(image.to_linear_pixels()) == frame.tobytes()


def test_notifications_exists_and_session_delete(client: m.MosaicoClient, name: str) -> None:
    assert not client.sequence_exists(name)
    with client.sequence_create(name, {}) as seq:
        write_imu(seq.topic_create("/imu", {}, IMU), 10)
    assert client.sequence_exists(name)
    assert client.topic_exists(name, "imu")
    assert not client.topic_exists(name, "/missing")
    assert client.list_sequence_notifications(name) == []

    with pytest.raises(RuntimeError, match="boom"):
        with client.sequence_update(name) as update:
            write_imu(update.topic_create("/extra", {}, IMU), 10)
            raise RuntimeError("boom")
    notes = client.list_sequence_notifications(name) + client.list_topic_notifications(name, "/extra")
    assert notes and all(note.sequence_name == name for note in notes)
    assert any("boom" in note.message for note in notes)
    topic_notes = client.list_topic_notifications(name, "/extra")
    assert all(note.topic_name == "/extra" for note in topic_notes)
    client.clear_sequence_notifications(name)
    client.clear_topic_notifications(name, "/extra")
    assert client.list_sequence_notifications(name) == []
    assert client.list_topic_notifications(name, "/extra") == []

    with client.sequence_update(name) as update:
        write_imu(update.topic_create("/later", {}, IMU), 5)
    assert client.topic_exists(name, "/later")
    client.session_delete(update.session_locator)
    assert not client.topic_exists(name, "/later")
    assert client.topic_exists(name, "/imu")
