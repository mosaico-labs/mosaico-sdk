# Mosaico SDK

Draft of the design of the Mosaico SDK.

Parts marked *planned* are designed but not in the code yet: multi-topic reads and parallel ranges, queries, `CancelToken`, the Rust `blocking` module and `RowRef`.

## Layers

![SDK layers](docs/diagrams/layers.svg)

- **SDK.** What users import. Python keeps what needs Python: ontology classes and views, exceptions, pandas and ml helpers.
- **API.** The public objects, the same in Python and Rust.
- **Reader and writer.** Internal blocks that run one Flight stream each, or merge several.
- **Connection.** One per client, cloned into every block that calls the server. Not public in either SDK.
- **mosaicod.** The server. The SDK uses only its five Flight calls.

## Flows

### Topic read

![Topic read](docs/diagrams/topic-read.svg)

1. `topic_handler()` calls `get_flight_info` once and keeps the metadata and the ticket.
2. A read with no time range reuses that ticket. A read with a range asks for the ticket of that window first. A topic that had no data at open has no ticket, so each of its reads asks for one again: data written since then is read, but `timestamp_ns_min/max` keep the values read at open.
3. The `BatchReader` runs `do_get` and decodes batches in the background.
4. The user gets messages (views), record batches, one Arrow table or one DataFrame.

A topic with no data gives an empty stream. Reads are independent: several can run at once on the same topic. Ranges include the start and exclude the end; either bound can be left out. A range whose start is not before its end is refused by mosaicod (`InvalidArgumentError`), as in today's SDK. In Python a bound that is not an int64 raises `InvalidArgumentError` before any call.

### Multi-topic read · planned

![Multi-topic read](docs/diagrams/multi-topic-read.svg)

1. `sequence_handler()` calls `get_flight_info` on the sequence once: one ticket per topic.
2. `get_data_streamer(topics, start, end)` hands the K tickets to the `MultiReader`.
3. The `MultiReader` opens K streams and merges them by timestamp into windows: one slice per topic plus the row order. Equal timestamps follow the topic order.
4. The handler walks the order and yields `(topic, msg)`, with `msg.data` as a view.

### Parallel ranges · planned

![Parallel ranges](docs/diagrams/parallel-ranges.svg)

The handler splits a time range into N parts and asks for one ticket per part. The `MultiReader` reads the N streams in parallel and returns them in order, so the user gets the same result as a single read.

### Write

![Write](docs/diagrams/write.svg)

1. `sequence_create()` or `sequence_update()` opens a write session with `do_action`.
2. `topic_create()` creates the topic and opens its `BatchWriter` with `do_put`.
3. `push` appends the row to the topic's builder in the core and hands it to the `BatchWriter` at about 1 MiB, or earlier on the flush policy limits. Builders exist for the ontologies written by hand (IMU, Image, CompressedImage). For the other ontologies Python converts the message to one Arrow row, as today's SDK does, and the core buffers the rows with `push_row` under the same rules. A row that alone exceeds the server limit is dropped, notified and counted, and the writer status becomes `RecordTooLarge`.
4. The `BatchWriter` splits batches under the server limit and sends them in the background. The queued data of all topics shares one budget per client: when it is full, the topics send what they have joined so far.
5. Leaving the `with` block (or `complete(result)` in Rust) waits for the sends and finalizes the session. On error, the session policy deletes or reports it. Server errors surface at the next push or when the session finishes.

### Other operations

![Other operations](docs/diagrams/other-operations.svg)

- Sequence lists go through `list_flights` in one call that returns the whole list, like `list_sequences()` in today's SDK. The protocol can also return it in pages with a cursor, which today's SDK exposes as `list_sequences_page`; the core has no paged listing. Queries, notifications and deletes go through `do_action`. All on the same connection.
- *Planned:* queries are built in each SDK and run by the core.
- *Planned:* reads and queries accept a `CancelToken`: cancelling it stops the call and its streams. Until then `close()` stops them.
- `close()` on the client, a sequence or a topic closes every stream opened below it. A handler stays usable after `close()`. Closing the client is permanent.

## Dependencies

Solid arrows: who creates whom. Dashed arrows: who calls mosaicod through the connection.

![Dependencies](docs/diagrams/dependencies.svg)

- `MosaicoClient` is the only object the user creates directly. It opens the `Connection` and uses it directly for lists, queries, notifications and deletes.
- The client gives handlers, write sessions and, from `query()`, a `QueryResponse`. A `SequenceHandler` also gives its `TopicHandler`s and the `SequenceUpdater`.
- A read returns a streamer. It wraps a `BatchReader` for one topic, or a `MultiReader` for several topics or parallel ranges, which opens one `BatchReader` per stream.
- A write session owns its `Session` and its topics. `topic_create` registers the topic through the `Session`, then builds its `TopicWriter` and opens its `BatchWriter`, which receives the `push_batch` calls and the rows joined by `push`.
- Every block that calls mosaicod uses a clone of the client's `Connection`. Internal blocks have no public constructor.

## Classes

### Python SDK · public

The Python package, rebuilt on the Rust core through PyO3. Calls are synchronous, release the GIL while waiting and stop on Ctrl+C.

| Member | What it does |
|---|---|
| `msg.data` | Read-only view of one row, generated in Python. A field is read only when accessed |
| `msg.data` (type without generated code) | *Planned*: a generic view built at runtime from the topic schema, with the same access and no static types. Today Python reads messages only for `IMU`; `get_data_streamer` on another ontology raises `NotImplementedError` and points to `get_batch_streamer()` and `to_arrow()`. Rust reads `Imu`, `Image` and `CompressedImage` as messages |
| `msg.get_data()` | Owned ontology object, a copy of the row |
| `to_pandas(start, end)` | One DataFrame with dotted column names |
| `MosaicoError` | Base class of every exception the SDK raises. Subclasses by cause: `ConnectError` (also a `ConnectionError`), `SequenceNotFoundError` (also from a write session whose sequence or session is gone), `TopicNotFoundError`, `InvalidArgumentError`, `SchemaError` and `StreamClosedError` (also `ValueError`), `AlreadyExistsError`, `PermissionDeniedError` (missing or wrong API key, also a `PermissionError`), `OntologyMismatchError`, `StreamError`, and `ServerError` for the rest |
| `image_from_linear_pixels(data, stride, height, width, encoding)` | Same signature as today's SDK; PNG is encoded in Rust without the GIL, same layout (8-bit gray, height × stride). A size mismatch raises instead of falling back to RAW |
| `encode_png(data, height, stride)` · `encode_png_batch(batch)` | PNG of one buffer, or of every `raw` row of an `Image` batch on all cores, for `push_batch` |
| `decode_png(data)` · `decode_png_batch(batch)` · `decode_png_table(table)` | Back to the raw bytes, as `to_linear_pixels` of today's SDK but returning `bytes`. The batch and table forms decode every `png` row and mark it `raw`; the table form decodes all its rows on all cores (the server sends images a row or two per batch). Every PNG type gives the same bytes as Pillow, which today's SDK uses (low bit depths expanded to one byte per pixel, 16-bit color to 8 bits). An invalid PNG raises instead of returning the data unchanged. `MOSAICO_PNG_THREADS` limits the threads of encoding and decoding |

### Rust SDK · public

The core crate itself, used directly by Rust programs. Async on tokio; a blocking module that mirrors the same API is planned. Errors are `Result<T, Error>` with an error kind. Generated ontology types read rows as typed views.

| Member | What it does |
|---|---|
| `MosaicoClient::connect(ConnectOptions::new(host, port)).await` | Async entry point. TLS and other options on `ConnectOptions`, set with its methods: `ConnectOptions::new(host, port).api_key(key).tls(true)` |
| `#[non_exhaustive]` | One rule for the public types, so later additions do not break callers. Options (`ConnectOptions`, `FlushPolicy`) are built with `new`/`default()` and their methods, not with `{ … }`; their fields stay readable and settable. What the SDK returns (`Notification`, `ServerInfo`, `SessionInfo`, `Message`) cannot be built outside the crate. Enums that may grow (`ErrorKind`, `TopicWriterStatus`, `Compression`, `Format`, the two error policies) need a `_` arm in a `match` |
| `topic.get_data_streamer::<Imu>(start, end)` | Typed read. The type must match the topic ontology tag exactly |
| `while let Some(msg) = s.next().await?` | Iteration over messages or batches |
| `trait Ontology` | `TAG`, `Columns`, `View<'a>` and `from_view`, one per generated type. Columns are looked up and checked once per batch; a view is a row index into them |
| `view.acceleration()?.z()?` | View fields are methods returning `Result`, since a required field can be null. The owned model has plain pub fields |
| `blocking::MosaicoClient` | *Planned.* The same API without async |
| `Error::kind()` | Connection, not found, invalid argument, schema, closed, server, … |
| `RecordBatch` | arrow-rs batches, passed without conversion |
| `encode_png_column(&[Pixels])` · `encode_png_batch(&batch)` | Raw images to the PNG `data` column of `Image`, on all cores: the layout of today's SDK (8-bit gray, height × stride). The batch form encodes only the `raw` rows. Behind the `png` feature |
| `decode_png(&[u8])` · `decode_png_column` · `decode_png_batch` · `decode_png_batches(&[batch])` | PNG back to raw bytes, the same bytes as today's SDK; the batch forms decode the `png` rows on all cores. Behind the `png` feature |
| `trait Encode` · `RecordBuilder` | Write side of an ontology: `FORMAT`, payload `fields()`, `size()` of a row and a builder that appends rows to Arrow columns. Written by hand for `Imu`, `Image`, `CompressedImage` |
| `seq.topic_create_for::<Imu>(name, metadata, on_error)` · `topic.push(ts, &imu)` | Typed write: the topic schema comes from the type, rows go through the builder. `push` refuses a type whose tag or format differs from the topic's (invalid argument), as Python does |
| `topic.push_row(row)` | One record already in Arrow (one row with the topic schema), for schemas with no `Encode` type: buffered and sent like typed records, where `push_batch` would queue every one-row batch on its own. The core estimates its size from the Arrow buffers, as for splitting batches; a row whose encoded form still exceeds the limit is dropped at encoding. The Python package uses it for the ontologies without a builder |
| `topic.push_stream(stream)` | Any `Stream` of `Result<RecordBatch, E>`, pushed batch by batch. Reading the source is up to the caller, so it never blocks the runtime |
| `RowRef` · `view::<T>()` | *Planned*, with multi-topic reads: one row of any topic, read as type `T`; `None` if the topic is not of type `T` |
| `view.to_model()` · `msg.to_model()` | Owned copy of a typed view: an `ImuView` gives an `Imu` |

### MosaicoClient · public

Entry point. Holds the single connection and reads the server version and limits once, at connect. Creates handlers and write sessions. Catalog operations and queries go straight to the connection.

| Member | What it does |
|---|---|
| `connect(host, port, tls, api_key, compression)` | Opens the connection and calls `info`. Python also takes today's `GRPCCompression`: its `level` is ignored, and `StreamGzip` gives no compression, with a warning. Today's SDK behaves the same, because its gRPC ignores the level on the client and no longer knows `StreamGzip`. `gzip` is applied by gRPC on the I/O threads (two in Python), a whole message at a time, in both directions; `lz4` and `zstd` compress the Arrow buffers while encoding, off those threads, so they suit heavy writes better |
| `sequence_handler(name)` | `SequenceHandler`. Raises if the sequence does not exist |
| `topic_handler(sequence, topic, ontology=…)` | `TopicHandler`, checked against the ontology |
| `sequence_create(name, metadata)` · `sequence_update(name)` | A write session, used in a `with` block. A new name is checked as in today's SDK: letters, digits, `-` and `_`, starting with a letter or a digit; otherwise `InvalidArgumentError` |
| `list_sequences()` · `sequence_delete(name)` · `session_delete(locator)` | `list_sequences` returns every name in one call through `list_flights`; deletes go through `do_action` |
| `sequence_exists(name)` · `topic_exists(sequence, topic)` | `False` when the server answers not found or rejects the name as invalid; other errors raise |
| `list_*_notifications(…)` · `clear_*_notifications(…)` | Notifications of a sequence or topic, such as dropped records or a failed session |
| `query(…)` | *Planned.* Runs a query. Results grouped by sequence |
| `close()` | Closes every stream of the client and its handlers. Permanent: later calls fail as closed. A write session still open stops its uploads and can no longer finalize or delete itself, so it stays open on the server, as its error says |

### TopicHandler · public

One topic. Opening it reads the metadata once: ontology tag, time range and ticket. The Arrow schema arrives at the start of each read.

| Member | What it does |
|---|---|
| `name` · `ontology_tag` · `timestamp_ns_min` · `timestamp_ns_max` | Metadata read at open |
| `get_data_streamer(start, end)` | Messages in time order; `msg.data` is a view. The schema is checked when the stream opens. In Rust the type is a parameter: `get_data_streamer::<T>()` |
| `get_batch_streamer(start, end)` | Plain Arrow record batches; also an Arrow stream that DuckDB, Polars or a C program read directly |
| `to_arrow(start, end)` | One Arrow table, without copies. In Rust the schema and the batches. In Python a read without data has the same schema as one with data: top-level strings and binaries as views, as mosaicod sends them. In Rust it has an empty schema, also from `get_batch_streamer`, since the core knows the schema only from the stream |
| `to_pandas(start, end)` | One DataFrame (Python only) |
| `cancel=token` | *Planned.* Accepted by every read; cancelling stops the stream |
| `close()` | Closes every stream opened from this handler and its clones. The handler stays usable |

### SequenceHandler · public

One sequence. A single `get_flight_info` returns the metadata and the tickets of every topic, so its topic handlers and multi-topic reads need no other metadata call.

| Member | What it does |
|---|---|
| `topics` · `sessions` · `user_metadata` · `timestamp_ns_min/max` | Metadata read at open |
| `topic_handler(topic, ontology=…)` | `TopicHandler` built from the saved ticket, checked against the ontology as `client.topic_handler`. Same name as on the client, in Python and Rust (where it takes no ontology); today's SDK calls it `get_topic_handler` |
| `get_data_streamer(topics, start, end)` | *Planned.* `(TopicHandler, msg)` in global time order. In Rust `msg` is a `RowRef` |
| `update(on_error)` | Python only: `SequenceUpdater` for this sequence, as in today's SDK. Rust uses `client.sequence_update(name, on_error)` |
| `close()` | Closes the streams of all its topic handlers. The handler stays usable |

### SequenceWriter · SequenceUpdater · public

A write session: a new sequence (`sequence_create`) or new topics in an existing one (`sequence_update`). Same API; they differ only in how they open. In Python the `with` block finishes it, or applies the error policy on an exception. A Ctrl+C counts as an exception, also during the final `finish`: the session closes per the policy without sending what is still queued, and a second Ctrl+C stops its uploads, leaves it open on the server and logs its locator. A Ctrl+C during a call that waits for a close (`topic_create`, `finish`, `abort`, `finalize`, the Finalize policy on `with topic:` exit) interrupts the whole session: if user code catches it and goes on, every topic refuses new pushes and `finish` closes per the policy. Anywhere else (a push, `flush`, `set_flush_policy`, user code) it only raises `KeyboardInterrupt`, and interrupts the session when it reaches the session's `with` exit; caught before, the session goes on. A Ctrl+C on a session already interrupted stops waiting. A session entered without leaving its `with` (or `__exit__`) is cancelled when it is garbage-collected and stays open on the server; Python logs a warning with its locator, as today's SDK does. In Rust `finish()` and `abort(reason)` are explicit. Dropping the writer without them cancels the topic streams, also while some `TopicWriter`s are still around (their next push gets `Closed`), but the session stays open on the server.

| Member | What it does |
|---|---|
| `topic_create(name, metadata, ontology, on_error)` | A `TopicWriter` for one topic. Rust: `topic_create(name, ontology_tag, format, schema, metadata, on_error)`, or `topic_create_for::<T>(name, metadata, on_error)` for a type. The name is checked as in today's SDK (letters, digits, `-`, `_` and `/`, starting with a letter or a digit) and normalized like a path. A topic created twice raises `AlreadyExistsError`. The schema is checked before the topic exists on the server, with mosaicod's rules: an Int64 `timestamp_ns` column, unique names among sibling fields (also inside structs and lists) and field names made only of letters, digits and `_`. A schema that breaks them raises `SchemaError`; mosaicod would refuse it only at the first upload, with the topic already registered, and the session could no longer be finalized. In Python a Ctrl+C during the call waits for the creation to end and then raises, so the session closes the new topic with the others; a second Ctrl+C stops waiting. Stopped that way, or cancelled in Rust (a dropped future), the call can leave a topic that the server created but the session does not know, and mosaicod then refuses to finalize the session |
| `get_topic_writer(name)` | The `TopicWriter` already opened with `topic_create`. Raises if there is none. Python also has `topic_writer_exists(name)` and `list_topic_writers()`. `list_topic_writers()` and `TopicWriter.name` give the normalized name, as reads do (`imu` becomes `/imu`), and lookups take either form. Rust exposes the normalization as `topic_name(name)`, which Python uses too. Today's SDK gives back the name as passed and finds a topic only by that exact string |
| `finish()` · `abort(reason)` | Finalize, or end the session per the policy. With Delete, `abort` cancels the topic uploads without waiting, also one still closing after a cancelled `finalize`, and then deletes. Today's SDK closes them first: slower, and mosaicod finalizes those topics and tells its hooks (`TopicUploaded`) just before deleting them. Here mosaicod logs each cancelled upload as an error. `complete(result)` takes the outcome of the work: `finish()` on `Ok`, `abort` on `Err`, and it returns the error it was given, with the reason added if the session stays open; a Rust `?` that leaves without it drops the session without applying the policy. mosaicod refuses to finalize a session that has no topic, so `finish` on it fails with that refusal (Python raises it at the `with` exit) and the policy applies: with Report, the session is deleted instead of finalized and the notification stays on the sequence. A topic whose upload failed makes `finish` fail, so the policy applies. With Report, if mosaicod then refuses to finalize because of that topic (it names it), the session deletes it, notifies the sequence (the topic's own notifications go with it) and finalizes the rest; if it was the only topic, the session is deleted. mosaicod leaves a broken `do_put` uploading for good, so the topic is deleted only when that is safe: mosaicod says it holds no data, or the client never handed the end of its stream to gRPC, so the server cannot be finalizing it. If the end was handed over (it may still be in the client's buffers when the connection drops), the session waits about two seconds for mosaicod to finish and never deletes the topic. If mosaicod answers that the session is already finalized (a finalize answer lost on the way, then repeated by Report), the session counts as finalized and `finish` returns only the first error. Only topics whose upload failed are deleted, never one stopped by Ctrl+C; any other refusal leaves the session open, as the error says. Deleting a topic needs the delete permission; without it the error says the topic could not be deleted. Today's SDK leaves such a session open. When `finish` fails it applies the policy; if that cannot close the session either (the finalize or the delete is refused), the error says so and `Error::left_open()` is true. In Python that logs the session as left open, as a second Ctrl+C does |
| `session_locator` | The session on the server, as `sequence:id` |
| `interrupter()` | Rust: a handle whose `interrupt()` stops the session from another task, and `interrupted()` tells whether it was; a Python Ctrl+C uses it. Topics drop what is not sent yet, `finish()` closes per the policy and returns `Closed`, also when that closing fails (then with `left_open()` true). The topics it stopped end `FinalizedWithError`; the others keep their status, as today's SDK keeps the topics it already finalized |
| `status` | Session state. Python only: in Rust `finish()` and `abort()` take the writer and return the outcome |

### TopicWriter · public

Writes one topic. Created by `topic_create`, lives as long as its session or until `finalize`. Rows are buffered in the core for every ontology: through a builder for the types written by hand, as Arrow rows (`push_row`) for the others. `push_batch` hands whole batches to the core directly.

| Member | What it does |
|---|---|
| `push(msg)` | Adds a row to the topic's buffer in the core; sends it when it reaches about 1 MiB or a flush policy limit. Same timestamp order as `push_batch`; a row too large alone is dropped and notified (`RecordTooLarge`), as in today's SDK. Python takes the `Message` of today's SDK. A message read from a streamer has a view as `data`, so it goes back as `Message(timestamp_ns=m.timestamp_ns, data=m.get_data())`, where today's SDK pushes it as it is. To copy a whole topic, `push_stream(src.get_batch_streamer())` avoids building one object per message. In Rust, a `push` cancelled while it waits (a dropped future) may already hold the row, which is sent later: do not repeat it |
| `push_batch(batch)` | Sends what the buffer holds first, then the batch. Small batches are joined up to the server limit; a batch that fills it alone goes without copies. `timestamp_ns` must not decrease, within the batch or from the previous push. Python copies the batch at import, so the caller can reuse its buffers. A call cancelled after the first piece of its batch is queued (a dropped future; a signal in Python) keeps the rest and sends it before the next push or at finish; cancelled earlier, it sends nothing |
| `push_stream(stream)` | Writes a whole Arrow stream, batch by batch. Each batch already has `timestamp_ns` as its first column, as on the wire; `push_batch` takes it apart. An error while reading the stream sets `Raised`, like a failed push: Rust returns it as an invalid argument, Python raises it as it is. In Python an object that is not an Arrow stream raises `InvalidArgumentError` and sets `Raised` too. In Rust it takes an async `Stream` of batches |
| `flush()` | Sends what the buffer and the joined batches hold; `finish()` does it too |
| `finalize(error)` | Closes the topic before its session ends, as `_finalize` in today's SDK (Python keeps that name as an alias). Without an error it sends what the topic holds; with one it reports it and drops the rows `push` has not handed over yet. Raises if closing fails; on a finalized topic it does nothing, unless that topic's upload is still ending: then it waits for it. In Python a Ctrl+C during it stops sending what is queued, as during the session's finish. If the wait is cancelled (a dropped future or a timeout in Rust, a second Ctrl+C in Python), the upload goes on, and the session's finish or abort waits for it before closing the session |
| `set_flush_policy(policy)` | When joined data is sent: bytes (default, up to the server limit), rows or age, checked at each push. The same for every ontology |
| `reject(error)` | Binding only (hidden from the Rust docs): records a push that failed before reaching the writer, such as a check in the binding (status `Raised`, `last_error`), and returns the error. On a finalized topic it returns the finalized error (`StreamClosedError` in Python) and leaves the status. Rust code returns such errors itself |
| `handle_error(reason)` | Applies the topic policy when user code fails; Python calls it on `with topic:` exit, Rust code calls it where a task writing the topic fails. It returns whether the policy absorbed the error: `Ok(true)` under Ignore or Finalize, `Ok(false)` under Raise, where the caller lets its own error through. The policy (`on_error` of `topic_create`) also decides whether a failed upload is reported to the topic. Raise lets the error through; Ignore reports it and suppresses it; Finalize reports it, closes the topic and suppresses it, but if closing fails it returns that error and Python lets the exception through, as today's SDK. A finalized topic keeps its final status: Ignore no longer reports for it and still returns `Ok(true)`; Finalize has nothing to close and returns `Ok(false)`, so the caller's error goes through. In Python a Ctrl+C while Finalize closes the topic stops sending what is queued, as during `finalize`. Python applies the policy only to an `Exception`: a Ctrl+C or `sys.exit()` in the `with topic:` body leaves the topic as it is and goes to the session. Today's SDK applies the policy to them too, so Ignore and Finalize swallow a Ctrl+C there |
| `status` · `last_error` · `dropped_record_count` | Writer state. A row too large alone is reported the same way wherever it is dropped, at the push or at encoding (`N record(s) dropped for topic '/imu': …`, with the limit and the largest size). Dropped at the push, it sets `RecordTooLarge` like any push outcome; dropped later at encoding (`push_batch`, or a size estimate below the encoded size), it sets `RecordTooLarge` only if the topic is `Active` or already `RecordTooLarge`, so a newer error (`Raised`, `IgnoredLastError`) stays. `dropped_record_count` counts them all, and Python logs one warning per topic when it closes, with the same wording and the count. If the topic upload fails (the connection drops, or the server refuses the data), the next call on the topic ends it: `FinalizedWithError` with the cause, `is_active` false, the rows not yet sent dropped, and one report unless the policy is Raise. Then every call raises `StreamClosedError` with that cause. Today's SDK keeps such a topic active and fails again at every flush |

### Streamers · public

What a read returns. `get_data_streamer` gives messages, `get_batch_streamer` plain record batches. `to_arrow` and `to_pandas` read one to the end. Streams are independent. At the end of the data a stream just ends, and further calls end again. After an error, `close()`, a drop, or when its handler or client closes, using it raises `StreamClosedError` (`ErrorKind::Closed` in Rust). `close()` also stops a stream that is still opening.

| Member | What it does |
|---|---|
| iteration | Messages (`msg.data` is a view) or record batches |
| Arrow stream | The batch streamer is also an Arrow stream for DuckDB, Polars or C |
| `close()` | Stops the request |

### QueryResponse · public · planned

What `query()` returns: the matching sequences and their topics. It keeps a clone of the connection, so `clusterize` and `intersect` call the server directly. A query with no results is an empty response.

| Member | What it does |
|---|---|
| `items` | Sequences with their matching topics |
| `clusterize(…)` | Time intervals where the matches of a topic cluster |
| `intersect(…)` | Intervals where several topics overlap |
| `to_query_sequence()` · `to_query_topic()` | A new query built from the results |

### BatchReader · internal

One `do_get`. A background task receives and decodes the batches into a bounded queue, so the network keeps going while user code runs. Dropping it cancels the request.

| Member | What it does |
|---|---|
| `open(ticket)` | Starts the `do_get` |
| `schema()` | Arrow schema of the stream |
| `next_batch()` | The next decoded batch, or the end |
| `close()` | Stops the request |

### MultiReader · internal

Written but not used yet: multi-topic reads and parallel ranges are planned. Drives several `BatchReader`s. **merge**: K topics by timestamp, cut into windows of one slice per topic plus the row order; ties follow the topic order. **concat**: N parallel ranges of one topic, returned in order.

| Member | What it does |
|---|---|
| `merge(streams, limits)` | k-way merge by timestamp |
| `concat(streams)` | Parallel ranges, back in order |
| `next_window()` | merge: one slice per topic plus the order array |
| `next_batch()` | concat: the next batch in order |

### Session · internal

The write lifecycle through `do_action`: create or update, add topics, finalize. On error it applies the session policy, report or delete. Topic policies (raise, finalize, ignore) live in `TopicWriter`.

| Member | What it does |
|---|---|
| `create(sequence, metadata)` · `update(sequence)` | Opens the session |
| `create_topic(name, tag, format, metadata)` | Registers the topic on the server and returns its locator and id. The `SequenceWriter` then builds the topic: its reporter (status and notifications), its `BatchWriter`, which cannot fail to open (the IPC options are checked at connect and the schema before the topic is registered), and its `TopicWriter` |
| `finalize(topics)` | Finalizes the session; every topic must be finalized first. If mosaicod answers that it is already finalized, it counts as done |
| `abort(reason)` | Report: notifies the sequence and finalizes, or deletes the session if it has no topic. If mosaicod refuses because of a topic whose upload failed, deletes that topic and tries again when that is safe (mosaicod says it holds no data, or the end of its stream was never handed to gRPC); otherwise waits about two seconds for mosaicod to finish it. A session that mosaicod says is already finalized counts as finalized. Delete: deletes the session, and the sequence if this session created it |

### BatchWriter · internal

One `do_put` per topic, on the client's connection as in today's SDK: HTTP/2 carries the uploads of all topics side by side. Receives the batches of `push_batch` and the rows joined by `push`, and knows no ontology type. Converts view columns (`Utf8View`, `BinaryView`) to plain ones, splits batches over the server limit read at connect, joins small ones up to it, and sends them in the background from a queue bounded in bytes, about four messages at the server limit, and by the client's write budget. The budget is shared by all the topics of a client: 512 MiB, or four messages at the server limit if that is larger. A message stays counted until it has left the connection: its share is released when two newer messages of the topic have been handed to gRPC, at the end of the stream, or when the budget is full and the topic has nothing else to send. Not counted: a batch while it is being encoded, as raw and encoded copy side by side (at most one message per topic). When the budget is full, every topic sends what it has joined so far instead of waiting to fill a message; below it nothing changes. A single row over the limit is dropped and reported as a row dropped at the push is: status and notifications belong to the topic's reporter, which the `BatchWriter` shares with its `TopicWriter`. Local errors are raised at once; server errors at the next push or at finish.

| Member | What it does |
|---|---|
| `enqueue(batch)` · `drain()` | Queues a batch, waiting for room in the queue and the budget. A call cancelled after its first piece is queued keeps the rest, which `drain` sends before anything new |
| `flush()` | Sends the joined batches now |
| `finish()` | Ends the `do_put` cleanly, so the server finalizes the topic, and reports the first error |
| drop | Without `finish()`, or with `finish()` dropped before it ends, cancels the `do_put`: the topic is not finalized |

### Connection · internal

One gRPC connection per client, shared by reads, actions and the uploads of every topic: the five Flight calls, TLS, the API key header and gzip, in both directions. Reads version and limits with `info` at connect, maps errors to categories and holds the client's write budget.

| Member | What it does |
|---|---|
| `connect(options)` | Opens the channel |
| `info()` | Server version and message size limits |
| `get_flight_info(descriptor)` | Metadata and tickets |
| `do_get(ticket)` · `do_put(descriptor)` | Batch streams |
| `list_flights(criteria)` | Every sequence name, in one response |
| `do_action(name, body)` | Every other operation |

