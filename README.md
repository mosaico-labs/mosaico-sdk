# Mosaico SDK

Draft of the design of the Mosaico SDK.

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
2. A read with no time range reuses that ticket. A read with a range asks for the ticket of that window first.
3. The `BatchReader` runs `do_get` and decodes batches in the background.
4. The user gets messages (views), record batches, one Arrow table or one DataFrame.

A topic with no data gives an empty stream. Reads are independent: several can run at once on the same topic. Ranges include the start and exclude the end.

### Multi-topic read

![Multi-topic read](docs/diagrams/multi-topic-read.svg)

1. `sequence_handler()` calls `get_flight_info` on the sequence once: one ticket per topic.
2. `get_data_streamer(topics, start, end)` hands the K tickets to the `MultiReader`.
3. The `MultiReader` opens K streams and merges them by timestamp into windows: one slice per topic plus the row order. Equal timestamps follow the topic order.
4. The handler walks the order and yields `(topic, msg)`, with `msg.data` as a view.

### Parallel ranges

![Parallel ranges](docs/diagrams/parallel-ranges.svg)

The handler splits a time range into N parts and asks for one ticket per part. The `MultiReader` reads the N streams in parallel and returns them in order, so the user gets the same result as a single read.

### Write

![Write](docs/diagrams/write.svg)

1. `sequence_create()` or `sequence_update()` opens a write session with `do_action`.
2. `topic_create()` creates the topic and opens its `BatchWriter` with `do_put`.
3. `push` fills a builder on the language side and hands it to `push_batch` on a row, byte or time limit. `push_batch` is the only point where data enters the core.
4. The `BatchWriter` splits batches under the server limit and sends them in the background.
5. Leaving the `with` block (or `finish()` in Rust) waits for the sends and finalizes the session. On error, the session policy deletes or reports it. Server errors surface at the next push or when the session finishes.

### Other operations

![Other operations](docs/diagrams/other-operations.svg)

- Sequence lists go through `list_flights`, one page at a time. Queries, notifications and deletes go through `do_action`. All on the same connection.
- Queries are built in each SDK and run by the core.
- Reads and queries accept a `CancelToken`: cancelling it stops the call and its streams.
- `close()` on the client, a sequence or a topic closes every stream opened below it. A handler stays usable after `close()`. Closing the client is permanent.

## Dependencies

Solid arrows: who creates whom. Dashed arrows: who calls mosaicod through the connection.

![Dependencies](docs/diagrams/dependencies.svg)

- `MosaicoClient` is the only object the user creates directly. It opens the `Connection` and uses it directly for lists, queries, notifications and deletes.
- The client gives handlers, write sessions and, from `query()`, a `QueryResponse`. A `SequenceHandler` also gives its `TopicHandler`s and the `SequenceUpdater`.
- A read returns a streamer. It wraps a `BatchReader` for one topic, or a `MultiReader` for several topics or parallel ranges, which opens one `BatchReader` per stream.
- A write session owns its `Session`. `topic_create` gives a `TopicWriter`, and the `Session` opens its `BatchWriter`, which receives the `push_batch` calls.
- Every block that calls mosaicod uses a clone of the client's `Connection`. Internal blocks have no public constructor.

## Classes

### Python SDK · public

The Python package, rebuilt on the Rust core through PyO3. Calls are synchronous, release the GIL while waiting and stop on Ctrl+C.

| Member | What it does |
|---|---|
| `msg.data` | Read-only view of one row, generated in Python. A field is read only when accessed |
| `msg.data` (type without generated code) | Open: a generic view built at runtime from the topic schema, with the same access and no static types |
| `msg.get_data()` | Owned ontology object, a copy of the row |
| `to_pandas(start, end)` | One DataFrame with dotted column names |
| `MosaicoError` | Base class of every exception the SDK raises |

### Rust SDK · public

The core crate itself, used directly by Rust programs. Async on tokio, with a blocking module that mirrors the same API. Errors are `Result<T, Error>` with an error kind. Generated ontology types read rows as typed views.

| Member | What it does |
|---|---|
| `MosaicoClient::connect(ConnectOptions::new(host, port)).await` | Async entry point. TLS and other options on `ConnectOptions` |
| `topic.get_data_streamer::<Imu>(start, end)` | Typed read. The type must match the topic ontology tag exactly |
| `while let Some(msg) = s.next().await?` | Iteration over messages or batches |
| `trait Ontology` | `TAG`, `Columns`, `View<'a>` and `from_view`, one per generated type. Columns are looked up and checked once per batch; a view is a row index into them |
| `view.acceleration()?.z()?` | View fields are methods returning `Result`, since a required field can be null. The owned model has plain pub fields |
| `blocking::MosaicoClient` | The same API without async |
| `Error::kind()` | Connection, not found, invalid argument, schema, closed, server, … |
| `RecordBatch` | arrow-rs batches, passed without conversion |
| `RowRef` · `view::<T>()` | Multi-topic reads: one row of any topic, read as type `T`; `None` if the topic is not of type `T` |
| `view.to_model()` · `msg.get_data()` | Owned copy of a typed view: an `ImuView` gives an `Imu` |

### MosaicoClient · public

Entry point. Holds the single connection and reads the server version and limits once, at connect. Creates handlers and write sessions. Catalog operations and queries go straight to the connection.

| Member | What it does |
|---|---|
| `connect(host, port, tls, api_key, compression)` | Opens the connection and calls `info` |
| `sequence_handler(name)` | `SequenceHandler`. Raises if the sequence does not exist |
| `topic_handler(sequence, topic, ontology=…)` | `TopicHandler`, checked against the ontology |
| `sequence_create(name, metadata)` · `sequence_update(name)` | A write session, used in a `with` block |
| `list_sequences()` · `sequence_delete(name)` · notifications | Lists through `list_flights`, a page at a time; the rest through `do_action` |
| `query(…)` | Runs a query. Results grouped by sequence |
| `close()` | Closes every stream of the client and its handlers. Permanent: later calls fail as closed |

### TopicHandler · public

One topic. Opening it reads the metadata once: ontology tag, time range and ticket. The Arrow schema arrives at the start of each read.

| Member | What it does |
|---|---|
| `name` · `ontology_tag` · `timestamp_ns_min` · `timestamp_ns_max` | Metadata read at open |
| `get_data_streamer(start, end)` | Messages in time order; `msg.data` is a view. The schema is checked when the stream opens. In Rust the type is a parameter: `get_data_streamer::<T>()` |
| `get_batch_streamer(start, end)` | Plain Arrow record batches; also an Arrow stream that DuckDB, Polars or a C program read directly |
| `to_arrow(start, end)` | One Arrow table, without copies. In Rust the schema and the batches |
| `to_pandas(start, end)` | One DataFrame (Python only) |
| `cancel=token` | Accepted by every read; cancelling stops the stream |
| `close()` | Closes every stream opened from this handler and its clones. The handler stays usable |

### SequenceHandler · public

One sequence. A single `get_flight_info` returns the metadata and the tickets of every topic, so its topic handlers and multi-topic reads need no other metadata call.

| Member | What it does |
|---|---|
| `topics` · `sessions` · `user_metadata` · `timestamp_ns_min/max` | Metadata read at open |
| `topic_handler(topic, ontology=…)` | `TopicHandler` built from the saved ticket |
| `get_data_streamer(topics, start, end)` | `(TopicHandler, msg)` in global time order. In Rust `msg` is a `RowRef` |
| `update()` | `SequenceUpdater` for this sequence |
| `close()` | Closes the streams of all its topic handlers. The handler stays usable |

### SequenceWriter · SequenceUpdater · public

A write session: a new sequence (`sequence_create`) or new topics in an existing one (`sequence_update`). Same API; they differ only in how they open. In Python the `with` block finishes it, or applies the error policy on an exception. In Rust `finish()` is explicit, and dropping the session without it aborts.

| Member | What it does |
|---|---|
| `topic_create(name, ontology, metadata, on_error)` | A `TopicWriter` for one topic |
| `get_topic_writer(name)` | The `TopicWriter` already opened with `topic_create`. Raises if there is none |
| `finish()` · `abort()` | Finalize, or end the session per the policy |
| `status` | Session state |

### TopicWriter · public

Writes one topic. Created by `topic_create`, lives as long as its session. `push` and its builder live in each language; `push_batch` is where data enters the core, the same in every language.

| Member | What it does |
|---|---|
| `push(msg)` | Adds a row to a builder generated from the ontology; sends it with `push_batch` when it reaches a row, byte or time limit |
| `push_batch(batch)` | Sends what the builder holds first, then the batch as it is, without copies |
| `push_stream(reader)` | Writes a whole Arrow stream, batch by batch |
| `flush()` | Sends what the builder holds; `finish()` does it too |
| `status` · `last_error` · `dropped_record_count` | Writer state |

### Streamers · public

What a read returns. `get_data_streamer` gives messages, `get_batch_streamer` plain record batches. `to_arrow` and `to_pandas` read one to the end. Streams are independent. A stream closes at the end of the data, after an error, with `close()`, when dropped, or when its handler or client closes. Using it afterwards raises `StreamClosedError` (`ErrorKind::Closed` in Rust).

| Member | What it does |
|---|---|
| iteration | Messages (`msg.data` is a view) or record batches |
| Arrow stream | The batch streamer is also an Arrow stream for DuckDB, Polars or C |
| `close()` | Stops the request |

### QueryResponse · public

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

Drives several `BatchReader`s. **merge**: K topics by timestamp, cut into windows of one slice per topic plus the row order; ties follow the topic order. **concat**: N parallel ranges of one topic, returned in order.

| Member | What it does |
|---|---|
| `merge(streams, limits)` | k-way merge by timestamp |
| `concat(streams)` | Parallel ranges, back in order |
| `next_window()` | merge: one slice per topic plus the order array |
| `next_batch()` | concat: the next batch in order |

### Session · internal

The write lifecycle through `do_action`: create or update, add topics, finalize. On error it applies the policy: report or delete for the session; raise, finalize or ignore for a topic.

| Member | What it does |
|---|---|
| `create(sequence, metadata)` · `update(sequence)` | Opens the session |
| `create_topic(name, tag, schema)` | Registers the topic and returns its `BatchWriter`. If the `do_put` fails to start, it deletes the topic |
| `finish()` | Waits for every `BatchWriter`, then finalizes |
| `abort()` | Applies the session policy |

### BatchWriter · internal

One `do_put` per topic. Receives the batches of `push_batch` and knows no ontology type. Splits batches under the server limit read at connect and sends them in the background from a bounded queue. Local errors are raised at once; server errors at the next push or at finish.

| Member | What it does |
|---|---|
| `send(batch)` | Queues a batch; waits when the queue is full |
| `flush()` | Waits until the queue is sent |
| `finish()` | Closes the `do_put` and reports the first error |

### Connection · internal

One gRPC connection per client: the five Flight calls, TLS, the API key header and gzip. Reads version and limits with `info` at connect and maps errors to categories.

| Member | What it does |
|---|---|
| `connect(options)` | Opens the channel |
| `info()` | Server version and message size limits |
| `get_flight_info(descriptor)` | Metadata and tickets |
| `do_get(ticket)` · `do_put(descriptor)` | Batch streams |
| `list_flights(criteria)` | A page of sequence names, with a cursor |
| `do_action(name, body)` | Every other operation |

