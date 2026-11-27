use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::{Fields, SchemaRef};
use prost::Message;
use serde::de::DeserializeOwned;

use crate::comm::connection::Connection;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{sequence_name, topic_name};
use crate::metadata::{StoredMetadata, UserMetadata};
use crate::ontology::TIMESTAMP_COLUMN;
use crate::ontology::{Format, Ontology};
use crate::read::batch_reader::BatchReader;
use crate::read::data_streamer::TopicDataStreamer;
use crate::read::streamer::{ReadInfo, TopicBatchStreamer};
use crate::scope::{Scope, Token};
use crate::time::{Timestamp, TimestampRange};

#[derive(Clone)]
pub struct TopicHandler {
    connection: Connection,
    scope: Scope,
    sequence: String,
    name: String,
    ontology_tag: String,
    metadata: Arc<StoredMetadata>,
    created_timestamp: Timestamp,
    completed_timestamp: Option<Timestamp>,
    locked: bool,
    serialization_format: i32,
    chunks_number: u64,
    total_size_bytes: u64,
    ontology_schema: Arc<OnceLock<Fields>>,
    info: ReadInfo,
    ticket: Option<Ticket>,
}

impl TopicHandler {
    pub(crate) async fn open(connection: &Connection, sequence: &str, name: &str) -> Result<Self> {
        let sequence = sequence_name(sequence)?;
        let locator = format!("{sequence}{}", topic_name(name));
        Self::fetch(
            connection,
            connection.scope().child(),
            sequence,
            &locator,
            None,
        )
        .await
    }

    pub(crate) fn from_endpoint(
        connection: &Connection,
        scope: Scope,
        sequence: &str,
        endpoint: &FlightEndpoint,
    ) -> Result<Self> {
        let (meta, data_info) = decode(endpoint)?;
        let metadata = StoredMetadata::from_wire(meta.user_metadata)
            .map_err(|e| protocol(&meta.locator, "user metadata is not UTF-8").with_source(e))?;
        let name = topic_in_sequence(&meta.locator, sequence)?;
        Ok(Self {
            connection: connection.clone(),
            scope,
            sequence: sequence.to_owned(),
            name,
            ontology_tag: meta.ontology_tag,
            metadata: Arc::new(metadata),
            created_timestamp: Timestamp::from_ns(meta.created_at_ns),
            completed_timestamp: meta.completed_at_ns.map(Timestamp::from_ns),
            locked: meta.locked,
            serialization_format: meta.serialization_format,
            chunks_number: data_info.total_chunks_count,
            total_size_bytes: data_info.total_bytes,
            ontology_schema: Arc::default(),
            info: read_info(data_info.total_row_count, data_info.interval),
            ticket: ticket(endpoint, &data_info).filter(|_| meta.completed_at_ns.is_some()),
        })
    }

    async fn fetch(
        connection: &Connection,
        scope: Scope,
        sequence: &str,
        locator: &str,
        range: Option<proto::time::TimestampRange>,
    ) -> Result<Self> {
        let endpoint = topic_endpoint(connection, locator, range).await?;
        Self::from_endpoint(connection, scope, sequence, &endpoint)
    }

    pub fn sequence_name(&self) -> &str {
        &self.sequence
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ontology_tag(&self) -> &str {
        &self.ontology_tag
    }

    pub fn user_metadata(&self) -> Result<Option<&UserMetadata>> {
        self.metadata.get(|| format!("topic '{}'", self.locator()))
    }

    pub fn user_metadata_as<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        self.user_metadata()?
            .map(UserMetadata::deserialize_as)
            .transpose()
    }

    pub fn user_metadata_json(&self) -> Option<&str> {
        self.metadata.json()
    }

    pub fn timestamp_ns_min(&self) -> Option<Timestamp> {
        self.info.timestamp_ns_min
    }

    pub fn timestamp_ns_max(&self) -> Option<Timestamp> {
        self.info.timestamp_ns_max
    }

    pub fn msg_count(&self) -> u64 {
        self.info.msg_count
    }

    pub fn created_timestamp(&self) -> Timestamp {
        self.created_timestamp
    }

    pub fn completed_timestamp(&self) -> Option<Timestamp> {
        self.completed_timestamp
    }

    pub fn locked(&self) -> bool {
        self.locked
    }

    pub fn serialization_format(&self) -> Result<Format> {
        Format::from_proto(self.serialization_format).ok_or_else(|| {
            protocol(
                &self.locator(),
                &format!("unknown serialization format {}", self.serialization_format),
            )
        })
    }

    pub fn chunks_number(&self) -> u64 {
        self.chunks_number
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    pub async fn ontology_schema(&self) -> Result<Fields> {
        if let Some(fields) = self.ontology_schema.get() {
            return Ok(fields.clone());
        }
        self.connection.ensure_open()?;
        let locator = self.locator();
        let cmd = proto::flight::GetSchemaCmd {
            locator: locator.clone(),
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.encode_to_vec());
        let schema = self
            .scope
            .token()
            .run_until_closed(self.connection.get_schema(descriptor))
            .await
            .unwrap_or_else(|| Err(closed()))
            .map_err(|e| e.or_not_found(|| format!("topic '{locator}' not found")))?;
        let fields = payload_fields(schema.fields());
        if !fields.is_empty() {
            return Ok(self.ontology_schema.get_or_init(|| fields).clone());
        }
        Ok(fields)
    }

    pub async fn get_batch_streamer(
        &self,
        range: impl Into<TimestampRange>,
    ) -> Result<TopicBatchStreamer> {
        let plan = self.read_plan(range.into()).await?;
        let reader = match plan.ticket {
            Some(_) => self.connection.reader().await,
            None => self.connection.clone(),
        };
        self.open_plan(&reader, plan).await
    }

    pub(crate) async fn read_plan(&self, range: TimestampRange) -> Result<ReadPlan> {
        self.connection.ensure_open()?;
        range.check()?;
        let range = range.to_proto();
        let token = self.scope.token();
        let (ticket, info) = token
            .run_until_closed(self.resolve(range))
            .await
            .unwrap_or_else(|| Err(closed()))?;
        Ok(ReadPlan {
            token,
            ticket,
            info,
        })
    }

    async fn resolve(
        &self,
        range: Option<proto::time::TimestampRange>,
    ) -> Result<(Option<Ticket>, ReadInfo)> {
        match (&self.ticket, range) {
            (Some(ticket), None) => Ok((Some(ticket.clone()), self.info)),
            (_, range) => {
                let endpoint = topic_endpoint(&self.connection, &self.locator(), range).await?;
                let (meta, data_info) = decode(&endpoint)?;
                let info = match meta.time_window_info {
                    Some(window) => read_info(window.row_count, window.interval),
                    None => read_info(data_info.total_row_count, data_info.interval),
                };
                Ok((ticket(&endpoint, &data_info), info))
            }
        }
    }

    pub(crate) async fn open_plan(
        &self,
        reader: &Connection,
        plan: ReadPlan,
    ) -> Result<TopicBatchStreamer> {
        let ReadPlan {
            token,
            ticket,
            info,
        } = plan;
        let Some(ticket) = ticket else {
            return Ok(TopicBatchStreamer::empty(token));
        };
        let opened = token
            .run_until_closed(BatchReader::open(reader, ticket, token.clone()))
            .await
            .unwrap_or_else(|| Err(closed()))
            .map_err(|e| e.or_not_found(|| format!("topic '{}' not found", self.locator())))?;
        Ok(TopicBatchStreamer::single(opened, info, token))
    }

    pub(crate) fn same_client(&self, connection: &Connection) -> bool {
        self.connection.same_client(connection)
    }

    pub async fn get_data_streamer<T: Ontology>(
        &self,
        range: impl Into<TimestampRange>,
    ) -> Result<TopicDataStreamer<T>> {
        self.data_streamer(range.into()).await
    }

    async fn data_streamer<T: Ontology>(
        &self,
        range: TimestampRange,
    ) -> Result<TopicDataStreamer<T>> {
        self.connection.ensure_open()?;
        if self.ontology_tag != T::TAG {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "topic '{}' has ontology '{}', not '{}'",
                    self.locator(),
                    self.ontology_tag,
                    T::TAG
                ),
            ));
        }
        let batches = self.get_batch_streamer(range).await?;
        TopicDataStreamer::new(batches)
    }

    pub async fn to_arrow(
        &self,
        range: impl Into<TimestampRange>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        self.arrow(range.into()).await
    }

    async fn arrow(&self, range: TimestampRange) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let mut streamer = self.get_batch_streamer(range).await?;
        let schema = streamer.schema().clone();
        let mut batches = Vec::new();
        while let Some(batch) = streamer.next_batch().await? {
            batches.push(batch);
        }
        Ok((schema, batches))
    }

    pub fn close(&self) {
        self.scope.close();
    }

    pub(crate) fn locator(&self) -> String {
        format!("{}{}", self.sequence, self.name)
    }
}

pub(crate) fn topic_in_sequence(locator: &str, sequence: &str) -> Result<String> {
    locator
        .strip_prefix(sequence)
        .filter(|rest| rest.starts_with('/'))
        .map(str::to_owned)
        .ok_or_else(|| protocol(locator, &format!("not in sequence '{sequence}'")))
}

pub(crate) async fn flight_info(
    connection: &Connection,
    locator: &str,
    range: Option<proto::time::TimestampRange>,
) -> Result<FlightInfo> {
    let cmd = proto::flight::GetFlightInfoCmd {
        locator: locator.to_owned(),
        timestamp_range: range,
    };
    let descriptor = FlightDescriptor::new_cmd(cmd.encode_to_vec());
    connection.get_flight_info(descriptor).await
}

async fn topic_endpoint(
    connection: &Connection,
    locator: &str,
    range: Option<proto::time::TimestampRange>,
) -> Result<FlightEndpoint> {
    let info = flight_info(connection, locator, range)
        .await
        .map_err(|e| e.or_not_found(|| format!("topic '{locator}' not found")))?;
    let [endpoint] = <[FlightEndpoint; 1]>::try_from(info.endpoint).map_err(|endpoints| {
        protocol(
            locator,
            &format!("{} endpoints, expected 1", endpoints.len()),
        )
    })?;
    Ok(endpoint)
}

fn decode(
    endpoint: &FlightEndpoint,
) -> Result<(
    proto::flight::TopicAppMetadata,
    proto::flight::TopicAppMetadataDataInfo,
)> {
    let mut meta = proto::flight::TopicAppMetadata::decode(endpoint.app_metadata.clone())
        .map_err(|e| Error::new(ErrorKind::Protocol, "invalid topic metadata").with_source(e))?;
    let data_info = meta
        .data_info
        .take()
        .ok_or_else(|| protocol(&meta.locator, "metadata without data_info"))?;
    Ok((meta, data_info))
}

fn read_info(msg_count: u64, interval: Option<proto::time::TimestampRange>) -> ReadInfo {
    let interval = interval.unwrap_or_default();
    ReadInfo {
        msg_count,
        timestamp_ns_min: interval.start_ns.map(Timestamp::from_ns),
        timestamp_ns_max: interval.end_ns.map(Timestamp::from_ns),
    }
}

fn ticket(
    endpoint: &FlightEndpoint,
    data_info: &proto::flight::TopicAppMetadataDataInfo,
) -> Option<Ticket> {
    endpoint
        .ticket
        .clone()
        .filter(|_| data_info.total_chunks_count > 0)
}

pub(crate) struct ReadPlan {
    token: Arc<Token>,
    ticket: Option<Ticket>,
    info: ReadInfo,
}

impl ReadPlan {
    pub(crate) fn has_data(&self) -> bool {
        self.ticket.is_some() && self.info.msg_count > 0
    }

    pub(crate) fn skip_if_empty(mut self) -> Self {
        if self.info.msg_count == 0 {
            self.ticket = None;
        }
        self
    }
}

fn payload_fields(fields: &Fields) -> Fields {
    fields
        .iter()
        .filter(|f| f.name() != TIMESTAMP_COLUMN)
        .cloned()
        .collect()
}

fn closed() -> Error {
    Error::new(ErrorKind::Closed, "stream closed")
}

fn protocol(locator: &str, problem: &str) -> Error {
    Error::new(ErrorKind::Protocol, format!("topic '{locator}': {problem}"))
}

#[cfg(test)]
impl TopicHandler {
    pub(crate) fn offline(
        connection: &Connection,
        scope: Scope,
        sequence: &str,
        name: &str,
        ontology_tag: &str,
    ) -> Self {
        Self {
            connection: connection.clone(),
            scope,
            sequence: sequence.to_owned(),
            name: name.to_owned(),
            ontology_tag: ontology_tag.to_owned(),
            metadata: Arc::default(),
            created_timestamp: Timestamp::from_ns(0),
            completed_timestamp: None,
            locked: false,
            serialization_format: Format::Default.to_proto(),
            chunks_number: 0,
            total_size_bytes: 0,
            ontology_schema: Arc::default(),
            info: ReadInfo::default(),
            ticket: None,
        }
    }

    pub(crate) fn token(&self) -> Arc<Token> {
        self.scope.token()
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightEndpoint;
    use arrow_schema::{DataType, Field};
    use prost::Message;

    use super::*;

    fn endpoint(format: i32, completed: Option<i64>) -> FlightEndpoint {
        let meta = proto::flight::TopicAppMetadata {
            created_at_ns: 11,
            completed_at_ns: completed,
            locked: completed.is_some(),
            locator: "seq/imu".to_owned(),
            ontology_tag: "IMU".to_owned(),
            serialization_format: format,
            user_metadata: Vec::new(),
            data_info: Some(proto::flight::TopicAppMetadataDataInfo {
                interval: Some(proto::time::TimestampRange {
                    start_ns: Some(5),
                    end_ns: Some(9),
                }),
                total_row_count: 7,
                total_bytes: 4096,
                total_chunks_count: 2,
            }),
            time_window_info: None,
            session_locator: String::new(),
        };
        FlightEndpoint::new()
            .with_ticket(Ticket::new("t"))
            .with_app_metadata(meta.encode_to_vec())
    }

    #[tokio::test]
    async fn metadata_at_open() {
        let connection = Connection::offline(1 << 20);
        let format = proto::core::Format::Image as i32;
        let topic = TopicHandler::from_endpoint(
            &connection,
            Scope::root(),
            "seq",
            &endpoint(format, Some(13)),
        )
        .unwrap();
        assert_eq!(topic.name(), "/imu");
        assert_eq!(topic.created_timestamp(), Timestamp::from_ns(11));
        assert_eq!(topic.completed_timestamp(), Some(Timestamp::from_ns(13)));
        assert!(topic.locked());
        assert_eq!(topic.serialization_format().unwrap(), Format::Image);
        assert_eq!(topic.chunks_number(), 2);
        assert_eq!(topic.total_size_bytes(), 4096);
        assert_eq!(topic.msg_count(), 7);
        assert_eq!(topic.timestamp_ns_min(), Some(Timestamp::from_ns(5)));
        assert_eq!(topic.timestamp_ns_max(), Some(Timestamp::from_ns(9)));
        let open =
            TopicHandler::from_endpoint(&connection, Scope::root(), "seq", &endpoint(1, None))
                .unwrap();
        assert_eq!(open.completed_timestamp(), None);
        assert!(!open.locked());
        assert_eq!(open.serialization_format().unwrap(), Format::Default);
    }

    #[tokio::test]
    async fn ticket_kept_if_finalized() {
        let connection = Connection::offline(1 << 20);
        let open = |completed| {
            TopicHandler::from_endpoint(&connection, Scope::root(), "seq", &endpoint(1, completed))
                .unwrap()
        };
        assert!(open(Some(13)).ticket.is_some());
        assert!(open(None).ticket.is_none());
        assert_eq!(open(None).msg_count(), 7);
    }

    #[tokio::test]
    async fn unknown_format_opens() {
        let connection = Connection::offline(1 << 20);
        for format in [0, 99] {
            let topic = TopicHandler::from_endpoint(
                &connection,
                Scope::root(),
                "seq",
                &endpoint(format, None),
            )
            .unwrap();
            assert_eq!(topic.msg_count(), 7);
            let error = topic.serialization_format().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Protocol);
        }
    }

    #[test]
    fn schema_drops_timestamp() {
        let fields = Fields::from(vec![
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
            Field::new("x", DataType::Float64, true),
        ]);
        let payload = payload_fields(&fields);
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0].name(), "x");
    }

    #[tokio::test]
    async fn schema_after_close() {
        let connection = Connection::offline(1 << 20);
        let topic = TopicHandler::offline(&connection, Scope::root(), "seq", "/imu", "IMU");
        connection.close();
        let error = topic.ontology_schema().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }
}
