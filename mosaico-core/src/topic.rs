use arrow_array::RecordBatch;
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::SchemaRef;
use prost::Message;

use crate::batch_reader::BatchReader;
use crate::connection::Connection;
use crate::data_streamer::DataStreamer;
use crate::error::{Error, ErrorKind, Result};
use crate::ontology::Ontology;
use crate::proto;
use crate::scope::Scope;
use crate::streamer::BatchStreamer;

#[derive(Clone)]
pub struct TopicHandler {
    connection: Connection,
    scope: Scope,
    sequence: String,
    name: String,
    ontology_tag: String,
    timestamp_ns_min: Option<i64>,
    timestamp_ns_max: Option<i64>,
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
        let meta = proto::TopicAppMetadata::decode(endpoint.app_metadata.clone()).map_err(|e| {
            Error::new(ErrorKind::Protocol, "invalid topic metadata").with_source(e)
        })?;
        let name = topic_in_sequence(&meta.locator, sequence)?;
        let data_info = meta
            .data_info
            .ok_or_else(|| protocol(&meta.locator, "metadata without data_info"))?;
        let interval = data_info.interval.unwrap_or_default();
        Ok(Self {
            connection: connection.clone(),
            scope,
            sequence: sequence.to_owned(),
            name,
            ontology_tag: meta.ontology_tag,
            timestamp_ns_min: interval.start_ns,
            timestamp_ns_max: interval.end_ns,
            ticket: endpoint
                .ticket
                .clone()
                .filter(|_| data_info.total_chunks_count > 0),
        })
    }

    async fn fetch(
        connection: &Connection,
        scope: Scope,
        sequence: &str,
        locator: &str,
        range: Option<proto::TimestampRange>,
    ) -> Result<Self> {
        let info = flight_info(connection, locator, range)
            .await
            .map_err(|e| e.or_not_found(|| format!("topic '{locator}' not found")))?;
        let [endpoint] = <[FlightEndpoint; 1]>::try_from(info.endpoint).map_err(|endpoints| {
            protocol(
                locator,
                &format!("{} endpoints, expected 1", endpoints.len()),
            )
        })?;
        Self::from_endpoint(connection, scope, sequence, &endpoint)
    }

    pub fn sequence(&self) -> &str {
        &self.sequence
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ontology_tag(&self) -> &str {
        &self.ontology_tag
    }

    pub fn timestamp_ns_min(&self) -> Option<i64> {
        self.timestamp_ns_min
    }

    pub fn timestamp_ns_max(&self) -> Option<i64> {
        self.timestamp_ns_max
    }

    pub async fn get_batch_streamer(
        &self,
        start_ns: Option<i64>,
        end_ns: Option<i64>,
    ) -> Result<BatchStreamer> {
        self.connection.ensure_open()?;
        let token = self.scope.token();
        let locator = self.locator();
        let ticket = match &self.ticket {
            Some(ticket) => Some(ticket.clone()),
            None => self.refetch(None).await?.ticket,
        };
        let ticket = match ticket {
            Some(ticket) if start_ns.is_none() && end_ns.is_none() => Some(ticket),
            Some(_) => {
                self.refetch(Some(proto::TimestampRange { start_ns, end_ns }))
                    .await?
                    .ticket
            }
            None => None,
        };
        let Some(ticket) = ticket else {
            return Ok(BatchStreamer::empty(token));
        };
        let reader = BatchReader::open(&self.connection, ticket, token.clone())
            .await
            .map_err(|e| e.or_not_found(|| format!("topic '{locator}' not found")))?;
        Ok(BatchStreamer::single(reader, token))
    }

    pub async fn get_data_streamer<T: Ontology>(
        &self,
        start_ns: Option<i64>,
        end_ns: Option<i64>,
    ) -> Result<DataStreamer<T>> {
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
        let batches = self.get_batch_streamer(start_ns, end_ns).await?;
        DataStreamer::new(batches)
    }

    pub async fn to_arrow(
        &self,
        start_ns: Option<i64>,
        end_ns: Option<i64>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let mut streamer = self.get_batch_streamer(start_ns, end_ns).await?;
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

    async fn refetch(&self, range: Option<proto::TimestampRange>) -> Result<Self> {
        Self::fetch(
            &self.connection,
            self.scope.clone(),
            &self.sequence,
            &self.locator(),
            range,
        )
        .await
    }

    fn locator(&self) -> String {
        format!("{}{}", self.sequence, self.name)
    }
}

pub(crate) fn topic_name(raw: &str) -> String {
    format!("/{}", raw.trim_matches('/'))
}

pub(crate) fn topic_in_sequence(locator: &str, sequence: &str) -> Result<String> {
    locator
        .strip_prefix(sequence)
        .filter(|rest| rest.starts_with('/'))
        .map(str::to_owned)
        .ok_or_else(|| protocol(locator, &format!("not in sequence '{sequence}'")))
}

pub(crate) fn sequence_name(raw: &str) -> Result<&str> {
    let name = raw.trim_matches('/');
    if name.contains('/') {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("invalid sequence name '{raw}': '/' is not allowed"),
        ));
    }
    Ok(name)
}

pub(crate) async fn flight_info(
    connection: &Connection,
    locator: &str,
    range: Option<proto::TimestampRange>,
) -> Result<FlightInfo> {
    let cmd = proto::GetFlightInfoCmd {
        locator: locator.to_owned(),
        timestamp_range: range,
    };
    let descriptor = FlightDescriptor::new_cmd(cmd.encode_to_vec());
    connection.get_flight_info(descriptor).await
}

fn protocol(locator: &str, problem: &str) -> Error {
    Error::new(ErrorKind::Protocol, format!("topic '{locator}': {problem}"))
}
