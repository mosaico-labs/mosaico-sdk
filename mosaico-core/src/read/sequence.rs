use futures::future::try_join_all;
use prost::Message;

use crate::comm::connection::Connection;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{self, sequence_name};
use crate::read::sequence_streamer::SequenceDataStreamer;
use crate::read::topic::{TopicHandler, flight_info, topic_in_sequence};
use crate::scope::Scope;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionInfo {
    pub locator: String,
    pub created_timestamp: i64,
    pub completed_timestamp: Option<i64>,
    pub topics: Vec<String>,
    pub locked: bool,
}

#[derive(Clone)]
pub struct SequenceHandler {
    connection: Connection,
    scope: Scope,
    name: String,
    created_timestamp: i64,
    user_metadata: Option<String>,
    sessions: Vec<SessionInfo>,
    topics: Vec<TopicHandler>,
}

impl SequenceHandler {
    pub(crate) async fn open(connection: &Connection, name: &str) -> Result<Self> {
        let name = sequence_name(name)?;
        let info = flight_info(connection, name, None)
            .await
            .map_err(|e| e.or_not_found(|| format!("sequence '{name}' not found")))?;
        let meta = proto::flight::SequenceAppMetadata::decode(info.app_metadata).map_err(|e| {
            Error::new(ErrorKind::Protocol, "invalid sequence metadata").with_source(e)
        })?;
        let user_metadata = if meta.user_metadata.is_empty() {
            None
        } else {
            let text = String::from_utf8(meta.user_metadata).map_err(|e| {
                Error::new(ErrorKind::Protocol, "sequence metadata is not UTF-8").with_source(e)
            })?;
            Some(text)
        };
        let sessions = meta
            .sessions
            .into_iter()
            .map(|s| -> Result<SessionInfo> {
                Ok(SessionInfo {
                    locator: s.locator,
                    created_timestamp: s.created_at_ns,
                    completed_timestamp: s.completed_at_ns,
                    topics: s
                        .topics
                        .iter()
                        .map(|locator| topic_in_sequence(locator, name))
                        .collect::<Result<_>>()?,
                    locked: s.locked,
                })
            })
            .collect::<Result<_>>()?;
        let scope = connection.scope().child();
        let mut topics = info
            .endpoint
            .iter()
            .map(|endpoint| TopicHandler::from_endpoint(connection, scope.child(), name, endpoint))
            .collect::<Result<Vec<_>>>()?;
        topics.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(Self {
            connection: connection.clone(),
            scope,
            name: name.to_owned(),
            created_timestamp: meta.created_at_ns,
            user_metadata,
            sessions,
            topics,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn created_timestamp(&self) -> i64 {
        self.created_timestamp
    }

    pub fn user_metadata(&self) -> Option<&str> {
        self.user_metadata.as_deref()
    }

    pub fn sessions(&self) -> &[SessionInfo] {
        &self.sessions
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.topics.iter().map(|t| t.name())
    }

    pub fn timestamp_ns_min(&self) -> Option<i64> {
        self.topics
            .iter()
            .filter_map(|t| t.timestamp_ns_min())
            .min()
    }

    pub fn timestamp_ns_max(&self) -> Option<i64> {
        self.topics
            .iter()
            .filter_map(|t| t.timestamp_ns_max())
            .max()
    }

    pub fn topic_handler(&self, topic_name: &str) -> Result<TopicHandler> {
        self.connection.ensure_open()?;
        let topic = locator::topic_name(topic_name);
        self.topics
            .iter()
            .find(|t| t.name() == topic)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("topic '{}{topic}' not found", self.name),
                )
            })
    }

    pub async fn get_data_streamer(
        &self,
        topics: &[&str],
        start_timestamp_ns: Option<i64>,
        end_timestamp_ns: Option<i64>,
    ) -> Result<SequenceDataStreamer> {
        self.connection.ensure_open()?;
        let topics = self.select(topics)?;
        let streams = try_join_all(
            topics
                .iter()
                .map(|t| t.get_batch_streamer(start_timestamp_ns, end_timestamp_ns)),
        )
        .await?;
        SequenceDataStreamer::new(topics, streams)
    }

    pub fn close(&self) {
        self.scope.close();
    }

    fn select(&self, names: &[&str]) -> Result<Vec<TopicHandler>> {
        if names.is_empty() {
            return Ok(self.topics.clone());
        }
        let mut selected: Vec<TopicHandler> = Vec::with_capacity(names.len());
        for name in names {
            let wanted = locator::topic_name(name);
            let Some(topic) = self.topics.iter().find(|t| t.name() == wanted) else {
                let known = self.topics().collect::<Vec<_>>().join(", ");
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "topic '{}{wanted}' not found; topics in the sequence: {known}",
                        self.name
                    ),
                ));
            };
            if selected.iter().any(|t| t.name() == wanted) {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("topic '{wanted}' given twice"),
                ));
            }
            selected.push(topic.clone());
        }
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offline(names: &[&str]) -> SequenceHandler {
        let connection = Connection::offline(1 << 20);
        let scope = connection.scope().child();
        let topics = names
            .iter()
            .map(|n| TopicHandler::offline(&connection, scope.child(), "seq", n, "Value"))
            .collect();
        SequenceHandler {
            connection,
            scope,
            name: "seq".to_owned(),
            created_timestamp: 0,
            user_metadata: None,
            sessions: Vec::new(),
            topics,
        }
    }

    fn names(topics: &[TopicHandler]) -> Vec<&str> {
        topics.iter().map(|t| t.name()).collect()
    }

    #[tokio::test]
    async fn select_all_when_empty() {
        let seq = offline(&["/a", "/b", "/c"]);
        assert_eq!(names(&seq.select(&[]).unwrap()), ["/a", "/b", "/c"]);
    }

    #[tokio::test]
    async fn select_keeps_order() {
        let seq = offline(&["/a", "/b", "/c"]);
        assert_eq!(names(&seq.select(&["c", "/a"]).unwrap()), ["/c", "/a"]);
    }

    #[tokio::test]
    async fn select_unknown_topic() {
        let seq = offline(&["/a", "/b"]);
        let error = seq.select(&["/a", "/x"]).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert!(error.to_string().contains("seq/x"));
        assert!(error.to_string().contains("/a, /b"));
    }

    #[tokio::test]
    async fn select_twice_refused() {
        let seq = offline(&["/a", "/b"]);
        let error = seq.select(&["/a", "a"]).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn closed_client_refused() {
        let seq = offline(&["/a"]);
        seq.connection.scope().close();
        let error = seq.get_data_streamer(&[], None, None).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }
}
