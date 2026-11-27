use std::collections::HashSet;
use std::sync::Arc;

use futures::TryFutureExt;
use futures::future::try_join_all;
use prost::Message;
use serde::de::DeserializeOwned;

use crate::comm::connection::Connection;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{self, sequence_name};
use crate::metadata::{StoredMetadata, UserMetadata};
use crate::read::merge_reader::{WINDOW_ROWS, check_topic_count};
use crate::read::sequence_streamer::{SequenceDataStreamer, SequenceWindowStreamer};
use crate::read::streamer::TopicBatchStreamer;
use crate::read::topic::{TopicHandler, flight_info, topic_in_sequence};
use crate::scope::Scope;
use crate::time::{Timestamp, TimestampRange};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionInfo {
    pub locator: String,
    pub created_timestamp: Timestamp,
    pub completed_timestamp: Option<Timestamp>,
    pub topics: Vec<String>,
    pub locked: bool,
}

#[derive(Clone)]
pub struct SequenceHandler {
    connection: Connection,
    scope: Scope,
    name: String,
    created_timestamp: Timestamp,
    metadata: Arc<StoredMetadata>,
    sessions: Vec<SessionInfo>,
    topics: Vec<TopicHandler>,
}

impl SequenceHandler {
    pub(crate) async fn open(connection: &Connection, name: &str) -> Result<Self> {
        let name = sequence_name(name)?;
        Self::fetch(connection, connection.scope().child(), name).await
    }

    async fn fetch(connection: &Connection, scope: Scope, name: &str) -> Result<Self> {
        let info = flight_info(connection, name, None)
            .await
            .map_err(|e| e.or_not_found(|| format!("sequence '{name}' not found")))?;
        let meta = proto::flight::SequenceAppMetadata::decode(info.app_metadata).map_err(|e| {
            Error::new(ErrorKind::Protocol, "invalid sequence metadata").with_source(e)
        })?;
        let metadata = StoredMetadata::from_wire(meta.user_metadata).map_err(|e| {
            Error::new(
                ErrorKind::Protocol,
                format!("sequence '{name}': user metadata is not UTF-8"),
            )
            .with_source(e)
        })?;
        let sessions = meta
            .sessions
            .into_iter()
            .map(|s| -> Result<SessionInfo> {
                Ok(SessionInfo {
                    locator: s.locator,
                    created_timestamp: Timestamp::from_ns(s.created_at_ns),
                    completed_timestamp: s.completed_at_ns.map(Timestamp::from_ns),
                    topics: s
                        .topics
                        .iter()
                        .map(|locator| topic_in_sequence(locator, name))
                        .collect::<Result<_>>()?,
                    locked: s.locked,
                })
            })
            .collect::<Result<_>>()?;
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
            created_timestamp: Timestamp::from_ns(meta.created_at_ns),
            metadata: Arc::new(metadata),
            sessions,
            topics,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn created_timestamp(&self) -> Timestamp {
        self.created_timestamp
    }

    pub fn user_metadata(&self) -> Result<Option<&UserMetadata>> {
        self.metadata.get(|| format!("sequence '{}'", self.name))
    }

    pub fn user_metadata_as<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        self.user_metadata()?
            .map(UserMetadata::deserialize_as)
            .transpose()
    }

    pub fn user_metadata_json(&self) -> Option<&str> {
        self.metadata.json()
    }

    pub fn sessions(&self) -> &[SessionInfo] {
        &self.sessions
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.topics.iter().map(|t| t.name())
    }

    pub fn timestamp_ns_min(&self) -> Option<Timestamp> {
        self.topics
            .iter()
            .filter_map(|t| t.timestamp_ns_min())
            .min()
    }

    pub fn timestamp_ns_max(&self) -> Option<Timestamp> {
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
                let known = self.topics().collect::<Vec<_>>().join(", ");
                Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "topic '{}{topic}' not found; topics in the sequence: {known}",
                        self.name
                    ),
                )
            })
    }

    pub async fn get_data_streamer(
        &self,
        topics: &[&str],
        range: impl Into<TimestampRange>,
    ) -> Result<SequenceDataStreamer> {
        let windows = self.get_window_streamer(topics, range).await?;
        Ok(SequenceDataStreamer::from_windows(windows))
    }

    pub async fn get_window_streamer(
        &self,
        topics: &[&str],
        range: impl Into<TimestampRange>,
    ) -> Result<SequenceWindowStreamer> {
        let range = range.into();
        let topics = topics
            .iter()
            .map(|name| self.topic_handler(name))
            .collect::<Result<Vec<_>>>()?;
        self.window_streamer(topics, range).await
    }

    pub async fn get_window_streamer_for(
        &self,
        topics: Vec<TopicHandler>,
        range: impl Into<TimestampRange>,
    ) -> Result<SequenceWindowStreamer> {
        self.window_streamer(topics, range.into()).await
    }

    async fn window_streamer(
        &self,
        topics: Vec<TopicHandler>,
        range: TimestampRange,
    ) -> Result<SequenceWindowStreamer> {
        self.connection.ensure_open()?;
        range.check()?;
        let topics = self.check(topics)?;
        let token = self.scope.token();
        let streams = token
            .run_until_closed(open_streams(&self.connection, &topics, range))
            .await
            .unwrap_or_else(|| Err(Error::new(ErrorKind::Closed, "stream closed")))?;
        SequenceWindowStreamer::new(topics, streams, WINDOW_ROWS, token)
    }

    pub async fn reload(&mut self) -> Result<()> {
        self.connection.ensure_open()?;
        let fresh = self
            .scope
            .token()
            .run_until_closed(Self::fetch(
                &self.connection,
                self.scope.clone(),
                &self.name,
            ))
            .await
            .unwrap_or_else(|| Err(Error::new(ErrorKind::Closed, "sequence handler closed")))?;
        *self = fresh;
        Ok(())
    }

    pub fn close(&self) {
        self.scope.close();
    }

    fn check(&self, topics: Vec<TopicHandler>) -> Result<Vec<TopicHandler>> {
        if topics.is_empty() {
            check_topic_count(self.topics.len())?;
            return Ok(self.topics.clone());
        }
        check_topic_count(topics.len())?;
        let mut seen = HashSet::with_capacity(topics.len());
        for topic in &topics {
            if topic.sequence_name() != self.name {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "topic '{}{}' is not in sequence '{}'",
                        topic.sequence_name(),
                        topic.name(),
                        self.name
                    ),
                ));
            }
            if !topic.same_client(&self.connection) {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "topic '{}{}' was opened by another client",
                        topic.sequence_name(),
                        topic.name()
                    ),
                ));
            }
            if !seen.insert(topic.name()) {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("topic '{}' given twice", topic.name()),
                ));
            }
        }
        Ok(topics)
    }
}

async fn open_streams(
    connection: &Connection,
    topics: &[TopicHandler],
    range: TimestampRange,
) -> Result<Vec<TopicBatchStreamer>> {
    let ranged = !range.is_all();
    let plans = try_join_all(
        topics
            .iter()
            .map(|t| t.read_plan(range).map_err(|e| from_topic(e, t))),
    )
    .await?;
    let plans: Vec<_> = plans
        .into_iter()
        .map(|p| if ranged { p.skip_if_empty() } else { p })
        .collect();
    let groups = connection
        .readers(plans.iter().filter(|p| p.has_data()).count())
        .await;
    let mut slots = groups
        .iter()
        .flat_map(|group| std::iter::repeat_n(group, group.leased_streams()));
    let opens: Vec<_> = topics
        .iter()
        .zip(plans)
        .map(|(t, plan)| {
            let reader = if plan.has_data() {
                slots.next().unwrap_or(connection)
            } else {
                connection
            };
            t.open_plan(reader, plan).map_err(|e| from_topic(e, t))
        })
        .collect();
    try_join_all(opens).await
}

fn from_topic(error: Error, topic: &TopicHandler) -> Error {
    if error.kind() == ErrorKind::NotFound {
        error
    } else {
        error.in_topic(&topic.locator())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offline(names: &[&str]) -> SequenceHandler {
        offline_on(Connection::offline(1 << 20), names)
    }

    fn offline_on(connection: Connection, names: &[&str]) -> SequenceHandler {
        let scope = connection.scope().child();
        let topics = names
            .iter()
            .map(|n| TopicHandler::offline(&connection, scope.child(), "seq", n, "Value"))
            .collect();
        SequenceHandler {
            connection,
            scope,
            name: "seq".to_owned(),
            created_timestamp: Timestamp::default(),
            metadata: Arc::default(),
            sessions: Vec::new(),
            topics,
        }
    }

    fn names(topics: &[TopicHandler]) -> Vec<&str> {
        topics.iter().map(|t| t.name()).collect()
    }

    impl SequenceHandler {
        fn select(&self, names: &[&str]) -> Result<Vec<TopicHandler>> {
            let topics = names
                .iter()
                .map(|n| self.topic_handler(n))
                .collect::<Result<Vec<_>>>()?;
            self.check(topics)
        }
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
    async fn select_over_cap() {
        let names: Vec<String> = (0..=u16::MAX as usize + 1)
            .map(|i| format!("/t{i}"))
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let seq = offline(&names);
        let error = seq.select(&[]).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let error = seq.check(seq.topics.clone()).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let fit = seq.topics[1..].to_vec();
        assert_eq!(seq.check(fit).unwrap().len(), names.len() - 1);
    }

    #[tokio::test]
    async fn select_twice_refused() {
        let seq = offline(&["/a", "/b"]);
        let error = seq.select(&["/a", "a"]).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn other_sequence_refused() {
        let seq = offline(&["/a"]);
        let connection = Connection::offline(1 << 20);
        let other = TopicHandler::offline(
            &connection,
            connection.scope().child(),
            "other",
            "/a",
            "Value",
        );
        let error = seq.check(vec![other]).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn other_client_refused() {
        let seq = offline(&["/a"]);
        let connection = Connection::offline(1 << 20);
        let other = TopicHandler::offline(
            &connection,
            connection.scope().child(),
            "seq",
            "/a",
            "Value",
        );
        let error = seq.check(vec![other]).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("another client"));
    }

    #[tokio::test]
    async fn closed_client_refused() {
        let seq = offline(&["/a"]);
        seq.connection.scope().close();
        let error = seq.get_data_streamer(&[], ..).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn close_stops_open() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let _server = tokio::spawn(async move {
            let _socket = listener.accept().await;
            std::future::pending::<()>().await
        });
        let seq = offline_on(Connection::offline_at(1 << 20, port), &[]);
        let connection = &seq.connection;
        let external =
            TopicHandler::offline(connection, connection.scope().child(), "seq", "/a", "Value");
        let opening = seq.get_window_streamer_for(vec![external], ..);
        let close = async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            seq.close();
        };
        let (opened, ()) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::join(opening, close),
        )
        .await
        .unwrap();
        assert_eq!(opened.err().unwrap().kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn open_error_names_topic() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let seq = offline_on(Connection::offline_at(1 << 20, port), &["/a"]);
        let error = seq.get_data_streamer(&[], 1..5).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Connection);
        assert!(error.to_string().starts_with("topic 'seq/a': "), "{error}");
        assert_eq!(error.to_string().matches("seq/a").count(), 1, "{error}");
        let labelled = error.in_topic("seq/a");
        assert_eq!(labelled.to_string().matches("seq/a").count(), 1);
        let missing = Error::new(ErrorKind::NotFound, "topic 'seq/a' not found");
        assert_eq!(
            from_topic(missing, &seq.topics[0]).to_string(),
            "topic 'seq/a' not found"
        );
    }

    #[tokio::test]
    async fn bad_range_empty() {
        let seq = offline(&[]);
        let error = seq.get_data_streamer(&[], 5..5).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let error = seq
            .get_window_streamer_for(vec![], Timestamp::MAX..)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn bad_range_offline() {
        let seq = offline(&["/a"]);
        let error = seq.get_data_streamer(&[], 5..5).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let topic = seq.topic_handler("/a").unwrap();
        let error = topic
            .get_batch_streamer(Timestamp::from_ns(9)..Timestamp::from_ns(1))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let error = seq
            .get_window_streamer_for(vec![topic], Timestamp::MAX..)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}
