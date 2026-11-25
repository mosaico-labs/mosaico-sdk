use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::comm::action::ActionKind;
use crate::comm::connection::Connection;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{new_sequence_name, sequence_name};
use crate::ontology::Format;
use crate::scope::{Scope, Token};
use crate::write::topic::TopicState;

#[derive(Debug, Clone)]
pub struct Interrupter(Arc<AtomicBool>);

impl Interrupter {
    pub(crate) fn new() -> Self {
        Self(Arc::default())
    }

    pub fn interrupt(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn interrupted(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

const FINALIZE_WAIT: Duration = Duration::from_millis(200);
const FINALIZE_WAITS: u32 = 10;

pub(crate) fn interrupted_error() -> Error {
    Error::new(ErrorKind::Closed, "the write session was interrupted")
}

pub(crate) struct CreatedTopic {
    pub(crate) locator: String,
    pub(crate) uuid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionErrorPolicy {
    Report,
    Delete,
}

pub(crate) struct Session {
    connection: Connection,
    scope: Scope,
    sequence: String,
    locator: String,
    uuid: String,
    policy: SessionErrorPolicy,
    owns_sequence: bool,
    has_topics: AtomicBool,
    interrupter: Interrupter,
}

impl Session {
    pub(crate) async fn create(
        connection: &Connection,
        sequence: &str,
        metadata: Option<Vec<u8>>,
        policy: SessionErrorPolicy,
    ) -> Result<Self> {
        let sequence = new_sequence_name(sequence)?.to_owned();
        let request = proto::requests::SequenceCreate {
            locator: sequence.clone(),
            user_metadata: metadata.unwrap_or_default(),
        };
        connection
            .action_empty(ActionKind::SequenceCreate, &request)
            .await?;
        match Self::open(connection, sequence.clone(), policy, true).await {
            Ok(session) => Ok(session),
            Err(error) => {
                let request = proto::requests::ResourceLocator { locator: sequence };
                let _ = connection
                    .action_empty(ActionKind::SequenceDelete, &request)
                    .await;
                Err(error)
            }
        }
    }

    pub(crate) async fn update(
        connection: &Connection,
        sequence: &str,
        policy: SessionErrorPolicy,
    ) -> Result<Self> {
        let sequence = sequence_name(sequence)?.to_owned();
        Self::open(connection, sequence, policy, false).await
    }

    async fn open(
        connection: &Connection,
        sequence: String,
        policy: SessionErrorPolicy,
        owns_sequence: bool,
    ) -> Result<Self> {
        let request = proto::requests::ResourceLocator {
            locator: sequence.clone(),
        };
        let created: proto::responses::SessionCreate = connection
            .action(ActionKind::SessionCreate, &request)
            .await?;
        Ok(Self {
            connection: connection.clone(),
            scope: connection.scope().child(),
            sequence,
            locator: created.locator,
            uuid: created.uuid,
            policy,
            owns_sequence,
            has_topics: AtomicBool::new(false),
            interrupter: Interrupter::new(),
        })
    }

    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn deletes_on_error(&self) -> bool {
        self.policy == SessionErrorPolicy::Delete
    }

    pub(crate) fn sequence(&self) -> &str {
        &self.sequence
    }

    pub(crate) fn locator(&self) -> &str {
        &self.locator
    }

    pub(crate) fn interrupter(&self) -> &Interrupter {
        &self.interrupter
    }

    pub(crate) async fn create_topic(
        &self,
        name: &str,
        ontology_tag: &str,
        format: Format,
        metadata: Option<Vec<u8>>,
    ) -> Result<CreatedTopic> {
        let locator = format!("{}{name}", self.sequence);
        let request = proto::requests::TopicCreate {
            locator: locator.clone(),
            session_uuid: self.uuid.clone(),
            serialization_format: format.to_proto(),
            ontology_tag: ontology_tag.to_owned(),
            user_metadata: metadata.unwrap_or_default(),
        };
        let created: proto::responses::ResourceUuid = self
            .connection
            .action(ActionKind::TopicCreate, &request)
            .await?;
        self.has_topics.store(true, Ordering::Relaxed);
        Ok(CreatedTopic {
            locator,
            uuid: created.uuid,
        })
    }

    pub(crate) fn token(&self) -> Arc<Token> {
        self.scope.token()
    }

    pub(super) async fn abort(self, reason: &str, topics: &[Arc<TopicState>]) -> Result<()> {
        match self.policy {
            SessionErrorPolicy::Report => {
                let message = format!(
                    "session {} of sequence '{}' failed: {reason}",
                    self.locator, self.sequence
                );
                let reported = self
                    .connection
                    .notify_error(
                        ActionKind::SequenceNotificationCreate,
                        &self.sequence,
                        message,
                    )
                    .await;
                let closed = if self.has_topics.load(Ordering::Relaxed) {
                    self.finalize(topics).await
                } else {
                    self.delete().await
                };
                match closed {
                    Ok(()) => reported,
                    Err(error) => Err(error.session_left_open()),
                }
            }
            SessionErrorPolicy::Delete => {
                let deleted = self.delete().await.map_err(Error::session_left_open);
                if !self.owns_sequence {
                    return deleted;
                }
                let request = proto::requests::ResourceLocator {
                    locator: self.sequence.clone(),
                };
                let removed = self
                    .connection
                    .action_empty(ActionKind::SequenceDelete, &request)
                    .await;
                removed.or_else(|error| deleted.and(Err(error)))
            }
        }
    }

    pub(super) async fn finalize(&self, topics: &[Arc<TopicState>]) -> Result<()> {
        let mut failed: Vec<(&str, Error, bool)> = topics
            .iter()
            .filter_map(|topic| {
                let (error, end_sent) = topic.failed_upload()?;
                Some((topic.locator(), error, end_sent))
            })
            .collect();
        let mut kept = topics.len();
        let mut waits = 0;
        loop {
            let refusal = match self.send_finalize().await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            let index = match refused_topic(&refusal, &self.locator, &failed) {
                Refusal::Finalized => return Ok(()),
                Refusal::Empty(index) => index,
                Refusal::Uploading(index) if !failed[index].2 => index,
                Refusal::Uploading(_) if waits < FINALIZE_WAITS => {
                    waits += 1;
                    tokio::time::sleep(FINALIZE_WAIT).await;
                    continue;
                }
                Refusal::Uploading(_) | Refusal::Other => return Err(refusal),
            };
            let (locator, cause, _) = failed.swap_remove(index);
            let request = proto::requests::ResourceLocator {
                locator: locator.to_owned(),
            };
            if let Err(denied) = self
                .connection
                .action_empty(ActionKind::TopicDelete, &request)
                .await
            {
                return Err(Error::new(
                    refusal.kind(),
                    format!("{refusal}; topic '{locator}' could not be deleted: {denied}"),
                ));
            }
            let message = format!("topic '{locator}' was deleted: {cause}");
            let _ = self
                .connection
                .notify_error(
                    ActionKind::SequenceNotificationCreate,
                    &self.sequence,
                    message,
                )
                .await;
            kept -= 1;
            if kept == 0 {
                return self.delete().await;
            }
        }
    }

    async fn delete(&self) -> Result<()> {
        let request = proto::requests::ResourceLocator {
            locator: self.locator.clone(),
        };
        self.connection
            .action_empty(ActionKind::SessionDelete, &request)
            .await
    }

    async fn send_finalize(&self) -> Result<()> {
        let request = proto::requests::SessionUuid {
            session_uuid: self.uuid.clone(),
        };
        self.connection
            .action_empty(ActionKind::SessionFinalize, &request)
            .await
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    Finalized,
    Empty(usize),
    Uploading(usize),
    Other,
}

fn refused_topic(refusal: &Error, session: &str, failed: &[(&str, Error, bool)]) -> Refusal {
    if refusal.kind() != ErrorKind::FailedPrecondition {
        return Refusal::Other;
    }
    let text = refusal.to_string();
    if text.contains(&format!("Session `{session}` already finalized")) {
        return Refusal::Finalized;
    }
    for (index, (locator, _, _)) in failed.iter().enumerate() {
        if text.contains(&format!("DoPut not called for topic `{locator}`")) {
            return Refusal::Empty(index);
        }
        if text.contains(&format!("Topic `{locator}` is already uploading data")) {
            return Refusal::Uploading(index);
        }
    }
    Refusal::Other
}

#[cfg(test)]
impl Session {
    pub(crate) fn offline(policy: SessionErrorPolicy) -> Self {
        let connection = Connection::offline(1 << 20);
        Self {
            scope: connection.scope().child(),
            connection,
            sequence: "run".to_owned(),
            locator: "run:1".to_owned(),
            uuid: String::new(),
            policy,
            owns_sequence: true,
            has_topics: AtomicBool::new(false),
            interrupter: Interrupter::new(),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.scope.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offline_session(policy: SessionErrorPolicy) -> Session {
        Session::offline(policy)
    }

    #[test]
    fn refused_topic_matching() {
        let failed = vec![
            (
                "run/imu",
                Error::new(ErrorKind::Connection, "broken pipe"),
                false,
            ),
            ("run/imu2", Error::new(ErrorKind::Connection, "reset"), true),
        ];
        let refusal = |kind, topic: &str| {
            Error::new(
                kind,
                format!(
                    "DoPut not called for topic `{topic}`. At least one with an empty batch is required to finalize the topic. (FailedPrecondition)"
                ),
            )
        };
        let named = refusal(ErrorKind::FailedPrecondition, "run/imu2");
        assert_eq!(refused_topic(&named, "run:1", &failed), Refusal::Empty(1));
        let other = refusal(ErrorKind::FailedPrecondition, "run/camera");
        assert_eq!(refused_topic(&other, "run:1", &failed), Refusal::Other);
        let lost = refusal(ErrorKind::Connection, "run/imu");
        assert_eq!(refused_topic(&lost, "run:1", &failed), Refusal::Other);
        assert_eq!(refused_topic(&named, "run:1", &[]), Refusal::Other);
        let uploading = Error::new(
            ErrorKind::FailedPrecondition,
            "Topic `run/imu` is already uploading data. (FailedPrecondition)",
        );
        assert_eq!(
            refused_topic(&uploading, "run:1", &failed),
            Refusal::Uploading(0)
        );
        let finalized = Error::new(
            ErrorKind::FailedPrecondition,
            "Session `run:1` already finalized. (FailedPrecondition)",
        );
        assert_eq!(
            refused_topic(&finalized, "run:1", &failed),
            Refusal::Finalized
        );
        assert_eq!(refused_topic(&finalized, "run:2", &failed), Refusal::Other);
    }

    #[tokio::test]
    async fn failed_abort_left_open() {
        for policy in [SessionErrorPolicy::Report, SessionErrorPolicy::Delete] {
            let error = offline_session(policy)
                .abort("boom", &[])
                .await
                .unwrap_err();
            assert!(error.left_open(), "{policy:?}");
        }
    }

    #[test]
    fn left_open_error_message() {
        let abort = Error::new(ErrorKind::Connection, "connection refused").session_left_open();
        let error = Error::new(ErrorKind::FailedPrecondition, "empty session").left_open_by(&abort);
        assert!(error.left_open());
        assert_eq!(error.kind(), ErrorKind::FailedPrecondition);
        assert_eq!(
            error.to_string(),
            "empty session; the session stays open on the server: connection refused"
        );
        let same = Error::new(ErrorKind::FailedPrecondition, "empty session").left_open_by(
            &Error::new(ErrorKind::FailedPrecondition, "empty session").session_left_open(),
        );
        assert_eq!(
            same.to_string(),
            "empty session; the session stays open on the server"
        );
        assert!(!Error::new(ErrorKind::Server, "x").left_open());
    }

    #[tokio::test]
    async fn drop_closes_topics() {
        let session = offline_session(SessionErrorPolicy::Report);
        let client = session.connection.clone();
        let topic = session.scope.token();
        drop(session);
        assert!(topic.is_closed());
        assert!(!client.scope().is_closed());
    }
}
