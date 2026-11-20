use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_schema::SchemaRef;

use crate::comm::action::ActionKind;
use crate::comm::connection::Connection;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{new_sequence_name, sequence_name, topic_name};
use crate::ontology::Format;
use crate::scope::Scope;
use crate::write::batch_writer::BatchWriter;

#[derive(Debug, Clone, Default)]
pub struct Interrupter(Arc<AtomicBool>);

impl Interrupter {
    pub fn interrupt(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

pub(crate) fn interrupted_error() -> Error {
    Error::new(ErrorKind::Closed, "the write session was interrupted")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        let request = proto::SequenceCreate {
            locator: sequence.clone(),
            user_metadata: metadata.unwrap_or_default(),
        };
        connection
            .action_empty(ActionKind::SequenceCreate, &request)
            .await?;
        match Self::open(connection, sequence.clone(), policy, true).await {
            Ok(session) => Ok(session),
            Err(error) => {
                let request = proto::ResourceLocator { locator: sequence };
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
        let request = proto::ResourceLocator {
            locator: sequence.clone(),
        };
        let created: proto::SessionCreate = connection
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
            interrupter: Interrupter::default(),
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
        schema: SchemaRef,
        metadata: Option<Vec<u8>>,
    ) -> Result<BatchWriter> {
        let locator = format!("{}{}", self.sequence, topic_name(name));
        let request = proto::TopicCreate {
            locator: locator.clone(),
            session_uuid: self.uuid.clone(),
            serialization_format: format.to_proto(),
            ontology_tag: ontology_tag.to_owned(),
            user_metadata: metadata.unwrap_or_default(),
        };
        let created: proto::ResourceUuid = self
            .connection
            .action(ActionKind::TopicCreate, &request)
            .await?;
        match BatchWriter::open(
            &self.connection,
            locator.clone(),
            created.uuid,
            schema,
            self.scope.token(),
            self.interrupter.clone(),
        ) {
            Ok(writer) => Ok(writer),
            Err(error) => {
                let request = proto::ResourceLocator { locator };
                let _ = self
                    .connection
                    .action_empty(ActionKind::TopicDelete, &request)
                    .await;
                Err(error)
            }
        }
    }

    pub(crate) async fn abort(self, reason: &str) -> Result<()> {
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
                let finalized = self.finish().await;
                reported.and(finalized)
            }
            SessionErrorPolicy::Delete => {
                let request = proto::ResourceLocator {
                    locator: self.locator.clone(),
                };
                let deleted = self
                    .connection
                    .action_empty(ActionKind::SessionDelete, &request)
                    .await;
                if !self.owns_sequence {
                    return deleted;
                }
                let request = proto::ResourceLocator {
                    locator: self.sequence.clone(),
                };
                let removed = self
                    .connection
                    .action_empty(ActionKind::SequenceDelete, &request)
                    .await;
                deleted.and(removed)
            }
        }
    }

    pub(crate) async fn finish(&self) -> Result<()> {
        let request = proto::SessionUuid {
            session_uuid: self.uuid.clone(),
        };
        self.connection
            .action_empty(ActionKind::SessionFinalize, &request)
            .await
    }
}
