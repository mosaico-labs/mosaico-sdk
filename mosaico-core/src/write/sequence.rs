use std::sync::Arc;

use arrow_schema::SchemaRef;

use crate::comm::connection::Connection;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{self, new_topic_name};
use crate::ontology::{Encode, Format};
use crate::write::batch_writer::BatchWriter;
use crate::write::rows::record_schema;
use crate::write::schema_rules::check_schema;
use crate::write::session::{Interrupter, Session, SessionLevelErrorPolicy, interrupted_error};
use crate::write::topic::{Close, Reporter, TopicLevelErrorPolicy, TopicState, TopicWriter};

const INTERRUPTED: &str = "interrupted";

pub struct SequenceWriter {
    session: Session,
    topics: Vec<Arc<TopicState>>,
}

impl SequenceWriter {
    pub(crate) async fn create(
        connection: &Connection,
        name: &str,
        metadata: Option<Vec<u8>>,
        on_error: SessionLevelErrorPolicy,
    ) -> Result<Self> {
        let session = Session::create(connection, name, metadata, on_error).await?;
        Ok(Self {
            session,
            topics: Vec::new(),
        })
    }

    pub(crate) async fn update(
        connection: &Connection,
        name: &str,
        on_error: SessionLevelErrorPolicy,
    ) -> Result<Self> {
        let session = Session::update(connection, name, on_error).await?;
        Ok(Self {
            session,
            topics: Vec::new(),
        })
    }

    pub fn name(&self) -> &str {
        self.session.sequence()
    }

    pub fn session_locator(&self) -> &str {
        self.session.locator()
    }

    pub fn interrupter(&self) -> Interrupter {
        self.session.interrupter().clone()
    }

    pub async fn topic_create(
        &mut self,
        topic_name: &str,
        ontology_tag: &str,
        format: Format,
        schema: SchemaRef,
        metadata: Option<Vec<u8>>,
        on_error: TopicLevelErrorPolicy,
    ) -> Result<TopicWriter> {
        let name = new_topic_name(topic_name)?;
        if self.topics.iter().any(|topic| topic.name() == name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("topic '{name}' already created in this session"),
            ));
        }
        check_schema(&schema)?;
        let created = self
            .session
            .create_topic(&name, ontology_tag, format, metadata)
            .await?;
        let connection = self.session.connection();
        let reporter = Arc::new(Reporter::new(name, created.locator, connection.clone()));
        let writer = BatchWriter::open(
            connection,
            reporter.clone(),
            created.uuid,
            schema,
            self.session.token(),
            self.session.interrupter().clone(),
        );
        let topic = Arc::new(TopicState::new(
            ontology_tag.to_owned(),
            format,
            on_error,
            reporter,
            writer,
        ));
        self.topics.push(topic.clone());
        Ok(TopicWriter::new(topic))
    }

    pub async fn topic_create_for<T: Encode>(
        &mut self,
        topic_name: &str,
        metadata: Option<Vec<u8>>,
        on_error: TopicLevelErrorPolicy,
    ) -> Result<TopicWriter> {
        let schema = record_schema::<T>();
        self.topic_create(topic_name, T::TAG, T::FORMAT, schema, metadata, on_error)
            .await
    }

    pub fn topic_writer(&self, topic_name: &str) -> Result<TopicWriter> {
        let name = locator::topic_name(topic_name);
        self.topics
            .iter()
            .find(|topic| topic.name() == name)
            .map(|topic| TopicWriter::new(topic.clone()))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("topic '{name}' was not created in this session"),
                )
            })
    }

    pub async fn finish(self) -> Result<()> {
        let closed =
            futures::future::join_all(self.topics.iter().map(|topic| topic.close(Close::Finish)))
                .await;
        if self.session.interrupter().interrupted() {
            return self.close_interrupted().await;
        }
        let failed = closed
            .into_iter()
            .find_map(Result::err)
            .or_else(|| self.upload_failure());
        let error = match failed {
            Some(error) => error,
            None => match self.session.finalize(&self.topics).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            },
        };
        match self.session.abort(&error.to_string(), &self.topics).await {
            Err(abort) if abort.left_open() => Err(error.left_open_by(&abort)),
            _ => Err(error),
        }
    }

    pub async fn complete<T>(self, result: Result<T>) -> Result<T> {
        let error = match result {
            Ok(value) => return self.finish().await.map(|()| value),
            Err(error) => error,
        };
        match self.abort(&error.to_string()).await {
            Err(abort) if abort.left_open() => Err(error.left_open_by(&abort)),
            _ => Err(error),
        }
    }

    pub async fn abort(self, reason: &str) -> Result<()> {
        let close = if self.session.deletes_on_error() {
            Close::Discard(reason)
        } else {
            Close::Report(reason)
        };
        futures::future::join_all(self.topics.iter().map(|topic| topic.close(close))).await;
        self.session.abort(reason, &self.topics).await
    }

    fn upload_failure(&self) -> Option<Error> {
        self.topics.iter().find_map(|topic| topic.upload_failure())
    }

    async fn close_interrupted(self) -> Result<()> {
        match self.session.abort(INTERRUPTED, &self.topics).await {
            Err(abort) if abort.left_open() => Err(interrupted_error().left_open_by(&abort)),
            _ => Err(interrupted_error()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offline(policy: SessionLevelErrorPolicy) -> SequenceWriter {
        SequenceWriter {
            session: Session::offline(policy),
            topics: Vec::new(),
        }
    }

    #[tokio::test]
    async fn complete_err_returns_error() {
        for policy in [
            SessionLevelErrorPolicy::Report,
            SessionLevelErrorPolicy::Delete,
        ] {
            let failed: Result<()> = Err(Error::invalid_argument("camera unplugged"));
            let error = offline(policy).complete(failed).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(error.left_open());
            assert!(
                error
                    .to_string()
                    .starts_with("camera unplugged; the session stays open on the server")
            );
        }
    }

    #[tokio::test]
    async fn interrupted_finish() {
        for policy in [
            SessionLevelErrorPolicy::Report,
            SessionLevelErrorPolicy::Delete,
        ] {
            let writer = offline(policy);
            writer.session.interrupter().interrupt();
            let error = writer.finish().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Closed);
            assert!(error.left_open());
            assert!(error.to_string().starts_with(&format!(
                "{}; the session stays open on the server",
                interrupted_error()
            )));
        }
    }

    #[tokio::test]
    async fn complete_ok_finishes() {
        let error = offline(SessionLevelErrorPolicy::Report)
            .complete(Ok(7))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Connection);
        assert!(error.left_open());
    }
}
