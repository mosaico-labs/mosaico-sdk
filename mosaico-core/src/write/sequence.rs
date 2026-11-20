use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};

use crate::comm::connection::Connection;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{new_topic_name, topic_name};
use crate::ontology::{Encode, Format};
use crate::write::rows::record_schema;
use crate::write::session::{Interrupter, Session, SessionErrorPolicy, interrupted_error};
use crate::write::topic::{
    Close, TIMESTAMP, TopicErrorPolicy, TopicState, TopicWriter, TopicWriterStatus,
};

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
        on_error: SessionErrorPolicy,
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
        on_error: SessionErrorPolicy,
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
        name: &str,
        ontology_tag: &str,
        format: Format,
        schema: SchemaRef,
        metadata: Option<Vec<u8>>,
        on_error: TopicErrorPolicy,
    ) -> Result<TopicWriter> {
        let name = new_topic_name(name)?;
        if self.topics.iter().any(|topic| topic.name() == name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("topic '{name}' already created in this session"),
            ));
        }
        match schema
            .field_with_name(TIMESTAMP)
            .map(|field| field.data_type())
        {
            Ok(DataType::Int64) => {}
            _ => {
                return Err(Error::schema(format!(
                    "schema must have a '{TIMESTAMP}' column of type Int64"
                )));
            }
        }
        let writer = self
            .session
            .create_topic(&name, ontology_tag, format, schema, metadata)
            .await?;
        let locator = format!("{}{name}", self.session.sequence());
        let connection = self.session.connection().clone();
        let topic = Arc::new(TopicState::new(name, locator, connection, on_error, writer));
        self.topics.push(topic.clone());
        Ok(TopicWriter::new(topic))
    }

    pub async fn topic_create_for<T: Encode>(
        &mut self,
        name: &str,
        metadata: Option<Vec<u8>>,
        on_error: TopicErrorPolicy,
    ) -> Result<TopicWriter> {
        let schema = record_schema::<T>();
        self.topic_create(name, T::TAG, T::FORMAT, schema, metadata, on_error)
            .await
    }

    pub fn get_topic_writer(&self, name: &str) -> Result<TopicWriter> {
        let name = topic_name(name);
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
        let error = match closed.into_iter().find_map(Result::err) {
            Some(error) => error,
            None => match self.session.finish().await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            },
        };
        let _ = self.session.abort(&error.to_string()).await;
        Err(error)
    }

    pub async fn abort(self, reason: &str) -> Result<()> {
        let close = if self.session.deletes_on_error() {
            Close::Discard(reason)
        } else {
            Close::Report(reason)
        };
        futures::future::join_all(self.topics.iter().map(|topic| topic.close(close))).await;
        self.session.abort(reason).await
    }

    async fn close_interrupted(self) -> Result<()> {
        for topic in &self.topics {
            topic.set_state(
                TopicWriterStatus::FinalizedWithError,
                Some(INTERRUPTED.to_owned()),
            );
        }
        self.session.abort(INTERRUPTED).await?;
        Err(interrupted_error())
    }
}
