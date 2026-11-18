use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use tokio::sync::Mutex;

use crate::batch_writer::BatchWriter;
use crate::connection::Connection;
use crate::error::Result;
use crate::session::{Session, SessionErrorPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicErrorPolicy {
    Raise,
    Finalize,
    Ignore,
}

type SharedWriter = Arc<Mutex<Option<BatchWriter>>>;

pub struct SequenceWriter {
    session: Session,
    topics: Vec<SharedWriter>,
}

impl SequenceWriter {
    pub(crate) async fn create(
        connection: &Connection,
        name: &str,
        metadata: Option<Vec<u8>>,
        on_error: SessionErrorPolicy,
    ) -> Result<Self> {
        todo!()
    }

    pub(crate) async fn update(
        connection: &Connection,
        name: &str,
        on_error: SessionErrorPolicy,
    ) -> Result<Self> {
        todo!()
    }

    pub async fn topic_create(
        &mut self,
        name: &str,
        ontology_tag: &str,
        schema: SchemaRef,
        metadata: Option<Vec<u8>>,
        on_error: TopicErrorPolicy,
    ) -> Result<TopicWriter> {
        todo!()
    }

    pub async fn finish(self) -> Result<()> {
        todo!()
    }

    pub async fn abort(self) -> Result<()> {
        todo!()
    }
}

pub struct TopicWriter {
    name: String,
    on_error: TopicErrorPolicy,
    writer: SharedWriter,
}

impl TopicWriter {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn push_batch(&self, batch: RecordBatch) -> Result<()> {
        todo!()
    }

    pub async fn push_stream(&self, reader: impl RecordBatchReader + Send) -> Result<()> {
        todo!()
    }

    pub async fn flush(&self) -> Result<()> {
        todo!()
    }
}
