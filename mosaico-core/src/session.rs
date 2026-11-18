use arrow_schema::SchemaRef;

use crate::batch_writer::BatchWriter;
use crate::connection::Connection;
use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionErrorPolicy {
    Report,
    Delete,
}

pub(crate) struct Session {
    connection: Connection,
    sequence: String,
    uuid: String,
    policy: SessionErrorPolicy,
}

impl Session {
    pub(crate) async fn create(
        connection: &Connection,
        sequence: &str,
        metadata: Option<Vec<u8>>,
        policy: SessionErrorPolicy,
    ) -> Result<Self> {
        todo!()
    }

    pub(crate) async fn update(
        connection: &Connection,
        sequence: &str,
        policy: SessionErrorPolicy,
    ) -> Result<Self> {
        todo!()
    }

    pub(crate) async fn create_topic(
        &self,
        name: &str,
        ontology_tag: &str,
        schema: SchemaRef,
        metadata: Option<Vec<u8>>,
    ) -> Result<BatchWriter> {
        todo!()
    }

    pub(crate) async fn finish(self) -> Result<()> {
        todo!()
    }

    pub(crate) async fn abort(self) -> Result<()> {
        todo!()
    }
}
