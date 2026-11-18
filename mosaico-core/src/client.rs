use crate::connection::{ConnectOptions, Connection, ServerInfo};
use crate::error::Result;
use crate::sequence::SequenceHandler;
use crate::session::SessionErrorPolicy;
use crate::topic::TopicHandler;
use crate::writer::SequenceWriter;

#[derive(Clone)]
pub struct MosaicoClient {
    connection: Connection,
}

impl MosaicoClient {
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        let connection = Connection::connect(options).await?;
        Ok(Self { connection })
    }

    pub fn server_info(&self) -> &ServerInfo {
        self.connection.info()
    }

    pub fn close(&self) {
        self.connection.close();
    }

    pub async fn sequence_handler(&self, name: &str) -> Result<SequenceHandler> {
        SequenceHandler::open(&self.connection, name).await
    }

    pub async fn topic_handler(&self, sequence: &str, topic: &str) -> Result<TopicHandler> {
        TopicHandler::open(&self.connection, sequence, topic).await
    }

    pub async fn sequence_create(
        &self,
        name: &str,
        metadata: Option<Vec<u8>>,
        on_error: SessionErrorPolicy,
    ) -> Result<SequenceWriter> {
        SequenceWriter::create(&self.connection, name, metadata, on_error).await
    }

    pub async fn sequence_update(
        &self,
        name: &str,
        on_error: SessionErrorPolicy,
    ) -> Result<SequenceWriter> {
        SequenceWriter::update(&self.connection, name, on_error).await
    }

    pub async fn list_sequences(&self) -> Result<Vec<String>> {
        todo!()
    }

    pub async fn sequence_delete(&self, name: &str) -> Result<()> {
        todo!()
    }
}
