use arrow_array::RecordBatch;
use arrow_flight::FlightDescriptor;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::connection::Connection;
use crate::error::Result;

enum Command {
    Batch(RecordBatch),
    Flush(oneshot::Sender<()>),
}

pub(crate) struct BatchWriter {
    commands: mpsc::Sender<Command>,
    task: JoinHandle<Result<()>>,
}

impl BatchWriter {
    pub(crate) async fn open(
        connection: &Connection,
        descriptor: FlightDescriptor,
    ) -> Result<Self> {
        todo!()
    }

    pub(crate) async fn send(&mut self, batch: RecordBatch) -> Result<()> {
        todo!()
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        todo!()
    }

    pub(crate) async fn finish(self) -> Result<()> {
        todo!()
    }
}

impl Drop for BatchWriter {
    fn drop(&mut self) {
        self.task.abort();
    }
}
