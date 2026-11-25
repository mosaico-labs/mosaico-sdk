use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, watch};

use crate::comm::connection::ServerInfo;

const BUDGET_BYTES: usize = 512 << 20;
const PRESSURE_RETRY: Duration = Duration::from_millis(10);
pub(crate) const QUEUE_MESSAGES: usize = 4;

pub(crate) fn limits(info: &ServerInfo) -> (usize, usize) {
    let limit = usize::try_from(info.max_batch_size_bytes()).unwrap_or(usize::MAX);
    (limit, limit.clamp(1, u32::MAX as usize / QUEUE_MESSAGES))
}

pub(crate) struct WriteBudget {
    semaphore: Arc<Semaphore>,
    pressure: watch::Sender<u64>,
}

impl WriteBudget {
    pub(crate) fn for_server(info: &ServerInfo) -> Self {
        let (_, target) = limits(info);
        Self::new(BUDGET_BYTES.max(QUEUE_MESSAGES * target))
    }

    pub(crate) fn new(bytes: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(bytes.clamp(1, Semaphore::MAX_PERMITS))),
            pressure: watch::Sender::new(0),
        }
    }

    pub(crate) fn pressure(&self) -> watch::Receiver<u64> {
        self.pressure.subscribe()
    }

    pub(crate) async fn acquire(
        &self,
        permits: u32,
        topic: &Semaphore,
    ) -> Option<OwnedSemaphorePermit> {
        match self.semaphore.clone().try_acquire_many_owned(permits) {
            Ok(permit) => return Some(permit),
            Err(TryAcquireError::Closed) => return None,
            Err(TryAcquireError::NoPermits) => {}
        }
        let mut waiting = pin!(self.semaphore.clone().acquire_many_owned(permits));
        loop {
            self.pressure
                .send_modify(|generation| *generation = generation.wrapping_add(1));
            if let Ok(permit) = tokio::time::timeout(PRESSURE_RETRY, waiting.as_mut()).await {
                return permit.ok();
            }
            if topic.is_closed() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn full_budget_signals_pressure() {
        let budget = Arc::new(WriteBudget::new(100));
        let mut pressure = budget.pressure();
        let topic = Semaphore::new(1000);
        let held = budget.acquire(100, &topic).await.unwrap();
        assert!(!pressure.has_changed().unwrap());
        let waiting = tokio::spawn({
            let budget = budget.clone();
            async move {
                let topic = Semaphore::new(1000);
                budget.acquire(10, &topic).await.is_some()
            }
        });
        pressure.changed().await.unwrap();
        drop(held);
        assert!(waiting.await.unwrap());
    }

    #[tokio::test]
    async fn closed_topic_stops_waiting() {
        let budget = WriteBudget::new(10);
        let topic = Semaphore::new(10);
        let _held = budget.acquire(10, &topic).await.unwrap();
        topic.close();
        let waited = tokio::time::timeout(Duration::from_secs(1), budget.acquire(5, &topic)).await;
        assert!(waited.is_ok_and(|permit| permit.is_none()));
    }
}
