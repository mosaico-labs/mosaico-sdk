use std::time::Instant;

use crate::write::topic::FlushPolicy;

use super::Queued;

pub(super) struct Pending {
    pub(super) target: usize,
    pub(super) policy: FlushPolicy,
    pub(super) queued: Vec<Queued>,
    pub(super) bytes: usize,
    pub(super) rows: usize,
    pub(super) since: Option<Instant>,
}

impl Pending {
    fn max_bytes(&self) -> usize {
        self.policy
            .max_bytes
            .map_or(self.target, |bytes| bytes.min(self.target))
    }

    fn exceeded_by(&self, bytes: usize, rows: usize) -> bool {
        self.bytes + bytes > self.max_bytes()
            || self
                .policy
                .max_rows
                .is_some_and(|max| self.rows + rows > max)
    }

    fn full(&self) -> bool {
        self.bytes >= self.max_bytes()
            || self.policy.max_rows.is_some_and(|max| self.rows >= max)
            || self
                .policy
                .max_age
                .zip(self.since)
                .is_some_and(|(age, since)| since.elapsed() >= age)
    }

    pub(super) fn add(&mut self, item: Queued) -> Vec<Vec<Queued>> {
        let rows = item.batch.num_rows();
        let mut ready = Vec::new();
        if !self.queued.is_empty() && self.exceeded_by(item.bytes, rows) {
            ready.extend(self.take());
        }
        let since = item.since.unwrap_or_else(Instant::now);
        self.since = Some(self.since.map_or(since, |held| held.min(since)));
        self.bytes += item.bytes;
        self.rows += rows;
        let urgent = item.urgent;
        self.queued.push(item);
        if urgent || self.full() {
            ready.extend(self.take());
        }
        ready
    }

    pub(super) fn set_policy(&mut self, policy: FlushPolicy) -> Vec<Vec<Queued>> {
        self.policy = policy;
        if self.full() {
            self.take().into_iter().collect()
        } else {
            Vec::new()
        }
    }

    pub(super) fn take(&mut self) -> Option<Vec<Queued>> {
        self.bytes = 0;
        self.rows = 0;
        self.since = None;
        if self.queued.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.queued))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};

    use arrow_schema::{DataType, Field, Schema};
    use tokio::sync::Semaphore;

    use super::super::Permits;
    use super::*;

    fn pending(policy: FlushPolicy) -> Pending {
        Pending {
            target: 100,
            policy,
            queued: Vec::new(),
            bytes: 0,
            rows: 0,
            since: None,
        }
    }

    fn queued(rows: usize, bytes: usize) -> Queued {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values = Int64Array::from_iter_values(0..rows as i64);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let permit = Permits {
            _topic: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            _budget: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        };
        Queued {
            batch,
            bytes,
            since: None,
            urgent: false,
            permit,
        }
    }

    #[test]
    fn urgent_item_flushes() {
        let mut pending = pending(FlushPolicy::default());
        assert!(pending.add(queued(1, 10)).is_empty());
        let urgent = Queued {
            urgent: true,
            ..queued(1, 10)
        };
        assert_eq!(sizes(&pending.add(urgent)), [2]);
    }

    #[test]
    fn age_from_oldest() {
        let mut pending = pending(FlushPolicy {
            max_age: Some(Duration::from_secs(60)),
            ..FlushPolicy::default()
        });
        let old = Queued {
            since: Instant::now().checked_sub(Duration::from_secs(120)),
            ..queued(1, 10)
        };
        assert_eq!(sizes(&pending.add(old)), [1]);
    }

    fn sizes(groups: &[Vec<Queued>]) -> Vec<usize> {
        groups.iter().map(Vec::len).collect()
    }

    #[test]
    fn joins_to_target() {
        let mut pending = pending(FlushPolicy::default());
        assert!(pending.add(queued(1, 40)).is_empty());
        assert!(pending.add(queued(1, 40)).is_empty());
        assert_eq!(sizes(&pending.add(queued(1, 40))), vec![2]);
        assert_eq!(
            sizes(&pending.take().into_iter().collect::<Vec<_>>()),
            vec![1]
        );
    }

    #[test]
    fn full_batch_sent_at_once() {
        let mut pending = pending(FlushPolicy::default());
        assert!(pending.add(queued(1, 30)).is_empty());
        assert_eq!(sizes(&pending.add(queued(1, 100))), vec![1, 1]);
        assert!(pending.take().is_none());
    }

    #[test]
    fn max_bytes_capped_by_target() {
        let mut pending = pending(FlushPolicy {
            max_bytes: Some(1000),
            ..FlushPolicy::default()
        });
        assert_eq!(sizes(&pending.add(queued(1, 100))), vec![1]);
    }

    #[test]
    fn max_rows_limit() {
        let mut pending = pending(FlushPolicy {
            max_rows: Some(5),
            ..FlushPolicy::default()
        });
        assert!(pending.add(queued(3, 1)).is_empty());
        assert_eq!(sizes(&pending.add(queued(3, 1))), vec![1]);
        assert_eq!(sizes(&pending.add(queued(2, 1))), vec![2]);
    }

    #[test]
    fn zero_limits_send_every_batch() {
        let mut pending = pending(FlushPolicy {
            max_rows: Some(0),
            ..FlushPolicy::default()
        });
        assert_eq!(sizes(&pending.add(queued(1, 1))), vec![1]);
        assert_eq!(sizes(&pending.add(queued(1, 1))), vec![1]);
    }

    #[test]
    fn age_checked_at_push() {
        let mut pending = pending(FlushPolicy {
            max_age: Some(Duration::from_millis(20)),
            ..FlushPolicy::default()
        });
        assert!(pending.add(queued(1, 1)).is_empty());
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(sizes(&pending.add(queued(1, 1))), vec![2]);
    }

    #[test]
    fn stricter_policy_flushes() {
        let mut pending = pending(FlushPolicy::default());
        assert!(pending.add(queued(4, 10)).is_empty());
        let ready = pending.set_policy(FlushPolicy {
            max_rows: Some(2),
            ..FlushPolicy::default()
        });
        assert_eq!(sizes(&ready), vec![1]);
    }
}
