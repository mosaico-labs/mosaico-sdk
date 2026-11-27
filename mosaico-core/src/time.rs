use std::fmt;
use std::ops::{Range, RangeFrom, RangeFull, RangeTo};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::comm::proto;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const MIN: Self = Self(i64::MIN);
    pub const MAX: Self = Self(i64::MAX);

    pub const fn from_ns(ns: i64) -> Self {
        Self(ns)
    }

    pub const fn as_ns(self) -> i64 {
        self.0
    }
}

impl From<i64> for Timestamp {
    fn from(ns: i64) -> Self {
        Self(ns)
    }
}

impl From<Timestamp> for i64 {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.0
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = Error;

    fn try_from(time: SystemTime) -> Result<Self> {
        let ns = match time.duration_since(UNIX_EPOCH) {
            Ok(after) => i128::try_from(after.as_nanos()).unwrap_or(i128::MAX),
            Err(before) => -i128::try_from(before.duration().as_nanos()).unwrap_or(i128::MAX),
        };
        i64::try_from(ns).map(Self).map_err(|_| {
            Error::invalid_argument(
                "time outside the i64 nanosecond range (1677-09-21 to 2262-04-11)",
            )
        })
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TimestampRange {
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

impl TimestampRange {
    pub const ALL: Self = Self {
        start: None,
        end: None,
    };

    pub const fn new(start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        Self { start, end }
    }

    pub const fn start(&self) -> Option<Timestamp> {
        self.start
    }

    pub const fn end(&self) -> Option<Timestamp> {
        self.end
    }

    pub const fn is_all(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }

    pub(crate) fn check(&self) -> Result<()> {
        let start = self.start.unwrap_or(Timestamp::MIN);
        let end = self.end.unwrap_or(Timestamp::MAX);
        if start >= end {
            return Err(Error::invalid_argument(format!(
                "invalid time range {self}: the start must be before the end (a missing start counts as i64::MIN, a missing end as i64::MAX)"
            )));
        }
        Ok(())
    }

    pub(crate) fn to_proto(self) -> Option<proto::time::TimestampRange> {
        (!self.is_all()).then(|| proto::time::TimestampRange {
            start_ns: self.start.map(Timestamp::as_ns),
            end_ns: self.end.map(Timestamp::as_ns),
        })
    }
}

impl From<RangeFull> for TimestampRange {
    fn from(_: RangeFull) -> Self {
        Self::ALL
    }
}

impl From<Range<Timestamp>> for TimestampRange {
    fn from(range: Range<Timestamp>) -> Self {
        Self::new(Some(range.start), Some(range.end))
    }
}

impl From<RangeFrom<Timestamp>> for TimestampRange {
    fn from(range: RangeFrom<Timestamp>) -> Self {
        Self::new(Some(range.start), None)
    }
}

impl From<RangeTo<Timestamp>> for TimestampRange {
    fn from(range: RangeTo<Timestamp>) -> Self {
        Self::new(None, Some(range.end))
    }
}

impl From<Range<i64>> for TimestampRange {
    fn from(range: Range<i64>) -> Self {
        Self::new(Some(range.start.into()), Some(range.end.into()))
    }
}

impl From<RangeFrom<i64>> for TimestampRange {
    fn from(range: RangeFrom<i64>) -> Self {
        Self::new(Some(range.start.into()), None)
    }
}

impl From<RangeTo<i64>> for TimestampRange {
    fn from(range: RangeTo<i64>) -> Self {
        Self::new(None, Some(range.end.into()))
    }
}

impl fmt::Display for TimestampRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.start {
            Some(start) => write!(f, "[{start}, ")?,
            None => f.write_str("[.., ")?,
        }
        match self.end {
            Some(end) => write!(f, "{end})"),
            None => f.write_str("..)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::error::ErrorKind;

    fn range(r: impl Into<TimestampRange>) -> TimestampRange {
        r.into()
    }

    fn ts(ns: i64) -> Timestamp {
        Timestamp::from_ns(ns)
    }

    fn refused(r: TimestampRange) {
        assert_eq!(r.check().unwrap_err().kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn range_from_std() {
        assert_eq!(range(..), TimestampRange::ALL);
        assert_eq!(
            range(ts(1)..ts(2)),
            TimestampRange::new(Some(ts(1)), Some(ts(2)))
        );
        assert_eq!(range(ts(1)..), TimestampRange::new(Some(ts(1)), None));
        assert_eq!(range(..ts(2)), TimestampRange::new(None, Some(ts(2))));
    }

    #[test]
    fn integer_range_infers() {
        let t0 = ts(100);
        assert_eq!(range(10..20), range(ts(10)..ts(20)));
        assert_eq!(range(5..), range(ts(5)..));
        assert_eq!(range(..7), range(..ts(7)));
        assert_eq!(
            range(t0..ts(t0.as_ns() + 60_000_000_000)).end(),
            Some(ts(60_000_000_100))
        );
        let t: i64 = 3;
        assert_eq!(range(t..t + 1).start(), Some(ts(3)));
    }

    #[test]
    fn inverted_range_refused() {
        refused(TimestampRange::new(Some(ts(20)), Some(ts(10))));
        refused(range(ts(20)..ts(10)));
    }

    #[test]
    fn equal_bounds_refused() {
        refused(range(10..10));
    }

    #[test]
    fn max_start_refused() {
        refused(range(Timestamp::MAX..));
        refused(range(i64::MAX..i64::MAX));
    }

    #[test]
    fn min_end_refused() {
        refused(range(..i64::MIN));
        refused(range(..Timestamp::MIN));
    }

    #[test]
    fn min_start_accepted() {
        assert!(range(i64::MIN..).check().is_ok());
        assert!(range(..i64::MAX).check().is_ok());
        assert!(range(..).check().is_ok());
        assert!(range(i64::MIN..i64::MAX).check().is_ok());
    }

    #[test]
    fn all_sends_nothing() {
        assert_eq!(TimestampRange::ALL.to_proto(), None);
        assert!(TimestampRange::default().is_all());
    }

    #[test]
    fn bounds_to_proto() {
        let proto = range(5..).to_proto().unwrap();
        assert_eq!((proto.start_ns, proto.end_ns), (Some(5), None));
        let proto = range(..9).to_proto().unwrap();
        assert_eq!((proto.start_ns, proto.end_ns), (None, Some(9)));
        let proto = range(-3..9).to_proto().unwrap();
        assert_eq!((proto.start_ns, proto.end_ns), (Some(-3), Some(9)));
    }

    #[test]
    fn range_display() {
        assert_eq!(range(1..2).to_string(), "[1, 2)");
        assert_eq!(range(1..).to_string(), "[1, ..)");
        assert_eq!(range(..2).to_string(), "[.., 2)");
        assert_eq!(range(..).to_string(), "[.., ..)");
        assert!(
            range(3..3)
                .check()
                .unwrap_err()
                .to_string()
                .contains("[3, 3)")
        );
    }

    #[test]
    fn system_time_roundtrip() {
        let after = UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
        assert_eq!(
            Timestamp::try_from(after).unwrap(),
            ts(1_700_000_000_123_456_789)
        );
        let before = UNIX_EPOCH - Duration::from_nanos(5);
        assert_eq!(Timestamp::try_from(before).unwrap(), ts(-5));
        assert_eq!(Timestamp::try_from(UNIX_EPOCH).unwrap(), ts(0));
        let min = UNIX_EPOCH - Duration::from_nanos(i64::MIN.unsigned_abs());
        assert_eq!(Timestamp::try_from(min).unwrap(), Timestamp::MIN);
    }

    #[test]
    fn system_time_out_of_range() {
        let late = UNIX_EPOCH + Duration::from_nanos(i64::MAX as u64) + Duration::from_nanos(1);
        let error = Timestamp::try_from(late).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let early = UNIX_EPOCH - Duration::from_nanos(i64::MIN.unsigned_abs() + 1);
        assert!(Timestamp::try_from(early).is_err());
    }
}
