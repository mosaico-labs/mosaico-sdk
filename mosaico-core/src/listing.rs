use arrow_flight::{Criteria, FlightInfo};
use prost::Message;

use crate::client::MosaicoClient;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SequenceOrder {
    #[default]
    CreatedAt,
    Name,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SequenceCursor(Box<[u8]>);

impl SequenceCursor {
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.into();
        match proto::flight::SequenceCursor::decode(bytes.as_slice()) {
            Ok(cursor) if cursor.key.is_some() => Ok(Self(bytes.into_boxed_slice())),
            _ => Err(Error::invalid_argument("invalid sequence cursor")),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_vec()
    }

    fn to_proto(&self) -> Result<proto::flight::SequenceCursor> {
        proto::flight::SequenceCursor::decode(&*self.0)
            .map_err(|e| Error::invalid_argument("invalid sequence cursor").with_source(e))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageStart {
    First(SequenceOrder),
    After(SequenceCursor),
}

impl Default for PageStart {
    fn default() -> Self {
        Self::First(SequenceOrder::CreatedAt)
    }
}

impl From<SequenceOrder> for PageStart {
    fn from(order: SequenceOrder) -> Self {
        Self::First(order)
    }
}

impl From<SequenceCursor> for PageStart {
    fn from(cursor: SequenceCursor) -> Self {
        Self::After(cursor)
    }
}

impl From<&SequenceCursor> for PageStart {
    fn from(cursor: &SequenceCursor) -> Self {
        Self::After(cursor.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SequencePage {
    pub names: Vec<String>,
    pub next_cursor: Option<SequenceCursor>,
}

pub struct SequenceNames {
    client: MosaicoClient,
    page_size: u32,
    next: Option<PageStart>,
    names: std::vec::IntoIter<String>,
}

impl SequenceNames {
    pub(crate) fn new(client: MosaicoClient, page_size: u32, order_by: SequenceOrder) -> Self {
        Self {
            client,
            page_size,
            next: Some(order_by.into()),
            names: Vec::new().into_iter(),
        }
    }

    pub async fn next(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(name) = self.names.next() {
                return Ok(Some(name));
            }
            let Some(start) = self.next.clone() else {
                return Ok(None);
            };
            let page = self
                .client
                .list_sequences_page(self.page_size, start)
                .await?;
            self.next = page.next_cursor.map(PageStart::After);
            self.names = page.names.into_iter();
        }
    }
}

pub(crate) fn criteria(limit: u32, start: &PageStart) -> Result<Criteria> {
    use proto::flight::list_flights_cmd::Start;
    let start = match start {
        PageStart::First(SequenceOrder::CreatedAt) => {
            Start::OrderBy(proto::flight::SequenceOrder::CreatedAt as i32)
        }
        PageStart::First(SequenceOrder::Name) => {
            Start::OrderBy(proto::flight::SequenceOrder::Name as i32)
        }
        PageStart::After(cursor) => Start::Cursor(cursor.to_proto()?),
    };
    let request = proto::flight::ListFlightsCmd {
        limit,
        start: Some(start),
    };
    Ok(Criteria {
        expression: request.encode_to_vec().into(),
    })
}

pub(crate) fn page_from_infos(infos: Vec<FlightInfo>, limit: u32) -> Result<SequencePage> {
    let is_last = limit == 0 || infos.len() < limit as usize;
    let next_cursor = if is_last {
        None
    } else {
        let last = infos
            .last()
            .map(|info| info.app_metadata.clone())
            .unwrap_or_default();
        let cursor = proto::flight::SequenceListItemAppMetadata::decode(last)
            .ok()
            .and_then(|item| item.cursor)
            .filter(|cursor| cursor.key.is_some())
            .ok_or_else(|| Error::new(ErrorKind::Protocol, "sequence listing without a cursor"))?;
        Some(SequenceCursor(cursor.encode_to_vec().into_boxed_slice()))
    };
    let names = infos
        .into_iter()
        .map(|info| {
            info.flight_descriptor
                .and_then(|descriptor| descriptor.path.into_iter().next())
                .ok_or_else(|| Error::new(ErrorKind::Protocol, "sequence listing without a name"))
        })
        .collect::<Result<_>>()?;
    Ok(SequencePage { names, next_cursor })
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightDescriptor;
    use proto::flight::sequence_cursor::Key;

    use super::*;

    fn name_cursor(name: &str) -> proto::flight::SequenceCursor {
        proto::flight::SequenceCursor {
            key: Some(Key::Name(name.to_owned())),
        }
    }

    fn created_cursor(ns: i64, id: i32) -> proto::flight::SequenceCursor {
        proto::flight::SequenceCursor {
            key: Some(Key::CreatedAt(proto::flight::SequenceCursorCreatedAt {
                created_at_ns: ns,
                sequence_id: id,
            })),
        }
    }

    fn info(name: &str, cursor: Option<proto::flight::SequenceCursor>) -> FlightInfo {
        let item = proto::flight::SequenceListItemAppMetadata { cursor };
        FlightInfo::new()
            .with_descriptor(FlightDescriptor::new_path(vec![name.to_owned()]))
            .with_app_metadata(item.encode_to_vec())
    }

    fn infos(names: &[&str]) -> Vec<FlightInfo> {
        names
            .iter()
            .map(|name| info(name, Some(name_cursor(name))))
            .collect()
    }

    #[test]
    fn criteria_matches_sdk() {
        let first = criteria(50, &SequenceOrder::CreatedAt.into()).unwrap();
        assert_eq!(&first.expression[..], [0x08, 0x32, 0x10, 0x01]);
        let all = criteria(0, &PageStart::default()).unwrap();
        assert_eq!(&all.expression[..], [0x10, 0x01]);
        let name = criteria(0, &SequenceOrder::Name.into()).unwrap();
        assert_eq!(&name.expression[..], [0x10, 0x02]);
    }

    #[test]
    fn cursor_bytes_unchanged() {
        let bytes = created_cursor(1_700_000_000_000_000_000, 7).encode_to_vec();
        let cursor = SequenceCursor::from_bytes(bytes.clone()).unwrap();
        assert_eq!(cursor.as_bytes(), bytes);
        let sent = criteria(5, &(&cursor).into()).unwrap();
        let request = proto::flight::ListFlightsCmd::decode(sent.expression).unwrap();
        assert_eq!(request.limit, 5);
        let Some(proto::flight::list_flights_cmd::Start::Cursor(sent)) = request.start else {
            panic!("no cursor sent");
        };
        assert_eq!(sent.encode_to_vec(), bytes);
        assert_eq!(cursor.into_bytes(), bytes);
    }

    #[test]
    fn garbage_cursor_refused() {
        let error = SequenceCursor::from_bytes(vec![0xff]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn keyless_cursor_refused() {
        let error = SequenceCursor::from_bytes(Vec::new()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn short_page_last() {
        let page = page_from_infos(infos(&["a", "b"]), 3).unwrap();
        assert_eq!(page.names, ["a", "b"]);
        assert_eq!(page.next_cursor, None);
    }

    #[test]
    fn full_page_cursor() {
        let page = page_from_infos(infos(&["a", "b"]), 2).unwrap();
        assert_eq!(page.names, ["a", "b"]);
        let cursor = page.next_cursor.unwrap();
        assert_eq!(cursor.as_bytes(), name_cursor("b").encode_to_vec());
    }

    #[test]
    fn unlimited_page_last() {
        let page = page_from_infos(vec![info("a", None)], 0).unwrap();
        assert_eq!(page.names, ["a"]);
        assert_eq!(page.next_cursor, None);
        let empty = page_from_infos(Vec::new(), 2).unwrap();
        assert!(empty.names.is_empty() && empty.next_cursor.is_none());
    }

    #[test]
    fn missing_cursor_protocol() {
        let error = page_from_infos(vec![info("a", None)], 1).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        let keyless = proto::flight::SequenceCursor { key: None };
        let error = page_from_infos(vec![info("a", Some(keyless))], 1).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
    }

    #[test]
    fn missing_name_protocol() {
        let error = page_from_infos(vec![FlightInfo::new()], 0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
    }
}
