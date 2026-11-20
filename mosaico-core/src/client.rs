use arrow_flight::Criteria;
use prost::Message;

use crate::comm::action::ActionKind;
use crate::comm::connection::{ConnectOptions, Connection, ServerInfo};
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::locator::{sequence_name, topic_name};
use crate::read::sequence::SequenceHandler;
use crate::read::topic::TopicHandler;
use crate::write::sequence::SequenceWriter;
use crate::write::session::SessionErrorPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub sequence: String,
    pub topic: Option<String>,
    pub kind: String,
    pub message: String,
    pub created_at: String,
}

impl Notification {
    fn from_proto(item: proto::NotificationItem) -> Self {
        let name = item.name.trim_matches('/');
        let (sequence, topic) = match name.split_once('/') {
            Some((sequence, topic)) => (sequence.to_owned(), Some(topic_name(topic))),
            None => (name.to_owned(), None),
        };
        Self {
            sequence,
            topic,
            kind: item.notification_type,
            message: item.msg,
            created_at: item.created_datetime,
        }
    }
}

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
        let request = proto::ListFlightsCmd {
            limit: 0,
            order_by: Some(proto::SEQUENCE_ORDER_CREATED_AT),
        };
        let criteria = Criteria {
            expression: request.encode_to_vec().into(),
        };
        let infos = self.connection.list_flights(criteria).await?;
        infos
            .into_iter()
            .map(|info| {
                info.flight_descriptor
                    .and_then(|descriptor| descriptor.path.into_iter().next())
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Protocol, "sequence listing without a name")
                    })
            })
            .collect()
    }

    pub async fn sequence_delete(&self, name: &str) -> Result<()> {
        self.connection
            .action_empty(ActionKind::SequenceDelete, &sequence_locator(name)?)
            .await
    }

    pub async fn session_delete(&self, locator: &str) -> Result<()> {
        let request = proto::ResourceLocator {
            locator: locator.to_owned(),
        };
        self.connection
            .action_empty(ActionKind::SessionDelete, &request)
            .await
    }

    pub async fn sequence_exists(&self, name: &str) -> Result<bool> {
        exists(self.sequence_handler(name).await)
    }

    pub async fn topic_exists(&self, sequence: &str, topic: &str) -> Result<bool> {
        exists(self.topic_handler(sequence, topic).await)
    }

    pub async fn list_sequence_notifications(&self, sequence: &str) -> Result<Vec<Notification>> {
        let locator = sequence_locator(sequence)?;
        self.notifications(ActionKind::SequenceNotificationList, locator)
            .await
    }

    pub async fn clear_sequence_notifications(&self, sequence: &str) -> Result<()> {
        self.connection
            .action_empty(
                ActionKind::SequenceNotificationPurge,
                &sequence_locator(sequence)?,
            )
            .await
    }

    pub async fn list_topic_notifications(
        &self,
        sequence: &str,
        topic: &str,
    ) -> Result<Vec<Notification>> {
        let locator = topic_locator(sequence, topic)?;
        self.notifications(ActionKind::TopicNotificationList, locator)
            .await
    }

    pub async fn clear_topic_notifications(&self, sequence: &str, topic: &str) -> Result<()> {
        self.connection
            .action_empty(
                ActionKind::TopicNotificationPurge,
                &topic_locator(sequence, topic)?,
            )
            .await
    }

    async fn notifications(
        &self,
        action: ActionKind,
        locator: proto::ResourceLocator,
    ) -> Result<Vec<Notification>> {
        let list: proto::NotificationList = self.connection.action(action, &locator).await?;
        Ok(list
            .notifications
            .into_iter()
            .map(Notification::from_proto)
            .collect())
    }
}

fn sequence_locator(name: &str) -> Result<proto::ResourceLocator> {
    Ok(proto::ResourceLocator {
        locator: sequence_name(name)?.to_owned(),
    })
}

fn topic_locator(sequence: &str, topic: &str) -> Result<proto::ResourceLocator> {
    Ok(proto::ResourceLocator {
        locator: format!("{}{}", sequence_name(sequence)?, topic_name(topic)),
    })
}

fn exists<T>(handler: Result<T>) -> Result<bool> {
    match handler {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::InvalidArgument
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
