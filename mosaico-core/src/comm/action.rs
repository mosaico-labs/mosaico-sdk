use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionKind {
    Info,
    SequenceCreate,
    SequenceDelete,
    SequenceNotificationCreate,
    SequenceNotificationList,
    SequenceNotificationPurge,
    SessionCreate,
    SessionDelete,
    SessionFinalize,
    TopicCreate,
    TopicDelete,
    TopicNotificationCreate,
    TopicNotificationList,
    TopicNotificationPurge,
}

impl ActionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::SequenceCreate => "sequence_create",
            Self::SequenceDelete => "sequence_delete",
            Self::SequenceNotificationCreate => "sequence_notification_create",
            Self::SequenceNotificationList => "sequence_notification_list",
            Self::SequenceNotificationPurge => "sequence_notification_purge",
            Self::SessionCreate => "session_create",
            Self::SessionDelete => "session_delete",
            Self::SessionFinalize => "session_finalize",
            Self::TopicCreate => "topic_create",
            Self::TopicDelete => "topic_delete",
            Self::TopicNotificationCreate => "topic_notification_create",
            Self::TopicNotificationList => "topic_notification_list",
            Self::TopicNotificationPurge => "topic_notification_purge",
        }
    }
}

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
