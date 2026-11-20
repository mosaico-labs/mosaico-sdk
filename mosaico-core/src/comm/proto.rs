#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TimestampRange {
    #[prost(int64, optional, tag = "1")]
    pub start_ns: Option<i64>,
    #[prost(int64, optional, tag = "2")]
    pub end_ns: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct GetFlightInfoCmd {
    #[prost(string, tag = "1")]
    pub locator: String,
    #[prost(message, optional, tag = "2")]
    pub timestamp_range: Option<TimestampRange>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TopicAppMetadataDataInfo {
    #[prost(message, optional, tag = "1")]
    pub interval: Option<TimestampRange>,
    #[prost(uint64, tag = "4")]
    pub total_chunks_count: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TopicAppMetadata {
    #[prost(string, tag = "4")]
    pub locator: String,
    #[prost(string, tag = "5")]
    pub ontology_tag: String,
    #[prost(message, optional, tag = "8")]
    pub data_info: Option<TopicAppMetadataDataInfo>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ServerConfig {
    #[prost(uint64, tag = "1")]
    pub grpc_max_decode_message_size: u64,
    #[prost(uint64, tag = "2")]
    pub grpc_max_encode_message_size: u64,
    #[prost(uint64, tag = "3")]
    pub grpc_target_encode_message_size: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ServerInfo {
    #[prost(string, tag = "1")]
    pub version: String,
    #[prost(message, optional, tag = "3")]
    pub config: Option<ServerConfig>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SessionAppMetadata {
    #[prost(string, tag = "1")]
    pub locator: String,
    #[prost(int64, tag = "2")]
    pub created_at_ns: i64,
    #[prost(int64, optional, tag = "3")]
    pub completed_at_ns: Option<i64>,
    #[prost(string, repeated, tag = "4")]
    pub topics: Vec<String>,
    #[prost(bool, tag = "5")]
    pub locked: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SequenceAppMetadata {
    #[prost(int64, tag = "1")]
    pub created_at_ns: i64,
    #[prost(string, tag = "2")]
    pub locator: String,
    #[prost(message, repeated, tag = "3")]
    pub sessions: Vec<SessionAppMetadata>,
    #[prost(bytes = "vec", tag = "4")]
    pub user_metadata: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct DoPutCmd {
    #[prost(string, tag = "1")]
    pub locator: String,
    #[prost(string, tag = "2")]
    pub topic_uuid: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ResourceLocator {
    #[prost(string, tag = "1")]
    pub locator: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct NotificationItem {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub notification_type: String,
    #[prost(string, tag = "3")]
    pub msg: String,
    #[prost(string, tag = "4")]
    pub created_datetime: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct NotificationList {
    #[prost(message, repeated, tag = "1")]
    pub notifications: Vec<NotificationItem>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SessionUuid {
    #[prost(string, tag = "1")]
    pub session_uuid: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SequenceCreate {
    #[prost(string, tag = "1")]
    pub locator: String,
    #[prost(bytes = "vec", tag = "2")]
    pub user_metadata: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TopicCreate {
    #[prost(string, tag = "1")]
    pub locator: String,
    #[prost(string, tag = "2")]
    pub session_uuid: String,
    #[prost(int32, tag = "3")]
    pub serialization_format: i32,
    #[prost(string, tag = "4")]
    pub ontology_tag: String,
    #[prost(bytes = "vec", tag = "5")]
    pub user_metadata: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct NotificationCreate {
    #[prost(string, tag = "1")]
    pub locator: String,
    #[prost(string, tag = "2")]
    pub notification_type: String,
    #[prost(string, tag = "3")]
    pub msg: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ResourceUuid {
    #[prost(string, tag = "1")]
    pub uuid: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SessionCreate {
    #[prost(string, tag = "1")]
    pub uuid: String,
    #[prost(string, tag = "2")]
    pub locator: String,
}

pub(crate) const SEQUENCE_ORDER_CREATED_AT: i32 = 1;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ListFlightsCmd {
    #[prost(uint32, tag = "1")]
    pub limit: u32,
    #[prost(int32, optional, tag = "2")]
    pub order_by: Option<i32>,
}
