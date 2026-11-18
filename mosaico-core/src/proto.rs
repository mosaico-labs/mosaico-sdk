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
