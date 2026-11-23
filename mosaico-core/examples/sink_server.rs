use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use mosaicod_proto::v1::requests::ResourceLocator;
use mosaicod_proto::v1::responses::{ResourceUuid, ServerConfig, ServerInfo, SessionCreate};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

const MAX_DECODE: u64 = 50 * 1024 * 1024;
const MAX_ENCODE: u64 = 4 * 1024 * 1024 * 1024 - 1;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Default)]
struct Sink {
    counter: AtomicU64,
}

impl Sink {
    fn uuid(&self) -> String {
        format!(
            "00000000-0000-4000-8000-{:012x}",
            self.counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn respond(&self, action: &Action) -> Result<Vec<u8>, Status> {
        Ok(match action.r#type.as_str() {
            "info" => ServerInfo {
                version: "0.0.0-sink".to_owned(),
                semver: None,
                config: Some(ServerConfig {
                    grpc_max_decode_message_size: MAX_DECODE,
                    grpc_max_encode_message_size: MAX_ENCODE,
                    grpc_target_encode_message_size: MAX_DECODE,
                }),
            }
            .encode_to_vec(),
            "session_create" => {
                let request = ResourceLocator::decode(action.body.as_ref())
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                let uuid = self.uuid();
                SessionCreate {
                    locator: format!("{}:{uuid}", request.locator),
                    uuid,
                }
                .encode_to_vec()
            }
            "topic_create" => ResourceUuid { uuid: self.uuid() }.encode_to_vec(),
            _ => Vec::new(),
        })
    }
}

#[tonic::async_trait]
impl FlightService for Sink {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        let started = Instant::now();
        let (mut messages, mut bytes) = (0u64, 0u64);
        while let Some(data) = stream.message().await? {
            messages += 1;
            bytes += (data.data_header.len() + data.data_body.len()) as u64;
        }
        let seconds = started.elapsed().as_secs_f64();
        println!(
            "do_put: {messages} messages, {:.0} MB in {seconds:.2}s, {:.0} MB/s",
            bytes as f64 / 1e6,
            bytes as f64 / 1e6 / seconds
        );
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let body = self.respond(request.get_ref())?;
        let result = arrow_flight::Result { body: body.into() };
        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(result)
        }))))
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).map_or(Ok(6727), |p| p.parse())?;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let service = FlightServiceServer::new(Sink::default())
        .max_decoding_message_size(MAX_DECODE as usize)
        .max_encoding_message_size(MAX_ENCODE as usize);
    println!("sink listening on {address}");
    tonic::transport::Server::builder()
        .add_service(service)
        .serve(address)
        .await?;
    Ok(())
}
