extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

#[macro_use]
extern crate nom;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

use std::io;
use std::str;
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::pipeline::ServerProto;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_service::Service;
use futures::{future, Future, BoxFuture};
use tokio_proto::TcpServer;
use nom::IResult;

mod parser;
mod backend;
mod writer;

pub struct KafkaCodec;
use parser::KafkaRequest;
use backend::KafkaResponse;

impl Decoder for KafkaCodec {
    type Item = KafkaRequest;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<KafkaRequest>> {
        let imm_buf = buf.clone(); // make sure we do not use this buffer in a mutable way, except for the split_to call.
        if let IResult::Done(tail, body) = parser::size_header(&imm_buf[..]) {
            buf.split_to(imm_buf.len() - tail.len()); // A little bit funny way to determine how many bytes nom consumed
            info!("Got a message of {} bytes", body.len());
            if let IResult::Done(_, req) = parser::kafka_request(&body) {
                debug!("Parsed a message {:?} {:?}", req.header, req.req);
                Ok(Some(req))
            } else {
                info!("Did not deserialize");
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder for KafkaCodec {
    type Item = KafkaResponse;
    type Error = io::Error;

    fn encode(&mut self, msg: KafkaResponse, buf: &mut BytesMut) -> io::Result<()> {
        writer::to_bytes(&msg, buf);
        Ok(())
    }
}

pub struct KafkaProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for KafkaProto {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
    type Transport = Framed<T, KafkaCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(KafkaCodec))
    }
}

pub struct KafkaService;

impl Service for KafkaService {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        info!("Sending a request to the backend {:?}", req);
        let response = backend::handle_request(req);
        info!("Response from the backend {:?}", response);
        future::ok(response).boxed()
    }
}

fn main() {
    pretty_env_logger::init().unwrap();
    let addr = "0.0.0.0:9092".parse().expect("Please check the configured address and port number");
    let server = TcpServer::new(KafkaProto, addr);
    server.serve(|| Ok(KafkaService));
}

