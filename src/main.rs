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

pub struct KafkaCodec;
pub struct KafkaRequest;
pub struct KafkaResponse;

named!(size_header<&[u8], &[u8]>, length_bytes!(nom::be_u32) );

impl Decoder for KafkaCodec {
    type Item = KafkaRequest;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<KafkaRequest>> {
		let imm_buf = buf.clone(); // make sure we do not use this buffer in a mutable way, except for the split_to call.
		if let IResult::Done(tail, body) = size_header(&imm_buf[..]) {
			buf.split_to(imm_buf.len() - tail.len()); // A little bit funny way to determine how many bytes nom consumed
			info!("Got a message of {} bytes", body.len());
			Ok(Some(KafkaRequest{}))
		} else {
			Ok(None)
		}
	}
}

impl Encoder for KafkaCodec {
    type Item = KafkaResponse;
    type Error = io::Error;

    fn encode(&mut self, msg: KafkaResponse, buf: &mut BytesMut) -> io::Result<()> {
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
		// println!("Got here 1");
        future::ok(KafkaResponse{}).boxed()
    }
}

fn main() {
	pretty_env_logger::init().unwrap();
	let addr = "0.0.0.0:9092".parse().expect("Please check the configured address and port number");
	let server = TcpServer::new(KafkaProto, addr);
	server.serve(|| Ok(KafkaService));
}

