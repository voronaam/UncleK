extern crate bytes;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

extern crate r2d2;
extern crate r2d2_postgres;

#[macro_use]
extern crate nom;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

extern crate hostname;
extern crate crc;

use std::io;
use std::str;
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::pipeline::ServerProto;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_service::Service;
use futures::{future, Future, BoxFuture};
use futures_cpupool::CpuPool;
use tokio_proto::TcpServer;
use nom::IResult;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};

mod parser;
mod backend;
mod writer;

use parser::KafkaRequest;
use writer::KafkaResponse;

pub struct KafkaCodec;

impl Decoder for KafkaCodec {
    type Item = KafkaRequest;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<KafkaRequest>> {
        let imm_buf = buf.clone(); // make sure we do not use this buffer in a mutable way, except for the split_to call.
        if let IResult::Done(tail, body) = parser::size_header(&imm_buf[..]) {
            buf.split_to(imm_buf.len() - tail.len()); // A little bit funny way to determine how many bytes nom consumed
            debug!("Got a message of {} bytes", body.len());
            if let IResult::Done(_, req) = parser::kafka_request(&body) {
                debug!("Parsed a message {:?}", req);
                Ok(Some(req))
            } else {
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

pub struct KafkaService {
    thread_pool: CpuPool,
    db_pool: r2d2::Pool<r2d2_postgres::PostgresConnectionManager>, // Also to be moved into the backend
}

impl Service for KafkaService {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let db = self.db_pool.clone();
        let f = self.thread_pool.spawn_fn(move || {
            debug!("Sending a request to the backend {:?}", req);
            let response = backend::handle_request(req, db);
            debug!("Response from the backend {:?}", response);
            future::ok(response)
        });
        f.boxed()
        // future::ok(response).boxed()
    }
}

fn main() {
    pretty_env_logger::init().unwrap();
    let addr = "0.0.0.0:9092".parse().expect("Please check the configured address and port number");
    let server = TcpServer::new(KafkaProto, addr);

    let thread_pool = CpuPool::new(10);
    
    // DB config. Will need to move inside backend
    let db_url = "postgres://avorona:avorona@localhost";
    let db_config = r2d2::Config::default();
    let db_manager = PostgresConnectionManager::new(db_url, TlsMode::None).unwrap();
    let db_pool = r2d2::Pool::new(db_config, db_manager).unwrap();

    server.serve(move || Ok(KafkaService {
        thread_pool: thread_pool.clone(),
        db_pool: db_pool.clone()
    }));
}
