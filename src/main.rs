// Basic imports from the tokio framework
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_io;
extern crate tokio_core;
extern crate tokio_service;
extern crate tokio_timer;

// DB pool
extern crate r2d2;
extern crate r2d2_postgres;
#[macro_use(stmt)]
extern crate cassandra;
    
// Parser for the requests
#[macro_use]
extern crate nom;
// And the seriaizer
extern crate bytes;

// The logging library
#[macro_use]
extern crate log;
extern crate log4rs;

// Needed to produce auxilary fields in Kafka responses
extern crate hostname;
extern crate crc;

// Needed to parse the config file
extern crate config;
#[macro_use]
extern crate serde_derive;

use std::io;
use std::str;
use std::time::Duration;
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};
use tokio_io::AsyncRead;
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_service::{Service, NewService};
use futures::{future, Future, Stream, Sink, BoxFuture};
use tokio_timer::Timer;
use futures_cpupool::CpuPool;
use nom::IResult;

mod settings;
mod parser;
mod backend;
mod writer;

use settings::Settings;
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
            if let IResult::Done(_, req) = parser::kafka_request(body) {
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

#[derive(Clone)]
pub struct KafkaService {
    thread_pool: CpuPool,
    db_pool: backend::PgState,
    timer: tokio_timer::Timer,
}

impl Service for KafkaService {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let db = self.db_pool.clone();
        let timer = self.timer.clone();
        let f = self.thread_pool.spawn_fn(move || {
            debug!("Sending a request to the backend {:?}", req);
            let response = backend::handle_request(req, &db);
            debug!("Response from the backend {:?}", response);
            let delay = if response.is_empty() {1000} else {0};
            timer.sleep(Duration::from_millis(delay))
                 .then(|_| future::ok(response))
        });
        f.boxed()
    }
}

fn serve<S>(cnf: &Settings, svc: &KafkaService, factory: S) -> io::Result<()>
    where S: NewService<Request = KafkaRequest,
                        Response = KafkaResponse,
                        Error = io::Error> + 'static
{
    let addr = cnf.listen().parse().expect("Please check the configured address and port number");
	let mut core = Core::new().unwrap();
	let handle = core.handle();
	let listener = TcpListener::bind(&addr, &core.handle()).unwrap();
    let connections = listener.incoming();
    let server = connections.for_each(move |(socket, _peer_addr)| {
        let (writer, reader) = socket.framed(KafkaCodec).split();        
        let service = factory.new_service()?;
        let responses = reader.and_then(move |req| service.call(req));
        let server = writer.send_all(responses)
            .then(|_| Ok(()));
        handle.spawn(server);
        Ok(())
    });
    
    if let Some(freq) = cnf.cleanup {
		// TODO still thinking if we need a separate event loop for this one
		let timer = Timer::default();
		let t = timer.interval(Duration::from_millis(freq))
			.for_each(|_| {
				let db_pool = svc.db_pool.clone();
				svc.thread_pool.spawn_fn(move || {
					backend::cleanup(&db_pool);
					Ok(())
				})
			})
			.map_err(|_| io::Error::new(io::ErrorKind::Other, "Timer error"));
		let combined = server.select(t)
			.map(|(win, _)| win)
			.map_err(|(e, _)| e);
		info!("Started the main event loop with the listener and cleanup");
		core.run(combined)
	} else {
		info!("Started the main event loop with the listener");
		core.run(server)
	}
}

fn main() {
	log4rs::init_file("config/log4rs.yaml", Default::default()).expect("Failed to initialize logging");
    let cnf = Settings::new().expect("Failed to parse the configuration file");
    debug!("Using configuraion {:?}", cnf);

	let kafka_service = KafkaService {
        thread_pool: CpuPool::new(cnf.threads.unwrap_or(100)),
        db_pool: backend::initialize(&cnf),
        timer: Timer::default(),
    };
	
    if let Err(e) = serve(&cnf, &kafka_service.clone(), move || Ok(kafka_service.clone())) {
        error!("UncleK failed with {}", e);
    };
}
