use parser;
use std::fmt;
use std::marker;

use parser::KafkaRequest;

// Anything that is a Kafka response body.
pub trait ApiResponseLike: fmt::Debug + marker::Send {
}

#[derive(Debug)]
pub struct KafkaResponse {
    pub header: KafkaResponseHeader,
    pub req: Box<ApiResponseLike>
}

#[derive(Debug)]
pub struct KafkaResponseHeader {
	pub length: u32,
    pub correlation_id: i32
}
impl KafkaResponseHeader {
	fn new(correlation_id: i32) -> KafkaResponseHeader {
		KafkaResponseHeader {
			length: 0,
			correlation_id: correlation_id
		}
	}
}

#[derive(Debug)]
pub struct VersionsResponse {}
impl ApiResponseLike for VersionsResponse {}

#[derive(Debug)]
pub struct ErrorResponse {}
impl ApiResponseLike for ErrorResponse {}

pub fn handle_request(req: KafkaRequest) -> KafkaResponse {
	match (req.header.opcode, req.header.version) {
		(18, _) => handle_versions(&req),
		_ => handle_unknown(&req)
	}
}

fn handle_versions(req: &KafkaRequest) -> KafkaResponse {
	KafkaResponse {
		header: KafkaResponseHeader::new(req.header.correlation_id),
		req: Box::new(VersionsResponse{})
	}
}

fn handle_unknown(req: &KafkaRequest) -> KafkaResponse {
	warn!("Unknown request");
	KafkaResponse {
		header: KafkaResponseHeader::new(req.header.correlation_id),
		req: Box::new(ErrorResponse{})
	}
}
