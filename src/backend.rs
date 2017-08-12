use std::fmt;
use std::marker;

use bytes::{BytesMut, BufMut, BigEndian};

use parser::KafkaRequest;

// Anything that is a Kafka response body.
pub trait ApiResponseLike: fmt::Debug + marker::Send {
	fn to_bytes(self: &Self, out: &mut BytesMut);
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
impl ApiResponseLike for VersionsResponse {
	fn to_bytes(self: &Self, out: &mut BytesMut) {
		out.put_u16::<BigEndian>(0); // error_code
		out.put_u32::<BigEndian>(4); // number of api calls supported
		versions_supported_call(out, 0, 0, 2);
		versions_supported_call(out, 1, 0, 3);
		versions_supported_call(out, 2, 0, 1);
		versions_supported_call(out, 3, 0, 2);
	}
}
fn versions_supported_call(out: &mut BytesMut, opcode: u16, min: u16, max: u16) {
	out.put_u16::<BigEndian>(opcode);
	out.put_u16::<BigEndian>(min);
	out.put_u16::<BigEndian>(max);
}

#[derive(Debug)]
pub struct ErrorResponse {}
impl ApiResponseLike for ErrorResponse {
	fn to_bytes(self: &Self, out: &mut BytesMut) {}
}

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
