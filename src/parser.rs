use nom::{IResult,ErrorKind,be_u16,be_u32,be_i16,be_i32,alphanumeric};
use std::fmt;

// Anything that is a Kafka ApiKey request.
pub trait ApiRequestLike: fmt::Debug {
}

#[derive(Debug)]
pub struct KafkaRequest {
    pub header: KafkaRequestHeader,
    pub req: Box<ApiRequestLike>
}

#[derive(Debug)]
pub struct KafkaRequestHeader {
    pub opcode: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String
}

#[derive(Debug)]
pub struct VersionsRequest {}
impl ApiRequestLike for VersionsRequest {}

#[derive(Debug)]
pub struct PublishRequest {}
impl ApiRequestLike for PublishRequest {}

pub fn size_header(input: &[u8]) -> IResult<&[u8], &[u8]> {
    length_bytes!(input, be_u32)
}

fn request_header(input:&[u8]) -> IResult<&[u8], KafkaRequestHeader> {
  do_parse!(input,
    opcode: be_i16 >>
    version: be_i16 >>
    correlation_id: be_i32 >>
    client_id: length_bytes!(be_u16) >>  // TODO length -1 indicates null.
   (
     KafkaRequestHeader {
        opcode: opcode,
        version: version,
        correlation_id: correlation_id,
        client_id: String::from_utf8_lossy(client_id).to_string()
     }
   )
  )
}

fn versions(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    debug!("Parsing out ApiVersions request");
    // It has no content in the request
    IResult::Done(input, KafkaRequest{header: header, req: Box::new(VersionsRequest {})})
}

fn publish(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    IResult::Done(input, KafkaRequest{header: header, req: Box::new(PublishRequest {})})
}

pub fn kafka_request(input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    if let IResult::Done(tail, req) = request_header(input) {
        match req {
           KafkaRequestHeader {opcode:0, .. } => publish(req, tail),
           KafkaRequestHeader {opcode:18, .. } => versions(req, tail),
           _ => IResult::Error(error_code!(ErrorKind::Custom(1)))
        }
    } else {
        IResult::Error(error_code!(ErrorKind::Custom(0)))
    }
}
