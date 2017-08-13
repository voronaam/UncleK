use nom::{IResult,ErrorKind,be_u16,be_u32,be_i16,be_i32};

// Anything that is a Kafka ApiKey request.
#[derive(Debug)]
pub enum ApiRequest {
    Publish,
    Versions,
    Metadata {
        topics: Vec<String>
    }
}

#[derive(Debug)]
pub struct KafkaRequest {
    pub header: KafkaRequestHeader,
    pub req: ApiRequest
}

#[derive(Debug)]
pub struct KafkaRequestHeader {
    pub opcode: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String
}

pub fn size_header(input: &[u8]) -> IResult<&[u8], &[u8]> {
    length_bytes!(input, be_u32)
}

fn kafka_string(input:&[u8]) -> String {
    String::from_utf8_lossy(input).to_string()
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
        client_id: kafka_string(client_id)
     }
   )
  )
}

fn versions(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    IResult::Done(input, KafkaRequest{header: header, req: ApiRequest::Versions})
}

fn metadata(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    do_parse!(input,
      topics: length_count!(be_u32, map!(length_bytes!(be_u16), kafka_string)) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::Metadata {
          topics: topics
        }
      }
    )
   )
}

fn publish(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    IResult::Done(input, KafkaRequest{header: header, req: ApiRequest::Publish})
}

pub fn kafka_request(input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    if let IResult::Done(tail, req) = request_header(input) {
        match req {
           KafkaRequestHeader {opcode: 0, .. } => publish(req, tail),
           KafkaRequestHeader {opcode: 3, .. } => metadata(req, tail),
           KafkaRequestHeader {opcode:18, .. } => versions(req, tail),
           _ => {
               warn!("Not yet implemented request {:?}", req);
               IResult::Error(error_code!(ErrorKind::Custom(1)))
           }
        }
    } else {
        warn!("Could not parse even the header");
        IResult::Error(error_code!(ErrorKind::Custom(0)))
    }
}
