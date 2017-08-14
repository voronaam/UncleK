use nom::{IResult,ErrorKind,be_u16,be_u32,be_u64,be_i16,be_i32};

// Anything that is a Kafka ApiKey request.
#[derive(Debug)]
pub enum ApiRequest {
    Publish {
        acks: u16,
        timeout: u32,
        topics: Vec<(Option<String>, Vec<KafkaMessage>)>
    },
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

#[derive(Debug)]
pub struct KafkaMessage {
    pub partition: u32,
    timestamp: u64,
    key: Vec<u8>,
    value: Vec<u8>
}

pub fn size_header(input: &[u8]) -> IResult<&[u8], &[u8]> {
    length_bytes!(input, be_u32)
}

fn kafka_string(input:&[u8]) -> String {
    String::from_utf8_lossy(input).to_string()
}
named!(opt_kafka_string<&[u8], Option<String> >,
  alt!(
    tag!([0xff, 0xff])     => { |_| None } |
    length_bytes!(be_u16)  => { |s| Some(kafka_string(s)) }
  )
);

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
    do_parse!(input,
      acks: be_u16 >>
      timeout: be_u32 >>
      topics: length_count!(be_u32, publish_topic) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::Publish {
            acks: acks,
            timeout: timeout,
            topics: topics
        }
      }
    )
   )
}
named!(publish_topic<&[u8], (Option<String>, Vec<KafkaMessage>)>, do_parse!(
    name: opt_kafka_string >>
    streams: length_count!(be_u32, do_parse!(
        partition:     be_u32 >>
        /*msg_bytes*/  be_u32 >>
        /*offset */    be_u64 >>
        /*msg bytes*/  be_u32 >>
        /*crc */       be_u32 >> // TODO: we'll need this eventually
        /*magic */     tag!([1]) >>
        /*attributes*/ tag!([0]) >> // TODO: we'll need to parse it
        timestamp:     be_u64 >>
        /*undoc1*/     opt_kafka_string >>
        /*undoc2*/     opt_kafka_string >>
        key:           length_bytes!(be_u16) >>
        value:         length_bytes!(be_u16) >>
        (
          KafkaMessage {
            partition: partition,
            timestamp: timestamp,
            key: key.iter().cloned().collect(),
            value: value.iter().cloned().collect()
          }
        ))) >>
    (
      (name, streams)
    )
));

pub fn kafka_request(input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    if let IResult::Done(tail, req) = request_header(input) {
        match req {
           KafkaRequestHeader {opcode: 0, version: 2, .. } => publish(req, tail),
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
