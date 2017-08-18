use nom::{IResult,ErrorKind,be_u16,be_u32,be_u64,be_i16,be_i32};

// Anything that is a Kafka ApiKey request.
#[derive(Debug)]
pub enum ApiRequest {
    Publish {
        acks: u16,
        timeout: u32,
        topics: Vec<KafkaMessageSet>
    },
    Versions,
    Metadata {
        topics: Vec<String>
    },
    FindGroupCoordinator,
    JoinGroup {
        group_id: String,
        member_id: String,
        protocol_type: String,
        protocols: Vec<(String, Option<Vec<u8>>)>
    },
    SyncGroup {
        group_id: String,
        member_id: String,
        assignments: Vec<Option<Vec<u8>>>
    },
    FetchOffsets {
        group_id: String,
        topics: Vec<TopicWithPartitions>
    },
    Offsets {
        topics: Vec<TopicWithPartitions>
    },
    OffsetCommit {
        topics: Vec<TopicWithPartitions>
    },
    Heartbeat,
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
pub struct KafkaMessageSet {
    pub topic: String,
    pub messages: Vec<KafkaMessage>
}

#[derive(Debug)]
pub struct KafkaMessage {
    pub partition: u32,
    timestamp: u64,
    pub key: Option<Vec<u8> >,
    pub value: Option<Vec<u8> >
}

#[derive(Debug, Clone)]
pub struct TopicWithPartitions {
    pub name: String,
    pub partitions: Vec<u32>
}
impl TopicWithPartitions {
    pub fn new(name: String, partitions: Vec<u32>) -> TopicWithPartitions {
        TopicWithPartitions {
            name: name,
            partitions: partitions
        }
    }
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

named!(opt_kafka_bytes<&[u8], Option<Vec<u8> > >,
  alt!(
    tag!([0xff, 0xff, 0xff, 0xff]) => { |_| None } |
    length_bytes!(be_u32)          => { |b:&[u8]| Some(b.iter().cloned().collect()) }
  )
);


fn request_header(input:&[u8]) -> IResult<&[u8], KafkaRequestHeader> {
  do_parse!(input,
    opcode: be_i16 >>
    version: be_i16 >>
    correlation_id: be_i32 >>
    client_id: opt_kafka_string >>
   (
     KafkaRequestHeader {
        opcode: opcode,
        version: version,
        correlation_id: correlation_id,
        client_id: client_id.unwrap()
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
named!(publish_topic<&[u8], KafkaMessageSet>, do_parse!(
    name:    map!(length_bytes!(be_u16), kafka_string) >>
    streams: length_count!(be_u32, do_parse!(
        partition:     be_u32 >>
        /*msg_bytes*/  be_u32 >>
        /*offset */    be_u64 >>
        /*msg bytes*/  be_u32 >>
        /*crc */       be_u32 >> // TODO: we'll need this eventually
        /*magic */     tag!([1]) >>
        /*attributes*/ tag!([0]) >> // TODO: we'll need to parse it
        timestamp:     be_u64 >>
        key:           opt_kafka_bytes >>
        value:         opt_kafka_bytes >>
        (
          KafkaMessage {
            partition: partition,
            timestamp: timestamp,
            key: key,
            value: value
          }
        ))) >>
    (
      KafkaMessageSet {
          topic: name,
          messages: streams
      }
    )
));

fn join_group(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    do_parse!(input,
      group_id:             map!(length_bytes!(be_u16), kafka_string) >>
      /*session_timeout*/   be_u32 >>
      /*rebalance_timeout*/ be_u32 >>
      member_id:            map!(length_bytes!(be_u16), kafka_string) >>
      protocol_type:        map!(length_bytes!(be_u16), kafka_string) >>
      protocols:            length_count!(be_u32, do_parse!(
          name:               map!(length_bytes!(be_u16), kafka_string) >>
          metadata:           opt_kafka_bytes >>
                              ((name, metadata))
                            )) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::JoinGroup {
            group_id: group_id,
            member_id: member_id,
            protocol_type: protocol_type,
            protocols: protocols
        }
      }
    )
   )
}

fn sync_group(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    do_parse!(input,
      group_id:             map!(length_bytes!(be_u16), kafka_string) >>
      /*generation_id*/     be_u32 >>
      member_id:            map!(length_bytes!(be_u16), kafka_string) >>
      assignments:          length_count!(be_u32, do_parse!(
                              map!(length_bytes!(be_u16), kafka_string) >>
          assignment:         opt_kafka_bytes >>
                              (assignment)
                            )) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::SyncGroup {
            group_id: group_id,
            member_id: member_id,
            assignments: assignments
        }
      }
    )
   )
}

fn fetch_offset(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    do_parse!(input,
      group_id:             map!(length_bytes!(be_u16), kafka_string) >>
      topics:               length_count!(be_u32, do_parse!(
          topic:              map!(length_bytes!(be_u16), kafka_string) >>
          partitions:         length_count!(be_u32, be_u32) >>
                              (TopicWithPartitions::new(topic, partitions))
                            )) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::FetchOffsets {
            group_id: group_id,
            topics: topics
        }
      }
    )
   )
}

fn offsets(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    do_parse!(input,
      /* replica_id */      be_u32 >>
      topics:               length_count!(be_u32, do_parse!(
        topic:                map!(length_bytes!(be_u16), kafka_string) >>
        partitions:           length_count!(be_u32, do_parse!(
          partition:            be_u32 >>
          /*timestamp*/         be_u64 >>
                                (partition)
                              )) >>
                              (TopicWithPartitions::new(topic, partitions))
                            )) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::Offsets {
            topics: topics
        }
      }
    )
   )
}

fn offset_commit(header:KafkaRequestHeader, input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    do_parse!(input,
      /*group_id*/          length_bytes!(be_u16) >>
      /*generation*/        be_u32 >>
      /*member*/            length_bytes!(be_u16) >>
      /*retention*/         be_u64 >>
      topics:               length_count!(be_u32, do_parse!(
        topic:                map!(length_bytes!(be_u16), kafka_string) >>
        partitions:           length_count!(be_u32, do_parse!(
          partition:            be_u32 >>
          /*offset*/            be_u64 >>
          /*meta*/              opt_kafka_string >>
                                (partition)
                              )) >>
                              (TopicWithPartitions::new(topic, partitions))
                            )) >>
    (
      KafkaRequest {
        header: header,
        req: ApiRequest::OffsetCommit {
            topics: topics
        }
      }
    )
   )
}


pub fn kafka_request(input:&[u8]) -> IResult<&[u8], KafkaRequest> {
    if let IResult::Done(tail, req) = request_header(input) {
        match req {
           KafkaRequestHeader {opcode: 0, version: 2, .. } => publish(req, tail),
           KafkaRequestHeader {opcode: 2, version: 1, .. } => offsets(req, tail),
           KafkaRequestHeader {opcode: 3, .. } => metadata(req, tail),
           KafkaRequestHeader {opcode: 8, .. } => offset_commit(req, tail),
           KafkaRequestHeader {opcode: 9, .. } => fetch_offset(req, tail),
           KafkaRequestHeader {opcode:10, .. } => IResult::Done(input, KafkaRequest{header: req, req: ApiRequest::FindGroupCoordinator}),
           KafkaRequestHeader {opcode:11, .. } => join_group(req, tail),
           KafkaRequestHeader {opcode:12, .. } => IResult::Done(input, KafkaRequest{header: req, req: ApiRequest::Heartbeat}),
           KafkaRequestHeader {opcode:14, .. } => sync_group(req, tail),
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
