use bytes::{BytesMut, BufMut, BigEndian};
use hostname::get_hostname;
use parser::TopicWithPartitions; // TODO move to common place

// Anything that is a Kafka response body.
#[derive(Debug)]
pub enum ApiResponse {
    VersionsResponse,
    ErrorResponse,
    MetadataResponse {
        version: i16,
        cluster: ClusterMetadata
    },
    PublishResponse {
        version: i16,
        responses: Vec<(String, Vec<u32>)>
    },
    GroupCoordinatorResponse,
    JoinGroupResponse {
        protocol: Option<(String, Option<Vec<u8>>)>
    },
    SyncGroupResponse {
        assignment: Option<Vec<u8>>
    },
    FetchOffsetsResponse {
        topics: Vec<TopicWithPartitions>
    },
    OffsetsResponse {
        topics: Vec<TopicWithPartitions>
    },
    OffsetCommitResponse {
        topics: Vec<TopicWithPartitions>
    },
}

#[derive(Debug)]
pub struct KafkaResponse {
    pub header: KafkaResponseHeader,
    pub req: ApiResponse
}

#[derive(Debug)]
pub struct KafkaResponseHeader {
    pub length: u32,
    pub correlation_id: i32
}
impl KafkaResponseHeader {
    pub fn new(correlation_id: i32) -> KafkaResponseHeader {
        KafkaResponseHeader {
            length: 0, // not known in advance
            correlation_id: correlation_id
        }
    }
}

pub fn to_bytes(msg: &KafkaResponse, out: &mut BytesMut) {
    let mut buf = BytesMut::with_capacity(1024);
    match msg.req {
        ApiResponse::VersionsResponse => versions_to_bytes(&mut buf),
        ApiResponse::GroupCoordinatorResponse => coordinator_to_bytes(&mut buf),
        ApiResponse::JoinGroupResponse {ref protocol} => join_group_to_bytes(protocol, &mut buf),
        ApiResponse::MetadataResponse { version: 2, ref cluster } => metadata_to_bytes(cluster, &mut buf),
        ApiResponse::PublishResponse { version: 2, ref responses } => publish_to_bytes(responses, &mut buf),
        ApiResponse::SyncGroupResponse { ref assignment } => sync_group_to_bytes(assignment, &mut buf),
        ApiResponse::FetchOffsetsResponse { ref topics } => fetch_offsets_to_bytes(topics, &mut buf),
        ApiResponse::OffsetsResponse { ref topics } => offsets_to_bytes(topics, &mut buf),
        ApiResponse::OffsetCommitResponse { ref topics } => offset_commit_to_bytes(topics, &mut buf),
        _ => error_to_bytes(&mut buf)
    }
    out.put_u32::<BigEndian>(buf.len() as u32 + 4); // 4 is the length of the size correlation id.
    out.put_i32::<BigEndian>(msg.header.correlation_id);
    out.extend(buf.take());
}


fn versions_to_bytes(out: &mut BytesMut) {
    out.put_u16::<BigEndian>(0); // error_code
    out.put_u32::<BigEndian>(21); // number of api calls supported
    versions_supported_call(out, 0, 0, 2);
    versions_supported_call(out, 1, 0, 3);
    versions_supported_call(out, 2, 0, 1);
    versions_supported_call(out, 3, 0, 2);
    versions_supported_call(out, 4, 0, 0);
    versions_supported_call(out, 5, 0, 0);
    versions_supported_call(out, 6, 0, 3);
    versions_supported_call(out, 7, 1, 1);
    versions_supported_call(out, 8, 0, 2);
    versions_supported_call(out, 9, 0, 2);
    versions_supported_call(out, 10, 0, 0);
    versions_supported_call(out, 11, 0, 1);
    versions_supported_call(out, 12, 0, 0);
    versions_supported_call(out, 13, 0, 0);
    versions_supported_call(out, 14, 0, 0);
    versions_supported_call(out, 15, 0, 0);
    versions_supported_call(out, 16, 0, 0);
    versions_supported_call(out, 17, 0, 0);
    versions_supported_call(out, 18, 0, 0);
    versions_supported_call(out, 19, 0, 1);
    versions_supported_call(out, 20, 0, 0);
}
fn versions_supported_call(out: &mut BytesMut, opcode: u16, min: u16, max: u16) {
    out.put_u16::<BigEndian>(opcode);
    out.put_u16::<BigEndian>(min);
    out.put_u16::<BigEndian>(max);
}

fn error_to_bytes(out: &mut BytesMut) {
    // In Kafka error code is not always the first field in the response.
    // This will have to specialize based on the actual request in the future.
    out.put_u16::<BigEndian>(55); // OPERATION_NOT_ATTEMPTED
}

fn string_to_bytes(msg: &String, out: &mut BytesMut) {
    let b = msg.as_bytes();
    out.put_u16::<BigEndian>(b.len() as u16);
    out.put(b);
}

fn opt_string_to_bytes(msg: &Option<String>, out: &mut BytesMut) {
    match msg {
        &Some(ref s) => string_to_bytes(s, out),
        &None    => out.put_i16::<BigEndian>(-1)
    }
}

fn opt_vec_to_bytes(msg: &Option<Vec<u8>>, out: &mut BytesMut) {
    match msg {
        &None    => out.put_u32::<BigEndian>(0),
        &Some(ref a) => {
            out.put_u32::<BigEndian>(a.len() as u32);
            out.put(a);
        }
    }
}
fn metadata_to_bytes(msg: &ClusterMetadata, out: &mut BytesMut) {
    out.put_u32::<BigEndian>(msg.brokers.len() as u32);
    for b in &msg.brokers {
        out.put_u32::<BigEndian>(b.node_id);
        string_to_bytes(&b.host, out);
        out.put_u32::<BigEndian>(b.port);
        if let Some(ref r) = b.rack {
            string_to_bytes(r, out);
        } else {
            out.put_i16::<BigEndian>(-1);
        }
    }
    string_to_bytes(&msg.cluster_id, out);
    out.put_u32::<BigEndian>(msg.controller_id);
    out.put_u32::<BigEndian>(msg.topics.len() as u32);
    for t in &msg.topics {
        out.put_u16::<BigEndian>(t.error_code);
        string_to_bytes(&t.name, out);
        out.put_u8(t.is_internal);
        out.put_u32::<BigEndian>(t.partitions.len() as u32);
        for p in &t.partitions {
            out.put_u16::<BigEndian>(p.error_code);
            out.put_u32::<BigEndian>(p.id);
            out.put_u32::<BigEndian>(p.leader);
            out.put_u32::<BigEndian>(p.replicas.len() as u32);
            for r in &p.replicas {
                out.put_u32::<BigEndian>(*r);
            }
            out.put_u32::<BigEndian>(p.isr.len() as u32);
            for r in &p.isr {
                out.put_u32::<BigEndian>(*r);
            }
        }
    }
}

#[derive(Debug)]
pub struct ClusterMetadata {
    brokers: Vec<BrokerMetadata>,
    cluster_id: String,
    controller_id: u32,
    topics: Vec<TopicMetadata>
}

#[derive(Debug)]
pub struct BrokerMetadata {
    node_id: u32,
    host: String,
    port: u32,
    rack: Option<String>
}
#[derive(Debug)]
pub struct TopicMetadata {
    error_code: u16,
    name: String,
    is_internal: u8,
    partitions: Vec<PartitionMetadata>
}
#[derive(Debug)]
pub struct PartitionMetadata {
    error_code: u16,
    id: u32,
    leader: u32,
    replicas: Vec<u32>,
    isr: Vec<u32>
}
impl TopicMetadata {
    fn healthy(name: &String) -> TopicMetadata {
        TopicMetadata {
            error_code: 0,
            name: name.to_string(),
            is_internal: 0,
            partitions: vec![PartitionMetadata {
                error_code: 0,
                id: 0,
                leader: 0,
                replicas: vec![0],
                isr: vec![0]
            }]
        }
    }
}

fn publish_to_bytes(msg: &Vec<(String, Vec<u32>)>, out: &mut BytesMut) {
    out.put_u32::<BigEndian>(msg.len() as u32);
    for topic in msg {
        string_to_bytes(&topic.0, out);
        out.put_u32::<BigEndian>(topic.1.len() as u32);
        for partition in &topic.1 {
            out.put_u32::<BigEndian>(*partition);
            out.put_u16::<BigEndian>(0); // error code
            out.put_u64::<BigEndian>(0); // offset
            out.put_u64::<BigEndian>(0); // log append time
        }
    }
    out.put_u32::<BigEndian>(0); // throttle_time
}


impl ApiResponse {
    pub fn metadata_healthy(version: i16, topics: &Vec<String>) -> ApiResponse {
        ApiResponse::MetadataResponse {
            version: version,
            cluster: ClusterMetadata {
                brokers: vec![BrokerMetadata{
                    node_id: 0,
                    host: get_hostname().expect("Failed to get localhost's hostname"),
                    port: 9092, // TODO
                    rack: None
                }],
                cluster_id: "UncleK".to_string(),
                controller_id: 0,
                topics: topics.iter().map(TopicMetadata::healthy).collect()
            }
        }
    }
}

fn coordinator_to_bytes(out: &mut BytesMut) {
    out.put_u16::<BigEndian>(0); // error_code
    out.put_u32::<BigEndian>(0); // node_id
    string_to_bytes(&get_hostname().expect("Failed to get localhost's hostname"), out);
    out.put_u32::<BigEndian>(9092); // port
    
}

fn join_group_to_bytes(protocol: &Option<(String, Option<Vec<u8>>)>, out: &mut BytesMut) {
    out.put_u16::<BigEndian>(0); // error_code
    out.put_u32::<BigEndian>(0); // generation_id
    match protocol {
        &Some((ref s, _)) => string_to_bytes(s, out),
        &None             => out.put_i16::<BigEndian>(-1)
    }
    string_to_bytes(&String::from(""), out);   // leader_id
    string_to_bytes(&String::from(""), out);   // member_id
    // members
    match protocol {
        &Some((ref s, ref a)) => {
            out.put_u32::<BigEndian>(1);
            string_to_bytes(s, out);   // member_id
            opt_vec_to_bytes(a, out);  // metadata
        },
        &None => out.put_u32::<BigEndian>(0)
    }

}

fn sync_group_to_bytes(assignment: &Option<Vec<u8>>, out: &mut BytesMut) {
    out.put_u16::<BigEndian>(0); // error_code
    match assignment {
        &None => out.put_u32::<BigEndian>(0),
        &Some(ref a) => {
            out.put_u32::<BigEndian>(a.len() as u32);
            out.put(a);
        }
    }
}

fn fetch_offsets_to_bytes(topics: &Vec<TopicWithPartitions>, out: &mut BytesMut) {
    out.put_u32::<BigEndian>(topics.len() as u32);
    for topic in topics {
        string_to_bytes(&topic.name, out);
        out.put_u32::<BigEndian>(topic.partitions.len() as u32);
        for p in &topic.partitions {
            out.put_u32::<BigEndian>(*p); // partition
            out.put_i64::<BigEndian>(-1); // offset
            opt_string_to_bytes(&None, out);
            out.put_u16::<BigEndian>(0); // error_code
        }
    }
    out.put_u16::<BigEndian>(0); // error_code
}

fn offsets_to_bytes(topics: &Vec<TopicWithPartitions>, out: &mut BytesMut) {
    out.put_u32::<BigEndian>(topics.len() as u32);
    for topic in topics {
        string_to_bytes(&topic.name, out);
        out.put_u32::<BigEndian>(topic.partitions.len() as u32);
        for p in &topic.partitions {
            out.put_u32::<BigEndian>(*p); // partition
            out.put_u16::<BigEndian>(0); // error_code
            out.put_u64::<BigEndian>(0); // timestamp
            out.put_i64::<BigEndian>(0); // offset
        }
    }
}

fn offset_commit_to_bytes(topics: &Vec<TopicWithPartitions>, out: &mut BytesMut) {
    out.put_u32::<BigEndian>(topics.len() as u32);
    for topic in topics {
        string_to_bytes(&topic.name, out);
        out.put_u32::<BigEndian>(topic.partitions.len() as u32);
        for p in &topic.partitions {
            out.put_u32::<BigEndian>(*p); // partition
            out.put_u16::<BigEndian>(0); // error_code
        }
    }
}
