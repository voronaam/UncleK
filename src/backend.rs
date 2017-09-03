use parser::*;
use writer::*;
use r2d2_postgres;
use r2d2;
use r2d2::Pool;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use cdrs::connection_manager::ConnectionManager;
use cdrs::client::CDRS;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use cdrs::query::{QueryBuilder, QueryParamsBuilder};
use cdrs::types::value::{Value, Bytes};
use std::collections::HashMap;
use settings::Settings;
use settings::Topic;

#[derive(Clone)]
pub struct PgState {
    pub pool: Pool<r2d2_postgres::PostgresConnectionManager>,
    pub cassandra_pool: Pool<ConnectionManager<NoneAuthenticator, TransportTcp>>,
    pub topics: HashMap<String, Topic>,
    pub hostname: String,
}

pub fn initialize(cnf: &Settings) -> PgState {
    let db_url = cnf.database.url.to_string();
    let db_config = r2d2::Config::default();
    let db_manager = PostgresConnectionManager::new(db_url, TlsMode::None).unwrap();
    let db_pool = r2d2::Pool::new(db_config, db_manager).unwrap();
    create_tables(&cnf.topics, &db_pool);
    let mut map = HashMap::new();
    for topic in &cnf.topics {
        map.insert(topic.name.to_string(), topic.clone());
    }
    let config = r2d2::Config::builder()
        .pool_size(15)
        .build();
    let addr = "127.0.0.1:9042";
    let transport = TransportTcp::new(addr).unwrap();
    let authenticator = NoneAuthenticator;
    let manager = ConnectionManager::new(transport, authenticator, Compression::None);
    let cassandra_pool = r2d2::Pool::new(config, manager).unwrap();
    PgState {
        pool: db_pool,
        cassandra_pool: cassandra_pool,
        topics: map,
        hostname: cnf.get_hostname()
        
    }
}

fn create_tables(topics: &Vec<Topic>, db: &Pool<r2d2_postgres::PostgresConnectionManager>) {
    let conn = db.get().expect("Could not get a DB connection");
    for topic in topics {
        let uniq = if topic.compacted.unwrap_or(false) {"UNIQUE"} else {""};
        // TODO if topic has a defined retention we may need an index on "ts"
        conn.execute(format!(r#"
            CREATE TABLE IF NOT EXISTS "{}" (
                id bigserial PRIMARY KEY,
                partition int NOT NULL,
                ts timestamp NOT NULL,
                key BYTEA {},
                value BYTEA)
            "#,
            topic.name, uniq).as_str(), &[]).expect("Failed to create DB table");
    }
}

pub fn handle_request(req: KafkaRequest, db: &PgState) -> KafkaResponse {
    match req.req {
        ApiRequest::Metadata { topics } => handle_metadata(&req.header, &topics, db),
        ApiRequest::Publish { topics, .. } => handle_publish(&req.header, &topics, db),
        ApiRequest::Fetch { topics } => handle_fetch(&req.header, &topics, db),
        ApiRequest::Versions => handle_versions(&req),
        ApiRequest::FindGroupCoordinator => handle_find_coordinator(&req, db),
        ApiRequest::JoinGroup { protocols, .. } => handle_join_group(&req.header, &protocols),
        ApiRequest::SyncGroup { assignments, .. } => handle_sync_group(&req.header, &assignments),
        ApiRequest::FetchOffsets { topics, .. } => handle_fetch_offsets(&req.header, &topics),
        ApiRequest::Offsets { topics } => handle_offsets(&req.header, &topics, db),
        ApiRequest::OffsetCommit { topics } => handle_offset_commit(&req.header, &topics),
        ApiRequest::Heartbeat => handle_heartbeat(&req),
        ApiRequest::LeaveGroup => handle_leave_group(&req),
        _ => handle_unknown(&req)
    }
}

fn handle_versions(req: &KafkaRequest) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::VersionsResponse
    }
}

fn handle_unknown(req: &KafkaRequest) -> KafkaResponse {
    warn!("Unknown request {:?}", req);
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::ErrorResponse
    }
}

fn handle_metadata(header: &KafkaRequestHeader, topics: &Vec<String>, db: &PgState) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::metadata_healthy(header.version, topics, &db.hostname)
    }
}

const CREATE_KEY_SPACE: &'static str = "CREATE KEYSPACE IF NOT EXISTS uncle_ks WITH REPLICATION = { \
                                        'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

const CREATE_TABLE: &'static str = "CREATE TABLE IF NOT EXISTS uncle_ks.test (partition int PRIMARY KEY, key blob, value blob);";

const INSERT_STR: &'static str = "INSERT INTO uncle_ks.test (partition, key, value) \
                                  VALUES (?, ?, ?);";

// Void performance: 5000000 records sent, 76090.761060 records/sec (72.57 MB/sec), 400.84 ms avg latency, 666.00 ms max latency, 394 ms 50th, 435 ms 95th, 548 ms 99th, 628 ms 99.9th.
fn handle_publish(header: &KafkaRequestHeader, topics: &Vec<KafkaMessageSet>, db: &PgState) -> KafkaResponse {
    let mut conn = db.cassandra_pool.get().expect("Could not get a Cassandra connection");
    /*
    let ceate_q = QueryBuilder::new(CREATE_KEY_SPACE).finalize();
    conn.query(ceate_q, false, false).unwrap();
    let ceate_table = QueryBuilder::new(CREATE_TABLE).finalize();
    conn.query(ceate_table, false, false).unwrap();
    */
    let mut responses: Vec<(String, Vec<u32>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<u32> = Vec::new();
        for partition in &topic.messages {
            let &(ref p_num, ref values) = partition;
            for msg in values {
                debug!("Actually saving message {:?}:{:?} to topic {:?} partition {:?}", msg.key, msg.value, topic.topic, p_num);
                let k = match msg.key {
                    None => vec![],
                    Some(ref a) => a.clone()
                };
                let v = match msg.value {
                    None => vec![],
                    Some(ref a) => a.clone()
                };
                let values_s: Vec<Value> = vec![(*p_num as i32).into(),
                                                Bytes::new(k).into(),
                                                Bytes::new(v).into()];
                let query = QueryBuilder::new(INSERT_STR).values(values_s).finalize();
                conn.query(query, false, false).unwrap();
            }
            partition_responses.push(*p_num);
        }
        responses.push((topic.topic.to_string(), partition_responses));
    }
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::PublishResponse {
            version: header.version,
            responses: responses
        }
    }
}

fn handle_fetch(header: &KafkaRequestHeader, topics: &Vec<(String, Vec<(u32, u64)>)>, db: &PgState) -> KafkaResponse {
    let conn = db.pool.get().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<(u64, Option<Vec<u8>>, Vec<u8>)>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<(u64, Option<Vec<u8>>, Vec<u8>)> = Vec::new();
        let id = topic.1.get(0).expect("Need at least one partion in request").1 as i64;
        // TODO smart limit calculation
        let rs = conn.query(format!("SELECT id, partition, key, value FROM \"{}\" WHERE id >= $1 LIMIT 25", topic.0).as_str(), &[&id]).expect("DB query failed");
        for row in &rs {
            let offset: i64 = row.get(0);
            let key: Option<Vec<u8>> = row.get(2);
            let value: Vec<u8> = row.get(3);
            partition_responses.push((offset as u64, key, value));
        }
        responses.push((topic.0.to_string(), partition_responses));
    }
    debug!("About to send a fetch response with content {:?}", responses);
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::FetchResponse {
            version: header.version,
            responses: responses
        }
    }
}

fn handle_find_coordinator(req: &KafkaRequest, db: &PgState) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::GroupCoordinatorResponse {
            hostname: db.hostname.to_string()
        }
    }
}

fn handle_join_group(header: &KafkaRequestHeader, protocols: &Vec<(String, Option<Vec<u8>>)>) -> KafkaResponse {
    let selected = protocols.get(0).cloned();
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::JoinGroupResponse {
            protocol: selected
        }
    }
}

fn handle_sync_group(header: &KafkaRequestHeader, assignments: &Vec<Option<Vec<u8>>>) -> KafkaResponse {
    let selected = assignments.get(0).cloned().unwrap_or(None);
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::SyncGroupResponse {
            assignment: selected
        }
    }
}

fn handle_fetch_offsets(header: &KafkaRequestHeader, topics: &Vec<TopicWithPartitions>) -> KafkaResponse {
    let mut responses: Vec<(String, Vec<(u32, i64)>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<(u32, i64)> = Vec::new();
        let offset = -1; // Just say there is no offset on the group yet.
        for p in &topic.partitions {
            partition_responses.push((*p, offset));
        }
        responses.push((topic.name.to_string(), partition_responses));
    }
    debug!("About to send a fetch offsets response with content {:?}", responses);    
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::FetchOffsetsResponse {
            topics: responses
        }
    }
}

fn handle_offsets(header: &KafkaRequestHeader, topics: &Vec<(String, Vec<(u32, i64)>)>, db: &PgState) -> KafkaResponse {
    let conn = db.pool.get().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<(u32, i64)>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<(u32, i64)> = Vec::new();
        // Get offset by timestamp. Consider the two special values
        let offset: i64 = match topic.1.get(0).map(|t| t.1).unwrap_or(-1) {
            -2 => 0, // Start from the beginning
            -1 => {  // Start from the current HEAD
                let rs = conn.query(format!("SELECT max(id) + 1 FROM \"{}\"", topic.0).as_str(), &[]).expect("DB query failed");
                rs.iter().next().and_then(|r| r.get_opt(0)).unwrap_or(Ok(-1)).unwrap_or(-1)
            },
            _ => -1 // TODO Support lookup by an actual timestamp
        };
        for p in &topic.1 {
            partition_responses.push((p.0, offset));
        }
        responses.push((topic.0.to_string(), partition_responses));
    }
    debug!("About to send a offsets response with content {:?}", responses);

    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::OffsetsResponse {
            topics: responses
        }
    }
}

fn handle_offset_commit(header: &KafkaRequestHeader, topics: &Vec<TopicWithPartitions>) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::OffsetCommitResponse {
            topics: topics.to_vec()
        }
    }
}

fn handle_heartbeat(req: &KafkaRequest) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::HeartbeatResponse
    }
}

fn handle_leave_group(req: &KafkaRequest) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::LeaveGroupResponse
    }
}

pub fn cleanup(db: &PgState) {
    debug!("Cleanup thread is awake");
    let conn = db.pool.get().expect("Could not get a DB connection");
    for (_, topic) in &db.topics {
        if let Some(retention) = topic.retention {
            debug!("Cleaning up topic {}", topic.name);
            conn.execute(format!("DELETE FROM \"{}\" WHERE ts < now() - $1::text::interval",
                    topic.name).as_str(),
                    &[&format!("{}ms", retention)]).expect("Failed to delete from the DB");
        }
    }
    debug!("Cleanup thread is sleeping");
}
