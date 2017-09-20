use parser::*;
use writer::*;
use utils::net::config;
use utils::net::poolmgr;
use std::sync::Arc;
use std::collections::HashMap;
use settings::Settings;
use settings::Topic;
use std::fmt;
use std::io::Write;


#[derive(Clone)]
pub struct Connection {
    pub tcp: Arc<poolmgr::ConnectionPool>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection")
    }
}

#[derive(Debug, Clone)]
pub struct PgState {
    pub pool: Connection,
    pub topics: HashMap<String, Topic>,
    pub hostname: String,
}

pub fn initialize(cnf: &Settings) -> PgState {
    // let db_url = cnf.database.url.to_string();
    let mut cfg : config::Config = Default::default();
    cfg.port= Some(1234);
    cfg.server = Some("localhost".to_string());
    let mut pool = poolmgr::ConnectionPool::new(2, 20, true, &cfg);
    let pool = Arc::new(pool);
    let mut map = HashMap::new();
    for topic in &cnf.topics {
        map.insert(topic.name.to_string(), topic.clone());
    }
    PgState {
        pool: Connection {tcp: pool},
        topics: map,
        hostname: cnf.get_hostname()
        
    }
}

fn create_tables(topics: &Vec<Topic>, pool: &Connection) {
	let mut conn = pool.tcp.acquire().expect("Could not get a DB connection");
    for topic in topics {
		/*
        let uniq = if topic.compacted.unwrap_or(false) {"UNIQUE"} else {""};
        conn.execute(format!(r#"
            CREATE TABLE IF NOT EXISTS "{}" (
                id bigserial PRIMARY KEY,
                partition int NOT NULL,
                ts timestamp NOT NULL,
                key BYTEA {},
                value BYTEA)
            "#,
            topic.name, uniq).as_str(), &[]).expect("Failed to create DB table");
        */
    }
    pool.tcp.release(conn);
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

fn handle_publish(header: &KafkaRequestHeader, topics: &Vec<KafkaMessageSet>, db: &PgState) -> KafkaResponse {
	let mut conn = db.pool.tcp.acquire().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<u32>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<u32> = Vec::new();
        for partition in &topic.messages {
            let &(ref p_num, ref values) = partition;
            for msg in values {
                debug!("Actually saving message {:?}:{:?} to topic {:?} partition {:?}", msg.key, msg.value, topic.topic, p_num);
                if let Some(ref buf) = msg.value {
					conn.writer.write(&buf[..]).unwrap();
					conn.writer.flush().unwrap();
				}
                /*
                let uniq = if db.topics.get(&topic.topic).and_then(|t| t.compacted).unwrap_or(false) {
                    "ON CONFLICT (key) DO UPDATE SET value=$3, ts=now()"
                    // We can also easily update id to the next seq value. But this may have performance implications (index rebuild)
                } else {
                    ""
                };
                conn.execute(format!("INSERT INTO \"{}\" (partition, ts, key, value) VALUES ($1, now(), $2, $3) {}",
                    topic.topic, uniq).as_str(),
                    &[&(*p_num as i32), &msg.key, &msg.value]).expect("Failed to insert to the DB");
                */
            }
            partition_responses.push(*p_num);
        }
        responses.push((topic.topic.to_string(), partition_responses));
    }
    db.pool.tcp.release(conn);
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::PublishResponse {
            version: header.version,
            responses: responses
        }
    }
}

fn handle_fetch(header: &KafkaRequestHeader, topics: &Vec<(String, Vec<(u32, u64)>)>, db: &PgState) -> KafkaResponse {
    let conn = db.pool.tcp.acquire().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<(u64, Option<Vec<u8>>, Vec<u8>)>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<(u64, Option<Vec<u8>>, Vec<u8>)> = Vec::new();
        let id = topic.1.get(0).expect("Need at least one partion in request").1 as i64;
        // TODO smart limit calculation
        /*
        let rs = conn.query(format!("SELECT id, partition, key, value FROM \"{}\" WHERE id >= $1 LIMIT 25", topic.0).as_str(), &[&id]).expect("DB query failed");
        for row in &rs {
            let offset: i64 = row.get(0);
            let key: Option<Vec<u8>> = row.get(2);
            let value: Vec<u8> = row.get(3);
            partition_responses.push((offset as u64, key, value));
        }
        */
        responses.push((topic.0.to_string(), partition_responses));
    }
    db.pool.tcp.release(conn);
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
    let conn = db.pool.tcp.acquire().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<(u32, i64)>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<(u32, i64)> = Vec::new();
        // Get offset by timestamp. Consider the two special values
        let offset: i64 = match topic.1.get(0).map(|t| t.1).unwrap_or(-1) {
            -2 => 0, // Start from the beginning
            -1 => {  // Start from the current HEAD
				/*
                let rs = conn.query(format!("SELECT max(id) + 1 FROM \"{}\"", topic.0).as_str(), &[]).expect("DB query failed");
                rs.iter().next().and_then(|r| r.get_opt(0)).unwrap_or(Ok(-1)).unwrap_or(-1)
                */
                0
            },
            _ => -1 // TODO Support lookup by an actual timestamp
        };
        for p in &topic.1 {
            partition_responses.push((p.0, offset));
        }
        responses.push((topic.0.to_string(), partition_responses));
    }
    db.pool.tcp.release(conn);
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
}
