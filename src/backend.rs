
use parser::*;
use writer::*;
use r2d2_postgres;
use r2d2::Pool;

pub fn handle_request(req: KafkaRequest, db: Pool<r2d2_postgres::PostgresConnectionManager>) -> KafkaResponse {
    match req.req {
        ApiRequest::Metadata { topics } => handle_metadata(&req.header, &topics),
        ApiRequest::Publish { topics, .. } => handle_publish(&req.header, &topics, &db),
        ApiRequest::Fetch { topics } => handle_fetch(&req.header, &topics, &db),
        ApiRequest::Versions => handle_versions(&req),
        ApiRequest::FindGroupCoordinator => handle_find_coordinator(&req),
        ApiRequest::JoinGroup { protocols, .. } => handle_join_group(&req.header, &protocols),
        ApiRequest::SyncGroup { assignments, .. } => handle_sync_group(&req.header, &assignments),
        ApiRequest::FetchOffsets { topics, .. } => handle_fetch_offsets(&req.header, &topics),
        ApiRequest::Offsets { topics } => handle_offsets(&req.header, &topics),
        ApiRequest::OffsetCommit { topics } => handle_offset_commit(&req.header, &topics),
        ApiRequest::Heartbeat => handle_heartbeat(&req),
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

fn handle_metadata(header: &KafkaRequestHeader, topics: &Vec<String>) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::metadata_healthy(header.version, topics)
    }
}

fn handle_publish(header: &KafkaRequestHeader, topics: &Vec<KafkaMessageSet>, db: &Pool<r2d2_postgres::PostgresConnectionManager>) -> KafkaResponse {
    let conn = db.get().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<u32>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<u32> = Vec::new();
        for partition in &topic.messages {
            let &(ref p_num, ref values) = partition;
            for msg in values {
                info!("Actually saving message {:?}:{:?} to topic {:?} partition {:?}", msg.key, msg.value, topic.topic, p_num);
                
                // CREATE TABLE test (id bigserial, partition int NOT NULL, key BYTEA, value BYTEA);
                conn.execute(format!("INSERT INTO {} (partition, key, value) VALUES ($1, $2, $3)", topic.topic).as_str(),
                     &[&(*p_num as i32), &msg.key, &msg.value]).expect("Failed to insert to the DB");
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

fn handle_fetch(header: &KafkaRequestHeader, topics: &Vec<(String, Vec<(u32, u64)>)>, db: &Pool<r2d2_postgres::PostgresConnectionManager>) -> KafkaResponse {
    let conn = db.get().expect("Could not get a DB connection");
    let mut responses: Vec<(String, Vec<(u64, Option<Vec<u8>>, Vec<u8>)>)> = Vec::new();
    for topic in topics {
        let mut partition_responses: Vec<(u64, Option<Vec<u8>>, Vec<u8>)> = Vec::new();
        let rs = conn.query(format!("SELECT id, partition, key, value FROM {}", topic.0).as_str(), &[]).expect("DB query failed");
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

fn handle_find_coordinator(req: &KafkaRequest) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::GroupCoordinatorResponse
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
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::FetchOffsetsResponse {
            topics: topics.to_vec()
        }
    }
}

fn handle_offsets(header: &KafkaRequestHeader, topics: &Vec<TopicWithPartitions>) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(header.correlation_id),
        req: ApiResponse::OffsetsResponse {
            topics: topics.to_vec()
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
