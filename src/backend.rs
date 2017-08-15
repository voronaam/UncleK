
use parser::*;
use writer::*;
use r2d2_postgres;
use r2d2::Pool;

pub fn handle_request(req: KafkaRequest, db: Pool<r2d2_postgres::PostgresConnectionManager>) -> KafkaResponse {
    match req.req {
        ApiRequest::Metadata { topics } => handle_metadata(&req.header, &topics),
        ApiRequest::Publish { topics, .. } => handle_publish(&req.header, &topics, &db),
        ApiRequest::Versions => handle_versions(&req),
        ApiRequest::FindGroupCoordinator => handle_find_coordinator(&req),
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
    let mut responses: Vec<(String, Vec<u32>)> = Vec::new();
    for ref topic in topics {
        let mut partition_responses: Vec<u32> = Vec::new();
        for ref partition in &topic.messages {
            info!("Actually saving message {:?}:{:?} to topic {:?} partition {:?}", partition.key, partition.value, topic.topic, partition.partition);
            
            // CREATE TABLE test (id serial, partition int NOT NULL, key BYTEA, value BYTEA);
            let conn = db.get().expect("Could not get a DB connection");
            conn.execute(format!("INSERT INTO {} (partition, key, value) VALUES ($1, $2, $3)", topic.topic).as_str(),
                 &[&(partition.partition as i32), &partition.key, &partition.value]).expect("Failed to insert to the DB");
            partition_responses.push(partition.partition);
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

fn handle_find_coordinator(req: &KafkaRequest) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: ApiResponse::GroupCoordinatorResponse
    }
}
