
use parser::*;
use writer::*;

pub fn handle_request(req: KafkaRequest) -> KafkaResponse {
    match req.req {
        ApiRequest::Metadata { topics } => handle_metadata(&req.header, &topics),
        ApiRequest::Publish { topics, .. } => handle_publish(&req.header, &topics),
        ApiRequest::Versions => handle_versions(&req),
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

fn handle_publish(header: &KafkaRequestHeader, topics: &Vec<KafkaMessageSet>) -> KafkaResponse {
    let mut responses: Vec<(String, Vec<u32>)> = Vec::new();
    // TODO Actually save the data
    // Fake the response for now
    for ref topic in topics {
        let mut partition_responses: Vec<u32> = Vec::new();
        for ref partition in &topic.messages {
            info!("Actually saving message {:?}:{:?} to topic {:?} partition {:?}", partition.key, partition.value, topic.topic, partition.partition);
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
