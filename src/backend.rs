
use parser::KafkaRequest;
use writer::*;

pub fn handle_request(req: KafkaRequest) -> KafkaResponse {
    match (req.header.opcode, req.header.version) {
        (18, _) => handle_versions(&req),
        _ => handle_unknown(&req)
    }
}

fn handle_versions(req: &KafkaRequest) -> KafkaResponse {
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: Box::new(VersionsResponse{})
    }
}

fn handle_unknown(req: &KafkaRequest) -> KafkaResponse {
    warn!("Unknown request");
    KafkaResponse {
        header: KafkaResponseHeader::new(req.header.correlation_id),
        req: Box::new(ErrorResponse{})
    }
}
