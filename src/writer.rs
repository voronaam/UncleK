use std::fmt;
use std::marker;
use bytes::{BytesMut, BufMut, BigEndian};

pub fn to_bytes(msg: &KafkaResponse, out: &mut BytesMut) {
    let mut buf = BytesMut::with_capacity(1024);
    msg.req.to_bytes(&mut buf);
    out.put_u32::<BigEndian>(buf.len() as u32 + 4); // 4 is the length of the size correlation id.
    out.put_i32::<BigEndian>(msg.header.correlation_id);
    out.extend(buf.take());
}



// Anything that is a Kafka response body.
pub trait ApiResponseLike: fmt::Debug + marker::Send {
    fn to_bytes(self: &Self, out: &mut BytesMut);
}

#[derive(Debug)]
pub struct KafkaResponse {
    pub header: KafkaResponseHeader,
    pub req: Box<ApiResponseLike>
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

#[derive(Debug)]
pub struct VersionsResponse {}
impl ApiResponseLike for VersionsResponse {
    fn to_bytes(self: &Self, out: &mut BytesMut) {
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
}
fn versions_supported_call(out: &mut BytesMut, opcode: u16, min: u16, max: u16) {
    out.put_u16::<BigEndian>(opcode);
    out.put_u16::<BigEndian>(min);
    out.put_u16::<BigEndian>(max);
}

#[derive(Debug)]
pub struct ErrorResponse {}
impl ApiResponseLike for ErrorResponse {
    fn to_bytes(self: &Self, out: &mut BytesMut) {
        // In Kafka error code is not always the first field in the response.
        // This will have to specialize based on the actual request in the future.
        out.put_u16::<BigEndian>(55); // OPERATION_NOT_ATTEMPTED
    }
}
