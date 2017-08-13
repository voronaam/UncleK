use backend::KafkaResponse;

use bytes::{BytesMut, BufMut, BigEndian};

pub fn to_bytes(msg: &KafkaResponse, out: &mut BytesMut) {
    let mut buf = BytesMut::with_capacity(1024);
    msg.req.to_bytes(&mut buf);
    out.put_u32::<BigEndian>(buf.len() as u32 + 4); // 8 is the length of the size correlation id
    out.put_i32::<BigEndian>(msg.header.correlation_id);
    out.extend(buf.take());
}
