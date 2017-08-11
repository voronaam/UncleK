use nom::{IResult,be_u16,be_u32,be_i16,be_i32,alphanumeric};

pub fn size_header(input: &[u8]) -> IResult<&[u8], &[u8]> {
    length_bytes!(input, be_u32)
}

#[derive(Debug)]
pub struct KafkaRequestHeader {
    pub opcode: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String
}

pub fn request_header(input:&[u8]) -> IResult<&[u8], KafkaRequestHeader> {
  do_parse!(input,
    opcode: be_i16 >>
    version: be_i16 >>
    correlation_id: be_i32 >>
    client_id: length_bytes!(be_u16) >>  // TODO length -1 indicates null.
   (
     KafkaRequestHeader {
        opcode: opcode,
        version: version,
        correlation_id: correlation_id,
        client_id: String::from_utf8_lossy(client_id).to_string()
     }
   )
  )
}
