//! Length-delimited Protobuf message framing.

use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryLease;
use cdf_runtime::AccountedByteCursor;

use crate::options::MAXIMUM_LENGTH_PREFIX_BYTES;
use crate::wire::decode_varint;

pub(crate) struct BufferedMessage {
    pub(crate) bytes: Vec<u8>,
    pub(crate) _lease: MemoryLease,
}

pub(crate) async fn read_length_prefix(cursor: &mut AccountedByteCursor) -> Result<Option<u64>> {
    let Some(first) = cursor.next_byte().await? else {
        return Ok(None);
    };
    let mut bytes = [0_u8; MAXIMUM_LENGTH_PREFIX_BYTES];
    bytes[0] = first;
    if first & 0x80 == 0 {
        return Ok(Some(u64::from(first)));
    }
    for index in 1..MAXIMUM_LENGTH_PREFIX_BYTES {
        bytes[index] = cursor.next_byte().await?.ok_or_else(|| {
            CdfError::data("Protobuf stream ended inside a message length prefix")
        })?;
        if bytes[index] & 0x80 == 0 {
            return decode_varint(&bytes[..=index], "message length").map(|(value, _)| Some(value));
        }
    }
    Err(CdfError::data(
        "Protobuf message length prefix exceeds ten bytes",
    ))
}
