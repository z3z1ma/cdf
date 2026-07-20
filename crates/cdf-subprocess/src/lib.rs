#![doc = "Subprocess adapter boundary for cdf."]

mod airbyte;
mod command;
mod protocol;
mod protocol_stream;
mod runner;
mod singer;
#[cfg(test)]
mod tests;

pub use airbyte::{
    AirbyteCatalog, AirbyteMessage, AirbyteOther, AirbyteRecord, AirbyteState, AirbyteStateKind,
    decode_airbyte_message,
};
pub use command::{
    BoundedCommandBytes, BoundedCommandOutput, CommandSpec, DEFAULT_STDERR_LINE_LIMIT, StderrTrace,
    SubprocessProtocol, SupervisionOptions,
};
pub use protocol::StreamIdentity;
pub use runner::{SubprocessProducer, run_bounded_command};
pub use singer::{
    SingerMessage, SingerOther, SingerRecord, SingerSchema, SingerState, decode_singer_message,
};
