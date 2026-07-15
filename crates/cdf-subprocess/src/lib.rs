#![doc = "Subprocess adapter boundary for cdf."]

mod airbyte;
mod command;
mod protocol;
mod runner;
mod singer;
#[cfg(test)]
mod tests;

pub use airbyte::{
    AirbyteCatalog, AirbyteMessage, AirbyteOther, AirbyteRead, AirbyteRecord, AirbyteState,
    AirbyteStateKind, parse_airbyte_ndjson, read_airbyte_ndjson_bytes,
};
pub use command::{
    CommandSpec, DEFAULT_STDERR_LINE_LIMIT, StderrTrace, StdoutFormat, SubprocessOutput,
    SubprocessRead, SupervisionOptions,
};
pub use protocol::{ProtocolState, ProtocolStreamRead, StreamIdentity};
pub use runner::run_stdout_adapter;
pub use singer::{
    SingerMessage, SingerOther, SingerRead, SingerRecord, SingerSchema, SingerState,
    parse_singer_ndjson, read_singer_ndjson_bytes,
};
