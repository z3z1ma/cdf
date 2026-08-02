#![doc = "Descriptor-bound, length-delimited Protobuf format driver for cdf."]

mod decode;
mod driver;
mod framing;
mod materialize;
mod options;
mod schema;
mod wire;

pub use driver::ProtobufFormatDriver;

#[cfg(test)]
mod tests;
