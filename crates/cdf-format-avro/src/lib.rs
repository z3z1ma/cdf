#![doc = "Native Avro object-container and single-object format drivers for cdf."]

mod byte_source;
mod decode;
mod driver;
mod errors;
mod options;
mod planning;
mod validation;

pub use driver::{AvroOcfFormatDriver, AvroSingleObjectFormatDriver};

#[cfg(test)]
mod tests;
