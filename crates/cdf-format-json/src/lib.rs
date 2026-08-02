#![doc = "Streaming JSON format drivers for cdf."]

mod decode;
mod discovery;
mod driver;
mod framing;
mod options;
mod raw;
mod selection;

pub use driver::{JsonDocumentFormatDriver, NdjsonFormatDriver};
pub use selection::{BoundedJsonSelection, select_bounded_json_records};

#[cfg(test)]
mod tests;
