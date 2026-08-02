//! Avro/Arrow error adaptation with typed CDF provenance preservation.

use arrow_avro::errors::AvroError;
use cdf_kernel::CdfError;

pub(crate) fn cdf_to_avro(error: CdfError) -> AvroError {
    AvroError::External(Box::new(error))
}

pub(crate) fn avro_error(error: AvroError) -> CdfError {
    match error {
        AvroError::External(error) => match error.downcast::<CdfError>() {
            Ok(error) => *error,
            Err(error) => CdfError::data(format!("decode Avro: {error}")),
        },
        AvroError::ArrowError(error) => avro_arrow_error(*error),
        error => CdfError::data(format!("decode Avro: {error}")),
    }
}

pub(crate) fn avro_arrow_error(error: arrow_schema::ArrowError) -> CdfError {
    match error {
        arrow_schema::ArrowError::ExternalError(error) => match error.downcast::<CdfError>() {
            Ok(error) => *error,
            Err(error) => CdfError::data(format!("decode Avro: {error}")),
        },
        error => CdfError::data(format!("decode Avro: {error}")),
    }
}
