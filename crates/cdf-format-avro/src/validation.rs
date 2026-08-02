//! Decode-request, schema-authority, source-capability, and memory validation.

use std::sync::Arc;

use cdf_kernel::{CdfError, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    ByteSource, DecodePlanningRequest, DecodeSchemaAuthority, FormatDiscoveryKind,
    PhysicalDecodeRequest,
};

pub(crate) fn validate_seekable_source(source: &dyn ByteSource, label: &str) -> Result<u64> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    if !source.capabilities().known_length || !source.capabilities().exact_ranges {
        return Err(CdfError::contract(format!(
            "{label} requires known-length exact-range byte-source access"
        )));
    }
    source
        .identity()
        .size_bytes
        .filter(|size| *size > 0)
        .ok_or_else(|| CdfError::data(format!("{label} source length is missing or zero")))
}

pub(crate) fn validate_metadata_discovery(
    kind: FormatDiscoveryKind,
    maximum_bytes: u64,
) -> Result<()> {
    if kind != FormatDiscoveryKind::FormatMetadata || maximum_bytes == 0 {
        return Err(CdfError::contract(
            "Avro metadata discovery requires format_metadata coverage and a nonzero byte budget",
        ));
    }
    Ok(())
}

pub(crate) fn validate_decode_request(request: &DecodePlanningRequest, label: &str) -> Result<()> {
    if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
        return Err(CdfError::contract(format!(
            "{label} planning requires nonzero row and byte batch targets"
        )));
    }
    if !request.predicates.is_empty() {
        return Err(CdfError::contract(format!(
            "{label} predicate pushdown is unsupported"
        )));
    }
    Ok(())
}

pub(crate) fn validate_physical_decode_request(
    request: &PhysicalDecodeRequest,
    label: &str,
) -> Result<()> {
    if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
        return Err(CdfError::contract(format!(
            "{label} decode requires nonzero row and byte batch targets"
        )));
    }
    if !request.predicates.is_empty() {
        return Err(CdfError::contract(format!(
            "{label} predicate pushdown is unsupported"
        )));
    }
    Ok(())
}

pub(crate) fn validate_schema_authority(
    request: &PhysicalDecodeRequest,
    physical_schema: &arrow_schema::Schema,
    label: &str,
) -> Result<()> {
    if request.schema.authority == DecodeSchemaAuthority::VerifiedPhysicalObservation {
        let expected =
            cdf_kernel::canonical_arrow_schema_hash(request.schema.authority_schema.as_ref())?;
        let observed = cdf_kernel::canonical_arrow_schema_hash(physical_schema)?;
        if expected != observed {
            return Err(CdfError::data(format!(
                "{label} physical schema changed before decode: planned {expected}, observed {observed}"
            )));
        }
    }
    Ok(())
}

pub(crate) async fn reserve_output(
    request: &PhysicalDecodeRequest,
    consumer: &str,
    authority_bytes: u64,
) -> Result<MemoryLease> {
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new(consumer, MemoryClass::Decode)?,
            authority_bytes.max(1),
        )?
        .as_minimum_working_set(),
    )
    .await
}
