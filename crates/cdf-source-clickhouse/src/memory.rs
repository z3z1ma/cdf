use std::{num::NonZeroUsize, sync::Arc};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use clickhouse::ResponseLimits;
use clickhouse_ext_arrow::ArrowStreamLimits;

/// Complete CDF-owned decode authority for one ClickHouse cursor poll.
pub(crate) const CLICKHOUSE_DECODE_LEASE_BYTES: u64 = 64 * 1024 * 1024;
/// Persistent schema/decoder/input-buffer authority retained for one Arrow cursor lifetime.
///
/// One cursor can retain a 25 MiB decoded HTTP chunk between yielded batches, a bounded 4 MiB
/// owned schema, and up to 3 MiB of message/decoder/container state.
pub(crate) const CLICKHOUSE_CURSOR_STATE_BYTES: u64 = 32 * 1024 * 1024;
/// Fixed native HTTP/1 read-buffer authority retained by one official client connection.
pub(crate) const CLICKHOUSE_HTTP1_TRANSPORT_BYTES: u64 =
    clickhouse::DEFAULT_HTTP1_MAX_BUFFER_BYTES as u64;
/// Maximum Arrow IPC record-body allocation admitted under one decode lease.
pub(crate) const CLICKHOUSE_ARROW_BODY_BYTES: usize = 25 * 1024 * 1024;
/// Maximum Arrow IPC flatbuffer metadata allocation admitted under one decode lease.
pub(crate) const CLICKHOUSE_ARROW_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
/// Conservative pre-conversion ceiling for the owned Arrow schema retained by a cursor.
pub(crate) const CLICKHOUSE_ARROW_SCHEMA_BYTES: usize = 4 * 1024 * 1024;
/// Maximum number of custom metadata entries retained across one Arrow schema.
pub(crate) const CLICKHOUSE_ARROW_SCHEMA_METADATA_ENTRIES: usize = 4_096;
/// Maximum nested Arrow field depth accepted from a response schema.
pub(crate) const CLICKHOUSE_ARROW_SCHEMA_DEPTH: usize = 64;
/// Maximum allocator capacity reached while incrementally assembling one admitted Arrow body.
pub(crate) const CLICKHOUSE_ARROW_SCRATCH_CAPACITY_BYTES: usize = 32 * 1024 * 1024;
/// Reserved schema, ArrayData, field, and container bookkeeping within one decode lease.
pub(crate) const CLICKHOUSE_ARROW_CONTAINER_HEADROOM_BYTES: usize = 4 * 1024 * 1024;
/// Conservative retained discovery-model authority, separate from transient Arrow decode.
pub(crate) const CLICKHOUSE_CATALOG_METADATA_BYTES: u64 = 16 * 1024 * 1024;

const CLICKHOUSE_ERROR_BODY_BYTES: usize = 1024 * 1024;

pub(crate) fn arrow_stream_limits(
    lease: &MemoryLease,
    maximum_record_batch_rows: usize,
) -> Result<ArrowStreamLimits> {
    // MutableBuffer doubles while assembling a split 25 MiB body, so its real capacity can be
    // 32 MiB. That allocation can coexist with one body-sized alignment copy, 2 MiB metadata, one
    // bounded HTTP data frame, and explicit Arrow schema/container headroom. Dictionary messages
    // are rejected so retained dictionaries cannot accumulate across polls.
    let required = clickhouse_decode_envelope_bytes()?;
    if lease.bytes() < required {
        return Err(CdfError::internal(format!(
            "ClickHouse Arrow limits require a {required}-byte decode lease, admitted {} bytes",
            lease.bytes()
        )));
    }
    let transport_chunk_bytes = usize::try_from(CLICKHOUSE_HTTP1_TRANSPORT_BYTES)
        .map_err(|_| CdfError::internal("ClickHouse HTTP chunk limit exceeds usize"))?;
    let response = ResponseLimits::new(
        nonzero(CLICKHOUSE_ERROR_BODY_BYTES)?,
        nonzero(CLICKHOUSE_ERROR_BODY_BYTES)?,
        nonzero(transport_chunk_bytes)?,
        nonzero(CLICKHOUSE_ARROW_BODY_BYTES)?,
    );
    Ok(ArrowStreamLimits::new(
        response,
        nonzero(CLICKHOUSE_ARROW_MESSAGE_BYTES)?,
        nonzero(CLICKHOUSE_ARROW_BODY_BYTES)?,
    )
    .with_schema_limits(
        crate::types::CLICKHOUSE_MAXIMUM_SCHEMA_NODES,
        CLICKHOUSE_ARROW_SCHEMA_METADATA_ENTRIES,
        CLICKHOUSE_ARROW_SCHEMA_BYTES,
        CLICKHOUSE_ARROW_SCHEMA_DEPTH,
    )
    .with_max_record_batch_rows(maximum_record_batch_rows))
}

pub(crate) fn clickhouse_decode_envelope_bytes() -> Result<u64> {
    [
        CLICKHOUSE_ARROW_SCRATCH_CAPACITY_BYTES,
        CLICKHOUSE_ARROW_BODY_BYTES,
        CLICKHOUSE_ARROW_MESSAGE_BYTES,
        usize::try_from(CLICKHOUSE_HTTP1_TRANSPORT_BYTES)
            .map_err(|_| CdfError::internal("ClickHouse HTTP chunk limit exceeds usize"))?,
        CLICKHOUSE_ARROW_CONTAINER_HEADROOM_BYTES,
    ]
    .into_iter()
    .try_fold(0_u64, |total, bytes| {
        total
            .checked_add(
                u64::try_from(bytes)
                    .map_err(|_| CdfError::internal("ClickHouse decode term exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::internal("ClickHouse decode envelope overflow"))
    })
}

pub(crate) async fn reserve_transport(memory: Arc<dyn MemoryCoordinator>) -> Result<MemoryLease> {
    reserve(
        memory,
        ReservationRequest::new(
            ConsumerKey::new("clickhouse-http1-transport", MemoryClass::Source)?,
            CLICKHOUSE_HTTP1_TRANSPORT_BYTES,
        )?,
    )
    .await
}

pub(crate) async fn reserve_decode(memory: Arc<dyn MemoryCoordinator>) -> Result<MemoryLease> {
    reserve(
        memory,
        ReservationRequest::new(
            ConsumerKey::new("clickhouse-arrow-decode", MemoryClass::Decode)?,
            CLICKHOUSE_DECODE_LEASE_BYTES,
        )?,
    )
    .await
}

pub(crate) async fn reserve_cursor_state(
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<MemoryLease> {
    reserve(
        memory,
        ReservationRequest::new(
            ConsumerKey::new("clickhouse-arrow-cursor-state", MemoryClass::Decode)?,
            CLICKHOUSE_CURSOR_STATE_BYTES,
        )?,
    )
    .await
}

pub(crate) async fn reserve_catalog_metadata(
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<MemoryLease> {
    reserve(
        memory,
        ReservationRequest::new(
            ConsumerKey::new("clickhouse-catalog-metadata", MemoryClass::Discovery)?,
            CLICKHOUSE_CATALOG_METADATA_BYTES,
        )?,
    )
    .await
}

fn nonzero(value: usize) -> Result<NonZeroUsize> {
    NonZeroUsize::new(value)
        .ok_or_else(|| CdfError::internal("ClickHouse response limit must be nonzero"))
}
