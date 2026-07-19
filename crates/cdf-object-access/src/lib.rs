#![doc = "Neutral local, HTTP, and object-store access authority for cdf."]

mod evicting_spool_byte_source;
mod growing_spool_byte_source;
mod local_byte_source;
mod object_store_byte_source;
mod payload_cache;
mod transport;

pub use evicting_spool_byte_source::{EvictingSpoolSession, start_evicting_spool};
pub use growing_spool_byte_source::{GrowingSpoolSession, start_growing_spool};
pub use local_byte_source::{
    LocalByteSource, local_source_generation, open_identity_preserving_local_source,
};
pub use object_store_byte_source::ObjectStoreByteSource;
pub use payload_cache::{
    FilePayloadCache, FilePayloadCacheHit, FilePayloadCacheKey, FilePayloadCacheLookup,
    FilePayloadCachePolicy, FilePayloadCachePromotion, resolve_project_cache_root,
};
pub use transport::*;

#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(crate) use test_support::{
    test_egress_scope, test_execution_services, test_local_listing_lane,
};
