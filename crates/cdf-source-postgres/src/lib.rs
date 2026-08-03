#![doc = "Postgres source adapter for cdf."]

mod binary_copy;
mod catalog;
mod driver;
mod source;

pub use catalog::{
    POSTGRES_CATALOG_DISCOVERY_PROBE, PostgresCatalogDiscovery,
    discover_postgres_table_catalog_schema,
};
pub use cdf_postgres::{PostgresIdentifier, PostgresTarget};
pub use driver::PostgresSourceDriver;
pub(crate) use source::POSTGRES_MAXIMUM_BATCH_BYTES;
pub use source::{
    POSTGRES_SOURCE_BLOCKING_LANE_ID, PostgresTableResource, classify_postgres_table_predicates,
    negotiate_postgres_table_scan, open_postgres_table_with_connection,
    plan_postgres_table_partition, postgres_source_blocking_lane, postgres_table_capabilities,
    postgres_table_predicate_fidelity, validate_postgres_table_resource_shape,
};
