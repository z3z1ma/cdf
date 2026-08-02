#![doc = "Declarative resource authoring boundary for cdf."]

mod compiled;
mod declarations;
#[cfg(test)]
mod tests;

pub use cdf_kernel::parse_arrow_field_type;
pub use compiled::{
    CompiledResource, compile_document, compile_document_with_project_root,
    compile_execution_extent, physical_arrow_schema_hash, validate_document,
};
pub use declarations::{
    CursorDeclaration, CursorOrderingDeclaration, CursorValueDeclaration,
    DECLARATIVE_SCHEMA_ARTIFACT_PATH, DECLARATIVE_SCHEMA_VERSION, DeclarativeDocument,
    DeduplicationDeclaration, DrainTerminationDeclaration, EpochClosureDeclaration,
    EventTimeDomainDeclaration, ExecutionDeclaration, FieldDeclaration, FieldTypeDeclaration,
    FilePositionDeclaration, FilterFidelityDeclaration, FreshnessDeclaration, JsonSchemaArtifact,
    LateDataDeclaration, PartitionByDeclaration, PartitionDeclaration,
    PartitionWatermarkAggregationDeclaration, ResourceDeclaration, SafeFrontierDeclaration,
    SampleDeclaration, SchemaDeclaration, SchemaModeDeclaration, SourceDeclaration,
    SourcePositionDeclaration, TimeUnitDeclaration, TrustDeclaration, TypePolicyDeclaration,
    WatermarkAuthorityDeclaration, WatermarkDeclaration, WriteDispositionDeclaration,
    declarative_json_schema, declarative_json_schema_artifact, parse_toml, parse_yaml,
};
