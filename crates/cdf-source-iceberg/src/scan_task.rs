use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use cdf_kernel::{CdfError, CompiledScanIntent, Result, TableSnapshotPosition};
use cdf_runtime::artifact_hash;
use cdf_task_store::ExternalTaskSetWriter;
use serde::{Deserialize, Serialize};

pub const ICEBERG_SCAN_TASK_VERSION: u16 = 1;
pub const ICEBERG_TASK_SET_AUTHORITY_VERSION: u16 = 1;
pub const ICEBERG_TASK_SET_TYPE: &str = "iceberg-scan-v1";

/// Shared, immutable authority for every task in one Iceberg task-set artifact.
///
/// Schema, partition-spec, name-map, predicate, snapshot, and reader facts are encoded once rather
/// than repeated per file. The artifact header hashes these canonical bytes and every task binds
/// that hash, preserving isolated reconstruction without metadata cardinality amplification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergTaskSetAuthority {
    pub version: u16,
    pub snapshot: TableSnapshotPosition,
    pub table_format_version: u8,
    pub schemas: BTreeMap<i32, IcebergJsonAuthority>,
    pub projected_field_ids: Vec<i32>,
    pub partition_specs: BTreeMap<i32, IcebergJsonAuthority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_mapping: Option<IcebergJsonAuthority>,
    pub case_sensitive: bool,
    pub scan_intent: CompiledScanIntent,
    pub reader: IcebergReaderRequirements,
}

impl IcebergTaskSetAuthority {
    pub fn validate(&self) -> Result<()> {
        if self.version != ICEBERG_TASK_SET_AUTHORITY_VERSION {
            return Err(CdfError::contract(format!(
                "Iceberg task-set authority version {} is unsupported; expected {}",
                self.version, ICEBERG_TASK_SET_AUTHORITY_VERSION
            )));
        }
        self.snapshot.validate()?;
        if self.snapshot.protocol != "iceberg" {
            return Err(CdfError::contract(
                "Iceberg scan task requires an Iceberg table-snapshot position",
            ));
        }
        if !matches!(self.table_format_version, 1 | 2) {
            return Err(CdfError::contract(
                "Iceberg source currently supports table format version 1 or 2",
            ));
        }
        if self.schemas.is_empty() {
            return Err(CdfError::contract(
                "Iceberg task-set authority requires at least one schema",
            ));
        }
        for (schema_id, encoded) in &self.schemas {
            if *schema_id < 0 {
                return Err(CdfError::contract(
                    "Iceberg task-set schema ids must be nonnegative",
                ));
            }
            encoded.validate("Iceberg task-set schema")?;
            let schema = decode_schema(encoded)?;
            if schema.schema_id() != *schema_id {
                return Err(CdfError::contract(format!(
                    "Iceberg task-set schema key {schema_id} does not match encoded schema id {}",
                    schema.schema_id()
                )));
            }
        }
        if !strictly_increasing_positive(&self.projected_field_ids) {
            return Err(CdfError::contract(
                "Iceberg projected field ids must be positive and strictly increasing",
            ));
        }
        if self.partition_specs.is_empty() {
            return Err(CdfError::contract(
                "Iceberg task-set authority requires at least one partition spec",
            ));
        }
        for (spec_id, encoded) in &self.partition_specs {
            if *spec_id < 0 {
                return Err(CdfError::contract(
                    "Iceberg task-set partition spec ids must be nonnegative",
                ));
            }
            encoded.validate("Iceberg task-set partition spec")?;
            let spec = decode_partition_spec(encoded)?;
            if spec.spec_id() != *spec_id {
                return Err(CdfError::contract(format!(
                    "Iceberg task-set partition-spec key {spec_id} does not match encoded spec id {}",
                    spec.spec_id()
                )));
            }
        }
        if let Some(mapping) = &self.name_mapping {
            mapping.validate("Iceberg name mapping")?;
            serde_json::from_value::<iceberg::spec::NameMapping>(mapping.value.clone()).map_err(
                |error| CdfError::contract(format!("decode Iceberg name mapping: {error}")),
            )?;
        }
        self.scan_intent.validate()?;
        self.reader.validate()
    }

    pub fn content_sha256(&self) -> Result<String> {
        self.validate()?;
        artifact_hash(self)
    }

    pub fn encode_to(&self, output: &mut dyn Write) -> Result<()> {
        self.validate()?;
        serde_json::to_writer(output, self).map_err(|error| {
            CdfError::data(format!(
                "encode canonical Iceberg task-set authority: {error}"
            ))
        })
    }
}

/// CDF-owned, portable work record for one immutable Iceberg data-file range.
///
/// This deliberately does not mirror or serialize `iceberg::scan::FileScanTask`. In particular,
/// no credential, signed URL, key metadata, open handle, or callback can be represented here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergScanTask {
    pub version: u16,
    pub canonical_ordinal: u64,
    pub authority_sha256: String,
    pub data_file: IcebergDataFile,
    pub schema_id: i32,
    pub partition_spec_id: i32,
    /// Values are ordered by the selected partition spec and decoded against its typed fields.
    pub partition_values: Vec<Option<serde_json::Value>>,
    pub deletes: Vec<IcebergDeleteFile>,
}

impl IcebergScanTask {
    pub fn validate_against(&self, authority: &IcebergTaskSetAuthority) -> Result<()> {
        authority.validate()?;
        if self.version != ICEBERG_SCAN_TASK_VERSION {
            return Err(CdfError::contract(format!(
                "Iceberg scan task version {} is unsupported; expected {}",
                self.version, ICEBERG_SCAN_TASK_VERSION
            )));
        }
        validate_sha256("Iceberg task authority", &self.authority_sha256)?;
        if self.authority_sha256 != authority.content_sha256()? {
            return Err(CdfError::contract(
                "Iceberg scan task does not bind the selected task-set authority",
            ));
        }
        self.data_file.validate("Iceberg data file")?;
        if self.data_file.format != IcebergFileFormat::Parquet {
            return Err(CdfError::contract(
                "Iceberg source currently supports Parquet data files only",
            ));
        }
        let schema_authority = authority.schemas.get(&self.schema_id).ok_or_else(|| {
            CdfError::contract(format!(
                "Iceberg task schema id {} is absent from task-set authority",
                self.schema_id
            ))
        })?;
        let schema = decode_schema(schema_authority)?;
        for field_id in &authority.projected_field_ids {
            if schema.field_by_id(*field_id).is_none() {
                return Err(CdfError::contract(format!(
                    "Iceberg projected field id {field_id} is absent from task schema {}",
                    self.schema_id
                )));
            }
        }
        let spec_authority = authority
            .partition_specs
            .get(&self.partition_spec_id)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "Iceberg task partition spec id {} is absent from task-set authority",
                    self.partition_spec_id
                ))
            })?;
        let partition_spec = decode_partition_spec(spec_authority)?;
        let partition_type = partition_spec.partition_type(&schema).map_err(|error| {
            CdfError::contract(format!(
                "bind Iceberg partition spec to task schema: {error}"
            ))
        })?;
        if partition_type.fields().len() != self.partition_values.len() {
            return Err(CdfError::contract(format!(
                "Iceberg task carries {} partition values but spec {} requires {}",
                self.partition_values.len(),
                self.partition_spec_id,
                partition_type.fields().len()
            )));
        }
        for (index, (value, field)) in self
            .partition_values
            .iter()
            .zip(partition_type.fields())
            .enumerate()
        {
            iceberg::spec::Literal::try_from_json(
                value.clone().unwrap_or(serde_json::Value::Null),
                field.field_type.as_ref(),
            )
            .map_err(|error| {
                CdfError::contract(format!(
                    "decode Iceberg partition value {index} for field {}: {error}",
                    field.id
                ))
            })?;
        }
        if authority.table_format_version == 1 && !self.deletes.is_empty() {
            return Err(CdfError::contract(
                "Iceberg format v1 scan tasks cannot carry delete files",
            ));
        }
        let mut previous = None;
        for delete in &self.deletes {
            delete.validate()?;
            if !authority
                .partition_specs
                .contains_key(&delete.partition_spec_id)
            {
                return Err(CdfError::contract(format!(
                    "Iceberg delete partition spec id {} is absent from task-set authority",
                    delete.partition_spec_id
                )));
            }
            for field_id in &delete.equality_field_ids {
                if schema.field_by_id(*field_id).is_none() {
                    return Err(CdfError::contract(format!(
                        "Iceberg equality-delete field id {field_id} is absent from task schema {}",
                        self.schema_id
                    )));
                }
            }
            if delete
                .referenced_data_file
                .as_deref()
                .is_some_and(|path| path != self.data_file.path)
            {
                return Err(CdfError::contract(
                    "Iceberg position delete references a different data file than its scan task",
                ));
            }
            let key = delete.canonical_key();
            if previous.as_ref().is_some_and(|value| value >= &key) {
                return Err(CdfError::contract(
                    "Iceberg delete files must be unique and sorted by canonical identity",
                ));
            }
            previous = Some(key);
        }
        Ok(())
    }

    pub fn content_sha256(&self, authority: &IcebergTaskSetAuthority) -> Result<String> {
        self.validate_against(authority)?;
        artifact_hash(self)
    }

    /// Encodes this task into the source-neutral bounded task store.
    ///
    /// All maps in the CDF-owned shape are ordered and the struct field order is frozen by this
    /// version, so this is the sole canonical encoder for Iceberg task-set identity.
    pub fn append_to(
        &self,
        authority: &IcebergTaskSetAuthority,
        writer: &mut ExternalTaskSetWriter,
    ) -> Result<()> {
        self.validate_against(authority)?;
        if writer.authority_sha256() != self.authority_sha256 {
            return Err(CdfError::contract(
                "Iceberg task writer carries a different shared authority",
            ));
        }
        writer.push_with(self.canonical_ordinal, |output| {
            serde_json::to_writer(output, self)
                .map_err(|error| CdfError::data(format!("encode canonical Iceberg task: {error}")))
        })
    }
}

fn decode_schema(authority: &IcebergJsonAuthority) -> Result<iceberg::spec::Schema> {
    serde_json::from_value(authority.value.clone())
        .map_err(|error| CdfError::contract(format!("decode Iceberg schema authority: {error}")))
}

fn decode_partition_spec(authority: &IcebergJsonAuthority) -> Result<iceberg::spec::PartitionSpec> {
    serde_json::from_value(authority.value.clone()).map_err(|error| {
        CdfError::contract(format!("decode Iceberg partition spec authority: {error}"))
    })
}

fn strictly_increasing_positive(values: &[i32]) -> bool {
    values.iter().all(|value| *value > 0) && values.windows(2).all(|pair| pair[0] < pair[1])
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergDataFile {
    pub path: String,
    pub format: IcebergFileFormat,
    pub file_size_bytes: u64,
    pub range_start: u64,
    pub range_length: u64,
    pub object_generation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
}

impl IcebergDataFile {
    fn validate(&self, label: &str) -> Result<()> {
        validate_object_location(label, &self.path)?;
        require_text(&format!("{label} generation"), &self.object_generation)?;
        if self.file_size_bytes == 0 || self.range_length == 0 {
            return Err(CdfError::contract(format!(
                "{label} size and range length must be nonzero"
            )));
        }
        let range_end = self
            .range_start
            .checked_add(self.range_length)
            .ok_or_else(|| CdfError::contract(format!("{label} range overflows u64")))?;
        if range_end > self.file_size_bytes {
            return Err(CdfError::contract(format!(
                "{label} range exceeds the immutable object size"
            )));
        }
        if let Some(hash) = &self.content_sha256 {
            validate_sha256(label, hash)?;
        }
        for (name, value) in [
            ("sequence number", self.sequence_number),
            ("file sequence number", self.file_sequence_number),
        ] {
            if value.is_some_and(|value| value < 0) {
                return Err(CdfError::contract(format!(
                    "{label} {name} must be nonnegative"
                )));
            }
        }
        if self.sort_order_id.is_some_and(|value| value < 0) {
            return Err(CdfError::contract(format!(
                "{label} sort order id must be nonnegative"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergFileFormat {
    Parquet,
    Avro,
    Orc,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergDeleteFile {
    pub path: String,
    pub format: IcebergFileFormat,
    pub content: IcebergDeleteContent,
    pub file_size_bytes: u64,
    pub object_generation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    pub partition_spec_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub equality_field_ids: Vec<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,
}

impl IcebergDeleteFile {
    fn validate(&self) -> Result<()> {
        validate_object_location("Iceberg delete file", &self.path)?;
        require_text("Iceberg delete file generation", &self.object_generation)?;
        if self.file_size_bytes == 0 {
            return Err(CdfError::contract(
                "Iceberg delete file size must be nonzero",
            ));
        }
        if self.format != IcebergFileFormat::Parquet {
            return Err(CdfError::contract(
                "Iceberg source currently supports Parquet delete files only",
            ));
        }
        if self.partition_spec_id < 0 {
            return Err(CdfError::contract(
                "Iceberg delete file partition spec id must be nonnegative",
            ));
        }
        if let Some(hash) = &self.content_sha256 {
            validate_sha256("Iceberg delete file", hash)?;
        }
        if self.sequence_number.is_some_and(|value| value < 0)
            || self.file_sequence_number.is_some_and(|value| value < 0)
        {
            return Err(CdfError::contract(
                "Iceberg delete file sequence numbers must be nonnegative",
            ));
        }
        if !strictly_increasing_positive(&self.equality_field_ids)
            && !self.equality_field_ids.is_empty()
        {
            return Err(CdfError::contract(
                "Iceberg equality-delete field ids must be positive and strictly increasing",
            ));
        }
        match self.content {
            IcebergDeleteContent::Position if !self.equality_field_ids.is_empty() => {
                return Err(CdfError::contract(
                    "Iceberg position delete cannot carry equality field ids",
                ));
            }
            IcebergDeleteContent::Equality if self.equality_field_ids.is_empty() => {
                return Err(CdfError::contract(
                    "Iceberg equality delete requires equality field ids",
                ));
            }
            _ => {}
        }
        if let Some(path) = &self.referenced_data_file {
            validate_object_location("Iceberg referenced data file", path)?;
            if self.content != IcebergDeleteContent::Position {
                return Err(CdfError::contract(
                    "only an Iceberg position delete may name a referenced data file",
                ));
            }
        }
        Ok(())
    }

    fn canonical_key(&self) -> (&str, IcebergDeleteContent, u64) {
        (&self.path, self.content, self.file_size_bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergDeleteContent {
    Position,
    Equality,
}

/// A self-verifying canonical JSON fragment used for Iceberg schema/spec/name-map authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergJsonAuthority {
    pub content_sha256: String,
    pub value: serde_json::Value,
}

impl IcebergJsonAuthority {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let authority = Self {
            content_sha256: artifact_hash(&value)?,
            value,
        };
        authority.validate("Iceberg JSON authority")?;
        Ok(authority)
    }

    fn validate(&self, label: &str) -> Result<()> {
        validate_sha256(label, &self.content_sha256)?;
        if !self.value.is_object() && !self.value.is_array() {
            return Err(CdfError::contract(format!(
                "{label} must contain a JSON object or array"
            )));
        }
        if artifact_hash(&self.value)? != self.content_sha256 {
            return Err(CdfError::contract(format!(
                "{label} content does not match its SHA-256 identity"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergReaderRequirements {
    pub reader_protocol: String,
    pub reader_version: String,
    pub required_capabilities: BTreeSet<String>,
}

impl IcebergReaderRequirements {
    fn validate(&self) -> Result<()> {
        require_token("Iceberg reader protocol", &self.reader_protocol)?;
        require_token("Iceberg reader version", &self.reader_version)?;
        for capability in &self.required_capabilities {
            require_token("Iceberg reader capability", capability)?;
        }
        Ok(())
    }
}

fn validate_object_location(label: &str, value: &str) -> Result<()> {
    require_text(label, value)?;
    if value.contains(['?', '#']) {
        return Err(CdfError::contract(format!(
            "{label} cannot contain a query or fragment; signed URLs and credential-bearing locations are forbidden in tasks"
        )));
    }
    if let Some((_, remainder)) = value.split_once("://") {
        let authority = remainder.split('/').next().unwrap_or(remainder);
        if authority.contains('@') {
            return Err(CdfError::contract(format!(
                "{label} cannot contain URI user information"
            )));
        }
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::contract(format!(
            "{label} requires a sha256: content identity"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::contract(format!(
            "{label} contains an invalid SHA-256 identity"
        )));
    }
    Ok(())
}

fn require_text(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::contract(format!(
            "{label} must be non-empty text without control characters"
        )));
    }
    Ok(())
}

fn require_token(label: &str, value: &str) -> Result<()> {
    require_text(label, value)?;
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
    }) {
        return Err(CdfError::contract(format!(
            "{label} must be a canonical ASCII token"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use cdf_kernel::{
        COMPILED_SCAN_INTENT_VERSION, ContentStoreNamespace, SOURCE_POSITION_VERSION,
        TableSnapshotSelector,
    };
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_runtime::FixedSpillBudget;
    use cdf_task_store::{ExternalTaskStore, TaskSetLimits};
    use tempfile::TempDir;

    use super::*;

    fn snapshot() -> TableSnapshotPosition {
        TableSnapshotPosition {
            version: SOURCE_POSITION_VERSION,
            protocol: "iceberg".to_owned(),
            catalog: "filesystem:/warehouse".to_owned(),
            namespace: vec!["analytics".to_owned()],
            table: "events".to_owned(),
            selector: TableSnapshotSelector::Snapshot { snapshot_id: 7 },
            snapshot_id: 7,
            sequence_number: 3,
            parent_snapshot_id: Some(6),
            metadata_location: "file:///warehouse/analytics/events/metadata/v3.json".to_owned(),
            metadata_generation: "sha256:metadata".to_owned(),
        }
    }

    fn authority() -> IcebergTaskSetAuthority {
        IcebergTaskSetAuthority {
            version: ICEBERG_TASK_SET_AUTHORITY_VERSION,
            snapshot: snapshot(),
            table_format_version: 2,
            schemas: BTreeMap::from([(
                2,
                IcebergJsonAuthority::new(serde_json::json!({
                    "type": "struct",
                    "schema-id": 2,
                    "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
                }))
                .unwrap(),
            )]),
            projected_field_ids: vec![1],
            partition_specs: BTreeMap::from([(
                0,
                IcebergJsonAuthority::new(serde_json::json!({
                    "spec-id": 0,
                    "fields": []
                }))
                .unwrap(),
            )]),
            name_mapping: None,
            case_sensitive: true,
            scan_intent: CompiledScanIntent {
                version: COMPILED_SCAN_INTENT_VERSION,
                projection: Some(vec!["id".to_owned()]),
                predicates: Vec::new(),
                limit: None,
                order_by: Vec::new(),
            },
            reader: IcebergReaderRequirements {
                reader_protocol: "cdf-iceberg-parquet".to_owned(),
                reader_version: "1.0.0".to_owned(),
                required_capabilities: BTreeSet::from([
                    "position-deletes".to_owned(),
                    "schema-evolution".to_owned(),
                ]),
            },
        }
    }

    fn task() -> IcebergScanTask {
        IcebergScanTask {
            version: ICEBERG_SCAN_TASK_VERSION,
            canonical_ordinal: 4,
            authority_sha256: authority().content_sha256().unwrap(),
            data_file: IcebergDataFile {
                path: "s3://bucket/data/0004.parquet".to_owned(),
                format: IcebergFileFormat::Parquet,
                file_size_bytes: 1024,
                range_start: 0,
                range_length: 1024,
                object_generation: "version:v4".to_owned(),
                content_sha256: None,
                record_count: Some(10),
                sequence_number: Some(3),
                file_sequence_number: Some(3),
                sort_order_id: Some(0),
                first_row_id: None,
            },
            schema_id: 2,
            partition_spec_id: 0,
            partition_values: Vec::new(),
            deletes: vec![IcebergDeleteFile {
                path: "s3://bucket/delete/0004.parquet".to_owned(),
                format: IcebergFileFormat::Parquet,
                content: IcebergDeleteContent::Position,
                file_size_bytes: 128,
                object_generation: "version:d4".to_owned(),
                content_sha256: None,
                partition_spec_id: 0,
                record_count: Some(1),
                sequence_number: Some(4),
                file_sequence_number: Some(4),
                equality_field_ids: Vec::new(),
                referenced_data_file: Some("s3://bucket/data/0004.parquet".to_owned()),
            }],
        }
    }

    #[test]
    fn task_is_self_verifying_and_deterministic() {
        let authority = authority();
        let task = task();
        authority.validate().unwrap();
        task.validate_against(&authority).unwrap();
        assert_eq!(
            task.content_sha256(&authority).unwrap(),
            task.content_sha256(&authority).unwrap()
        );
        let encoded = serde_json::to_vec(&task).unwrap();
        let decoded: IcebergScanTask = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, task);
        assert_eq!(
            decoded.content_sha256(&authority).unwrap(),
            task.content_sha256(&authority).unwrap()
        );
    }

    #[test]
    fn canonical_task_uses_external_store_and_portable_reference() {
        let root = TempDir::new().unwrap();
        let store = ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("planner-artifacts").unwrap(),
        )
        .unwrap();
        let memory: std::sync::Arc<dyn MemoryCoordinator> = std::sync::Arc::new(
            DeterministicMemoryCoordinator::new(128 * 1024, Default::default()).unwrap(),
        );
        let spill = FixedSpillBudget::new(1024 * 1024).unwrap();
        let authority = authority();
        let mut writer = store
            .writer(
                ICEBERG_TASK_SET_TYPE,
                TaskSetLimits {
                    maximum_task_bytes: 16 * 1024,
                    maximum_authority_bytes: 64 * 1024,
                    writer_buffer_bytes: 16 * 1024,
                },
                std::sync::Arc::clone(&memory),
                &spill,
                |output| authority.encode_to(output),
            )
            .unwrap();
        assert_eq!(
            writer.authority_sha256(),
            authority.content_sha256().unwrap()
        );
        let mut expected = task();
        expected.canonical_ordinal = 0;
        expected.append_to(&authority, &mut writer).unwrap();
        let artifact = writer.finalize().unwrap();
        let portable = cdf_runtime::WorkerArtifactReference::from(&artifact.reference);
        portable.validate().unwrap();

        let mut reader = store
            .reader(
                artifact.reference,
                ICEBERG_TASK_SET_TYPE,
                16 * 1024,
                64 * 1024,
                memory,
            )
            .unwrap();
        let decoded_authority: IcebergTaskSetAuthority =
            serde_json::from_slice(reader.authority().payload()).unwrap();
        assert_eq!(decoded_authority, authority);
        let record = reader.next_record().unwrap().unwrap();
        let decoded: IcebergScanTask = serde_json::from_slice(record.payload.payload()).unwrap();
        decoded.validate_against(&decoded_authority).unwrap();
        assert_eq!(decoded, expected);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn task_shape_cannot_represent_secrets_or_plaintext_keys() {
        let encoded =
            String::from_utf8(serde_json::to_vec(&(authority(), task())).unwrap()).unwrap();
        for forbidden in [
            "credentials",
            "bearer",
            "key_metadata",
            "plaintext",
            "signed_url",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "found forbidden field {forbidden}"
            );
        }
        let task_json = serde_json::to_value(task()).unwrap();
        for shared_field in [
            "snapshot",
            "schemas",
            "partition_specs",
            "name_mapping",
            "scan_intent",
            "reader",
        ] {
            assert!(
                task_json.get(shared_field).is_none(),
                "shared authority field {shared_field} was repeated in a per-file task"
            );
        }
    }

    #[test]
    fn signed_location_and_json_tampering_fail_closed() {
        let authority = authority();
        let mut signed = task();
        signed.data_file.path = "https://example.test/data.parquet?token=secret".to_owned();
        assert!(
            signed
                .validate_against(&authority)
                .unwrap_err()
                .message
                .contains("signed URLs")
        );

        let mut tampered = authority;
        tampered.schemas.get_mut(&2).unwrap().value["schema-id"] = serde_json::json!(99);
        assert!(
            tampered
                .validate()
                .unwrap_err()
                .message
                .contains("does not match")
        );
    }

    #[test]
    fn delete_semantics_and_order_fail_closed() {
        let authority = authority();
        let mut equality = task();
        equality.deletes[0].content = IcebergDeleteContent::Equality;
        assert!(
            equality
                .validate_against(&authority)
                .unwrap_err()
                .message
                .contains("requires equality field ids")
        );

        let mut unordered = task();
        unordered.deletes.push(IcebergDeleteFile {
            path: "s3://bucket/delete/0003.parquet".to_owned(),
            ..unordered.deletes[0].clone()
        });
        assert!(
            unordered
                .validate_against(&authority)
                .unwrap_err()
                .message
                .contains("sorted by canonical identity")
        );
    }
}
