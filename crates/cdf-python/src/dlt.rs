use std::collections::BTreeMap;

use crate::internal::*;
use crate::*;
use cdf_kernel::{
    CheckpointStore, CompositePosition, ContractRef, CursorOrderingClaim, CursorPosition,
    CursorSpec, CursorValue, ForeignState, PipelineId,
};
use serde_json::{Value, json};

pub const DLT_METADATA_ATTR: &str = "__cdf_dlt_metadata__";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DltBridgeObjectKind {
    Resource,
    Source,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DltBridgeMappingStatus {
    Mapped,
    PreviewOnly,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DltBridgeMappingEntry {
    pub dlt_feature: String,
    pub cdf_mapping: String,
    pub status: DltBridgeMappingStatus,
    pub note: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DltBridgeMappingTable {
    pub entries: Vec<DltBridgeMappingEntry>,
}

impl DltBridgeMappingTable {
    pub fn mapped(
        &mut self,
        dlt_feature: impl Into<String>,
        cdf_mapping: impl Into<String>,
        note: impl Into<String>,
    ) {
        self.entries.push(DltBridgeMappingEntry {
            dlt_feature: dlt_feature.into(),
            cdf_mapping: cdf_mapping.into(),
            status: DltBridgeMappingStatus::Mapped,
            note: note.into(),
        });
    }

    pub fn preview_only(
        &mut self,
        dlt_feature: impl Into<String>,
        cdf_mapping: impl Into<String>,
        note: impl Into<String>,
    ) {
        self.entries.push(DltBridgeMappingEntry {
            dlt_feature: dlt_feature.into(),
            cdf_mapping: cdf_mapping.into(),
            status: DltBridgeMappingStatus::PreviewOnly,
            note: note.into(),
        });
    }

    pub fn unsupported(
        &mut self,
        dlt_feature: impl Into<String>,
        cdf_mapping: impl Into<String>,
        note: impl Into<String>,
    ) {
        self.entries.push(DltBridgeMappingEntry {
            dlt_feature: dlt_feature.into(),
            cdf_mapping: cdf_mapping.into(),
            status: DltBridgeMappingStatus::Unsupported,
            note: note.into(),
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DltWriteDisposition {
    Append,
    Replace,
    Merge,
    Skip,
}

impl DltWriteDisposition {
    fn to_cdf(&self) -> Option<WriteDisposition> {
        match self {
            Self::Append => Some(WriteDisposition::Append),
            Self::Replace => Some(WriteDisposition::Replace),
            Self::Merge => Some(WriteDisposition::Merge),
            Self::Skip => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Replace => "replace",
            Self::Merge => "merge",
            Self::Skip => "skip",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DltWriteDispositionHint {
    pub disposition: DltWriteDisposition,
    pub strategy: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DltIncrementalHint {
    pub cursor_path: String,
    pub ordering: CursorOrderingClaim,
    pub lag_tolerance_ms: u64,
    pub initial_value: Option<Value>,
    pub end_value: Option<Value>,
    pub row_order: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DltSchemaContractHint {
    pub mode: String,
    pub unsupported_modes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DltBridgeMetadata {
    pub kind: DltBridgeObjectKind,
    pub name: Option<String>,
    pub table_name: Option<String>,
    pub source_name: Option<String>,
    pub primary_key: Option<Vec<String>>,
    pub merge_key: Option<Vec<String>>,
    pub incremental: Option<DltIncrementalHint>,
    pub write_disposition: Option<DltWriteDispositionHint>,
    pub schema_contract: Option<DltSchemaContractHint>,
    pub selected: bool,
    pub parallelized: bool,
}

impl DltBridgeMetadata {
    pub fn selected_for_source_expansion(&self) -> bool {
        self.selected
            && !matches!(
                self.write_disposition
                    .as_ref()
                    .map(|hint| &hint.disposition),
                Some(DltWriteDisposition::Skip)
            )
    }

    pub fn resource_id_hint(&self) -> Option<&str> {
        self.table_name
            .as_deref()
            .or(self.name.as_deref())
            .filter(|value| !value.trim().is_empty())
    }

    pub fn source_scope(&self) -> Option<ScopeKey> {
        self.source_name
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .map(|source| ScopeKey::Stream {
                name: format!("dlt_source:{source}"),
            })
    }

    pub fn mapping_table(&self) -> DltBridgeMappingTable {
        let mut table = DltBridgeMappingTable::default();
        table.preview_only(
            "live dlt runtime",
            "deterministic shim metadata",
            "CDF preview consumes explicit metadata fixtures and does not import or delegate to a live dlt runtime.",
        );
        table.preview_only(
            "dlt.current.state writes",
            "CheckpointStore committed-head view",
            "CDF exposes current committed state by scope; staged state changes are checkpoint deltas and still require package receipts before becoming visible.",
        );
        table.unsupported(
            "dlt destination delegation",
            "cdf destination protocol",
            "CDF does not call dlt destinations; batches leave Python through CDF descriptors and normal package/checkpoint flow.",
        );
        if let Some(primary_key) = &self.primary_key {
            table.mapped(
                "primary_key",
                "ResourceDescriptor.primary_key",
                format!(
                    "{} key field(s) preserved in descriptor order.",
                    primary_key.len()
                ),
            );
        }
        if let Some(merge_key) = &self.merge_key {
            table.mapped(
                "merge_key",
                "ResourceDescriptor.merge_key",
                format!(
                    "{} merge key field(s) preserved in descriptor order.",
                    merge_key.len()
                ),
            );
        }
        if let Some(incremental) = &self.incremental {
            table.mapped(
                "dlt.sources.incremental cursor_path",
                "ResourceDescriptor.cursor",
                format!(
                    "Cursor field `{}` maps to a CDF cursor with {:?} ordering and {} ms lag.",
                    incremental.cursor_path, incremental.ordering, incremental.lag_tolerance_ms
                ),
            );
            if incremental.initial_value.is_some() || incremental.end_value.is_some() {
                table.preview_only(
                    "incremental initial/end value",
                    "checkpoint input/output positions",
                    "Initial and end bounds are recorded as preview metadata; CDF checkpoint commits still own durable cursor advancement.",
                );
            }
        }
        if let Some(disposition) = &self.write_disposition {
            match disposition.disposition.to_cdf() {
                Some(_) => table.mapped(
                    "write_disposition",
                    "ResourceDescriptor.write_disposition",
                    format!(
                        "dlt `{}` maps to the same CDF disposition.",
                        disposition.disposition.as_str()
                    ),
                ),
                None => table.unsupported(
                    "write_disposition=skip",
                    "no CDF write disposition",
                    "Skipped dlt resources are not converted into CDF resources in this preview.",
                ),
            }
            if let Some(strategy) = &disposition.strategy {
                table.preview_only(
                    "write_disposition.strategy",
                    "migration-table note",
                    format!(
                        "dlt strategy `{strategy}` is documented but not emulated by the CDF preview."
                    ),
                );
            }
        }
        if let Some(contract) = &self.schema_contract {
            table.mapped(
                "schema_contract",
                "ResourceDescriptor.contract",
                format!(
                    "dlt contract mode `{}` maps to a deterministic CDF contract reference.",
                    contract.mode
                ),
            );
            for mode in &contract.unsupported_modes {
                table.preview_only(
                    "schema_contract mode",
                    "migration-table note",
                    format!("dlt contract mode `{mode}` has no CDF preview equivalent."),
                );
            }
        }
        table
    }

    pub fn apply_to_descriptor(&self, descriptor: &mut ResourceDescriptor) -> Result<()> {
        if !self.selected {
            return Err(CdfError::contract(
                "dlt resource is not selected and cannot be previewed as a CDF resource",
            ));
        }
        if let Some(disposition) = &self.write_disposition {
            descriptor.write_disposition = disposition.disposition.to_cdf().ok_or_else(|| {
                CdfError::contract(
                    "dlt write_disposition=skip has no CDF resource descriptor mapping",
                )
            })?;
        }
        if let Some(primary_key) = &self.primary_key {
            descriptor.primary_key = primary_key.clone();
        }
        descriptor.merge_key = match &self.merge_key {
            Some(merge_key) => merge_key.clone(),
            None if descriptor.write_disposition == WriteDisposition::Merge => {
                descriptor.primary_key.clone()
            }
            None => descriptor.merge_key.clone(),
        };
        if let Some(incremental) = &self.incremental {
            descriptor.cursor = Some(CursorSpec {
                field: incremental.cursor_path.clone(),
                ordering: incremental.ordering.clone(),
                lag_tolerance_ms: incremental.lag_tolerance_ms,
            });
        }
        if let Some(contract) = &self.schema_contract {
            let contract_ref =
                contract_ref_for(descriptor.resource_id.as_str(), contract.mode.as_str())?;
            descriptor.contract = Some(contract_ref);
            descriptor.trust_level = TrustLevel::Governed;
        }
        descriptor.state_scope = ScopeKey::Resource;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DltBridgeSummary {
    pub metadata: DltBridgeMetadata,
    pub mapping_table: DltBridgeMappingTable,
    pub stream: PythonStreamSummary,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DltCurrentStateView {
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub resource_scope: ScopeKey,
    pub source_scope: Option<ScopeKey>,
    pub resource_state: Value,
    pub source_state: Option<Value>,
    pub writable: bool,
    pub note: String,
}

pub fn extract_dlt_metadata(object: &Bound<'_, PyAny>) -> Result<Option<DltBridgeMetadata>> {
    if object.hasattr(DLT_METADATA_ATTR).map_err(py_error)? {
        let metadata = object.getattr(DLT_METADATA_ATTR).map_err(py_error)?;
        return parse_dlt_metadata(&metadata).map(Some);
    }
    if object.hasattr("__cdf_resource__").map_err(py_error)? {
        return extract_cdf_resource_metadata(object).map(Some);
    }
    Ok(None)
}

pub fn dlt_current_state_view(
    store: &dyn CheckpointStore,
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    metadata: &DltBridgeMetadata,
) -> Result<DltCurrentStateView> {
    let resource_scope = ScopeKey::Resource;
    let resource_state = match store.head(&pipeline_id, &resource_id, &resource_scope)? {
        Some(head) => position_to_dlt_state(&head.delta.output_position)?,
        None => Value::Object(Default::default()),
    };
    let source_scope = metadata.source_scope();
    let source_state = match &source_scope {
        Some(scope) => match store.head(&pipeline_id, &resource_id, scope)? {
            Some(head) => Some(position_to_dlt_state(&head.delta.output_position)?),
            None => Some(Value::Object(Default::default())),
        },
        None => None,
    };

    Ok(DltCurrentStateView {
        pipeline_id,
        resource_id,
        resource_scope,
        source_scope,
        resource_state,
        source_state,
        writable: true,
        note: "Preview state is a view over committed CDF checkpoint heads; writes become durable only through checkpoint deltas committed with receipts.".to_owned(),
    })
}

fn parse_dlt_metadata(metadata: &Bound<'_, PyAny>) -> Result<DltBridgeMetadata> {
    let json_text = python_dict_to_json(metadata.py(), metadata)?;
    let raw: RawDltMetadata = serde_json::from_str(&json_text).map_err(json_error)?;
    Ok(DltBridgeMetadata {
        kind: parse_kind(raw.kind.as_deref())?,
        name: optional_string(raw.name)?,
        table_name: optional_string(raw.table_name)?,
        source_name: optional_string(raw.source_name)?,
        primary_key: optional_string_vec(raw.primary_key)?,
        merge_key: optional_string_vec(raw.merge_key)?,
        incremental: raw.incremental.map(parse_incremental).transpose()?,
        write_disposition: raw
            .write_disposition
            .map(parse_write_disposition)
            .transpose()?,
        schema_contract: raw.schema_contract.map(parse_contract).transpose()?,
        selected: raw.selected.unwrap_or(true),
        parallelized: raw.parallelized.unwrap_or(false),
    })
}

fn extract_cdf_resource_metadata(object: &Bound<'_, PyAny>) -> Result<DltBridgeMetadata> {
    Ok(DltBridgeMetadata {
        kind: DltBridgeObjectKind::Resource,
        name: string_attr(object, "__cdf_name__")?,
        table_name: None,
        source_name: None,
        primary_key: Some(tuple_attr(object, "__cdf_primary_key__")?),
        merge_key: Some(tuple_attr(object, "__cdf_merge_key__")?),
        incremental: string_attr(object, "__cdf_cursor__")?.map(|cursor_path| DltIncrementalHint {
            cursor_path,
            ordering: CursorOrderingClaim::Inexact,
            lag_tolerance_ms: 0,
            initial_value: None,
            end_value: None,
            row_order: None,
        }),
        write_disposition: None,
        schema_contract: None,
        selected: true,
        parallelized: bool_attr(object, "__cdf_parallel__")?.unwrap_or(false),
    })
}

fn position_to_dlt_state(position: &SourcePosition) -> Result<Value> {
    match position {
        SourcePosition::Cursor(cursor) => Ok(json!({
            "last_value": cursor_value_to_json(&cursor.value),
            "cursor_path": cursor.field,
        })),
        SourcePosition::Composite(composite) => composite_to_dlt_state(composite),
        SourcePosition::ForeignState(foreign) => foreign_state_to_json(foreign),
        SourcePosition::PageToken(page) => Ok(json!({ "page_token": page.token })),
        SourcePosition::Log(log) => Ok(json!({
            "log": log.log,
            "offset": log.offset,
            "sequence": log.sequence,
        })),
        SourcePosition::FileManifest(manifest) => {
            serde_json::to_value(manifest).map_err(|error| CdfError::data(error.to_string()))
        }
    }
}

fn composite_to_dlt_state(composite: &CompositePosition) -> Result<Value> {
    let mut object = serde_json::Map::new();
    for (key, position) in &composite.positions {
        object.insert(key.clone(), position_to_dlt_state(position)?);
    }
    Ok(Value::Object(object))
}

fn foreign_state_to_json(foreign: &ForeignState) -> Result<Value> {
    if foreign.protocol == "dlt-state-v1" {
        serde_json::from_slice(&foreign.opaque_blob).map_err(|error| {
            CdfError::data(format!(
                "dlt foreign state blob is not valid JSON for protocol dlt-state-v1: {error}"
            ))
        })
    } else {
        Ok(json!({
            "_cdf_foreign_state": {
                "protocol": foreign.protocol,
                "blob_sha256": foreign.blob_sha256,
            }
        }))
    }
}

fn cursor_value_to_json(value: &CursorValue) -> Value {
    match value {
        CursorValue::String(value) => Value::String(value.clone()),
        CursorValue::I64(value) => json!(value),
        CursorValue::U64(value) => json!(value),
        CursorValue::DecimalString(value) => Value::String(value.clone()),
        CursorValue::TimestampMicros { micros, timezone } => json!({
            "micros": micros,
            "timezone": timezone,
        }),
    }
}

fn contract_ref_for(resource_id: &str, mode: &str) -> Result<ContractRef> {
    ContractRef::new(format!(
        "dlt-{}-{}",
        sanitize_id_part(resource_id),
        sanitize_id_part(mode)
    ))
}

#[derive(Deserialize)]
struct RawDltMetadata {
    kind: Option<String>,
    name: Option<Value>,
    table_name: Option<Value>,
    source_name: Option<Value>,
    primary_key: Option<Value>,
    merge_key: Option<Value>,
    incremental: Option<RawIncremental>,
    write_disposition: Option<Value>,
    schema_contract: Option<Value>,
    selected: Option<bool>,
    parallelized: Option<bool>,
}

#[derive(Deserialize)]
struct RawIncremental {
    cursor_path: Option<String>,
    field: Option<String>,
    ordering: Option<String>,
    lag_tolerance_ms: Option<u64>,
    lag_ms: Option<u64>,
    initial_value: Option<Value>,
    end_value: Option<Value>,
    row_order: Option<String>,
}

fn parse_kind(value: Option<&str>) -> Result<DltBridgeObjectKind> {
    match value.unwrap_or("resource") {
        "resource" => Ok(DltBridgeObjectKind::Resource),
        "source" => Ok(DltBridgeObjectKind::Source),
        other => Err(CdfError::contract(format!(
            "unknown dlt bridge metadata kind `{other}`"
        ))),
    }
}

fn parse_incremental(raw: RawIncremental) -> Result<DltIncrementalHint> {
    let cursor_path = raw
        .cursor_path
        .or(raw.field)
        .ok_or_else(|| CdfError::contract("dlt incremental metadata must include cursor_path"))?;
    let ordering = match raw.ordering.as_deref().unwrap_or("inexact") {
        "exact" => CursorOrderingClaim::Exact,
        "inexact" => CursorOrderingClaim::Inexact,
        "unordered" => CursorOrderingClaim::Unordered,
        other => {
            return Err(CdfError::contract(format!(
                "unknown dlt incremental ordering `{other}`"
            )));
        }
    };
    Ok(DltIncrementalHint {
        cursor_path,
        ordering,
        lag_tolerance_ms: raw.lag_tolerance_ms.or(raw.lag_ms).unwrap_or(0),
        initial_value: raw.initial_value,
        end_value: raw.end_value,
        row_order: raw.row_order,
    })
}

fn parse_write_disposition(value: Value) -> Result<DltWriteDispositionHint> {
    let (disposition, strategy) = match value {
        Value::String(value) => (parse_disposition(&value)?, None),
        Value::Object(mut object) => {
            let disposition = object
                .remove("disposition")
                .or_else(|| object.remove("write_disposition"))
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .ok_or_else(|| {
                    CdfError::contract(
                        "dlt write_disposition object must include a string disposition",
                    )
                })?;
            let strategy = object
                .remove("strategy")
                .and_then(|value| value.as_str().map(ToOwned::to_owned));
            (parse_disposition(&disposition)?, strategy)
        }
        Value::Null => (DltWriteDisposition::Append, None),
        other => {
            return Err(CdfError::contract(format!(
                "unsupported dlt write_disposition metadata value `{other}`"
            )));
        }
    };
    Ok(DltWriteDispositionHint {
        disposition,
        strategy,
    })
}

fn parse_disposition(value: &str) -> Result<DltWriteDisposition> {
    match value {
        "append" => Ok(DltWriteDisposition::Append),
        "replace" => Ok(DltWriteDisposition::Replace),
        "merge" => Ok(DltWriteDisposition::Merge),
        "skip" => Ok(DltWriteDisposition::Skip),
        other => Err(CdfError::contract(format!(
            "unsupported dlt write_disposition `{other}`"
        ))),
    }
}

fn parse_contract(value: Value) -> Result<DltSchemaContractHint> {
    match value {
        Value::String(mode) => contract_from_modes([mode]),
        Value::Object(object) => contract_from_modes(
            object
                .values()
                .filter_map(|value| value.as_str().map(ToOwned::to_owned)),
        ),
        Value::Null => contract_from_modes(["evolve".to_owned()]),
        other => Err(CdfError::contract(format!(
            "unsupported dlt schema_contract metadata value `{other}`"
        ))),
    }
}

fn contract_from_modes<I>(modes: I) -> Result<DltSchemaContractHint>
where
    I: IntoIterator<Item = String>,
{
    let mut modes = modes.into_iter().collect::<Vec<_>>();
    if modes.is_empty() {
        modes.push("evolve".to_owned());
    }
    let mode = if modes.iter().any(|mode| mode == "freeze") {
        "freeze"
    } else {
        "evolve"
    };
    let unsupported_modes = modes
        .into_iter()
        .filter(|mode| !matches!(mode.as_str(), "freeze" | "evolve"))
        .collect();
    Ok(DltSchemaContractHint {
        mode: mode.to_owned(),
        unsupported_modes,
    })
}

fn optional_string(value: Option<Value>) -> Result<Option<String>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(other) => Err(CdfError::contract(format!(
            "expected dlt metadata string or null, got `{other}`"
        ))),
    }
}

fn optional_string_vec(value: Option<Value>) -> Result<Option<Vec<String>>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(Some(Vec::new())),
        Some(Value::String(value)) => Ok(Some(vec![value])),
        Some(Value::Array(values)) => values
            .into_iter()
            .map(|value| match value {
                Value::String(value) => Ok(value),
                other => Err(CdfError::contract(format!(
                    "expected dlt key metadata string, got `{other}`"
                ))),
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(other) => Err(CdfError::contract(format!(
            "expected dlt key metadata string, list, or null; got `{other}`"
        ))),
    }
}

fn string_attr(object: &Bound<'_, PyAny>, name: &str) -> Result<Option<String>> {
    match object.getattr(name) {
        Ok(value) if value.is_none() => Ok(None),
        Ok(value) => value.extract::<Option<String>>().map_err(py_error),
        Err(_) => Ok(None),
    }
}

fn bool_attr(object: &Bound<'_, PyAny>, name: &str) -> Result<Option<bool>> {
    match object.getattr(name) {
        Ok(value) if value.is_none() => Ok(None),
        Ok(value) => value.extract::<Option<bool>>().map_err(py_error),
        Err(_) => Ok(None),
    }
}

fn tuple_attr(object: &Bound<'_, PyAny>, name: &str) -> Result<Vec<String>> {
    match object.getattr(name) {
        Ok(value) if value.is_none() => Ok(Vec::new()),
        Ok(value) => value.extract::<Vec<String>>().map_err(py_error),
        Err(_) => Ok(Vec::new()),
    }
}

pub fn fixture_state_delta_position(field: &str, value: CursorValue) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: field.to_owned(),
        value,
    })
}

pub fn fixture_dlt_foreign_state(state: &Value) -> Result<SourcePosition> {
    let opaque_blob =
        serde_json::to_vec(state).map_err(|error| CdfError::data(error.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(&opaque_blob);
    Ok(SourcePosition::ForeignState(ForeignState {
        version: 1,
        protocol: "dlt-state-v1".to_owned(),
        blob_sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
        opaque_blob,
    }))
}

pub fn composite_dlt_state(parts: BTreeMap<String, SourcePosition>) -> SourcePosition {
    SourcePosition::Composite(CompositePosition {
        version: 1,
        positions: parts,
    })
}
