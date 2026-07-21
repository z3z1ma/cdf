use std::{collections::BTreeSet, sync::Arc};

use cdf_http::{EgressAllowlist, SecretUri};
use cdf_kernel::{
    CdfError, CompiledScanIntent, DeliveryGuarantee, PartitionAuthority, PlanId, PushdownFidelity,
    PushedPredicate, Result, ScanPlan, ScanPredicate, ScanRequest, WriteDisposition,
};
use cdf_object_access::{FileTransport, FileTransportControl, FileTransportResource};
use cdf_runtime::{ExecutionServices, SourceEgressScope};
use cdf_task_store::{ExternalTaskStore, TaskSetLimits};
use futures_util::TryStreamExt;

use crate::{
    GLUE_TASK_AUTHORITY_VERSION, GLUE_TASK_SET_TYPE, GLUE_TASK_VERSION, GlueCatalogClient,
    GlueGetPartitionsRequest, GlueObjectTask, GlueResourceOptions, GlueSourceOptions,
    GlueStorageDescriptor, GlueTable, GlueTableClass, GlueTaskAuthority, classify_table,
    lake_formation::LakeFormationRuntime, merge_descriptor, planning_index::GluePlanningIndex,
};

pub struct GluePlanningContext {
    pub catalog: Arc<dyn GlueCatalogClient>,
    pub object_access: Arc<dyn FileTransport>,
    pub execution: ExecutionServices,
    pub egress: SourceEgressScope,
    pub task_store: ExternalTaskStore,
    pub cancellation: cdf_runtime::RunCancellation,
    pub lake_formation: Option<LakeFormationRuntime>,
}

pub fn plan_glue_scan(
    descriptor: &cdf_kernel::ResourceDescriptor,
    source: &GlueSourceOptions,
    resource: &GlueResourceOptions,
    table: &GlueTable,
    table_generation: &str,
    request: &ScanRequest,
    context: GluePlanningContext,
) -> Result<ScanPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match compiled Glue resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    let partition_keys = table
        .partition_keys
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let (pushed_predicates, unsupported_predicates, expression) = partition_pushdown(
        &partition_keys,
        resource.partition_expression.as_deref(),
        &request.filters,
    )?;
    let scan_intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: request.projection.clone(),
        predicates: Vec::new(),
        limit: None,
        order_by: Vec::new(),
    };
    let table_descriptor = table
        .storage_descriptor
        .clone()
        .ok_or_else(|| CdfError::data("AWS Glue table omitted its StorageDescriptor"))?;
    let table_mapping = merge_format_options(
        conventional_mapping(table, resource.format.as_deref())?,
        &resource.format_options,
    )?;
    let index = GluePlanningIndex::create(
        &context.task_store,
        context.execution.spill(),
        source.planning_spill_growth_bytes,
    )?;
    let catalog = Arc::clone(&context.catalog);
    let object_access = Arc::clone(&context.object_access);
    let execution = context.execution.clone();
    let egress = context.egress.clone();
    let source_owned = source.clone();
    let resource_owned = resource.clone();
    let table_owned = table.clone();
    let expected_table_generation = table_generation.to_owned();
    let cancellation = context.cancellation.clone();
    let lake_formation = context.lake_formation.clone();
    let mut index = context.execution.run_io(async move {
        populate_index(
            index,
            catalog,
            object_access,
            execution,
            egress,
            source_owned,
            resource_owned,
            table_owned,
            expected_table_generation,
            table_descriptor,
            table_mapping,
            expression,
            cancellation,
            lake_formation,
        )
        .await
    })?;
    let mut writer = context.task_store.writer(
        GLUE_TASK_SET_TYPE,
        TaskSetLimits {
            maximum_task_bytes: source.maximum_task_bytes,
            maximum_authority_bytes: source.maximum_task_authority_bytes,
            writer_buffer_bytes: source.task_writer_buffer_bytes,
        },
        context.execution.memory(),
        context.execution.spill().as_ref(),
    )?;
    let authority = GlueTaskAuthority {
        version: GLUE_TASK_AUTHORITY_VERSION,
        region: source.region.clone(),
        catalog_id: source.catalog_id.clone(),
        database: resource.database.clone(),
        table: resource.table.clone(),
        table_generation: table_generation.to_owned(),
        partition_expression: resource.partition_expression.clone(),
        scan_intent,
    };
    index.for_each_canonical(|ordinal, task| {
        task.validate_against(&authority)?;
        writer.push_with(ordinal, |output| task.encode_to(output))
    })?;
    let estimated_bytes = index.estimated_bytes()?;
    let artifact = writer.finalize(|output| authority.encode_to(output))?;
    if artifact.authority_sha256 != authority.content_sha256()? {
        return Err(CdfError::internal(
            "Glue task authority hash does not match its task-store identity",
        ));
    }
    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("plan-{}", descriptor.resource_id))?,
        request.clone(),
        PartitionAuthority::External(artifact.reference),
        pushed_predicates,
        unsupported_predicates,
        None,
        Some(estimated_bytes),
        delivery_guarantee(descriptor.write_disposition.clone()),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn populate_index(
    mut index: GluePlanningIndex,
    catalog: Arc<dyn GlueCatalogClient>,
    object_access: Arc<dyn FileTransport>,
    execution: ExecutionServices,
    egress: SourceEgressScope,
    source: GlueSourceOptions,
    resource: GlueResourceOptions,
    table: GlueTable,
    expected_table_generation: String,
    table_descriptor: GlueStorageDescriptor,
    table_mapping: crate::GlueFormatMapping,
    expression: Option<String>,
    cancellation: cdf_runtime::RunCancellation,
    lake_formation: Option<LakeFormationRuntime>,
) -> Result<GluePlanningIndex> {
    if table.partition_keys.is_empty() {
        index_descriptor(
            &mut index,
            &object_access,
            &egress,
            &source,
            &table_descriptor,
            table_mapping,
            Vec::new(),
            &cancellation,
            lake_formation.as_ref(),
        )
        .await?;
        revalidate_table(
            &catalog,
            &source,
            &resource,
            &expected_table_generation,
            &cancellation,
        )
        .await?;
        return Ok(index);
    }
    let mut next_token = None;
    let mut seen_tokens = BTreeSet::new();
    let mut observed_partitions = 0_usize;
    loop {
        cancellation.check()?;
        let page = if let Some(governed) = &lake_formation {
            catalog
                .get_unfiltered_partitions(crate::GlueGetUnfilteredPartitionsRequest {
                    region: source.region.clone(),
                    catalog_id: source.catalog_id.clone().or_else(|| table.catalog_id.clone()).ok_or_else(|| {
                        CdfError::data("Lake Formation governed Glue partition request omitted catalog/account id")
                    })?,
                    database: resource.database.clone(),
                    table: resource.table.clone(),
                    expression: expression.clone(),
                    next_token: next_token.clone(),
                    page_size: 1000,
                    requested_columns: governed.authorization().authorized_columns.clone(),
                    all_columns_requested: false,
                    query_id: governed.authorization().query_id.clone(),
                    query_start_unix_seconds: governed
                        .authorization()
                        .query_start_unix_seconds,
                    query_authorization_id: governed.authorization().query_authorization_id.clone(),
                    endpoint: source.endpoint.clone(),
                    credentials: source
                        .credentials
                        .as_ref()
                        .map(|value| SecretUri::new(value.clone()))
                        .transpose()?,
                    maximum_response_bytes: source.maximum_response_bytes,
                    cancellation: cancellation.clone(),
                })
                .await?
        } else {
            catalog
                .get_partitions(GlueGetPartitionsRequest {
                    region: source.region.clone(),
                    catalog_id: source.catalog_id.clone(),
                    database: resource.database.clone(),
                    table: resource.table.clone(),
                    expression: expression.clone(),
                    next_token: next_token.clone(),
                    page_size: 1000,
                    endpoint: source.endpoint.clone(),
                    credentials: source
                        .credentials
                        .as_ref()
                        .map(|value| SecretUri::new(value.clone()))
                        .transpose()?,
                    maximum_response_bytes: source.maximum_response_bytes,
                    cancellation: cancellation.clone(),
                })
                .await?
        };
        // The transport lease accounts the encoded response only until JSON decoding completes.
        // Retain a conservative model reservation while this bounded page is classified and
        // expanded into spill-backed object tasks; no catalog-sized Vec escapes this scope.
        let page_model_bytes = page
            .bytes_read
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| CdfError::data("Glue partition page byte estimate overflowed"))?;
        let _page_model = cdf_memory::reserve(
            execution.memory(),
            cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "glue-partition-page",
                    cdf_memory::MemoryClass::Control,
                )?,
                page_model_bytes,
            )?,
        )
        .await?;
        observed_partitions = observed_partitions
            .checked_add(page.partitions.len())
            .ok_or_else(|| CdfError::data("Glue partition count overflowed usize"))?;
        if observed_partitions > source.maximum_partitions {
            return Err(CdfError::data(format!(
                "Glue partition planning exceeded configured maximum_partitions {}; narrow partition_expression or raise the bound",
                source.maximum_partitions
            )));
        }
        for partition in page.partitions {
            if partition.values.len() != table.partition_keys.len() {
                return Err(CdfError::data(format!(
                    "Glue partition has {} values for {} partition keys",
                    partition.values.len(),
                    table.partition_keys.len()
                )));
            }
            let descriptor =
                merge_descriptor(&table_descriptor, partition.storage_descriptor.as_ref())?;
            let mut partition_table = table.clone();
            partition_table.storage_descriptor = Some(descriptor.clone());
            let mapping = conventional_mapping(&partition_table, resource.format.as_deref())?;
            index_descriptor(
                &mut index,
                &object_access,
                &egress,
                &source,
                &descriptor,
                merge_format_options(mapping, &resource.format_options)?,
                partition.values.into_iter().map(Some).collect(),
                &cancellation,
                lake_formation.as_ref(),
            )
            .await?;
        }
        next_token = advance_page_token(page.next_token, &mut seen_tokens)?;
        if next_token.is_none() {
            break;
        }
    }
    revalidate_table(
        &catalog,
        &source,
        &resource,
        &expected_table_generation,
        &cancellation,
    )
    .await?;
    Ok(index)
}

fn advance_page_token(
    token: Option<String>,
    seen: &mut BTreeSet<String>,
) -> Result<Option<String>> {
    if let Some(token) = &token
        && !seen.insert(token.clone())
    {
        return Err(CdfError::data(
            "Glue GetPartitions repeated a continuation token",
        ));
    }
    Ok(token)
}

async fn revalidate_table(
    catalog: &Arc<dyn GlueCatalogClient>,
    source: &GlueSourceOptions,
    resource: &GlueResourceOptions,
    expected_generation: &str,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    let observed = catalog
        .get_table(crate::GlueGetTableRequest {
            region: source.region.clone(),
            catalog_id: source.catalog_id.clone(),
            database: resource.database.clone(),
            table: resource.table.clone(),
            endpoint: source.endpoint.clone(),
            credentials: source
                .credentials
                .as_ref()
                .map(|value| SecretUri::new(value.clone()))
                .transpose()?,
            maximum_response_bytes: source.maximum_response_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    if crate::model::table_generation(&observed.table)? != expected_generation {
        return Err(CdfError::data(
            "Glue table generation changed during partition/object planning; retry to compile one coherent catalog observation",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn index_descriptor(
    index: &mut GluePlanningIndex,
    object_access: &Arc<dyn FileTransport>,
    egress: &SourceEgressScope,
    source: &GlueSourceOptions,
    descriptor: &GlueStorageDescriptor,
    mapping: crate::GlueFormatMapping,
    partition_values: Vec<Option<String>>,
    cancellation: &cdf_runtime::RunCancellation,
    lake_formation: Option<&LakeFormationRuntime>,
) -> Result<()> {
    let location = descriptor
        .location
        .as_ref()
        .ok_or_else(|| CdfError::data("Glue storage descriptor omitted Location"))?;
    let resource = transport_resource(location, source, lake_formation, &partition_values)?;
    let remaining = source
        .maximum_objects
        .saturating_sub(usize::try_from(index.object_count()?).unwrap_or(usize::MAX));
    if remaining == 0 {
        return Err(CdfError::data(format!(
            "Glue object planning reached configured maximum_objects {}; narrow the table extent or raise the bound",
            source.maximum_objects
        )));
    }
    let control = FileTransportControl::new(cancellation.clone(), None);
    let mut objects =
        object_access.list(egress, &resource, remaining.saturating_add(1), &control)?;
    while let Some(identity) = objects.try_next().await? {
        cancellation.check()?;
        if hidden_or_directory(&identity.location, identity.size_bytes) {
            continue;
        }
        if index.object_count()? >= u64::try_from(source.maximum_objects).unwrap_or(u64::MAX) {
            return Err(CdfError::data(format!(
                "Glue object planning exceeded configured maximum_objects {}; narrow the table extent or raise the bound",
                source.maximum_objects
            )));
        }
        let file = identity.file_position_evidence()?;
        let task = GlueObjectTask {
            version: GLUE_TASK_VERSION,
            canonical_ordinal: 0,
            file,
            format: mapping.clone(),
            data_columns: descriptor
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            partition_values: partition_values.clone(),
        };
        index.insert(&task)?;
    }
    Ok(())
}

fn transport_resource(
    location: &str,
    source: &GlueSourceOptions,
    lake_formation: Option<&LakeFormationRuntime>,
    partition_values: &[Option<String>],
) -> Result<FileTransportResource> {
    let mut resource = FileTransportResource::remote_url(location.to_owned())
        .with_egress_allowlist(if source.egress_allowlist.is_empty() {
            EgressAllowlist::allow_any()
        } else {
            EgressAllowlist::from_hosts(source.egress_allowlist.clone())
        });
    if let Some(lake_formation) = lake_formation {
        resource = resource
            .with_runtime_aws_credentials(lake_formation.binding(location, partition_values)?)?;
    } else if let Some(reference) = &source.object_credentials {
        resource = resource.with_credentials(SecretUri::new(reference.clone())?);
    }
    Ok(resource)
}

fn hidden_or_directory(path: &str, size: Option<u64>) -> bool {
    if path.ends_with('/') && size.unwrap_or(0) == 0 {
        return true;
    }
    path.rsplit('/')
        .next()
        .is_some_and(|name| name.starts_with('.') || name.starts_with('_'))
}

fn conventional_mapping(
    table: &GlueTable,
    override_format: Option<&str>,
) -> Result<crate::GlueFormatMapping> {
    match classify_table(table, override_format)? {
        GlueTableClass::Conventional(mapping) => Ok(mapping),
        GlueTableClass::Iceberg => Err(CdfError::contract(
            "Glue table is Iceberg; configure source kind `iceberg` with catalog.kind = `glue`",
        )),
        GlueTableClass::Delta => Err(CdfError::contract(
            "Glue table is Delta; use the Delta source or query it through Athena/Trino",
        )),
        GlueTableClass::Hudi => Err(CdfError::contract(
            "Glue table is Hudi; use the Hudi source or query it through Athena/Trino",
        )),
        GlueTableClass::View => Err(CdfError::contract(
            "Glue object is a view; execute it through Athena/Trino rather than the external-table source",
        )),
        GlueTableClass::Federated => Err(CdfError::contract(
            "Glue object is federated/JDBC-backed; use the owning database source or Athena/Trino",
        )),
        GlueTableClass::Stream => Err(CdfError::contract(
            "Glue object describes a stream; use the owning Kinesis/DynamoDB streaming source",
        )),
        GlueTableClass::UnsupportedSerde { serde } => Err(CdfError::contract(format!(
            "Glue table uses unsupported SerDe `{serde}`; specify an exact registered format override or use Athena/Trino"
        ))),
    }
}

fn merge_format_options(
    mut mapping: crate::GlueFormatMapping,
    overrides: &serde_json::Value,
) -> Result<crate::GlueFormatMapping> {
    let mapping_options = mapping
        .options
        .as_object_mut()
        .ok_or_else(|| CdfError::internal("Glue format mapping options are not an object"))?;
    let overrides = overrides
        .as_object()
        .ok_or_else(|| CdfError::contract("Glue format_options override must be an object"))?;
    mapping_options.extend(overrides.clone());
    Ok(mapping)
}

fn partition_pushdown(
    partition_keys: &BTreeSet<String>,
    configured: Option<&str>,
    predicates: &[ScanPredicate],
) -> Result<(Vec<PushedPredicate>, Vec<ScanPredicate>, Option<String>)> {
    let mut pushed = Vec::new();
    let mut unsupported = Vec::new();
    let mut terms = configured
        .map(str::to_owned)
        .into_iter()
        .collect::<Vec<_>>();
    for predicate in predicates {
        predicate.canonical_expression.validate()?;
        let Some((column, _, literal)) = predicate.canonical_expression.comparison() else {
            unsupported.push(predicate.clone());
            continue;
        };
        let Some(operator) = predicate.canonical_expression.comparison_operator() else {
            unsupported.push(predicate.clone());
            continue;
        };
        if !partition_keys.contains(column) || !is_glue_expression_identifier(column) {
            unsupported.push(predicate.clone());
            continue;
        }
        terms.push(format!("{} {operator} {}", column, glue_literal(literal)?));
        pushed.push(PushedPredicate {
            predicate: predicate.clone(),
            fidelity: PushdownFidelity::Exact,
        });
    }
    let expression = (!terms.is_empty()).then(|| terms.join(" AND "));
    Ok((pushed, unsupported, expression))
}

fn is_glue_expression_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn glue_literal(value: &cdf_kernel::ExpressionLiteral) -> Result<String> {
    Ok(match value {
        cdf_kernel::ExpressionLiteral::Boolean(value) => value.to_string(),
        cdf_kernel::ExpressionLiteral::Signed(value) => value.to_string(),
        cdf_kernel::ExpressionLiteral::Unsigned(value) => value.to_string(),
        cdf_kernel::ExpressionLiteral::Float64Bits(bits) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() {
                return Err(CdfError::contract("Glue predicate floats must be finite"));
            }
            value.to_string()
        }
        cdf_kernel::ExpressionLiteral::String(value) => {
            format!("'{}'", value.replace('\'', "''"))
        }
        cdf_kernel::ExpressionLiteral::Null | cdf_kernel::ExpressionLiteral::StringList(_) => {
            return Err(CdfError::contract(
                "Glue partition predicate supports scalar boolean, numeric, or string literals",
            ));
        }
        _ => {
            return Err(CdfError::contract(
                "Glue partition predicate uses an unsupported literal kind",
            ));
        }
    })
}

fn delivery_guarantee(disposition: WriteDisposition) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_rejects_repeated_tokens() {
        let mut seen = BTreeSet::new();
        assert_eq!(
            advance_page_token(Some("next-1".to_owned()), &mut seen).unwrap(),
            Some("next-1".to_owned())
        );
        assert!(
            advance_page_token(Some("next-1".to_owned()), &mut seen)
                .unwrap_err()
                .message
                .contains("repeated")
        );
        assert_eq!(advance_page_token(None, &mut seen).unwrap(), None);
    }

    #[test]
    fn partition_pushdown_emits_documented_glue_syntax_only() {
        let partition_keys = BTreeSet::from(["year".to_owned(), "unsafe-name".to_owned()]);
        let year = ScanPredicate::new(
            cdf_kernel::PredicateId::new("year-filter").unwrap(),
            "year = '2026'",
        )
        .unwrap();
        let unsafe_name = ScanPredicate::from_expression(
            cdf_kernel::PredicateId::new("unsafe-name-filter").unwrap(),
            "unsafe-name = 'x'",
            cdf_kernel::Expression::call(
                "eq",
                vec![
                    cdf_kernel::ExpressionNode::Column {
                        name: "unsafe-name".to_owned(),
                    },
                    cdf_kernel::ExpressionNode::Literal {
                        value: cdf_kernel::ExpressionLiteral::String("x".to_owned()),
                    },
                ],
            ),
        )
        .unwrap();

        let (pushed, unsupported, expression) =
            partition_pushdown(&partition_keys, None, &[year, unsafe_name]).unwrap();

        assert_eq!(pushed.len(), 1);
        assert_eq!(unsupported.len(), 1);
        assert_eq!(expression.as_deref(), Some("year = '2026'"));
    }
}
