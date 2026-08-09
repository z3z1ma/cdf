use cdf_kernel::{
    CdfError, CommitCounts, CommitPlan, CommitSegment, CommitSegmentIterator,
    DeleteApplicationAuthority, DeleteApplicationPolicy, DeliveryGuarantee,
    DestinationCommitRequest, IdempotencySupport, MigrationRecord, PackageContentAuthority,
    PackageSegmentKind, PlanId, Result, RoutedTargetCommitCounts, SegmentAck, WriteDisposition,
};
use cdf_package_contract::PackageReplayInputs;
use duckdb::Connection;

use crate::{
    CDF_ROW_KEY_COLUMN, DESTINATION_ID,
    commit::apply_table_plan,
    mirrors::{
        advance_row_key_allocator, ensure_mirror_tables, find_duplicate_receipt, insert_mirrors,
        next_row_key,
    },
    models::{FieldPlan, ReceiptBuildContext, TargetRef},
    package::{field_plan, persistence_fields, validate_field_names, validate_user_schema_fields},
    receipts::build_receipt,
    sql::{duckdb_error, duckdb_version, parse_target, quote_ident, validate_system_ident},
    table::{create_columns_sql, plan_absent_table, plan_table},
};

struct RoutedOutputPlan {
    binding: cdf_kernel::RouteTargetBinding,
    content: PackageContentAuthority,
    user_fields: Vec<FieldPlan>,
    target: TargetRef,
    table_plan: crate::models::TablePlan,
    unique_index_ddl: Option<String>,
}

#[derive(Clone, Default)]
struct RoutedCounts {
    rows_written: u64,
    inserted: u64,
    updated: u64,
    deleted: u64,
}

impl crate::DuckDbDestination {
    pub(crate) fn plan_routed(&self, commit: &DestinationCommitRequest) -> Result<CommitPlan> {
        let (_, plan) = if self.database_path.exists() {
            let conn = self.open_read_only_connection()?;
            prepare_routed_commit(Some(&conn), commit)?
        } else {
            prepare_routed_commit(None, commit)?
        };
        Ok(plan)
    }

    pub(crate) fn commit_routed(
        &mut self,
        inputs: &PackageReplayInputs,
        segments: CommitSegmentIterator,
    ) -> Result<cdf_runtime::DestinationCommitOutcome> {
        inputs.destination_commit.content.validate_segments(
            inputs
                .destination_commit
                .segments
                .iter()
                .map(|segment| (&segment.segment_id, &segment.kind, segment.row_count)),
        )?;
        if self.sheet.destination.as_str() != DESTINATION_ID {
            return Err(CdfError::internal(
                "DuckDB routed commit destination identity drifted",
            ));
        }
        let _lock = self.acquire_writer_lock()?;
        let conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|error| duckdb_error("begin routed DuckDB transaction", error))?;
        match commit_routed_transaction(self, &conn, inputs, segments) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let rollback = conn.execute_batch("ROLLBACK").map_err(|rollback| {
                    duckdb_error("rollback routed DuckDB transaction", rollback)
                });
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(CdfError::destination(format!(
                        "{}; routed DuckDB rollback also failed: {}",
                        error.message, rollback.message
                    ))),
                }
            }
        }
    }
}

fn commit_routed_transaction(
    destination: &crate::DuckDbDestination,
    conn: &Connection,
    inputs: &PackageReplayInputs,
    segments: CommitSegmentIterator,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let commit = &inputs.destination_commit;
    let PackageContentAuthority::Routed { family, .. } = &commit.content else {
        return Err(CdfError::internal(
            "DuckDB routed commit received ordinary package content",
        ));
    };
    if family.logical_target != commit.target || family.schema_family_hash != inputs.schema_hash {
        return Err(CdfError::data(
            "DuckDB routed target family differs from package commit authority",
        ));
    }
    let (plans, plan) = prepare_routed_commit(Some(conn), commit)?;
    let PackageContentAuthority::Routed { outputs, .. } = &commit.content else {
        unreachable!("routed commit was validated above")
    };
    if plan.target != commit.target || plan.disposition != commit.disposition {
        return Err(CdfError::internal(
            "DuckDB routed plan changed its commit target or disposition",
        ));
    }
    let segment_acks = commit
        .segments
        .iter()
        .map(|segment| SegmentAck {
            kind: segment.kind,
            segment_id: segment.segment_id.clone(),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect::<Vec<_>>();
    if let Some(receipt) =
        find_duplicate_receipt(conn, commit, &plan, &inputs.schema_hash, &segment_acks)?
    {
        conn.execute_batch("ROLLBACK")
            .map_err(|error| duckdb_error("rollback duplicate routed DuckDB transaction", error))?;
        return Ok(cdf_runtime::DestinationCommitOutcome::new(
            receipt,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true },
        ));
    }

    for output in &plans {
        apply_table_plan(conn, &output.table_plan, commit.disposition.clone())?;
        if let Some(ddl) = &output.unique_index_ddl {
            conn.execute_batch(ddl).map_err(|error| {
                duckdb_error("create routed DuckDB key uniqueness authority", error)
            })?;
        }
    }

    let first_row_key = next_row_key(conn)?;
    let mut counts = vec![RoutedCounts::default(); plans.len()];
    let mut observed = Vec::new();
    for (ordinal, segment) in segments.enumerate() {
        let segment = segment?;
        let output_index = output_index_for_segment(outputs, &segment)?;
        apply_segment(
            conn,
            ordinal,
            &plans[output_index],
            &segment,
            first_row_key,
            &mut counts[output_index],
        )?;
        observed.push(segment.state.segment_id);
    }
    let expected = commit
        .segments
        .iter()
        .map(|segment| segment.segment_id.clone())
        .collect::<Vec<_>>();
    if observed != expected {
        return Err(CdfError::data(
            "routed DuckDB segment stream differs from canonical package order",
        ));
    }

    let total_rows = commit.segments.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(segment.row_count)
            .ok_or_else(|| CdfError::data("routed DuckDB row-key range overflowed u64"))
    })?;
    advance_row_key_allocator(
        conn,
        first_row_key,
        first_row_key
            .checked_add(total_rows)
            .ok_or_else(|| CdfError::data("routed DuckDB row-key frontier overflowed u64"))?,
    )?;
    let commit_counts = routed_commit_counts(&plans, counts)?;
    let committed_at_ms = destination.committed_at_ms()?;
    let version = duckdb_version(conn).unwrap_or_else(|_| "unknown".to_owned());
    let receipt = build_receipt(
        commit,
        &plan,
        &inputs.schema_hash,
        &segment_acks,
        commit_counts,
        &ReceiptBuildContext {
            committed_at_ms,
            duckdb_version: &version,
            database_path: &destination.database_path,
            lock_path: &destination.lock_path(),
        },
    )?;
    insert_mirrors(conn, commit, &segment_acks, &receipt, None, None)?;
    conn.execute_batch("COMMIT")
        .map_err(|error| duckdb_error("commit routed DuckDB transaction", error))?;
    Ok(cdf_runtime::DestinationCommitOutcome::new(
        receipt,
        cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
    ))
}

fn prepare_routed_commit(
    conn: Option<&Connection>,
    commit: &DestinationCommitRequest,
) -> Result<(Vec<RoutedOutputPlan>, CommitPlan)> {
    let PackageContentAuthority::Routed { family, outputs } = &commit.content else {
        return Err(CdfError::contract(
            "DuckDB routed planning requires routed package content",
        ));
    };
    if family.logical_target != commit.target {
        return Err(CdfError::data(
            "DuckDB routed target family differs from its logical commit target",
        ));
    }
    let plans = plan_outputs(conn, commit, family, outputs)?;
    let migrations = plans
        .iter()
        .flat_map(|plan| {
            plan.table_plan
                .ddl
                .iter()
                .chain(plan.unique_index_ddl.iter())
                .map(move |ddl| (plan.binding.output_binding.clone(), ddl.clone()))
        })
        .enumerate()
        .map(|(index, (binding, ddl))| MigrationRecord {
            migration_id: format!("duckdb-route-{:03}-{}", index + 1, binding.as_str()),
            description: ddl,
        })
        .collect::<Vec<_>>();
    let plan = CommitPlan {
        plan_id: PlanId::new(format!(
            "duckdb-routed:{}:{}",
            commit.target.as_str(),
            commit.idempotency_token.as_str()
        ))?,
        target: commit.target.clone(),
        disposition: commit.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations,
        delivery_guarantee: match commit.disposition {
            WriteDisposition::Append => DeliveryGuarantee::EffectivelyOncePerPackage,
            WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
            WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
            WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
        },
    };
    Ok((plans, plan))
}

fn plan_outputs(
    conn: Option<&Connection>,
    commit: &DestinationCommitRequest,
    family: &cdf_kernel::RouteTargetFamily,
    outputs: &[cdf_kernel::RoutedOutputContentAuthority],
) -> Result<Vec<RoutedOutputPlan>> {
    family
        .bindings
        .iter()
        .zip(outputs)
        .map(|(binding, output)| {
            let schema = output.schema.to_arrow()?;
            validate_user_schema_fields(schema.as_ref())?;
            let user_fields = schema
                .fields()
                .iter()
                .map(|field| field_plan(field.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            validate_field_names(&user_fields)?;
            let persisted_fields = persistence_fields(&user_fields);
            let target = parse_target(&binding.physical_target)?;
            let table_plan = match conn {
                Some(conn) => plan_table(
                    conn,
                    target.clone(),
                    &persisted_fields,
                    commit.disposition.clone(),
                )?,
                None => plan_absent_table(
                    target.clone(),
                    &persisted_fields,
                    commit.disposition.clone(),
                )?,
            };
            let unique_index_ddl = match output.content.as_ref() {
                PackageContentAuthority::Rows { .. } => None,
                PackageContentAuthority::KeyedChanges { key, .. } => {
                    let suffix = binding
                        .output_binding
                        .as_str()
                        .strip_prefix("route_")
                        .unwrap_or(binding.output_binding.as_str());
                    let name = validate_system_ident(&format!("_cdf_route_key_{suffix}"))?;
                    let keys = key
                        .fields
                        .iter()
                        .map(|key| crate::sql::validate_ident(key).map(|key| quote_ident(&key)))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    Some(format!(
                        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({keys})",
                        quote_ident(&name),
                        target.sql_name(),
                    ))
                }
                PackageContentAuthority::Routed { .. } => {
                    return Err(CdfError::data("nested routed content is invalid"));
                }
            };
            Ok(RoutedOutputPlan {
                binding: binding.clone(),
                content: output.content.as_ref().clone(),
                user_fields,
                target,
                table_plan,
                unique_index_ddl,
            })
        })
        .collect()
}

fn output_index_for_segment(
    outputs: &[cdf_kernel::RoutedOutputContentAuthority],
    segment: &CommitSegment,
) -> Result<usize> {
    outputs
        .iter()
        .position(|output| output.segment_ids.contains(&segment.state.segment_id))
        .ok_or_else(|| CdfError::data("routed DuckDB segment has no admitted output binding"))
}

fn apply_segment(
    conn: &Connection,
    ordinal: usize,
    output: &RoutedOutputPlan,
    segment: &CommitSegment,
    first_row_key: u64,
    counts: &mut RoutedCounts,
) -> Result<()> {
    if segment.batches.is_empty() {
        return Err(CdfError::data("routed DuckDB segment contains no batches"));
    }
    let schema = segment.batches[0].schema();
    if segment
        .batches
        .iter()
        .any(|batch| batch.schema().as_ref() != schema.as_ref())
    {
        return Err(CdfError::data(
            "routed DuckDB segment batches do not share one schema",
        ));
    }
    let stage = validate_system_ident(&format!("_cdf_route_stage_{ordinal:08}"))?;
    let stage_fields = schema
        .fields()
        .iter()
        .map(|field| field_plan(field.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE {} ({})",
        quote_ident(&stage),
        create_columns_sql(&stage_fields)
    ))
    .map_err(|error| duckdb_error("create routed DuckDB segment stage", error))?;
    {
        let mut appender = conn
            .appender(stage.as_str())
            .map_err(|error| duckdb_error("open routed DuckDB Arrow appender", error))?;
        for batch in &segment.batches {
            appender
                .append_record_batch(batch.clone())
                .map_err(|error| duckdb_error("append routed DuckDB Arrow batch", error))?;
        }
        appender
            .flush()
            .map_err(|error| duckdb_error("flush routed DuckDB Arrow appender", error))?;
    }
    match (&output.content, segment.state.kind) {
        (PackageContentAuthority::Rows { .. }, PackageSegmentKind::Row) => {
            let written = insert_rows(conn, output, &stage, first_row_key, None)?;
            counts.rows_written = checked_add(counts.rows_written, written)?;
            counts.inserted = checked_add(counts.inserted, written)?;
        }
        (PackageContentAuthority::KeyedChanges { key, .. }, PackageSegmentKind::Upsert) => {
            let existing = matching_key_count(conn, output, &stage, &key.fields)?;
            let written = insert_rows(conn, output, &stage, first_row_key, Some(&key.fields))?;
            counts.rows_written = checked_add(counts.rows_written, written)?;
            counts.updated = checked_add(counts.updated, existing)?;
            counts.inserted = checked_add(
                counts.inserted,
                written.checked_sub(existing).ok_or_else(|| {
                    CdfError::internal("routed DuckDB existing-key count exceeds staged upserts")
                })?,
            )?;
        }
        (
            PackageContentAuthority::KeyedChanges {
                key,
                delete_application,
                ..
            },
            PackageSegmentKind::Delete,
        ) => {
            let deleted = apply_deletes(conn, output, &stage, &key.fields, delete_application)?;
            counts.deleted = checked_add(counts.deleted, deleted)?;
        }
        _ => {
            return Err(CdfError::data(
                "routed DuckDB segment kind differs from output content authority",
            ));
        }
    }
    conn.execute_batch(&format!("DROP TABLE {}", quote_ident(&stage)))
        .map_err(|error| duckdb_error("drop routed DuckDB segment stage", error))?;
    Ok(())
}

fn insert_rows(
    conn: &Connection,
    output: &RoutedOutputPlan,
    stage: &cdf_dest_sql::ValidatedSqlIdentifier,
    first_row_key: u64,
    keys: Option<&[String]>,
) -> Result<u64> {
    let user_columns = output
        .user_fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    let mut target_columns = user_columns.clone();
    target_columns.push(quote_ident(&crate::sql::framework_ident(
        CDF_ROW_KEY_COLUMN,
    )));
    let mut selected = user_columns;
    selected.push(format!(
        "{first_row_key} + {}",
        quote_ident(&crate::sql::framework_ident(
            cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD,
        ))
    ));
    let conflict = match keys {
        None => String::new(),
        Some(keys) => {
            let key_set = keys
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            let key_sql = keys
                .iter()
                .map(|key| crate::sql::validate_ident(key).map(|key| quote_ident(&key)))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            let mut assignments = output
                .user_fields
                .iter()
                .filter(|field| !key_set.contains(field.name.as_str()))
                .map(|field| {
                    let name = quote_ident(&field.name);
                    format!("{name} = excluded.{name}")
                })
                .collect::<Vec<_>>();
            let row_key = quote_ident(&crate::sql::framework_ident(CDF_ROW_KEY_COLUMN));
            assignments.push(format!("{row_key} = excluded.{row_key}"));
            format!(
                " ON CONFLICT ({key_sql}) DO UPDATE SET {}",
                assignments.join(", ")
            )
        }
    };
    let sql = format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}{}",
        output.target.sql_name(),
        target_columns.join(", "),
        selected.join(", "),
        quote_ident(stage),
        conflict,
    );
    conn.execute(&sql, [])
        .map(|rows| rows as u64)
        .map_err(|error| duckdb_error("apply routed DuckDB row/upsert segment", error))
}

fn matching_key_count(
    conn: &Connection,
    output: &RoutedOutputPlan,
    stage: &cdf_dest_sql::ValidatedSqlIdentifier,
    keys: &[String],
) -> Result<u64> {
    let predicate = key_predicate("target", "stage", keys)?;
    conn.query_row(
        &format!(
            "SELECT count(*) FROM {} AS stage JOIN {} AS target ON {predicate}",
            quote_ident(stage),
            output.target.sql_name(),
        ),
        [],
        |row| row.get(0),
    )
    .map_err(|error| duckdb_error("count routed DuckDB existing keys", error))
}

fn apply_deletes(
    conn: &Connection,
    output: &RoutedOutputPlan,
    stage: &cdf_dest_sql::ValidatedSqlIdentifier,
    keys: &[String],
    authority: &DeleteApplicationAuthority,
) -> Result<u64> {
    let predicate = key_predicate("target", "stage", keys)?;
    let sql = match authority {
        DeleteApplicationAuthority::NotApplicable => {
            return Err(CdfError::data(
                "routed DuckDB delete segment has no delete application authority",
            ));
        }
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Ignore,
        } => return Ok(0),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Hard,
        } => format!(
            "DELETE FROM {} AS target USING {} AS stage WHERE {predicate}",
            output.target.sql_name(),
            quote_ident(stage),
        ),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Soft { marker_field },
        } => format!(
            "UPDATE {} AS target SET {} = TRUE FROM {} AS stage WHERE {predicate}",
            output.target.sql_name(),
            quote_ident(&crate::sql::validate_ident(marker_field)?),
            quote_ident(stage),
        ),
    };
    conn.execute(&sql, [])
        .map(|rows| rows as u64)
        .map_err(|error| duckdb_error("apply routed DuckDB delete segment", error))
}

fn key_predicate(left: &str, right: &str, keys: &[String]) -> Result<String> {
    keys.iter()
        .map(|key| {
            let key = quote_ident(&crate::sql::validate_ident(key)?);
            Ok(format!("{left}.{key} IS NOT DISTINCT FROM {right}.{key}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join(" AND "))
}

fn routed_commit_counts(
    plans: &[RoutedOutputPlan],
    counts: Vec<RoutedCounts>,
) -> Result<CommitCounts> {
    let targets = plans
        .iter()
        .zip(counts)
        .map(|(plan, counts)| {
            let counts = match &plan.content {
                PackageContentAuthority::Rows { .. } => {
                    CommitCounts::rows(counts.rows_written, Some(counts.inserted), Some(0), Some(0))
                }
                PackageContentAuthority::KeyedChanges {
                    reduction,
                    delete_application,
                    ..
                } => {
                    let (hard, soft, ignored) = match delete_application {
                        DeleteApplicationAuthority::NotApplicable => (None, None, None),
                        DeleteApplicationAuthority::Apply {
                            policy: DeleteApplicationPolicy::Ignore,
                        } => (None, None, Some(reduction.surviving.deletes)),
                        DeleteApplicationAuthority::Apply {
                            policy: DeleteApplicationPolicy::Hard,
                        } => (Some(counts.deleted), None, None),
                        DeleteApplicationAuthority::Apply {
                            policy: DeleteApplicationPolicy::Soft { .. },
                        } => (None, Some(counts.deleted), None),
                    };
                    let missing =
                        hard.or(soft)
                            .map(|applied| {
                                reduction.surviving.deletes.checked_sub(applied).ok_or_else(|| {
                                CdfError::internal(
                                    "routed DuckDB applied delete count exceeds package intent",
                                )
                            })
                            })
                            .transpose()?;
                    CommitCounts::keyed_changes(
                        reduction.surviving,
                        Some(counts.inserted),
                        Some(counts.updated),
                        hard,
                        soft,
                        missing,
                        ignored,
                    )
                }
                PackageContentAuthority::Routed { .. } => {
                    return Err(CdfError::data("nested routed content is invalid"));
                }
            };
            Ok(RoutedTargetCommitCounts {
                output_binding: plan.binding.output_binding.clone(),
                target: plan.binding.physical_target.clone(),
                schema_hash: plan.binding.schema_hash.clone(),
                counts: Box::new(counts),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CommitCounts::Routed { targets })
}

fn checked_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| CdfError::data("routed DuckDB commit count overflowed u64"))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        CHECKPOINT_STATE_VERSION, CanonicalArrowSchema, CheckpointId, CursorPosition, CursorValue,
        IdempotencyToken, PackageHash, PartitionId, PipelineId, ResourceId, RoutePlan, RouteScalar,
        RouteTargetFamily, ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment,
        TargetName,
    };

    use super::*;

    #[test]
    fn routed_rows_commit_atomically_and_replay_once() {
        let temp = tempfile::tempdir().unwrap();
        let (_, execution) =
            cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
        let mut destination = crate::DuckDbDestination::new(temp.path().join("routed.duckdb"))
            .unwrap()
            .with_test_execution_clock(execution);
        let user_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let canonical_schema = CanonicalArrowSchema::from_arrow(&user_schema).unwrap();
        let schema_hash = cdf_kernel::canonical_arrow_schema_hash(&user_schema).unwrap();
        let route_values = StringArray::from(vec!["orders", "invoices"]);
        let family = RouteTargetFamily::new(
            RoutePlan::new("source_collection", 2).unwrap(),
            TargetName::new("events").unwrap(),
            Some(128),
            (0..route_values.len()).map(|row| {
                (
                    RouteScalar::from_array(&route_values, row).unwrap(),
                    schema_hash.clone(),
                )
            }),
        )
        .unwrap();
        let state_segments = family
            .bindings
            .iter()
            .enumerate()
            .map(|(ordinal, _binding)| StateSegment {
                kind: PackageSegmentKind::Row,
                segment_id: SegmentId::new(format!("route-{ordinal:03}")).unwrap(),
                scope: ScopeKey::Partition {
                    partition_id: PartitionId::new("p0").unwrap(),
                },
                output_position: cursor_position(ordinal as i64 + 1),
                row_count: 1,
                byte_count: 128,
            })
            .collect::<Vec<_>>();
        let content = PackageContentAuthority::Routed {
            outputs: family
                .bindings
                .iter()
                .zip(&state_segments)
                .map(
                    |(binding, segment)| cdf_kernel::RoutedOutputContentAuthority {
                        output_binding: binding.output_binding.clone(),
                        schema: canonical_schema.clone(),
                        content: Box::new(PackageContentAuthority::rows(schema_hash.clone())),
                        segment_ids: vec![segment.segment_id.clone()],
                    },
                )
                .collect(),
            family: family.clone(),
        };
        let package_hash = PackageHash::new("sha256:routed-test-package").unwrap();
        let commit = DestinationCommitRequest {
            package_hash: package_hash.clone(),
            content: content.clone(),
            target: family.logical_target.clone(),
            disposition: WriteDisposition::Append,
            segments: state_segments.clone(),
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        };
        let inputs = PackageReplayInputs {
            input_checkpoint: None,
            state_delta: StateDelta {
                checkpoint_id: CheckpointId::new("checkpoint-routed-test").unwrap(),
                pipeline_id: PipelineId::new("pipeline-routed-test").unwrap(),
                resource_id: ResourceId::new("events").unwrap(),
                scope: ScopeKey::Resource,
                state_version: CHECKPOINT_STATE_VERSION,
                parent_checkpoint_id: None,
                input_position: None,
                output_position: cursor_position(2),
                output_watermark: None,
                partition_watermarks: Vec::new(),
                late_data_carryover: Vec::new(),
                source_continuation: None,
                package_hash,
                content,
                schema_hash: family.schema_family_hash.clone(),
                segments: state_segments.clone(),
            },
            destination_commit: commit,
            merge_keys: Vec::new(),
            schema_hash: family.schema_family_hash.clone(),
            destination_policy: BTreeMap::new(),
            run_schema_authority: None,
        };

        let first = destination
            .commit_routed(&inputs, routed_segments(&family, &state_segments))
            .unwrap();
        assert!(matches!(
            first.reporting_policy,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false }
        ));
        let CommitCounts::Routed { targets } = &first.receipt.counts else {
            panic!("routed receipt counts expected");
        };
        assert_eq!(targets.len(), 2);
        assert_eq!(first.receipt.counts.settled_effect_count(), Some(2));

        let replay = destination
            .commit_routed(&inputs, routed_segments(&family, &state_segments))
            .unwrap();
        assert!(matches!(
            replay.reporting_policy,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true }
        ));
        assert_eq!(replay.receipt.receipt_id, first.receipt.receipt_id);

        let conn = Connection::open(temp.path().join("routed.duckdb")).unwrap();
        for binding in &family.bindings {
            let target = parse_target(&binding.physical_target).unwrap();
            let count: u64 = conn
                .query_row(
                    &format!("SELECT count(*) FROM {}", target.sql_name()),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
        }
    }

    fn routed_segments(
        family: &RouteTargetFamily,
        states: &[StateSegment],
    ) -> CommitSegmentIterator {
        let segments = family
            .bindings
            .iter()
            .zip(states)
            .enumerate()
            .map(|(ordinal, (binding, state))| {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new(
                        cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD,
                        DataType::UInt64,
                        false,
                    ),
                ]));
                let id: ArrayRef = Arc::new(Int64Array::from(vec![ordinal as i64 + 1]));
                let name: ArrayRef =
                    Arc::new(StringArray::from(vec![Some(binding.route_token.as_str())]));
                let row_ord: ArrayRef = Arc::new(UInt64Array::from(vec![ordinal as u64]));
                Ok(CommitSegment::new(
                    state.clone(),
                    state.byte_count,
                    vec![RecordBatch::try_new(schema, vec![id, name, row_ord]).unwrap()],
                ))
            })
            .collect::<Vec<_>>();
        Box::new(segments.into_iter())
    }

    fn cursor_position(value: i64) -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "position".to_owned(),
            value: CursorValue::I64(value),
        })
    }
}
