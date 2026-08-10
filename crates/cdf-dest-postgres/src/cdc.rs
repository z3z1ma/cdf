use std::collections::BTreeSet;

use arrow_schema::Schema;
use cdf_dest_sql::SegmentRowRange;
use cdf_kernel::{
    CdfError, CommitCounts, CommitPlan, CommitSegment, CommitSegmentIterator,
    DeleteApplicationAuthority, DeleteApplicationPolicy, DestinationCommitRequest,
    IdempotencySupport, MigrationRecord, PackageContentAuthority, PackageSegmentKind, PlanId,
    Result, RoutedTargetCommitCounts, SegmentId, WriteDisposition,
};
use cdf_package_contract::PackageReplayInputs;
use cdf_postgres::{PostgresIdentifier, PostgresTarget};
use postgres::{Client, NoTls};

use crate::{
    api::{build_receipt_for_request, plan_postgres_load},
    binary_copy::BinaryCopyEncoder,
    commit::{
        allocate_row_key_range, apply_mirror_commit, find_duplicate_receipt, now_ms,
        postgres_copy_error, postgres_error, query_xid, receipt_id, verify_receipt_in_transaction,
    },
    ddl::{provenance_unique_index_statement, system_table_ddl, target_migrations},
    dml::{binary_copy_sql, create_stage_sql, quoted_system_target_column_names},
    identifiers::{
        PostgresColumn, quote_column_identifier, quote_system_identifier, quote_user_identifier,
        validated_target_sql,
    },
    mirrors::{add_segments_to_verify_parameters, drift_hooks, mirror_statements, verify_clause},
    models::PostgresDestination,
    plan::{PostgresLoadPlan, PostgresLoadPlanInput, PostgresReceiptInput},
    rows::{postgres_columns_for_schema, validate_schema_matches_plan},
    validate::token_suffix,
};

#[derive(Clone)]
struct PostgresOutputPlan {
    binding: Option<cdf_kernel::RouteTargetBinding>,
    content: PackageContentAuthority,
    load: PostgresLoadPlan,
    segment_ids: BTreeSet<SegmentId>,
}

#[derive(Clone, Default)]
struct PostgresOutputCounts {
    rows_written: u64,
    inserted: u64,
    updated: u64,
    deleted: u64,
    already_deleted: u64,
}

struct SegmentApplyContext<'a> {
    ordinal: usize,
    output: &'a PostgresOutputPlan,
    seen_table: Option<&'a PostgresIdentifier>,
    first_row_key: i64,
    execution: &'a cdf_runtime::ExecutionServices,
}

pub(crate) fn plan_routed(commit: &DestinationCommitRequest) -> Result<CommitPlan> {
    let (outputs, plan) = prepare_routed(commit, None)?;
    if outputs.is_empty() {
        return Err(CdfError::contract(
            "Postgres routed target family requires at least one output",
        ));
    }
    Ok(plan)
}

pub(crate) fn commit_single_target(
    destination: &PostgresDestination,
    package: &dyn cdf_package_contract::VerifiedPackageAccess,
    inputs: &PackageReplayInputs,
    output_schema: &Schema,
    segments: CommitSegmentIterator,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let request = &inputs.destination_commit;
    if request.disposition != WriteDisposition::CdcApply {
        return Err(CdfError::contract(
            "Postgres finalized CDC application requires disposition cdc_apply",
        ));
    }
    let target = PostgresTarget::parse(request.target.as_str())?;
    let columns = postgres_columns_for_schema(output_schema)?;
    let load = plan_postgres_load(
        PostgresLoadPlanInput {
            package_hash: request.package_hash.clone(),
            content: request.content.clone(),
            idempotency_token: request.idempotency_token.clone(),
            target,
            disposition: request.disposition.clone(),
            schema_hash: inputs.schema_hash.clone(),
            segments: request.segments.clone(),
            columns,
            merge_keys: inputs
                .merge_keys
                .iter()
                .map(PostgresIdentifier::user)
                .collect::<Result<Vec<_>>>()?,
            existing_table: None,
            resource_id: Some(inputs.state_delta.resource_id.clone()),
            state_delta: Some(inputs.state_delta.clone()),
        },
        destination.postgres_sheet(),
    )?;
    let output = PostgresOutputPlan {
        binding: None,
        content: request.content.clone(),
        segment_ids: request
            .segments
            .iter()
            .map(|segment| segment.segment_id.clone())
            .collect(),
        load: load.clone(),
    };
    commit_transaction(
        destination,
        package,
        inputs,
        vec![output],
        load,
        segments,
        false,
    )
}

pub(crate) fn commit_routed(
    destination: &PostgresDestination,
    package: &dyn cdf_package_contract::VerifiedPackageAccess,
    inputs: &PackageReplayInputs,
    segments: CommitSegmentIterator,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let (outputs, plan) = prepare_routed(&inputs.destination_commit, Some(inputs))?;
    let mirror = routed_mirror_plan(inputs, plan)?;
    commit_transaction(
        destination,
        package,
        inputs,
        outputs,
        mirror,
        segments,
        true,
    )
}

fn prepare_routed(
    commit: &DestinationCommitRequest,
    inputs: Option<&PackageReplayInputs>,
) -> Result<(Vec<PostgresOutputPlan>, CommitPlan)> {
    let PackageContentAuthority::Routed { family, outputs } = &commit.content else {
        return Err(CdfError::contract(
            "Postgres routed planning requires routed package content",
        ));
    };
    commit.content.validate_segments(
        commit
            .segments
            .iter()
            .map(|segment| (&segment.segment_id, &segment.kind, segment.row_count)),
    )?;
    if family.logical_target != commit.target {
        return Err(CdfError::data(
            "Postgres routed target family differs from its logical commit target",
        ));
    }
    let mut planned = Vec::with_capacity(outputs.len());
    let mut migrations = Vec::new();
    for (binding, output) in family.bindings.iter().zip(outputs) {
        let schema = output.schema.to_arrow()?;
        let columns = postgres_columns_for_schema(schema.as_ref())?;
        let merge_keys = match output.content.as_ref() {
            PackageContentAuthority::Rows { .. }
                if matches!(
                    commit.disposition,
                    WriteDisposition::Append | WriteDisposition::Replace
                ) =>
            {
                Vec::new()
            }
            PackageContentAuthority::KeyedChanges { key, .. }
                if matches!(
                    commit.disposition,
                    WriteDisposition::Merge | WriteDisposition::CdcApply
                ) =>
            {
                key.fields
                    .iter()
                    .map(PostgresIdentifier::user)
                    .collect::<Result<Vec<_>>>()?
            }
            PackageContentAuthority::Rows { .. } => {
                return Err(CdfError::contract(
                    "Postgres routed append/replace requires ordinary-row package content",
                ));
            }
            PackageContentAuthority::KeyedChanges { .. } => {
                return Err(CdfError::contract(
                    "Postgres routed merge/cdc_apply requires keyed-change package content",
                ));
            }
            PackageContentAuthority::Routed { .. } => {
                return Err(CdfError::data("nested routed content is invalid"));
            }
        };
        let target = PostgresTarget::parse(binding.physical_target.as_str())?;
        let output_segments = commit
            .segments
            .iter()
            .filter(|segment| output.segment_ids.contains(&segment.segment_id))
            .cloned()
            .collect::<Vec<_>>();
        // One nonempty routed package settles the complete admitted family. An output with zero
        // rows still needs its target planned (notably so replace can clear it); only a globally
        // empty package is the kernel-defined data no-op.
        let no_data = commit.is_data_noop();
        let load_input = PostgresLoadPlanInput {
            package_hash: commit.package_hash.clone(),
            content: output.content.as_ref().clone(),
            idempotency_token: commit.idempotency_token.clone(),
            target,
            disposition: commit.disposition.clone(),
            schema_hash: binding.schema_hash.clone(),
            segments: output_segments,
            columns,
            merge_keys,
            existing_table: None,
            resource_id: inputs.map(|inputs| inputs.state_delta.resource_id.clone()),
            state_delta: inputs.map(|inputs| inputs.state_delta.clone()),
        };
        let target_ddl = if no_data {
            Vec::new()
        } else {
            target_migrations(&load_input)?
        };
        let post_write_ddl = if no_data {
            Vec::new()
        } else {
            vec![provenance_unique_index_statement(&load_input.target)?]
        };
        let mut load = plan_postgres_load(load_input, &crate::sheet::postgres_destination_sheet())?;
        load.target_ddl = target_ddl;
        load.post_write_ddl = post_write_ddl;
        let binding_id = binding.output_binding.as_str();
        migrations.extend(
            load.target_ddl
                .iter()
                .chain(&load.post_write_ddl)
                .enumerate()
                .map(|(index, statement)| MigrationRecord {
                    migration_id: format!(
                        "postgres-route-{binding_id}-{:03}",
                        index.saturating_add(1)
                    ),
                    description: statement.sql.clone(),
                }),
        );
        planned.push(PostgresOutputPlan {
            binding: Some(binding.clone()),
            content: output.content.as_ref().clone(),
            load,
            segment_ids: output.segment_ids.iter().cloned().collect(),
        });
    }
    let plan = CommitPlan {
        plan_id: PlanId::new(format!(
            "postgres:routed:{}:{}",
            commit.target.as_str().replace('.', "_"),
            token_suffix(commit.idempotency_token.as_str())
        ))?,
        target: commit.target.clone(),
        disposition: commit.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations,
        delivery_guarantee: crate::validate::delivery_guarantee(&commit.disposition),
    };
    Ok((planned, plan))
}

fn routed_mirror_plan(
    inputs: &PackageReplayInputs,
    kernel: CommitPlan,
) -> Result<PostgresLoadPlan> {
    let request = &inputs.destination_commit;
    let target = PostgresTarget::parse(request.target.as_str())?;
    let mut verify = verify_clause(
        &request.target,
        target.schema.as_ref(),
        &request.package_hash,
        &request.idempotency_token,
        &inputs.schema_hash,
    );
    add_segments_to_verify_parameters(&mut verify, &request.segments);
    let dummy = PostgresLoadPlanInput {
        package_hash: request.package_hash.clone(),
        content: request.content.clone(),
        idempotency_token: request.idempotency_token.clone(),
        target: target.clone(),
        disposition: request.disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: request.segments.clone(),
        columns: Vec::new(),
        merge_keys: Vec::new(),
        existing_table: None,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    };
    Ok(PostgresLoadPlan {
        kernel,
        package_hash: request.package_hash.clone(),
        content: request.content.clone(),
        idempotency_token: request.idempotency_token.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: request.segments.clone(),
        target,
        stage_table: None,
        columns: Vec::new(),
        merge_keys: Vec::new(),
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
        system_ddl: system_table_ddl(),
        target_ddl: Vec::new(),
        post_write_ddl: Vec::new(),
        idempotency_lock: crate::ddl::idempotency_lock_statement(),
        idempotency_check: crate::ddl::idempotency_check_statement(),
        xid_probe: crate::plan::PostgresStatement::query(
            "capture_xid",
            crate::POSTGRES_XID_SQL,
            crate::plan::StatementExpectation::ReturnsXid,
        ),
        write_sql: Vec::new(),
        mirror_sql: mirror_statements(&dummy, &verify),
        verify,
        drift: drift_hooks(),
    })
}

fn commit_transaction(
    destination: &PostgresDestination,
    package: &dyn cdf_package_contract::VerifiedPackageAccess,
    inputs: &PackageReplayInputs,
    outputs: Vec<PostgresOutputPlan>,
    mirror_plan: PostgresLoadPlan,
    segments: CommitSegmentIterator,
    routed: bool,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let database_url = destination.database_url().ok_or_else(|| {
        CdfError::contract("Postgres CDC application requires a connected destination runtime")
    })?;
    let execution = destination.execution.as_ref().ok_or_else(|| {
        CdfError::contract("Postgres CDC application requires injected execution services")
    })?;
    let request = &inputs.destination_commit;
    if package.package_hash() != request.package_hash.as_str() {
        return Err(CdfError::data(
            "verified package access differs from the Postgres destination commit package",
        ));
    }
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| postgres_error("connect for Postgres CDC application", error))?;
    client
        .batch_execute("BEGIN")
        .map_err(|error| postgres_error("begin Postgres CDC transaction", error))?;
    let result = (|| {
        set_search_path(&mut client, &mirror_plan.target)?;
        execute_statements(&mut client, &mirror_plan.system_ddl)?;
        if let Some(receipt) = find_duplicate_receipt(&mut client, &mirror_plan)? {
            return Ok((receipt, true));
        }

        // Every target/schema/effect plan is already complete. Only after that succeeds do we
        // execute any target DDL or payload mutation inside this one transaction.
        if !request.is_data_noop() {
            for output in &outputs {
                execute_statements(&mut client, &output.load.target_ddl)?;
                validate_soft_marker_target(&mut client, output)?;
                execute_statements(&mut client, &output.load.post_write_ddl)?;
            }
        }
        let seen_tables = if request.is_data_noop() {
            vec![None; outputs.len()]
        } else {
            outputs
                .iter()
                .enumerate()
                .map(|(index, output)| create_seen_key_table(&mut client, index, output))
                .collect::<Result<Vec<_>>>()?
        };

        let mut counts = vec![PostgresOutputCounts::default(); outputs.len()];
        if request.disposition == WriteDisposition::Replace && !request.is_data_noop() {
            for (output, counts) in outputs.iter().zip(&mut counts) {
                counts.deleted = count_target_rows(&mut client, output)?;
                client
                    .batch_execute(&format!(
                        "TRUNCATE TABLE {}",
                        validated_target_sql(&output.load.target)?
                    ))
                    .map_err(|error| postgres_error("replace routed Postgres target", error))?;
            }
        }

        let total_rows = request.segments.iter().try_fold(0_u64, |total, segment| {
            total
                .checked_add(segment.row_count)
                .ok_or_else(|| CdfError::data("Postgres CDC effect row count overflowed u64"))
        })?;
        let first_row_key = if total_rows == 0 {
            None
        } else {
            Some(allocate_row_key_range(&mut client, total_rows)?)
        };
        let mut observed = Vec::new();
        let mut row_ranges = Vec::with_capacity(request.segments.len());
        for (ordinal, segment) in segments.enumerate() {
            let segment = segment?;
            let expected = request.segments.get(ordinal).ok_or_else(|| {
                CdfError::data("Postgres CDC stream contains an unplanned segment")
            })?;
            if segment.state != *expected {
                return Err(CdfError::data(format!(
                    "Postgres CDC segment {} differs from canonical package authority",
                    segment.state.segment_id.as_str()
                )));
            }
            let output_index = outputs
                .iter()
                .position(|output| output.segment_ids.contains(&segment.state.segment_id))
                .ok_or_else(|| {
                    CdfError::data("Postgres CDC segment has no admitted destination output")
                })?;
            let row_range = apply_segment(
                &mut client,
                &segment,
                SegmentApplyContext {
                    ordinal,
                    output: &outputs[output_index],
                    seen_table: seen_tables[output_index].as_ref(),
                    first_row_key: first_row_key.ok_or_else(|| {
                        CdfError::internal(
                            "nonempty Postgres CDC segment has no row-key allocation",
                        )
                    })?,
                    execution,
                },
                &mut counts[output_index],
            )?;
            row_ranges.push(row_range);
            observed.push(segment.state.segment_id);
        }
        let expected = request
            .segments
            .iter()
            .map(|segment| segment.segment_id.clone())
            .collect::<Vec<_>>();
        if observed != expected {
            return Err(CdfError::data(
                "Postgres CDC segment stream differs from canonical package order",
            ));
        }

        let commit_counts = if routed {
            routed_counts(&outputs, counts)?
        } else {
            keyed_counts(&outputs[0].content, &counts[0])?
        };
        let xid = query_xid(&mut client, &mirror_plan)?;
        let committed_at_ms = now_ms(execution)?;
        let receipt = build_receipt_for_request(
            request,
            &mirror_plan.kernel,
            &inputs.schema_hash,
            &mirror_plan.verify,
            PostgresReceiptInput {
                receipt_id: receipt_id(&mirror_plan)?,
                xid,
                committed_at_ms,
                counts: commit_counts,
                duplicate: false,
            },
        )?;
        apply_mirror_commit(&mut client, package, &mirror_plan, &receipt, row_ranges)?;
        verify_receipt_in_transaction(&mut client, &receipt)?;
        Ok((receipt, false))
    })();

    match result {
        Ok((receipt, duplicate)) => {
            if duplicate {
                client.batch_execute("ROLLBACK").map_err(|error| {
                    postgres_error("rollback duplicate Postgres CDC transaction", error)
                })?;
            } else {
                client
                    .batch_execute("COMMIT")
                    .map_err(|error| postgres_error("commit Postgres CDC transaction", error))?;
            }
            Ok(cdf_runtime::DestinationCommitOutcome::new(
                receipt,
                cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate },
            ))
        }
        Err(error) => match client.batch_execute("ROLLBACK") {
            Ok(()) => Err(error),
            Err(rollback) => Err(CdfError::destination(format!(
                "{}; Postgres CDC rollback also failed: {}",
                error.message, rollback
            ))),
        },
    }
}

fn apply_segment(
    client: &mut Client,
    segment: &CommitSegment,
    context: SegmentApplyContext<'_>,
    counts: &mut PostgresOutputCounts,
) -> Result<SegmentRowRange> {
    if segment.batches.is_empty() {
        return Err(CdfError::data("Postgres CDC segment contains no batches"));
    }
    let schema = segment.batches[0].schema();
    if segment
        .batches
        .iter()
        .any(|batch| batch.schema().as_ref() != schema.as_ref())
    {
        return Err(CdfError::data(
            "Postgres CDC segment batches do not share one schema",
        ));
    }
    let logical_schema = cdf_package_contract::logical_output_schema(schema.as_ref())?;
    let stage_columns = postgres_columns_for_schema(&logical_schema)?;
    match segment.state.kind {
        PackageSegmentKind::Upsert => {
            validate_schema_matches_plan(&logical_schema, &context.output.load.columns)?;
        }
        PackageSegmentKind::Delete => validate_delete_schema(context.output, &stage_columns)?,
        PackageSegmentKind::Row => {
            validate_schema_matches_plan(&logical_schema, &context.output.load.columns)?
        }
    }

    let stage = PostgresIdentifier::system(format!("_cdf_cdc_stage_{:08}", context.ordinal))?;
    let create = create_stage_sql(&stage, &stage_columns)?;
    client
        .batch_execute(&create)
        .map_err(|error| postgres_error("create Postgres CDC stage table", error))?;
    let copy_sql = binary_copy_sql(&quote_system_identifier(&stage)?, &stage_columns)?;
    let writer = client
        .copy_in(&copy_sql)
        .map_err(|error| postgres_error("open Postgres CDC binary COPY", error))?;
    let mut encoder = BinaryCopyEncoder::new(writer, stage_columns.len())?;
    let loaded_at_ms = now_ms(context.execution)?;
    for batch in &segment.batches {
        encoder.write_batch(batch, context.first_row_key, loaded_at_ms)?;
    }
    let (writer, encoded) = encoder.finish()?;
    let copied = writer
        .finish()
        .map_err(|error| postgres_copy_error("finish Postgres CDC binary COPY", error))?;
    if copied != encoded || copied != segment.state.row_count {
        return Err(CdfError::destination(format!(
            "Postgres CDC COPY accepted {copied} rows for a {}-row segment",
            segment.state.row_count
        )));
    }
    if let Some(seen_table) = context.seen_table {
        guard_unique_effect_keys(
            client,
            context.output,
            &stage,
            seen_table,
            segment.state.row_count,
        )?;
    }

    match segment.state.kind {
        PackageSegmentKind::Upsert => apply_upserts(client, context.output, &stage, counts)?,
        PackageSegmentKind::Delete => apply_deletes(client, context.output, &stage, counts)?,
        PackageSegmentKind::Row => apply_rows(client, context.output, &stage, counts)?,
    }
    client
        .batch_execute(&format!("DROP TABLE {}", quote_system_identifier(&stage)?))
        .map_err(|error| postgres_error("drop Postgres CDC stage table", error))?;

    let package_ord = cdf_package_contract::package_row_ord_array(&segment.batches[0])?;
    let ordinal_start = package_ord.value(0);
    cdf_package_contract::validate_package_row_ord_batches(
        &segment.batches,
        ordinal_start,
        segment.state.row_count,
    )?;
    let start = u64::try_from(context.first_row_key)
        .map_err(|_| CdfError::internal("Postgres row-key allocator returned a negative key"))?
        .checked_add(ordinal_start)
        .ok_or_else(|| CdfError::data("Postgres CDC segment row-key start overflowed"))?;
    Ok(SegmentRowRange {
        segment_id: segment.state.segment_id.clone(),
        row_key_start: start,
        row_key_end: start
            .checked_add(segment.state.row_count)
            .ok_or_else(|| CdfError::data("Postgres CDC segment row-key end overflowed"))?,
    })
}

fn create_seen_key_table(
    client: &mut Client,
    output_index: usize,
    output: &PostgresOutputPlan,
) -> Result<Option<PostgresIdentifier>> {
    let PackageContentAuthority::KeyedChanges { key, .. } = &output.content else {
        return Ok(None);
    };
    let table = PostgresIdentifier::system(format!("_cdf_cdc_seen_{output_index:08}"))?;
    let keys = key
        .fields
        .iter()
        .map(|key| PostgresIdentifier::user(key).and_then(|key| quote_user_identifier(&key)))
        .collect::<Result<Vec<_>>>()?;
    client
        .batch_execute(&format!(
            "CREATE TEMP TABLE {} AS SELECT {} FROM {} WHERE FALSE; ALTER TABLE {} ADD PRIMARY KEY ({})",
            quote_system_identifier(&table)?,
            keys.join(", "),
            validated_target_sql(&output.load.target)?,
            quote_system_identifier(&table)?,
            keys.join(", ")
        ))
        .map_err(|error| postgres_error("create Postgres CDC effect-key guard", error))?;
    Ok(Some(table))
}

fn guard_unique_effect_keys(
    client: &mut Client,
    output: &PostgresOutputPlan,
    stage: &PostgresIdentifier,
    seen: &PostgresIdentifier,
    expected_rows: u64,
) -> Result<()> {
    let PackageContentAuthority::KeyedChanges { key, .. } = &output.content else {
        return Err(CdfError::data(
            "Postgres CDC output lacks keyed-change authority",
        ));
    };
    let keys = key
        .fields
        .iter()
        .map(|key| PostgresIdentifier::user(key).and_then(|key| quote_user_identifier(&key)))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let inserted = client
        .execute(
            &format!(
                "INSERT INTO {} ({keys}) SELECT {keys} FROM {} ON CONFLICT DO NOTHING",
                quote_system_identifier(seen)?,
                quote_system_identifier(stage)?
            ),
            &[],
        )
        .map_err(|error| postgres_error("validate Postgres CDC effect-key uniqueness", error))?;
    if inserted != expected_rows {
        return Err(CdfError::data(
            "Postgres finalized CDC package repeats an effect key across upsert/delete segments",
        ));
    }
    Ok(())
}

fn validate_delete_schema(output: &PostgresOutputPlan, columns: &[PostgresColumn]) -> Result<()> {
    let PackageContentAuthority::KeyedChanges { key, .. } = &output.content else {
        return Err(CdfError::data(
            "Postgres delete segment output lacks keyed-change authority",
        ));
    };
    let names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if key.fields.iter().any(|key| !names.contains(key.as_str())) {
        return Err(CdfError::data(
            "Postgres delete segment omits an ordered effect-key field",
        ));
    }
    Ok(())
}

fn apply_rows(
    client: &mut Client,
    output: &PostgresOutputPlan,
    stage: &PostgresIdentifier,
    counts: &mut PostgresOutputCounts,
) -> Result<()> {
    if !matches!(output.content, PackageContentAuthority::Rows { .. }) {
        return Err(CdfError::data(
            "Postgres ordinary-row segment differs from routed output content authority",
        ));
    }
    let mut columns = output
        .load
        .columns
        .iter()
        .map(|column| quote_column_identifier(&column.name))
        .collect::<Result<Vec<_>>>()?;
    columns.extend(quoted_system_target_column_names());
    let written = client
        .execute(
            &format!(
                "INSERT INTO {} ({}) SELECT {} FROM {}",
                validated_target_sql(&output.load.target)?,
                columns.join(", "),
                columns.join(", "),
                quote_system_identifier(stage)?
            ),
            &[],
        )
        .map_err(|error| postgres_error("apply routed Postgres row segment", error))?;
    counts.rows_written = checked_add(counts.rows_written, written)?;
    counts.inserted = checked_add(counts.inserted, written)?;
    Ok(())
}

fn apply_upserts(
    client: &mut Client,
    output: &PostgresOutputPlan,
    stage: &PostgresIdentifier,
    counts: &mut PostgresOutputCounts,
) -> Result<()> {
    let PackageContentAuthority::KeyedChanges {
        key,
        delete_application,
        ..
    } = &output.content
    else {
        return Err(CdfError::data("Postgres upsert output is not keyed"));
    };
    let existing = matching_key_count(client, output, stage, &key.fields)?;
    let marker = soft_marker(delete_application)
        .map(PostgresIdentifier::user)
        .transpose()?
        .as_ref()
        .map(quote_user_identifier)
        .transpose()?;
    let mut target_columns = output
        .load
        .columns
        .iter()
        .map(|column| quote_column_identifier(&column.name))
        .collect::<Result<Vec<_>>>()?;
    if let Some(marker) = &marker {
        target_columns.push(marker.clone());
    }
    target_columns.extend(quoted_system_target_column_names());
    let mut selected = output
        .load
        .columns
        .iter()
        .map(|column| quote_column_identifier(&column.name))
        .collect::<Result<Vec<_>>>()?;
    if marker.is_some() {
        selected.push("FALSE".to_owned());
    }
    selected.extend(quoted_system_target_column_names());
    let conflict = key
        .fields
        .iter()
        .map(|key| PostgresIdentifier::user(key).and_then(|key| quote_user_identifier(&key)))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let key_set = key
        .fields
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut assignments = output
        .load
        .columns
        .iter()
        .filter(|column| !key_set.contains(column.name.as_str()))
        .map(|column| {
            let name = quote_column_identifier(&column.name)?;
            Ok(format!("{name} = EXCLUDED.{name}"))
        })
        .collect::<Result<Vec<_>>>()?;
    for system in quoted_system_target_column_names() {
        assignments.push(format!("{system} = EXCLUDED.{system}"));
    }
    if let Some(marker) = &marker {
        assignments.push(format!("{marker} = FALSE"));
    }
    let sql = format!(
        "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({conflict}) DO UPDATE SET {}",
        validated_target_sql(&output.load.target)?,
        target_columns.join(", "),
        selected.join(", "),
        quote_system_identifier(stage)?,
        assignments.join(", ")
    );
    let written = client
        .execute(&sql, &[])
        .map_err(|error| postgres_error("apply Postgres CDC upserts", error))?;
    counts.updated = checked_add(counts.updated, existing)?;
    counts.inserted = checked_add(
        counts.inserted,
        written.checked_sub(existing).ok_or_else(|| {
            CdfError::internal("Postgres existing-key count exceeds staged upserts")
        })?,
    )?;
    Ok(())
}

fn apply_deletes(
    client: &mut Client,
    output: &PostgresOutputPlan,
    stage: &PostgresIdentifier,
    counts: &mut PostgresOutputCounts,
) -> Result<()> {
    let PackageContentAuthority::KeyedChanges {
        key,
        delete_application,
        ..
    } = &output.content
    else {
        return Err(CdfError::data("Postgres delete output is not keyed"));
    };
    let predicate = key_predicate(&key.fields)?;
    let stage_sql = quote_system_identifier(stage)?;
    let target = validated_target_sql(&output.load.target)?;
    let (sql, soft_matched) = match delete_application {
        DeleteApplicationAuthority::NotApplicable => {
            return Err(CdfError::contract(
                "Postgres delete segment has no delete application policy",
            ));
        }
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Ignore,
        } => return Ok(()),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Hard,
        } => (
            format!("DELETE FROM {target} AS target USING {stage_sql} AS stage WHERE {predicate}"),
            None,
        ),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Soft { marker_field },
        } => {
            let marker = quote_user_identifier(&PostgresIdentifier::user(marker_field)?)?;
            let matched = matching_key_count(client, output, stage, &key.fields)?;
            (
                format!(
                    "UPDATE {target} AS target SET {marker} = TRUE FROM {stage_sql} AS stage WHERE {predicate} AND target.{marker} IS DISTINCT FROM TRUE"
                ),
                Some(matched),
            )
        }
    };
    let deleted = client
        .execute(&sql, &[])
        .map_err(|error| postgres_error("apply Postgres CDC delete effects", error))?;
    counts.deleted = checked_add(counts.deleted, deleted)?;
    if let Some(matched) = soft_matched {
        counts.already_deleted = checked_add(
            counts.already_deleted,
            matched.checked_sub(deleted).ok_or_else(|| {
                CdfError::internal("Postgres soft-delete transitions exceed matched keys")
            })?,
        )?;
    }
    Ok(())
}

fn matching_key_count(
    client: &mut Client,
    output: &PostgresOutputPlan,
    stage: &PostgresIdentifier,
    keys: &[String],
) -> Result<u64> {
    let count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} AS target JOIN {} AS stage ON {}",
                validated_target_sql(&output.load.target)?,
                quote_system_identifier(stage)?,
                key_predicate(keys)?
            ),
            &[],
        )
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("count existing Postgres CDC keys", error))?;
    u64::try_from(count).map_err(|_| CdfError::internal("Postgres key count was negative"))
}

fn count_target_rows(client: &mut Client, output: &PostgresOutputPlan) -> Result<u64> {
    let count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                validated_target_sql(&output.load.target)?
            ),
            &[],
        )
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("count routed Postgres rows before replace", error))?;
    u64::try_from(count).map_err(|_| CdfError::internal("Postgres target row count was negative"))
}

fn key_predicate(keys: &[String]) -> Result<String> {
    keys.iter()
        .map(|key| {
            let key = quote_user_identifier(&PostgresIdentifier::user(key)?)?;
            Ok(format!("target.{key} = stage.{key}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|predicates| predicates.join(" AND "))
}

fn keyed_counts(
    content: &PackageContentAuthority,
    counts: &PostgresOutputCounts,
) -> Result<CommitCounts> {
    let PackageContentAuthority::KeyedChanges {
        reduction,
        delete_application,
        ..
    } = content
    else {
        return Err(CdfError::data(
            "Postgres keyed receipt counts require keyed-change content",
        ));
    };
    if counts.inserted.checked_add(counts.updated) != Some(reduction.surviving.upserts) {
        return Err(CdfError::destination(
            "Postgres applied upsert outcomes do not match package intent",
        ));
    }
    let (hard, soft, missing, ignored) = match delete_application {
        DeleteApplicationAuthority::NotApplicable => (None, None, None, None),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Ignore,
        } => (None, None, None, Some(reduction.surviving.deletes)),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Hard,
        } => (
            Some(counts.deleted),
            None,
            Some(
                reduction
                    .surviving
                    .deletes
                    .checked_sub(counts.deleted)
                    .ok_or_else(|| {
                        CdfError::internal("Postgres hard deletes exceed package intent")
                    })?,
            ),
            None,
        ),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Soft { .. },
        } if counts.already_deleted == 0 => (
            None,
            Some(counts.deleted),
            Some(
                reduction
                    .surviving
                    .deletes
                    .checked_sub(counts.deleted)
                    .ok_or_else(|| {
                        CdfError::internal("Postgres soft deletes exceed package intent")
                    })?,
            ),
            None,
        ),
        DeleteApplicationAuthority::Apply {
            policy: DeleteApplicationPolicy::Soft { .. },
        } => (None, None, None, None),
    };
    Ok(CommitCounts::keyed_changes(
        reduction.surviving,
        Some(counts.inserted),
        Some(counts.updated),
        hard,
        soft,
        missing,
        ignored,
    ))
}

fn routed_counts(
    outputs: &[PostgresOutputPlan],
    counts: Vec<PostgresOutputCounts>,
) -> Result<CommitCounts> {
    let targets = outputs
        .iter()
        .zip(counts)
        .map(|(output, counts)| {
            let binding = output
                .binding
                .as_ref()
                .ok_or_else(|| CdfError::internal("Postgres routed output omits route binding"))?;
            let counts = match &output.content {
                PackageContentAuthority::Rows { .. } => CommitCounts::rows(
                    counts.rows_written,
                    Some(counts.inserted),
                    Some(0),
                    Some(counts.deleted),
                ),
                PackageContentAuthority::KeyedChanges { .. } => {
                    keyed_counts(&output.content, &counts)?
                }
                PackageContentAuthority::Routed { .. } => {
                    return Err(CdfError::data("nested routed content is invalid"));
                }
            };
            Ok(RoutedTargetCommitCounts {
                output_binding: binding.output_binding.clone(),
                target: binding.physical_target.clone(),
                schema_hash: binding.schema_hash.clone(),
                counts: Box::new(counts),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CommitCounts::Routed { targets })
}

fn soft_marker(authority: &DeleteApplicationAuthority) -> Option<&str> {
    let DeleteApplicationAuthority::Apply {
        policy: DeleteApplicationPolicy::Soft { marker_field },
    } = authority
    else {
        return None;
    };
    Some(marker_field)
}

fn validate_soft_marker_target(client: &mut Client, output: &PostgresOutputPlan) -> Result<()> {
    let PackageContentAuthority::KeyedChanges {
        delete_application, ..
    } = &output.content
    else {
        return Ok(());
    };
    let Some(marker) = soft_marker(delete_application) else {
        return Ok(());
    };
    let schema = output
        .load
        .target
        .schema
        .as_ref()
        .map(PostgresIdentifier::as_str);
    let row = client
        .query_opt(
            "SELECT data_type, is_nullable FROM information_schema.columns WHERE table_schema = COALESCE($1, current_schema()) AND table_name = $2 AND column_name = $3",
            &[&schema, &output.load.target.table.as_str(), &marker],
        )
        .map_err(|error| postgres_error("validate Postgres soft-delete marker", error))?
        .ok_or_else(|| {
            CdfError::destination(format!(
                "Postgres soft-delete marker `{marker}` is absent after target migration"
            ))
        })?;
    let data_type: String = row.get(0);
    let nullable: String = row.get(1);
    if !data_type.eq_ignore_ascii_case("boolean") || nullable != "NO" {
        return Err(CdfError::destination(format!(
            "existing Postgres soft-delete marker `{marker}` is {data_type} {nullable}; it must be BOOLEAN NOT NULL"
        )));
    }
    Ok(())
}

fn execute_statements(
    client: &mut Client,
    statements: &[crate::plan::PostgresStatement],
) -> Result<()> {
    for statement in statements {
        client
            .batch_execute(&statement.sql)
            .map_err(|error| postgres_error(format!("execute {}", statement.name), error))?;
    }
    Ok(())
}

fn set_search_path(client: &mut Client, target: &PostgresTarget) -> Result<()> {
    let Some(schema) = &target.schema else {
        return Ok(());
    };
    client
        .batch_execute(&format!(
            "SET LOCAL search_path = {}, public",
            schema.quoted()
        ))
        .map_err(|error| postgres_error("set Postgres CDC transaction search_path", error))
}

fn checked_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| CdfError::data("Postgres CDC commit count overflowed u64"))
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field};
    use cdf_kernel::{
        CanonicalArrowSchema, DEDUP_KEY_ENCODING_VERSION, DeletionCaptureAuthority,
        DeletionCaptureSupport, DeliveryGuarantee, KeyAuthority, KeyedEffectCounts,
        KeyedEffectInputOrder, KeyedEffectReductionAuthority, KeyedEffectWinnerPolicy,
        OutputBindingId, PackageHash, RoutePlan, RouteScalar, RouteTargetFamily, SchemaHash,
        TargetName,
    };

    use super::*;

    fn keyed_content(schema_hash: &SchemaHash) -> PackageContentAuthority {
        PackageContentAuthority::KeyedChanges {
            logical_schema_hash: schema_hash.clone(),
            upsert_schema_hash: schema_hash.clone(),
            delete_schema_hash: SchemaHash::new("sha256:delete-schema").unwrap(),
            key: KeyAuthority {
                version: 1,
                fields: vec!["id".to_owned()],
                encoding: DEDUP_KEY_ENCODING_VERSION.to_owned(),
                schema_hash: SchemaHash::new("sha256:key-schema").unwrap(),
            },
            reduction: Box::new(KeyedEffectReductionAuthority {
                version: 1,
                winner: KeyedEffectWinnerPolicy::Last,
                input_order: KeyedEffectInputOrder::SourceProtocol {
                    protocol: "test".to_owned(),
                    version: 1,
                    scope_sha256: format!("sha256:{}", "a".repeat(64)),
                },
                input: KeyedEffectCounts::default(),
                duplicate_key_count: 0,
                surviving: KeyedEffectCounts::default(),
                provenance_format: "parquet".to_owned(),
                provenance_version: 1,
            }),
            deletion_capture: DeletionCaptureAuthority {
                support: DeletionCaptureSupport::Inherent,
                enabled: true,
                semantics_sha256: format!("sha256:{}", "b".repeat(64)),
            },
            delete_application: DeleteApplicationAuthority::Apply {
                policy: DeleteApplicationPolicy::Hard,
            },
        }
    }

    #[test]
    fn empty_routed_planning_is_a_data_noop() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let canonical = CanonicalArrowSchema::from_arrow(&schema).unwrap();
        let schema_hash = cdf_kernel::canonical_arrow_schema_hash(&schema).unwrap();
        let route_plan = RoutePlan::new("kind", 2).unwrap();
        let route_values = StringArray::from(vec!["alpha", "beta"]);
        let family = RouteTargetFamily::new(
            route_plan,
            TargetName::new("public.events").unwrap(),
            Some(63),
            vec![
                (
                    RouteScalar::from_array(&route_values, 0).unwrap(),
                    schema_hash.clone(),
                ),
                (
                    RouteScalar::from_array(&route_values, 1).unwrap(),
                    schema_hash.clone(),
                ),
            ],
        )
        .unwrap();
        let outputs = family
            .bindings
            .iter()
            .map(|binding| cdf_kernel::RoutedOutputContentAuthority {
                output_binding: OutputBindingId::new(binding.output_binding.as_str()).unwrap(),
                schema: canonical.clone(),
                content: Box::new(keyed_content(&schema_hash)),
                segment_ids: Vec::new(),
            })
            .collect();
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("sha256:routed-postgres-test").unwrap(),
            content: PackageContentAuthority::Routed { family, outputs },
            target: TargetName::new("public.events").unwrap(),
            disposition: WriteDisposition::CdcApply,
            segments: Vec::new(),
            idempotency_token: cdf_kernel::IdempotencyToken::new("sha256:routed-postgres-test")
                .unwrap(),
        };
        let plan = plan_routed(&request).unwrap();
        assert_eq!(
            plan.delivery_guarantee,
            DeliveryGuarantee::EffectivelyOncePerPosition
        );
        assert!(plan.migrations.is_empty());
    }

    #[test]
    fn keyed_receipt_counts_partition_delete_outcomes_exactly() {
        let schema_hash = SchemaHash::new("sha256:logical").unwrap();
        let mut content = keyed_content(&schema_hash);
        if let PackageContentAuthority::KeyedChanges { reduction, .. } = &mut content {
            reduction.input = KeyedEffectCounts {
                upserts: 3,
                deletes: 5,
            };
            reduction.surviving = KeyedEffectCounts {
                upserts: 3,
                deletes: 5,
            };
        }
        let counts = keyed_counts(
            &content,
            &PostgresOutputCounts {
                rows_written: 0,
                inserted: 2,
                updated: 1,
                deleted: 4,
                already_deleted: 0,
            },
        )
        .unwrap();
        assert_eq!(
            counts,
            CommitCounts::keyed_changes(
                KeyedEffectCounts {
                    upserts: 3,
                    deletes: 5,
                },
                Some(2),
                Some(1),
                Some(4),
                None,
                Some(1),
                None,
            )
        );
    }
}
