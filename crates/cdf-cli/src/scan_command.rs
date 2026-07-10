use std::{collections::BTreeMap, fs};

use cdf_contract::{ContractPolicy, IdentifierPolicy, ObservedSchema, compile_validation_program};
use cdf_declarative::{CompiledResource, CompiledResourcePlan};
use cdf_engine::{EnginePlan, EnginePlanInput, PlanBoundedness, Planner};
use cdf_kernel::{
    CapabilitySupport, CdfError, DeliveryGuarantee, DestinationSheet, IdempotencySupport, OrderBy,
    PartitionPlan, PredicateId, QueryableResource, ResourceStream, ScanPredicate, ScanRequest,
    SchemaSource, SortDirection, TargetName, TransactionSupport, WriteDisposition,
};
use futures_util::StreamExt;
use serde::Serialize;

use crate::{
    args::{Cli, ScanArgs},
    commands::json_cli_error,
    context::ProjectContext,
    destination_uri::{EnvironmentDestination, redact_error_value, resolve_selected_destination},
    error_catalog,
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
    project_run_resource::{
        CliProjectRunSource, build_project_run_resource, file_runtime_dependencies,
    },
    render::{
        RenderDocument,
        humanize::{humanize_bytes, humanize_rows},
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
    reports::{SchemaSnapshotActionReport, WriteEffects},
};

pub(crate) struct PreparedDiscoveryForCli {
    pub(crate) resource: CompiledResource,
    pub(crate) schema_snapshot: Option<SchemaSnapshotActionReport>,
}

pub(crate) fn plan_or_explain(
    cli: &Cli,
    args: ScanArgs,
    command: &'static str,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_for_command_with_locked_snapshots(
        command,
        cli.project.as_ref(),
        cli.env.as_deref(),
        !args.no_pin,
    )?;
    let target = scan_target(&args)?;
    let resource = context.resource(&args.resource_id)?;
    let prepared = prepare_discover_resource_for_cli(&context, resource, args.no_pin)?;
    let resolved =
        resolve_scan_destination(&context, &target, args.destination_uri.as_deref(), command)?;
    let identifier_policy = resolved.destination.column_identifier_policy()?;
    let runtime_resource = build_project_run_resource(&context, &prepared.resource)?;
    let plan = build_engine_plan_for_resource(
        runtime_resource.as_queryable(),
        &args,
        identifier_policy.as_ref(),
    )?;
    let report = scan_report(
        &context,
        &prepared.resource,
        &plan,
        command,
        resolved,
        prepared.schema_snapshot,
    )?;
    CommandOutput::rendered(
        command,
        scan_report_document(command, &report, args.destination_uri.as_deref()),
        report,
    )
}

pub(crate) fn preview(cli: &Cli, args: ScanArgs) -> Result<CommandOutput, CliError> {
    let context =
        ProjectContext::load_for_command("preview", cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&args.resource_id)?;
    let prepared = prepare_discover_resource_for_cli(&context, resource, false)?;
    let target = scan_target(&args)?;
    let resolved = resolve_scan_destination(
        &context,
        &target,
        args.destination_uri.as_deref(),
        "preview",
    )?;
    let identifier_policy = resolved.destination.column_identifier_policy()?;
    let runtime_resource = build_project_run_resource(&context, &prepared.resource)?;
    let plan = build_engine_plan_for_resource(
        runtime_resource.as_queryable(),
        &args,
        identifier_policy.as_ref(),
    )?;
    match preview_one_batch(&runtime_resource, &plan) {
        Ok(report) => CommandOutput::rendered("preview", preview_document(&report), report),
        Err(error) if lower_runtime_missing(&error) => Err(CliError::not_supported_with(
            "preview",
            error.message,
            "resource runtime open implementation",
            error_catalog::PREVIEW_RUNTIME_NOT_SUPPORTED,
        )),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn prepare_discover_resource_for_cli(
    context: &ProjectContext,
    resource: &CompiledResource,
    no_pin: bool,
) -> Result<PreparedDiscoveryForCli, CliError> {
    if let SchemaSource::Discovered { snapshot } = &resource.descriptor().schema_source
        && !no_pin
    {
        let prepared = cdf_project::prepare_pinned_resource_effective_schema(
            &context.root,
            resource,
            &context.secret_provider(),
        )?;
        return Ok(PreparedDiscoveryForCli {
            resource: prepared,
            schema_snapshot: Some(SchemaSnapshotActionReport {
                outcome: "unchanged",
                schema_hash: snapshot.schema_hash.to_string(),
                path: snapshot.path.clone(),
                snapshot_written: false,
                lockfile_written: false,
            }),
        });
    }
    let probe_resource = if no_pin
        && resource
            .descriptor()
            .schema_source
            .pinned_snapshot()
            .is_some()
    {
        resource.with_schema_source_and_schema(SchemaSource::Discover, resource.schema())
    } else {
        resource.clone()
    };
    if !matches!(
        probe_resource.descriptor().schema_source,
        SchemaSource::Discover
    ) {
        return Ok(PreparedDiscoveryForCli {
            resource: resource.clone(),
            schema_snapshot: None,
        });
    }
    let secret_provider = context.secret_provider();
    let options = match resource.descriptor().schema_source.pinned_snapshot() {
        Some(snapshot) => {
            let (_, verified_baseline) = cdf_project::SchemaSnapshotStore::new(&context.root)
                .read_with_verified_baseline(snapshot)?;
            cdf_project::SchemaDiscoveryExecutionOptions::new()
                .with_verified_baseline(verified_baseline)
        }
        None => cdf_project::SchemaDiscoveryExecutionOptions::new(),
    };
    let artifacts = if matches!(probe_resource.plan(), CompiledResourcePlan::Files(plan) if is_http_file_plan(plan))
    {
        cdf_project::discover_resource_schema_with_file_dependencies_artifacts(
            &probe_resource,
            &secret_provider,
            file_runtime_dependencies(context)?,
            options,
        )?
    } else if matches!(probe_resource.plan(), CompiledResourcePlan::Rest(_)) {
        let mut transport = ReqwestHttpTransport::new()?;
        cdf_project::ResourceSchemaDiscoveryArtifacts::new(
            cdf_project::discover_resource_schema_with_rest_transport(
                &probe_resource,
                &secret_provider,
                &mut transport,
            )?,
            None,
        )
    } else {
        cdf_project::discover_resource_schema_artifacts(&probe_resource, &secret_provider, options)?
    };
    let discovery = artifacts.discovery.clone();
    let artifact = discovery.snapshot.artifact.clone();
    let outcome = if no_pin { "inspection_only" } else { "added" };
    let prepared = cdf_project::apply_discovered_schema(&probe_resource, discovery);
    let (snapshot_written, lockfile_written) = if no_pin {
        (false, false)
    } else {
        let updated_lock = cdf_project::pin_schema_snapshot_in_project_lockfile(
            &context.config,
            &context.resources,
            context.lock.as_ref(),
            &context.environment.destination,
            &prepared.resource,
        )?;
        let encoded = cdf_project::lock_to_toml(&updated_lock)?;
        let snapshot_written =
            cdf_project::write_schema_discovery_artifacts(&context.root, &artifacts)?
                .snapshot_written;
        let lock_path = context.root.join(cdf_project::LOCK_FILE_NAME);
        let lockfile_written = fs::read_to_string(&lock_path).ok().as_deref() != Some(&encoded);
        if lockfile_written {
            cdf_project::write_lock_file_guarded(
                &lock_path,
                context.lock_authority.as_ref(),
                encoded,
            )?;
        }
        (snapshot_written, lockfile_written)
    };
    let prepared_resource = if !no_pin {
        cdf_project::prepare_pinned_resource_effective_schema(
            &context.root,
            &prepared.resource,
            &secret_provider,
        )?
    } else {
        prepared.resource
    };
    Ok(PreparedDiscoveryForCli {
        resource: prepared_resource,
        schema_snapshot: Some(SchemaSnapshotActionReport {
            outcome,
            schema_hash: artifact.schema_hash.to_string(),
            path: artifact.path.clone(),
            snapshot_written,
            lockfile_written,
        }),
    })
}

pub(crate) fn build_engine_plan_for_resource(
    resource: &dyn QueryableResource,
    args: &ScanArgs,
    identifier_policy: Option<&IdentifierPolicy>,
) -> Result<EnginePlan, CliError> {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    if let Some(identifier_policy) = identifier_policy {
        policy.normalization.identifier = identifier_policy.clone();
    }
    let validation_program = compile_validation_program(&policy, &observed_schema)?;
    let request = scan_request(resource.descriptor(), args)?;
    let input = EnginePlanInput {
        request,
        validation_program,
        boundedness: PlanBoundedness::Bounded,
        package_id: args
            .package_id
            .clone()
            .unwrap_or_else(|| format!("cli-{}", resource.descriptor().resource_id)),
    };
    Planner::new()
        .plan_tier_b(resource, input)
        .map_err(CliError::from)
}

fn scan_request(
    descriptor: &cdf_kernel::ResourceDescriptor,
    args: &ScanArgs,
) -> Result<ScanRequest, CliError> {
    let filters = args
        .filters
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok(ScanPredicate {
                predicate_id: PredicateId::new(format!("p{:03}", index + 1))?,
                expression: expression.clone(),
            })
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    Ok(ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: args.projection.clone(),
        filters,
        limit: args.limit,
        order_by: args
            .order_by
            .iter()
            .map(|order| parse_order_by(order))
            .collect::<Result<Vec<_>, _>>()?,
        scope: descriptor.state_scope.clone(),
    })
}

fn parse_order_by(raw: &str) -> Result<OrderBy, CliError> {
    let (field, direction) = raw.split_once(':').unwrap_or((raw, "asc"));
    let direction = match direction {
        "asc" => SortDirection::Asc,
        "desc" => SortDirection::Desc,
        other => {
            return Err(CliError::usage_with(
                format!("unsupported order direction `{other}`"),
                error_catalog::SCAN_ARGUMENT,
            ));
        }
    };
    Ok(OrderBy {
        field: field.to_owned(),
        direction,
    })
}

fn scan_report(
    context: &ProjectContext,
    resource: &CompiledResource,
    plan: &EnginePlan,
    command: &'static str,
    resolved: EnvironmentDestination,
    schema_snapshot: Option<SchemaSnapshotActionReport>,
) -> Result<ScanPlanReport, CliError> {
    let destination_plan = destination_plan_report(resolved, resource, command)?;
    Ok(ScanPlanReport {
        project: context.config.project.name.clone(),
        environment: context.environment.name.clone(),
        resource_id: plan.scan.request.resource_id.to_string(),
        resource_schema: resource_schema_report(
            resource,
            &destination_plan.schema_hash,
            &plan.validation_program,
            plan.effective_schema_evidence(),
        ),
        normalization: plan.validation_program.identifier_policy.clone(),
        will_fetch: FetchReport {
            partitions: plan.scan.partitions.iter().map(partition_report).collect(),
            projection: plan.scan.request.projection.clone().unwrap_or_default(),
            filters: plan
                .scan
                .request
                .filters
                .iter()
                .map(|predicate| predicate.expression.clone())
                .collect(),
            limit: plan.scan.request.limit,
        },
        pushdown: PushdownReport {
            pushed: plan.explain.pushed_predicates.clone(),
            inexact: plan.explain.inexact_predicates.clone(),
            unsupported: plan.explain.unsupported_predicates.clone(),
        },
        destination: destination_plan.destination,
        ddl_preview: destination_plan.ddl_preview,
        delivery_guarantee: destination_plan.delivery_guarantee.guarantee.clone(),
        delivery_guarantee_detail: destination_plan.delivery_guarantee,
        state_advancement: StateAdvancementReport {
            scope: serde_json::to_value(&resource.descriptor().state_scope)
                .map_err(json_cli_error)?,
            cursor: resource
                .descriptor()
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.clone()),
            advances_after:
                "destination receipt is recorded and CheckpointStore::commit verifies coverage"
                    .to_owned(),
        },
        explain: plan.explain.clone(),
        package_id: plan.package_id.clone(),
        schema_snapshot,
    })
}

fn scan_target(args: &ScanArgs) -> Result<TargetName, CliError> {
    let target = args
        .target
        .clone()
        .unwrap_or_else(|| default_target_for_resource(&args.resource_id));
    TargetName::new(target).map_err(CliError::from)
}

pub(crate) fn default_target_for_resource(resource_id: &str) -> String {
    resource_id
        .rsplit('.')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(resource_id)
        .to_owned()
}

fn destination_plan_report(
    resolved: EnvironmentDestination,
    resource: &cdf_declarative::CompiledResource,
    command: &'static str,
) -> Result<DestinationPlanReport, CliError> {
    let mut destination = resolved.destination;
    let plan = destination
        .plan_resource_commit(resource)
        .map_err(|error| {
            let mut error = redact_error_value(error, resolved.secret_redaction.as_deref());
            error.message = command_correct_scan_message(command, error.message);
            CliError::from(error)
        })?;
    DestinationPlanReport::from_project(plan, resource).map_err(CliError::from)
}

fn resolve_scan_destination(
    context: &ProjectContext,
    target: &TargetName,
    destination_uri: Option<&str>,
    command: &'static str,
) -> Result<EnvironmentDestination, CliError> {
    resolve_selected_destination(context, target, destination_uri).map_err(|error| {
        plan_destination_resolution_error(command, context, destination_uri, error)
    })
}

fn command_correct_scan_message(command: &str, message: String) -> String {
    if command == "run" {
        message
    } else {
        message.replace("cdf run ", &format!("cdf {command} "))
    }
}

fn plan_destination_resolution_error(
    command: &'static str,
    context: &ProjectContext,
    destination_uri: Option<&str>,
    error: CdfError,
) -> CliError {
    let error = redact_error_value(error, None);
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            command,
            error.message,
            "registered no-write project destination planner",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(crate::destination_uri::destination_error_suggestions(
            context,
            destination_uri,
        ))
    } else {
        error.into()
    }
}

fn partition_report(partition: &PartitionPlan) -> PartitionReport {
    PartitionReport {
        partition_id: partition.partition_id.to_string(),
        scope_kind: format!("{:?}", partition.scope.kind()),
        metadata: partition.metadata.clone(),
    }
}

fn preview_one_batch(
    resource: &CliProjectRunSource,
    plan: &EnginePlan,
) -> cdf_kernel::Result<PreviewReport> {
    validate_preview_direct_stream_plan(plan)?;
    let partition = plan
        .scan
        .partitions
        .first()
        .ok_or_else(|| CdfError::data("preview plan has no partitions"))?
        .clone();
    let mut stream = futures_executor::block_on(resource.open_preview(partition))?;
    let batch = futures_executor::block_on(async { stream.next().await })
        .ok_or_else(|| CdfError::data("resource produced no preview batch"))??;
    let normalized = cdf_engine::normalize_record_batch(
        batch
            .record_batch()
            .ok_or_else(|| CdfError::data("resource preview requires an Arrow record batch"))?
            .clone(),
        &plan.validation_program,
    )?;
    let writes = WriteEffects::default();
    Ok(PreviewReport {
        resource: batch.header.resource_id.to_string(),
        partition: batch.header.partition_id.to_string(),
        batch: batch.header.batch_id.to_string(),
        resource_id: batch.header.resource_id.to_string(),
        batch_id: batch.header.batch_id.to_string(),
        partition_id: batch.header.partition_id.to_string(),
        row_count: batch.header.row_count,
        byte_count: batch.header.byte_count,
        fields: normalized
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
        normalization: plan.validation_program.identifier_policy.clone(),
        write_effects: writes.clone(),
        writes,
    })
}

fn validate_preview_direct_stream_plan(plan: &EnginePlan) -> cdf_kernel::Result<()> {
    if !plan.residual_predicates.is_empty() {
        let expressions = plan
            .residual_predicates
            .iter()
            .map(|predicate| predicate.expression.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(CdfError::contract(format!(
            "cdf preview cannot apply residual predicates without creating a package; remove unsupported or inexact filters, or use a resource that pushes them exactly: {expressions}"
        )));
    }
    if plan.final_projection.is_some() && !plan.explain.projection_pushed {
        return Err(CdfError::contract(
            "cdf preview cannot apply projection without creating a package; use a resource with projection pushdown or omit --projection",
        ));
    }
    if plan.scan.request.limit.is_some() && !plan.explain.limit_pushed {
        return Err(CdfError::contract(
            "cdf preview cannot apply limit without creating a package; use a resource with limit pushdown or omit --limit",
        ));
    }
    Ok(())
}

fn lower_runtime_missing(error: &CdfError) -> bool {
    error
        .message
        .contains("execution is outside the MVP compiler crate")
}

fn is_http_file_plan(plan: &cdf_declarative::FileResourcePlan) -> bool {
    plan.root.starts_with("http://") || plan.root.starts_with("https://")
}

fn scan_report_document(
    command: &str,
    report: &ScanPlanReport,
    destination_uri: Option<&str>,
) -> RenderDocument {
    let pushed = report.pushdown.pushed.len();
    let inexact = report.pushdown.inexact.len();
    let unsupported = report.pushdown.unsupported.len();
    let migrations = report.ddl_preview.migrations.len();
    let document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "{command} {} -> {}",
                report.resource_id, report.destination.target
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Fetch")
                .row("project", report.project.clone())
                .row("environment", report.environment.clone())
                .row("package", report.package_id.clone())
                .row("partitions", report.will_fetch.partitions.len().to_string())
                .row(
                    "projection",
                    list_or_default(&report.will_fetch.projection, "all fields"),
                )
                .row(
                    "filters",
                    list_or_default(&report.will_fetch.filters, "none"),
                )
                .row("limit", optional_u64(report.will_fetch.limit)),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Pushdown")
                .row("pushed", pushed.to_string())
                .row("inexact", inexact.to_string())
                .row("unsupported", unsupported.to_string())
                .row("projection", yes_no(report.explain.projection_pushed))
                .row("limit", yes_no(report.explain.limit_pushed)),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Destination")
                .row("destination", report.destination.destination_id.clone())
                .row("target", report.destination.target.clone())
                .row("label", safe_display_value(&report.destination.label))
                .row("schemes", report.destination.schemes.join(", "))
                .row("disposition", report.destination.disposition.clone())
                .row("idempotency", report.destination.idempotency.clone()),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Guarantee")
                .row("guarantee", report.delivery_guarantee.clone())
                .row(
                    "qualifier",
                    report.delivery_guarantee_detail.qualifier.clone(),
                )
                .row("basis", report.delivery_guarantee_detail.basis.clone()),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Contract")
                .row("schema", report.resource_schema.schema_hash.clone())
                .row("normalizer", report.normalization.version.clone())
                .row(
                    "schema source",
                    report.resource_schema.schema_source.clone(),
                )
                .row(
                    "schema snapshot",
                    report
                        .resource_schema
                        .snapshot_path
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .row("fields", report.resource_schema.fields.len().to_string())
                .row("state scope", report.state_advancement.scope.to_string())
                .row(
                    "cursor",
                    report
                        .state_advancement
                        .cursor
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .row(
                    "advances after",
                    report.state_advancement.advances_after.clone(),
                ),
        );
    let mut document = if let Some(snapshot) = &report.schema_snapshot {
        document.blank_line().push(
            KeyValuePanel::new("Schema Snapshot")
                .row("outcome", snapshot.outcome)
                .row("hash", snapshot.schema_hash.clone())
                .row("path", snapshot.path.clone())
                .row("snapshot written", yes_no(snapshot.snapshot_written))
                .row("lockfile written", yes_no(snapshot.lockfile_written)),
        )
    } else {
        document
    };
    document = document.blank_line().push(
        KeyValuePanel::new("Migration")
            .row("supported", yes_no(report.ddl_preview.supported))
            .row("support", report.ddl_preview.migration_support.clone())
            .row("items", migrations.to_string())
            .row("target", report.ddl_preview.target.clone()),
    );

    if !report.ddl_preview.migrations.is_empty() {
        let table = report.ddl_preview.migrations.iter().fold(
            Table::new(["migration", "description"]),
            |table, migration| {
                table.row([
                    migration.migration_id.clone(),
                    safe_display_value(&migration.description),
                ])
            },
        );
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new(next_run_command(
            &report.resource_id,
            &report.destination.target,
            destination_uri,
        )))
}

fn list_or_default(values: &[String], default: &str) -> String {
    if values.is_empty() {
        default.to_owned()
    } else {
        values.join(", ")
    }
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}

fn next_run_command(resource_id: &str, target: &str, destination_uri: Option<&str>) -> String {
    let mut command = format!("cdf run {resource_id}");
    if target != default_target_for_resource(resource_id) {
        command.push_str(" --target ");
        command.push_str(target);
    }
    if let Some(destination_uri) = destination_uri {
        command.push_str(" --to ");
        command.push_str(&safe_display_value(destination_uri));
    }
    command
}

fn preview_document(report: &PreviewReport) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("previewed resource {}", report.resource_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Preview")
                .row("resource", report.resource.clone())
                .row("partition", report.partition.clone())
                .row("batch", report.batch.clone())
                .row("rows", humanize_rows(report.row_count))
                .row("bytes", humanize_bytes(report.byte_count))
                .row("normalizer", report.normalization.version.clone())
                .row("fields", report.fields.join(", ")),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Writes")
                .row("package", yes_no(report.writes.package()))
                .row("destination", yes_no(report.writes.destination()))
                .row("checkpoint", yes_no(report.writes.checkpoint())),
        )
        .blank_line()
        .push(NextCommand::new(format!("cdf plan {}", report.resource_id)))
}

#[cfg(test)]
mod render_tests {
    use super::*;

    #[test]
    fn next_run_command_includes_explicit_destination_without_minted_ids() {
        assert_eq!(
            next_run_command(
                "local.events",
                "events",
                Some("duckdb://.cdf/explain-render.duckdb")
            ),
            "cdf run local.events --to duckdb://.cdf/explain-render.duckdb"
        );
    }

    #[test]
    fn next_run_command_preserves_non_default_target_and_redacts_destination_userinfo() {
        let command = next_run_command(
            "local.events",
            "custom_events",
            Some("postgres://user:secret-value@localhost/db"),
        );

        assert_eq!(
            command,
            "cdf run local.events --target custom_events --to postgres://[redacted]@localhost/db"
        );
        assert!(!command.contains("secret-value"));
        assert!(!command.contains("--package-id"));
        assert!(!command.contains("--checkpoint-id"));
    }

    #[test]
    fn plan_error_wording_uses_plan_command_name() {
        assert_eq!(
            command_correct_scan_message(
                "plan",
                "cdf run requires a pinned schema hash".to_owned()
            ),
            "cdf plan requires a pinned schema hash"
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ScanPlanReport {
    project: String,
    environment: String,
    resource_id: String,
    resource_schema: ResourceSchemaReport,
    normalization: IdentifierPolicy,
    will_fetch: FetchReport,
    pushdown: PushdownReport,
    destination: DestinationReport,
    ddl_preview: DdlPreviewReport,
    delivery_guarantee: String,
    delivery_guarantee_detail: DeliveryGuaranteeReport,
    state_advancement: StateAdvancementReport,
    explain: cdf_engine::ExplainData,
    package_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_snapshot: Option<SchemaSnapshotActionReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSchemaReport {
    schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    baseline_snapshot_schema_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_snapshot_schema_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_arrow_schema_hash: Option<String>,
    schema_source: String,
    snapshot_path: Option<String>,
    snapshot_metadata: BTreeMap<String, String>,
    fields: Vec<ResourceSchemaFieldReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSchemaFieldReport {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FetchReport {
    partitions: Vec<PartitionReport>,
    projection: Vec<String>,
    filters: Vec<String>,
    limit: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PartitionReport {
    partition_id: String,
    scope_kind: String,
    metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PushdownReport {
    pushed: Vec<cdf_engine::PredicateExplain>,
    inexact: Vec<cdf_engine::PredicateExplain>,
    unsupported: Vec<cdf_engine::PredicateExplain>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DestinationReport {
    destination_id: String,
    schemes: Vec<String>,
    label: String,
    target: String,
    disposition: String,
    idempotency: String,
    supported_dispositions: Vec<String>,
    sheet: DestinationSheet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DdlPreviewReport {
    supported: bool,
    reason: Option<String>,
    target: String,
    disposition: String,
    migration_support: String,
    migrations: Vec<cdf_kernel::MigrationRecord>,
    synthetic_package_hash: String,
    synthetic_idempotency_token: String,
    synthetic_segments: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeliveryGuaranteeReport {
    guarantee: String,
    disposition: String,
    idempotency: String,
    qualifier: String,
    basis: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateAdvancementReport {
    scope: serde_json::Value,
    cursor: Option<String>,
    advances_after: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewReport {
    resource: String,
    partition: String,
    batch: String,
    resource_id: String,
    batch_id: String,
    partition_id: String,
    row_count: u64,
    byte_count: u64,
    fields: Vec<String>,
    normalization: IdentifierPolicy,
    write_effects: WriteEffects,
    writes: WriteEffects,
}

struct DestinationPlanReport {
    schema_hash: cdf_kernel::SchemaHash,
    destination: DestinationReport,
    ddl_preview: DdlPreviewReport,
    delivery_guarantee: DeliveryGuaranteeReport,
}

impl DestinationPlanReport {
    fn from_project(
        plan: cdf_project::ProjectDestinationCommitPlan,
        resource: &cdf_declarative::CompiledResource,
    ) -> cdf_kernel::Result<Self> {
        let guarantee = delivery_guarantee_report(
            &plan.commit_plan.delivery_guarantee,
            &plan.commit_plan.disposition,
            &plan.commit_plan.idempotency,
            &plan.sheet,
            resource,
        )?;
        let migration_support = capability_support_name(&plan.sheet.migration_support).to_owned();
        let ddl_supported = matches!(plan.sheet.migration_support, CapabilitySupport::Supported);
        Ok(Self {
            schema_hash: plan.schema_hash.clone(),
            destination: DestinationReport {
                destination_id: plan.description.destination_id.to_string(),
                schemes: plan
                    .description
                    .schemes
                    .iter()
                    .map(|scheme| (*scheme).to_owned())
                    .collect(),
                label: plan.description.label.clone(),
                target: plan.target.to_string(),
                disposition: write_disposition_name(&plan.commit_plan.disposition).to_owned(),
                idempotency: idempotency_name(&plan.commit_plan.idempotency).to_owned(),
                supported_dispositions: plan
                    .sheet
                    .supported_dispositions
                    .iter()
                    .map(|disposition| write_disposition_name(disposition).to_owned())
                    .collect(),
                sheet: plan.sheet.clone(),
            },
            ddl_preview: DdlPreviewReport {
                supported: ddl_supported,
                reason: if ddl_supported {
                    None
                } else {
                    Some(
                        "destination sheet declares migration_support unsupported; no DDL migration preview is produced for this commit plan"
                            .to_owned(),
                    )
                },
                target: plan.commit_plan.target.to_string(),
                disposition: write_disposition_name(&plan.commit_plan.disposition).to_owned(),
                migration_support,
                migrations: plan.commit_plan.migrations.clone(),
                synthetic_package_hash: plan.synthetic.package_hash.to_string(),
                synthetic_idempotency_token: plan.synthetic.idempotency_token.to_string(),
                synthetic_segments: plan
                    .synthetic
                    .segment_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
            delivery_guarantee: guarantee,
        })
    }
}

fn resource_schema_report(
    resource: &cdf_declarative::CompiledResource,
    schema_hash: &cdf_kernel::SchemaHash,
    program: &cdf_contract::ValidationProgram,
    effective: Option<&cdf_engine::EffectiveSchemaPlanEvidence>,
) -> ResourceSchemaReport {
    let snapshot = resource.descriptor().schema_source.pinned_snapshot();
    ResourceSchemaReport {
        schema_hash: schema_hash.to_string(),
        baseline_snapshot_schema_hash: effective
            .map(|evidence| evidence.authority.baseline_snapshot.schema_hash.to_string()),
        effective_snapshot_schema_hash: effective.map(|evidence| {
            evidence
                .authority
                .effective_snapshot_schema_hash
                .to_string()
        }),
        effective_arrow_schema_hash: effective
            .map(|evidence| evidence.effective_arrow_schema_hash.to_string()),
        schema_source: schema_source_name(&resource.descriptor().schema_source).to_owned(),
        snapshot_path: snapshot.map(|snapshot| snapshot.path.clone()),
        snapshot_metadata: snapshot
            .map(|snapshot| snapshot.metadata.clone())
            .unwrap_or_default(),
        fields: resource
            .schema()
            .fields()
            .iter()
            .zip(&program.column_programs)
            .map(|(field, column)| ResourceSchemaFieldReport {
                name: column.output_name.clone(),
                data_type: format!("{:?}", field.data_type()),
                nullable: field.is_nullable(),
            })
            .collect(),
    }
}

fn schema_source_name(source: &SchemaSource) -> &'static str {
    match source {
        SchemaSource::Declared { .. } => "declared",
        SchemaSource::Discover => "discover",
        SchemaSource::Discovered { .. } => "discovered",
        SchemaSource::Hints {
            snapshot: Some(_), ..
        } => "hints_pinned",
        SchemaSource::Hints { snapshot: None, .. } => "hints",
        SchemaSource::Contract { .. } => "contract",
    }
}

fn delivery_guarantee_report(
    planned: &DeliveryGuarantee,
    disposition: &WriteDisposition,
    idempotency: &IdempotencySupport,
    sheet: &DestinationSheet,
    resource: &cdf_declarative::CompiledResource,
) -> cdf_kernel::Result<DeliveryGuaranteeReport> {
    if idempotency != &sheet.idempotency {
        return Err(CdfError::internal(format!(
            "destination commit plan idempotency {} does not match destination sheet idempotency {}",
            idempotency_name(idempotency),
            idempotency_name(&sheet.idempotency)
        )));
    }
    let expected = derive_delivery_guarantee(disposition, idempotency, sheet, resource);
    if &expected != planned {
        return Err(CdfError::internal(format!(
            "destination commit plan guarantee {} does not match guarantee table result {}",
            delivery_guarantee_name(planned),
            delivery_guarantee_name(&expected)
        )));
    }
    Ok(DeliveryGuaranteeReport {
        guarantee: delivery_guarantee_name(planned).to_owned(),
        disposition: write_disposition_name(disposition).to_owned(),
        idempotency: idempotency_name(idempotency).to_owned(),
        qualifier: delivery_guarantee_qualifier(planned).to_owned(),
        basis: delivery_guarantee_basis(planned).to_owned(),
    })
}

fn derive_delivery_guarantee(
    disposition: &WriteDisposition,
    idempotency: &IdempotencySupport,
    sheet: &DestinationSheet,
    resource: &cdf_declarative::CompiledResource,
) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Merge if !resource.descriptor().merge_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        WriteDisposition::Append if idempotency == &IdempotencySupport::PackageToken => {
            DeliveryGuarantee::EffectivelyOncePerPackage
        }
        WriteDisposition::Replace
            if matches!(
                sheet.transactions,
                TransactionSupport::AtomicTarget | TransactionSupport::AtomicPackage
            ) =>
        {
            DeliveryGuarantee::EffectivelyOncePerTarget
        }
        WriteDisposition::CdcApply if idempotency == &IdempotencySupport::PackageToken => {
            DeliveryGuarantee::EffectivelyOncePerPosition
        }
        WriteDisposition::Append
        | WriteDisposition::Merge
        | WriteDisposition::Replace
        | WriteDisposition::CdcApply => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    }
}

fn delivery_guarantee_name(guarantee: &DeliveryGuarantee) -> &'static str {
    match guarantee {
        DeliveryGuarantee::AtLeastOnceDuplicateRisk => "at_least_once_duplicate_risk",
        DeliveryGuarantee::EffectivelyOncePerKey => "effectively_once_per_key",
        DeliveryGuarantee::EffectivelyOncePerPackage => "effectively_once_per_package",
        DeliveryGuarantee::EffectivelyOncePerTarget => "effectively_once_per_target",
        DeliveryGuarantee::EffectivelyOncePerPosition => "effectively_once_per_position",
    }
}

fn delivery_guarantee_qualifier(guarantee: &DeliveryGuarantee) -> &'static str {
    match guarantee {
        DeliveryGuarantee::AtLeastOnceDuplicateRisk => "duplicate_risk",
        DeliveryGuarantee::EffectivelyOncePerKey => "per_key",
        DeliveryGuarantee::EffectivelyOncePerPackage => "per_package",
        DeliveryGuarantee::EffectivelyOncePerTarget => "per_target",
        DeliveryGuarantee::EffectivelyOncePerPosition => "per_position",
    }
}

fn delivery_guarantee_basis(guarantee: &DeliveryGuarantee) -> &'static str {
    match guarantee {
        DeliveryGuarantee::AtLeastOnceDuplicateRisk => {
            "at-least-once extraction without a qualifying idempotent destination rule leaves duplicate risk"
        }
        DeliveryGuarantee::EffectivelyOncePerKey => {
            "at-least-once extraction plus merge with a merge key gives effectively-once per key"
        }
        DeliveryGuarantee::EffectivelyOncePerPackage => {
            "at-least-once extraction plus append with package-token idempotency gives effectively-once per package"
        }
        DeliveryGuarantee::EffectivelyOncePerTarget => {
            "at-least-once extraction plus atomic replace gives effectively-once per target"
        }
        DeliveryGuarantee::EffectivelyOncePerPosition => {
            "at-least-once extraction plus ordered cdc_apply with package-token idempotency gives effectively-once per position"
        }
    }
}

fn write_disposition_name(disposition: &WriteDisposition) -> &'static str {
    match disposition {
        WriteDisposition::Append => "append",
        WriteDisposition::Replace => "replace",
        WriteDisposition::Merge => "merge",
        WriteDisposition::CdcApply => "cdc_apply",
    }
}

fn idempotency_name(idempotency: &IdempotencySupport) -> &'static str {
    match idempotency {
        IdempotencySupport::None => "none",
        IdempotencySupport::PackageToken => "package_token",
        IdempotencySupport::SegmentToken => "segment_token",
    }
}

fn capability_support_name(support: &CapabilitySupport) -> &'static str {
    match support {
        CapabilitySupport::Supported => "supported",
        CapabilitySupport::Unsupported => "unsupported",
    }
}
