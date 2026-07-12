use std::{collections::BTreeMap, sync::Arc};

use cdf_contract::{
    ContractPolicy, ObservedSchema, compile_resource_validation_program, reconcile_schema,
};
use cdf_declarative::{CompiledResource, CompiledResourcePlan};
use cdf_engine::{EnginePlanInput, PlanBoundedness, Planner};
use cdf_kernel::{ResourceDescriptor, ResourceStream, ScanRequest, SchemaSource};
use cdf_project::{
    FileResourceSourceResolver, ProjectResourceOrigin, ResourceSchemaDiscovery, validate_project,
};
use serde::Serialize;

use crate::{
    args::Cli,
    context::ProjectContext,
    destination_uri::{redact_error_value, resolve_environment_destination},
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
    scan_command::default_target_for_resource,
};

pub(super) fn run(
    cli: &Cli,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_for_command(
        "validate --deep",
        cli.project.as_ref(),
        cli.env.as_deref(),
    )?;
    let resolver = FileResourceSourceResolver::new(&context.root);
    let provider = context.secret_provider();
    let validation = validate_project(
        &context.config,
        Some(&context.environment.name),
        &resolver,
        &provider,
    )?;
    let mut resources = Vec::with_capacity(context.resources.len());
    for (resource, origin) in context.resources.iter().zip(&context.resource_origins) {
        resources.push(deep_validate_resource(
            &context, resource, origin, execution,
        ));
    }
    let summary = DeepValidateSummary::from_resources(&resources);
    let exit_code = if summary.failed == 0 { 0 } else { 3 };
    let report = DeepValidateReport {
        mode: "deep".to_owned(),
        project: context.config.project.name.clone(),
        environment: context.environment.name.clone(),
        declarative_resources: validation.declarative_resources,
        external_resources: validation.external_resources,
        checked_secrets: validation.checked_secrets.len(),
        summary,
        resources,
        writes: DeepValidateWrites::default(),
    };
    CommandOutput::rendered_with_exit_code("validate", document(&report), report, exit_code)
}

fn deep_validate_resource(
    context: &ProjectContext,
    resource: &CompiledResource,
    origin: &ProjectResourceOrigin,
    execution: &cdf_runtime::ExecutionServices,
) -> DeepValidateResourceReport {
    let mut diagnostics = Vec::new();
    let mut working_resource = resource.clone();
    let partition_report = partition_check(context, resource, execution, &mut diagnostics);
    let discovery = discovery_check(context, resource, execution, &mut diagnostics);
    if let Some(discovery) = &discovery.discovery {
        working_resource = resource.with_schema_source_and_schema(
            SchemaSource::Discovered {
                snapshot: discovery.snapshot.reference.clone(),
            },
            Arc::clone(&discovery.normalized_schema),
        );
    }
    physical_schema_reconciliation_check(context, resource, execution, &mut diagnostics);
    let validation_program = validation_program_check(&working_resource, &mut diagnostics);
    let normalization = normalization_check(&working_resource, &mut diagnostics);
    let destination = destination_check(context, &working_resource, &mut diagnostics);
    let status = if diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == "error")
    {
        "failed"
    } else {
        "passed"
    };

    DeepValidateResourceReport {
        resource_id: resource.descriptor().resource_id.to_string(),
        source_name: origin.source_name.clone(),
        resource_name: origin.resource_name.clone(),
        source_file: origin.source_file.clone(),
        mapping_pattern: origin.mapping_pattern.clone(),
        mapping_status: origin.mapping_status.clone(),
        source_kind: resource_kind_name(resource.plan()).to_owned(),
        schema_source: schema_source_name(&working_resource.descriptor().schema_source).to_owned(),
        field_count: working_resource.schema().fields().len(),
        partitions: partition_report,
        discovery,
        validation_program,
        identifier_normalization: normalization,
        destination,
        diagnostics,
        status: status.to_owned(),
    }
}

fn partition_check(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
    diagnostics: &mut Vec<DeepValidateDiagnostic>,
) -> DeepValidatePartitionReport {
    let request = deep_scan_request(resource.descriptor());
    let partitions = request.and_then(|request| match resource.plan() {
        CompiledResourcePlan::Files(_) => resource
            .to_file_resource(crate::project_run_resource::file_runtime_dependencies(
                context,
                Some(execution),
            )?)?
            .plan_partitions(&request),
        CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => {
            resource.plan_partitions(&request)
        }
    });
    match partitions {
        Ok(partitions) => DeepValidatePartitionReport {
            status: "ok".to_owned(),
            count: partitions.len(),
            files: partitions
                .iter()
                .filter_map(|partition| partition.metadata.get("path").cloned())
                .collect(),
            detail: "resolved without extraction".to_owned(),
        },
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "partition_resolution",
                error.message,
                "Fix the file glob, source root, or resource declaration before running plan/run.",
            ));
            DeepValidatePartitionReport {
                status: "failed".to_owned(),
                count: 0,
                files: Vec::new(),
                detail: "partition planning failed".to_owned(),
            }
        }
    }
}

fn discovery_check(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
    diagnostics: &mut Vec<DeepValidateDiagnostic>,
) -> DeepValidateDiscoveryReport {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return DeepValidateDiscoveryReport {
            status: "not_required".to_owned(),
            schema_hash: None,
            snapshot_path: None,
            source_identity: BTreeMap::new(),
            detail: "resource already has a declared or pinned schema".to_owned(),
            discovery: None,
        };
    }

    match discover_for_deep_validate(context, resource, execution) {
        Ok(discovery) => DeepValidateDiscoveryReport {
            status: "ok".to_owned(),
            schema_hash: Some(discovery.snapshot.artifact.schema_hash.to_string()),
            snapshot_path: Some(discovery.snapshot.artifact.path.clone()),
            source_identity: discovery.snapshot.source_identity.clone(),
            detail: "no-write discovery probe succeeded".to_owned(),
            discovery: Some(discovery),
        },
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "schema_discovery",
                redact_uri_userinfo(&error.message),
                "Run `cdf schema discover <resource>` for a focused probe, then fix the source or schema declaration.",
            ));
            DeepValidateDiscoveryReport {
                status: "failed".to_owned(),
                schema_hash: None,
                snapshot_path: None,
                source_identity: BTreeMap::new(),
                detail: "no-write discovery probe failed".to_owned(),
                discovery: None,
            }
        }
    }
}

fn discover_for_deep_validate(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
) -> cdf_kernel::Result<ResourceSchemaDiscovery> {
    let secret_provider = context.secret_provider();
    if matches!(resource.plan(), CompiledResourcePlan::Rest(_)) {
        let transport = ReqwestHttpTransport::new()?;
        return cdf_project::discover_resource_schema_with_rest_transport(
            resource,
            &secret_provider,
            &transport,
        );
    }
    if matches!(resource.plan(), CompiledResourcePlan::Files(_)) {
        return Ok(
            cdf_project::discover_resource_schema_with_file_dependencies_artifacts(
                resource,
                &secret_provider,
                crate::project_run_resource::file_runtime_dependencies(context, Some(execution))?,
                cdf_project::SchemaDiscoveryExecutionOptions::default(),
            )?
            .discovery,
        );
    }
    cdf_project::discover_resource_schema(resource, &secret_provider)
}

fn physical_schema_reconciliation_check(
    context: &ProjectContext,
    resource: &CompiledResource,
    execution: &cdf_runtime::ExecutionServices,
    diagnostics: &mut Vec<DeepValidateDiagnostic>,
) {
    let CompiledResourcePlan::Files(plan) = resource.plan() else {
        return;
    };
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return;
    }
    let probe = resource.with_schema_source_and_schema(SchemaSource::Discover, resource.schema());
    let discovery = discover_for_deep_validate(context, &probe, execution);
    let observed = match discovery {
        Ok(discovery) => discovery.normalized_schema,
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "physical_schema_probe",
                format!(
                    "resource `{}` at {}: {}",
                    resource.descriptor().resource_id,
                    safe_file_location(&plan.root, &plan.glob),
                    redact_uri_userinfo(&error.message)
                ),
                "Fix the unreadable or malformed source before plan/run; schema mismatches that decode successfully remain governed verdicts.",
            ));
            return;
        }
    };
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let allowances = resource.type_policy_allowances();
    policy.types.coerce_types = allowances.coerce_types;
    policy.types.allow_lossy_mapping = allowances.allow_lossy_mapping;
    if let Err(error) =
        reconcile_schema(observed.as_ref(), resource.schema().as_ref(), &policy.types)
    {
        let row_local = matches!(plan.format.as_str(), "json" | "ndjson");
        diagnostics.push(diagnostic(
            if row_local { "warning" } else { "error" },
            if row_local {
                "row_local_schema_mismatch"
            } else {
                "schema_reconciliation"
            },
            format!(
                "resource `{}` at {}: {}",
                resource.descriptor().resource_id,
                safe_file_location(&plan.root, &plan.glob),
                error.message
            ),
            if row_local {
                "Rows outside the declaration will quarantine; add fields, widen types, or explicitly enable the named type allowance."
            } else {
                "Widen the declaration or explicitly enable the named type allowance; lossy and parse coercions remain opt-in."
            },
        ));
    }
}

fn safe_file_location(root: &str, glob: &str) -> String {
    let joined = format!(
        "{}/{}",
        root.trim_end_matches('/'),
        glob.trim_start_matches('/')
    );
    let without_fragment = joined.split('#').next().unwrap_or(&joined);
    match without_fragment.split_once('?') {
        Some((path, _)) => format!("{path}?<redacted>"),
        None => without_fragment.to_owned(),
    }
}

fn validation_program_check(
    resource: &CompiledResource,
    diagnostics: &mut Vec<DeepValidateDiagnostic>,
) -> DeepValidateCheckReport {
    let observed = ObservedSchema::from_arrow(resource.schema().as_ref());
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let allowances = resource.type_policy_allowances();
    policy.types.coerce_types = allowances.coerce_types;
    policy.types.allow_lossy_mapping = allowances.allow_lossy_mapping;
    match compile_resource_validation_program(&policy, &observed, resource.descriptor()) {
        Ok(program) => DeepValidateCheckReport {
            status: "ok".to_owned(),
            detail: format!(
                "compiled validation program with {} column(s)",
                program.column_programs.len()
            ),
        },
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "schema_reconciliation",
                error.message,
                "Fix schema declarations, type constraints, or identifier normalization inputs.",
            ));
            DeepValidateCheckReport {
                status: "failed".to_owned(),
                detail: "validation program compilation failed".to_owned(),
            }
        }
    }
}

fn normalization_check(
    resource: &CompiledResource,
    diagnostics: &mut Vec<DeepValidateDiagnostic>,
) -> DeepValidateCheckReport {
    let source_metadata_count = resource
        .schema()
        .fields()
        .iter()
        .filter(|field| cdf_kernel::source_name(field).is_some())
        .count();
    if source_metadata_count == resource.schema().fields().len() {
        return DeepValidateCheckReport {
            status: "ok".to_owned(),
            detail: format!("{source_metadata_count} field(s) carry cdf:source_name metadata"),
        };
    }
    diagnostics.push(diagnostic(
        "warning",
        "identifier_normalization",
        "one or more fields lack cdf:source_name metadata",
        "Use declarative schemas or discovery so namecase-v1 can preserve source identifiers.",
    ));
    DeepValidateCheckReport {
        status: "warning".to_owned(),
        detail: format!("{source_metadata_count} field(s) carry cdf:source_name metadata"),
    }
}

fn destination_check(
    context: &ProjectContext,
    resource: &CompiledResource,
    diagnostics: &mut Vec<DeepValidateDiagnostic>,
) -> DeepValidateDestinationReport {
    let target = match cdf_kernel::TargetName::new(default_target_for_resource(
        resource.descriptor().resource_id.as_str(),
    )) {
        Ok(target) => target,
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "destination_target",
                error.message,
                "Declare a valid destination target or use a resource id with a valid final segment.",
            ));
            return DeepValidateDestinationReport::failed("target derivation failed");
        }
    };
    let mut resolved = match resolve_environment_destination(context, &target) {
        Ok(resolved) => resolved,
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "destination_resolution",
                redact_uri_userinfo(&error.message),
                "Fix the environment destination URI or configured secret reference.",
            ));
            return DeepValidateDestinationReport::failed("destination resolution failed");
        }
    };
    let identifier_policy = match resolved.destination.column_identifier_policy() {
        Ok(policy) => policy,
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "destination_sheet",
                error.message,
                "Fix destination identifier policy before deep validation.",
            ));
            return DeepValidateDestinationReport::failed("destination identifier policy failed");
        }
    };
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let allowances = resource.type_policy_allowances();
    policy.types.coerce_types = allowances.coerce_types;
    policy.types.allow_lossy_mapping = allowances.allow_lossy_mapping;
    if let Some(identifier_policy) = identifier_policy {
        policy.normalization.identifier = identifier_policy;
    }
    let observed = ObservedSchema::from_arrow(resource.schema().as_ref());
    let engine_plan =
        compile_resource_validation_program(&policy, &observed, resource.descriptor()).and_then(
            |validation_program| {
                Planner::new().plan_tier_b(
                    resource,
                    EnginePlanInput {
                        request: deep_scan_request(resource.descriptor())?,
                        validation_program,
                        boundedness: PlanBoundedness::Bounded,
                        package_id: format!("deep-validate-{}", resource.descriptor().resource_id),
                    },
                )
            },
        );
    let engine_plan = match engine_plan {
        Ok(plan) => plan,
        Err(error) => {
            diagnostics.push(diagnostic(
                "error",
                "destination_sheet",
                error.message,
                "Fix compiler-front-end errors before destination planning.",
            ));
            return DeepValidateDestinationReport::failed("engine plan compilation failed");
        }
    };
    match resolved
        .destination
        .plan_resource_commit(resource, &engine_plan)
    {
        Ok(plan) => DeepValidateDestinationReport {
            status: "ok".to_owned(),
            destination_id: Some(plan.description.destination_id.to_string()),
            target: Some(plan.target.to_string()),
            disposition: Some(format!("{:?}", plan.commit_plan.disposition).to_lowercase()),
            migration_support: Some(format!("{:?}", plan.sheet.migration_support).to_lowercase()),
            detail: "destination sheet accepted the planned schema/disposition".to_owned(),
        },
        Err(error) => {
            let error = redact_error_value(error, resolved.secret_redaction.as_deref());
            diagnostics.push(diagnostic(
                "error",
                "destination_sheet",
                command_correct_validate_message(error.message),
                "Fix schema, disposition, target, or destination policy before running plan/run.",
            ));
            DeepValidateDestinationReport::failed("destination sheet compatibility failed")
        }
    }
}

fn command_correct_validate_message(message: String) -> String {
    message.replace("cdf run ", "cdf validate --deep ")
}

fn deep_scan_request(descriptor: &ResourceDescriptor) -> cdf_kernel::Result<ScanRequest> {
    Ok(ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: descriptor.state_scope.clone(),
    })
}

fn resource_kind_name(plan: &CompiledResourcePlan) -> &'static str {
    match plan {
        CompiledResourcePlan::Files(_) => "files",
        CompiledResourcePlan::Rest(_) => "rest",
        CompiledResourcePlan::Sql(_) => "sql",
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

fn diagnostic(
    severity: &'static str,
    check: &'static str,
    message: impl Into<String>,
    remediation: &'static str,
) -> DeepValidateDiagnostic {
    DeepValidateDiagnostic {
        severity: severity.to_owned(),
        check: check.to_owned(),
        code: format!("CDF-DEEP-{}", check.replace('_', "-").to_ascii_uppercase()),
        message: message.into(),
        remediation: remediation.to_owned(),
    }
}

#[derive(Serialize)]
struct DeepValidateReport {
    mode: String,
    project: String,
    environment: String,
    declarative_resources: usize,
    external_resources: usize,
    checked_secrets: usize,
    summary: DeepValidateSummary,
    resources: Vec<DeepValidateResourceReport>,
    writes: DeepValidateWrites,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeepValidateSummary {
    resources: usize,
    passed: usize,
    failed: usize,
    warnings: usize,
    partitions: usize,
    discovery_probes: usize,
}

impl DeepValidateSummary {
    fn from_resources(resources: &[DeepValidateResourceReport]) -> Self {
        Self {
            resources: resources.len(),
            passed: resources
                .iter()
                .filter(|resource| resource.status == "passed")
                .count(),
            failed: resources
                .iter()
                .filter(|resource| resource.status == "failed")
                .count(),
            warnings: resources
                .iter()
                .flat_map(|resource| &resource.diagnostics)
                .filter(|diagnostic| diagnostic.severity == "warning")
                .count(),
            partitions: resources
                .iter()
                .map(|resource| resource.partitions.count)
                .sum(),
            discovery_probes: resources
                .iter()
                .filter(|resource| resource.discovery.status == "ok")
                .count(),
        }
    }
}

#[derive(Serialize)]
struct DeepValidateResourceReport {
    resource_id: String,
    source_name: String,
    resource_name: String,
    source_file: Option<String>,
    mapping_pattern: String,
    mapping_status: String,
    source_kind: String,
    schema_source: String,
    field_count: usize,
    partitions: DeepValidatePartitionReport,
    discovery: DeepValidateDiscoveryReport,
    validation_program: DeepValidateCheckReport,
    identifier_normalization: DeepValidateCheckReport,
    destination: DeepValidateDestinationReport,
    diagnostics: Vec<DeepValidateDiagnostic>,
    status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeepValidatePartitionReport {
    status: String,
    count: usize,
    files: Vec<String>,
    detail: String,
}

#[derive(Serialize)]
struct DeepValidateDiscoveryReport {
    status: String,
    schema_hash: Option<String>,
    snapshot_path: Option<String>,
    source_identity: BTreeMap<String, String>,
    detail: String,
    #[serde(skip)]
    discovery: Option<ResourceSchemaDiscovery>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeepValidateCheckReport {
    status: String,
    detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeepValidateDestinationReport {
    status: String,
    destination_id: Option<String>,
    target: Option<String>,
    disposition: Option<String>,
    migration_support: Option<String>,
    detail: String,
}

impl DeepValidateDestinationReport {
    fn failed(detail: impl Into<String>) -> Self {
        Self {
            status: "failed".to_owned(),
            destination_id: None,
            target: None,
            disposition: None,
            migration_support: None,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DeepValidateDiagnostic {
    severity: String,
    check: String,
    code: String,
    message: String,
    remediation: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct DeepValidateWrites {
    package: bool,
    destination: bool,
    checkpoint: bool,
    schema_snapshot: bool,
    lockfile: bool,
}

fn document(report: &DeepValidateReport) -> RenderDocument {
    let status = if report.summary.failed == 0 {
        StatusKind::Success
    } else {
        StatusKind::Error
    };
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            status,
            format!(
                "deep validated project {} ({} passed, {} failed)",
                report.project, report.summary.passed, report.summary.failed
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Deep validate")
                .row("mode", report.mode.clone())
                .row("environment", report.environment.clone())
                .row("resources", report.summary.resources.to_string())
                .row("partitions", report.summary.partitions.to_string())
                .row(
                    "discovery probes",
                    report.summary.discovery_probes.to_string(),
                )
                .row("warnings", report.summary.warnings.to_string())
                .row("writes", "none"),
        );

    let table = report.resources.iter().fold(
        Table::new([
            "resource",
            "status",
            "kind",
            "schema",
            "partitions",
            "destination",
        ]),
        |table, resource| {
            table.row([
                resource.resource_id.clone(),
                resource.status.clone(),
                resource.source_kind.clone(),
                resource.schema_source.clone(),
                resource.partitions.count.to_string(),
                resource
                    .destination
                    .target
                    .clone()
                    .unwrap_or_else(|| resource.destination.status.clone()),
            ])
        },
    );
    document = document.blank_line().push(table);

    let diagnostics = report
        .resources
        .iter()
        .flat_map(|resource| {
            resource
                .diagnostics
                .iter()
                .map(move |diagnostic| (resource.resource_id.as_str(), diagnostic))
        })
        .collect::<Vec<_>>();
    if !diagnostics.is_empty() {
        let table = diagnostics.into_iter().fold(
            Table::new(["resource", "severity", "check", "message", "remediation"]),
            |table, (resource_id, diagnostic)| {
                table.row([
                    resource_id.to_owned(),
                    diagnostic.severity.clone(),
                    diagnostic.check.clone(),
                    diagnostic.message.clone(),
                    diagnostic.remediation.clone(),
                ])
            },
        );
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new(if report.summary.failed == 0 {
            "cdf plan <resource>"
        } else {
            "cdf inspect resources"
        }))
}

#[cfg(test)]
mod tests {
    use super::safe_file_location;

    #[test]
    fn safe_file_location_removes_every_query_value_and_fragment() {
        assert_eq!(
            safe_file_location(
                "https://example.test/data?X-Amz-Signature=secret&ordinary=value#fragment",
                "events.parquet"
            ),
            "https://example.test/data?<redacted>"
        );
    }
}
