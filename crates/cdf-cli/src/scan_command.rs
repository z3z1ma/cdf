use std::collections::BTreeMap;

use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_engine::{EnginePlan, EnginePlanInput, PlanBoundedness, Planner};
use cdf_kernel::{
    CdfError, OrderBy, PartitionPlan, PredicateId, ResourceStream, ScanPredicate, ScanRequest,
    SortDirection,
};
use futures_util::StreamExt;
use serde::Serialize;

use crate::{
    args::{Cli, ScanArgs},
    commands::{json_cli_error, output},
    context::ProjectContext,
    output::{CliError, CommandOutput},
    reports::WriteEffects,
};

pub(crate) fn plan_or_explain(
    cli: &Cli,
    args: ScanArgs,
    command: &'static str,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let plan = build_engine_plan(&context, &args)?;
    let report = scan_report(&context, &plan)?;
    let human = format_scan_report(command, &report);
    output(command, human, report)
}

pub(crate) fn preview(cli: &Cli, args: ScanArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&args.resource_id)?;
    let plan = build_engine_plan(&context, &args)?;
    match preview_one_batch(resource, &plan) {
        Ok(report) => output(
            "preview",
            format!(
                "previewed resource {}: {} row(s), {} byte(s); wrote no package, destination data, or checkpoint",
                report.resource_id, report.row_count, report.byte_count
            ),
            report,
        ),
        Err(error) if lower_runtime_missing(&error) => Err(CliError::not_supported(
            "preview",
            error.message,
            "resource runtime open implementation",
        )),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn build_engine_plan(
    context: &ProjectContext,
    args: &ScanArgs,
) -> Result<EnginePlan, CliError> {
    let resource = context.resource(&args.resource_id)?;
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
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
            return Err(CliError::usage(format!(
                "unsupported order direction `{other}`"
            )));
        }
    };
    Ok(OrderBy {
        field: field.to_owned(),
        direction,
    })
}

fn scan_report(context: &ProjectContext, plan: &EnginePlan) -> Result<ScanPlanReport, CliError> {
    let resource = context.resource(plan.scan.request.resource_id.as_str())?;
    Ok(ScanPlanReport {
        project: context.config.project.name.clone(),
        environment: context.environment.name.clone(),
        resource_id: plan.scan.request.resource_id.to_string(),
        will_fetch: FetchReport {
            partitions: plan
                .scan
                .partitions
                .iter()
                .map(partition_report)
                .collect(),
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
        ddl_preview: UnsupportedReport {
            supported: false,
            reason: "destination DDL preview requires a destination commit plan over a package schema; current lower APIs expose package commit planning, not scan-to-DDL planning".to_owned(),
            required_lower_layer: "scan/resource schema to destination DDL planning facade".to_owned(),
        },
        delivery_guarantee: format!("{:?}", plan.explain.delivery_guarantee),
        state_advancement: StateAdvancementReport {
            scope: serde_json::to_value(&resource.descriptor().state_scope)
                .map_err(json_cli_error)?,
            cursor: resource
                .descriptor()
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.clone()),
            advances_after: "destination receipt is recorded and CheckpointStore::commit verifies coverage".to_owned(),
        },
        explain: plan.explain.clone(),
        package_id: plan.package_id.clone(),
    })
}

fn partition_report(partition: &PartitionPlan) -> PartitionReport {
    PartitionReport {
        partition_id: partition.partition_id.to_string(),
        scope_kind: format!("{:?}", partition.scope.kind()),
        metadata: partition.metadata.clone(),
    }
}

fn preview_one_batch(
    resource: &cdf_declarative::CompiledResource,
    plan: &EnginePlan,
) -> cdf_kernel::Result<PreviewReport> {
    let partition = plan
        .scan
        .partitions
        .first()
        .ok_or_else(|| CdfError::data("preview plan has no partitions"))?
        .clone();
    let mut stream = futures_executor::block_on(resource.open(partition))?;
    let batch = futures_executor::block_on(async { stream.next().await })
        .ok_or_else(|| CdfError::data("resource produced no preview batch"))??;
    Ok(PreviewReport {
        resource_id: batch.header.resource_id.to_string(),
        batch_id: batch.header.batch_id.to_string(),
        partition_id: batch.header.partition_id.to_string(),
        row_count: batch.header.row_count,
        byte_count: batch.header.byte_count,
        writes: WriteEffects::default(),
    })
}

fn lower_runtime_missing(error: &CdfError) -> bool {
    error
        .message
        .contains("execution is outside the MVP compiler crate")
}

fn format_scan_report(command: &str, report: &ScanPlanReport) -> String {
    let pushed = report.pushdown.pushed.len();
    let inexact = report.pushdown.inexact.len();
    let unsupported = report.pushdown.unsupported.len();
    format!(
        "{command} {}: {} partition(s), {pushed} pushed predicate(s), {inexact} inexact, {unsupported} unsupported, guarantee {}",
        report.resource_id,
        report.will_fetch.partitions.len(),
        report.delivery_guarantee
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ScanPlanReport {
    project: String,
    environment: String,
    resource_id: String,
    will_fetch: FetchReport,
    pushdown: PushdownReport,
    ddl_preview: UnsupportedReport,
    delivery_guarantee: String,
    state_advancement: StateAdvancementReport,
    explain: cdf_engine::ExplainData,
    package_id: String,
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
struct UnsupportedReport {
    supported: bool,
    reason: String,
    required_lower_layer: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateAdvancementReport {
    scope: serde_json::Value,
    cursor: Option<String>,
    advances_after: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewReport {
    resource_id: String,
    batch_id: String,
    partition_id: String,
    row_count: u64,
    byte_count: u64,
    writes: WriteEffects,
}
