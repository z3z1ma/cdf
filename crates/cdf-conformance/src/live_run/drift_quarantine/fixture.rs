use std::{
    cell::Cell,
    fs,
    path::{Path, PathBuf},
};

use cdf_contract::{
    ContractPolicy, DedupKeep, ObservedSchema, RowRule, compile_validation_program,
};
use cdf_declarative::CompiledResource;
use cdf_engine::{EnginePlan, EnginePlanInput, PlanBoundedness, Planner};
use cdf_kernel::{
    CdfError, CheckpointId, CheckpointStatus, CheckpointStore, PipelineId, Receipt, ResourceId,
    ResourceStream, Result, RunId, ScanRequest, ScopeKey, TargetName, WriteDisposition,
};
use cdf_project::{
    InMemoryResourceSourceResolver, ProjectRunReport, ProjectRunRequest, ProjectRunSource,
    ResolvedProjectDestination, compile_project_declarative_resources_with_root, parse_cdf_toml,
    run_project,
};
use cdf_state_sqlite::SqliteCheckpointStore;

const PROJECT_TOML: &str = r#"
[project]
name = "drift_quarantine_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.drift_events"]
source = "resources/live.toml"
"#;

const RESOURCE_TOML: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.drift_events"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
partition = { by = "file" }
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "event_type", type = "string", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;

const RESOURCE_ID: &str = "local.drift_events";
const SOURCE_FILE: &str = "data/events.ndjson";
const SOURCE_SCOPE: &str = "events.ndjson";

pub(super) const TARGET: &str = "drift_events";
pub(super) const ALLOWED_EVENT_TYPE: &str = "order.created";
pub(super) const DRIFTED_EVENT_TYPE_OBSERVED: &str = "42";
pub(super) const CLEAN_SOURCE: &str =
    "{\"id\":1,\"event_type\":\"order.created\",\"name\":\"clean\"}\n";
pub(super) const DRIFT_SOURCE: &str = "\
{\"id\":1,\"event_type\":\"order.created\",\"name\":\"second-accepted-first\"}\n\
{\"id\":1,\"event_type\":\"order.created\",\"name\":\"second-accepted-last\"}\n\
{\"id\":2,\"event_type\":42,\"name\":\"drifted-event-type\"}\n";

#[derive(Clone, Debug)]
pub(super) struct ScenarioSpec {
    project_root: PathBuf,
    package_root: PathBuf,
    pub(super) destination_path: PathBuf,
    pub(super) state_store_path: PathBuf,
    pipeline_id: PipelineId,
    pub(super) target: TargetName,
}

impl ScenarioSpec {
    pub(super) fn new(project_root: &Path, label: &str) -> Result<Self> {
        Ok(Self {
            project_root: project_root.to_path_buf(),
            package_root: project_root.join(".cdf/packages"),
            destination_path: project_root.join(format!(".cdf/{label}.duckdb")),
            state_store_path: project_root.join(".cdf/state.sqlite"),
            pipeline_id: PipelineId::new(format!("pipeline-drift-quarantine-{label}"))?,
            target: TargetName::new(TARGET)?,
        })
    }

    pub(super) fn with_target(&self, target: TargetName) -> Self {
        let mut spec = self.clone();
        spec.target = target;
        spec
    }
}

pub(super) fn run_scenario(
    spec: &ScenarioSpec,
    source: &str,
    run_label: &str,
    destination: ResolvedProjectDestination,
) -> Result<ProjectRunReport> {
    write_source(&spec.project_root, source)?;
    let resource = compile_resource(&spec.project_root)?;
    let package_id = format!("pkg-e6-drift-quarantine-{run_label}");
    let checkpoint_id = CheckpointId::new(format!("checkpoint-e6-drift-quarantine-{run_label}"))?;
    let run_id = RunId::new(format!("run-e6-drift-quarantine-{run_label}"))?;
    let plan = drift_quarantine_plan(&resource, &package_id)?;
    assert_frozen_contract_program(&plan);

    fs::create_dir_all(&spec.package_root)
        .map_err(|error| CdfError::data(format!("create package root: {error}")))?;
    if let Some(parent) = spec.destination_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::data(format!("create destination parent: {error}")))?;
    }
    if let Some(parent) = spec.state_store_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::data(format!("create state parent: {error}")))?;
    }

    let gate_observed = Cell::new(false);
    let resource_id = resource.descriptor().resource_id.clone();
    let scope = resource.descriptor().state_scope.clone();
    let gate = |_: &Receipt| {
        assert_checkpoint_not_committed_at_receipt_gate(
            &spec.state_store_path,
            &spec.pipeline_id,
            &resource_id,
            &scope,
            &checkpoint_id,
        )?;
        gate_observed.set(true);
        Ok(())
    };

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::local_file(&resource),
        plan,
        package_root: spec.package_root.clone(),
        state_store_path: spec.state_store_path.clone(),
        pipeline_id: spec.pipeline_id.clone(),
        destination,
        package_id,
        checkpoint_id: checkpoint_id.clone(),
        run_id: Some(run_id),
        event_sink: None,
        after_receipt_verified: Some(&gate),
    }))?;
    assert!(
        gate_observed.get(),
        "receipt verification gate must be observed before checkpoint commit"
    );
    Ok(report)
}

fn write_source(project_root: &Path, source: &str) -> Result<()> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| CdfError::data(format!("create drift fixture data dir: {error}")))?;
    fs::write(project_root.join(SOURCE_FILE), source)
        .map_err(|error| CdfError::data(format!("write drift fixture source: {error}")))
}

fn compile_resource(project_root: &Path) -> Result<CompiledResource> {
    let config = parse_cdf_toml(PROJECT_TOML)?;
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/live.toml", RESOURCE_TOML);
    let mut resources =
        compile_project_declarative_resources_with_root(&config, &resolver, project_root)?;
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "E6 drift-quarantine fixture expected one resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "E6 drift-quarantine compiled unexpected resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn drift_quarantine_plan(resource: &CompiledResource, package_id: &str) -> Result<EnginePlan> {
    let mut policy = ContractPolicy::freeze();
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_quarantine = true;
    policy.rows.rules = vec![
        RowRule::Domain {
            column: "event_type".to_owned(),
            allowed: vec![ALLOWED_EVENT_TYPE.to_owned()],
        },
        RowRule::Dedup {
            keys: vec!["id".to_owned()],
            keep: DedupKeep::Last,
        },
    ];
    let validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )?;
    Planner::new().plan_tier_b(
        resource,
        EnginePlanInput {
            request: ScanRequest {
                resource_id: resource.descriptor().resource_id.clone(),
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::File {
                    path: SOURCE_SCOPE.to_owned(),
                },
            },
            validation_program,
            boundedness: PlanBoundedness::Bounded,
            package_id: package_id.to_owned(),
        },
    )
}

fn assert_frozen_contract_program(plan: &EnginePlan) {
    assert_eq!(plan.write_disposition, WriteDisposition::Merge);
    assert!(plan.validation_program.schema_verdicts.iter().any(|rule| {
        matches!(rule.change, cdf_contract::SchemaChangeKind::NewColumn)
            && matches!(rule.verdict, cdf_contract::VerdictAction::RejectRun)
    }));
    assert!(plan.validation_program.schema_verdicts.iter().any(|rule| {
        matches!(rule.change, cdf_contract::SchemaChangeKind::TypeNarrowing)
            && matches!(rule.verdict, cdf_contract::VerdictAction::Quarantine)
    }));
    assert!(plan.validation_program.row_rules.iter().any(|rule| {
        matches!(
            rule.predicate,
            cdf_contract::RowRulePredicate::Domain { .. }
        )
    }));
    assert!(plan.validation_program.has_dedup_rule());
}

fn assert_checkpoint_not_committed_at_receipt_gate(
    state_store_path: &Path,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
    checkpoint_id: &CheckpointId,
) -> Result<()> {
    let store = SqliteCheckpointStore::open(state_store_path)?;
    let history = store.history(pipeline_id, resource_id, scope)?;
    let proposed = history
        .iter()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == *checkpoint_id)
        .ok_or_else(|| {
            CdfError::contract(format!(
                "receipt gate did not observe proposed checkpoint {checkpoint_id}"
            ))
        })?;
    if proposed.status != CheckpointStatus::Proposed || proposed.is_head {
        return Err(CdfError::contract(
            "receipt gate observed current checkpoint outside proposed-only state",
        ));
    }
    if let Some(head) = store.head(pipeline_id, resource_id, scope)?
        && head.delta.checkpoint_id == *checkpoint_id
    {
        return Err(CdfError::contract(
            "checkpoint head advanced before receipt verification gate returned",
        ));
    }
    Ok(())
}
