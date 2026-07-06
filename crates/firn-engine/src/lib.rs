#![doc = "Planning and execution boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use firn_contract::{ValidationProgram, assert_verdict_lattice_total};
use firn_kernel::{
    BatchId, CapabilitySupport, DeliveryGuarantee, EstimateSupport, FirnError, PartitionPlan,
    PlanId, PushdownFidelity, QueryableResource, ResourceCapabilities, ResourceId, ResourceStream,
    Result, ScanPlan, ScanPredicate, ScanRequest, SegmentId, WriteDisposition, with_source_name,
};
use firn_package::{PackageBuilder, PackageManifest, PackageStatus, SegmentEntry};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

pub const DATAFUSION_TABLE_PROVIDER_KIND: &str = "datafusion_table_provider";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePlanInput {
    pub request: ScanRequest,
    pub validation_program: ValidationProgram,
    pub boundedness: PlanBoundedness,
    pub package_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanBoundedness {
    Bounded,
    UnboundedDrain,
    UnboundedLive {
        checkpoint_cadence_ms: Option<u64>,
        package_rotation_rows: Option<u64>,
        watermark: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePlan {
    pub scan: ScanPlan,
    pub final_projection: Option<Vec<String>>,
    pub residual_predicates: Vec<ScanPredicate>,
    pub boundedness: PlanBoundedness,
    pub validation_program: ValidationProgram,
    pub operator_chain: Vec<OperatorNode>,
    pub explain: ExplainData,
    pub package_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperatorNode {
    DataFusionTableProvider {
        provider_kind: String,
        resource_id: ResourceId,
    },
    DataFusionScanExec {
        projection: Option<Vec<String>>,
        residual_predicates: Vec<String>,
        limit: Option<u64>,
    },
    SchemaFingerprintExec,
    ContractExec {
        normalizer_version: String,
        column_program_count: usize,
    },
    NormalizeExec {
        normalizer_version: String,
    },
    ProfileExec,
    LineageExec,
    PackageSink {
        package_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainData {
    pub resource_id: ResourceId,
    pub projected_fields: Vec<String>,
    pub projection_pushed: bool,
    pub limit: Option<u64>,
    pub limit_pushed: bool,
    pub pushed_predicates: Vec<PredicateExplain>,
    pub inexact_predicates: Vec<PredicateExplain>,
    pub unsupported_predicates: Vec<PredicateExplain>,
    pub partitions: Vec<PartitionExplain>,
    pub estimates: EstimateExplain,
    pub delivery_guarantee: DeliveryGuarantee,
    pub boundedness: PlanBoundedness,
    pub operator_chain: Vec<OperatorNode>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateExplain {
    pub predicate_id: String,
    pub expression: String,
    pub fidelity: PushdownFidelity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionExplain {
    pub partition_id: String,
    pub scope_kind: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstimateExplain {
    pub support: EstimateSupport,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineRunOutput {
    pub manifest: PackageManifest,
    pub segments: Vec<SegmentEntry>,
    pub profile: ExecutionProfile,
    pub lineage: LineageSummary,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionProfile {
    pub output_rows: u64,
    pub output_bytes: u64,
    pub output_batches: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageSummary {
    pub input_batches: Vec<BatchId>,
    pub output_segments: Vec<SegmentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaArtifact {
    fields: Vec<SchemaFieldArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaFieldArtifact {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Debug, Default)]
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan_tier_a<R>(&self, resource: &R, input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: ResourceStream + ?Sized,
    {
        validate_boundedness(&input.boundedness)?;
        validate_program(&input.validation_program)?;

        let partitions = resource.plan_partitions(&input.request)?;
        let scan = ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", input.request.resource_id.as_str()))?,
            request: input.request.clone(),
            partitions,
            pushed_predicates: Vec::new(),
            unsupported_predicates: input.request.filters.clone(),
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(resource.descriptor().write_disposition.clone()),
        };

        self.finish_plan(scan, input, false, false, EstimateSupport::None)
    }

    pub fn plan_tier_b<R>(&self, resource: &R, input: EnginePlanInput) -> Result<EnginePlan>
    where
        R: QueryableResource + ?Sized,
    {
        validate_boundedness(&input.boundedness)?;
        validate_program(&input.validation_program)?;

        let scan = resource.negotiate(&input.request)?;
        self.finish_plan(
            scan,
            input,
            resource.capabilities().projection == CapabilitySupport::Supported,
            resource.capabilities().limits == CapabilitySupport::Supported,
            resource.capabilities().estimates.clone(),
        )
    }

    fn finish_plan(
        &self,
        scan: ScanPlan,
        input: EnginePlanInput,
        projection_pushed: bool,
        limit_pushed: bool,
        estimate_support: EstimateSupport,
    ) -> Result<EnginePlan> {
        let residual_predicates = residual_predicates(&scan);
        let final_projection = input.request.projection.clone();
        let operator_chain = operator_chain(
            &scan.request.resource_id,
            &final_projection,
            &residual_predicates,
            scan.request.limit,
            &input.validation_program,
            &input.package_id,
        );
        let explain = explain_data(
            &scan,
            &input.boundedness,
            &operator_chain,
            projection_pushed,
            limit_pushed,
            estimate_support,
        );

        Ok(EnginePlan {
            scan,
            final_projection,
            residual_predicates,
            boundedness: input.boundedness,
            validation_program: input.validation_program,
            operator_chain,
            explain,
            package_id: input.package_id,
        })
    }
}

pub async fn execute_to_package<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
) -> Result<EngineRunOutput>
where
    R: ResourceStream + ?Sized,
{
    validate_program(&plan.validation_program)?;

    let mut builder = PackageBuilder::create(package_dir, plan.package_id.clone())?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact("plan/scan.json", &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    builder.write_json_artifact("plan/validation-program.json", &plan.validation_program)?;

    let mut profile = ExecutionProfile::default();
    let mut lineage = LineageSummary::default();
    let mut segments = Vec::new();
    let mut remaining_limit = plan.scan.request.limit;
    let mut output_schema = None;

    for partition in plan.scan.partitions.clone() {
        if remaining_limit == Some(0) {
            break;
        }

        let mut stream = resource.open(partition).await?;
        while let Some(batch) = stream.next().await {
            if remaining_limit == Some(0) {
                break;
            }

            let batch = batch?;
            lineage.input_batches.push(batch.header.batch_id.clone());
            let Some(record_batch) = batch.record_batch() else {
                return Err(FirnError::data(
                    "package execution requires in-memory Arrow record batches at MVP",
                ));
            };

            let output = execute_batch(record_batch, plan, &mut remaining_limit)?;
            if output.num_rows() == 0 {
                continue;
            }

            let output = apply_contract_exec(output, &plan.validation_program)?;
            let output = apply_normalize_exec(output, &plan.validation_program)?;
            output_schema = Some(schema_artifact(output.schema().as_ref()));
            profile.output_rows += output.num_rows() as u64;
            profile.output_bytes += output.get_array_memory_size() as u64;
            profile.output_batches += 1;

            let segment_id = SegmentId::new(format!("seg-{:06}", segments.len() + 1))?;
            let segment = builder.write_segment(segment_id.clone(), &[output])?;
            lineage.output_segments.push(segment_id);
            segments.push(segment);
        }
    }

    builder.write_json_artifact(
        "schema/output.json",
        &output_schema.unwrap_or(SchemaArtifact { fields: Vec::new() }),
    )?;
    builder.write_stats_artifact(
        "profile.json",
        &firn_package::canonical_json_bytes(&profile)?,
    )?;
    builder.write_lineage_artifact(
        "lineage.json",
        &firn_package::canonical_json_bytes(&lineage)?,
    )?;
    builder.update_status(PackageStatus::Validated)?;
    let manifest = builder.finish()?;

    Ok(EngineRunOutput {
        manifest,
        segments,
        profile,
        lineage,
    })
}

pub fn negotiate_scan_plan(
    resource_id: ResourceId,
    request: ScanRequest,
    capabilities: &ResourceCapabilities,
    partitions: Vec<PartitionPlan>,
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    delivery_guarantee: DeliveryGuarantee,
) -> Result<ScanPlan> {
    let mut pushed_predicates = Vec::new();
    let mut unsupported_predicates = Vec::new();
    let supported_operators: BTreeSet<&str> = capabilities
        .filters
        .supported_operators
        .iter()
        .map(String::as_str)
        .collect();

    for predicate in &request.filters {
        let operator = predicate_operator(&predicate.expression);
        let supported = operator
            .as_deref()
            .is_some_and(|operator| supported_operators.contains(operator));
        if supported && capabilities.filters.default_fidelity != PushdownFidelity::Unsupported {
            pushed_predicates.push(firn_kernel::PushedPredicate {
                predicate: predicate.clone(),
                fidelity: capabilities.filters.default_fidelity.clone(),
            });
        } else {
            unsupported_predicates.push(predicate.clone());
        }
    }

    Ok(ScanPlan {
        plan_id: PlanId::new(format!("plan-{}", resource_id.as_str()))?,
        request,
        partitions,
        pushed_predicates,
        unsupported_predicates,
        estimated_rows,
        estimated_bytes,
        delivery_guarantee,
    })
}

pub fn datafusion_filter_pushdown(
    fidelity: &PushdownFidelity,
) -> datafusion::logical_expr::TableProviderFilterPushDown {
    match fidelity {
        PushdownFidelity::Exact => datafusion::logical_expr::TableProviderFilterPushDown::Exact,
        PushdownFidelity::Inexact => datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
        PushdownFidelity::Unsupported => {
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported
        }
    }
}

fn validate_program(program: &ValidationProgram) -> Result<()> {
    assert_verdict_lattice_total(program)
}

fn validate_boundedness(boundedness: &PlanBoundedness) -> Result<()> {
    match boundedness {
        PlanBoundedness::Bounded | PlanBoundedness::UnboundedDrain => Ok(()),
        PlanBoundedness::UnboundedLive { .. } => Err(FirnError::contract(
            "unbounded live plans are illegal in the MVP; use drain mode or add cadence, rotation, and watermark support in a later ticket",
        )),
    }
}

fn residual_predicates(scan: &ScanPlan) -> Vec<ScanPredicate> {
    let mut residual = scan.unsupported_predicates.clone();
    residual.extend(
        scan.pushed_predicates
            .iter()
            .filter(|pushed| pushed.fidelity == PushdownFidelity::Inexact)
            .map(|pushed| pushed.predicate.clone()),
    );
    residual
}

fn operator_chain(
    resource_id: &ResourceId,
    projection: &Option<Vec<String>>,
    residual_predicates: &[ScanPredicate],
    limit: Option<u64>,
    program: &ValidationProgram,
    package_id: &str,
) -> Vec<OperatorNode> {
    vec![
        OperatorNode::DataFusionTableProvider {
            provider_kind: DATAFUSION_TABLE_PROVIDER_KIND.to_owned(),
            resource_id: resource_id.clone(),
        },
        OperatorNode::DataFusionScanExec {
            projection: projection.clone(),
            residual_predicates: residual_predicates
                .iter()
                .map(|predicate| predicate.expression.clone())
                .collect(),
            limit,
        },
        OperatorNode::SchemaFingerprintExec,
        OperatorNode::ContractExec {
            normalizer_version: program.normalizer_version.clone(),
            column_program_count: program.column_programs.len(),
        },
        OperatorNode::NormalizeExec {
            normalizer_version: program.normalizer_version.clone(),
        },
        OperatorNode::ProfileExec,
        OperatorNode::LineageExec,
        OperatorNode::PackageSink {
            package_id: package_id.to_owned(),
        },
    ]
}

fn explain_data(
    scan: &ScanPlan,
    boundedness: &PlanBoundedness,
    operator_chain: &[OperatorNode],
    projection_pushed: bool,
    limit_pushed: bool,
    estimate_support: EstimateSupport,
) -> ExplainData {
    let pushed_predicates = scan
        .pushed_predicates
        .iter()
        .map(|pushed| PredicateExplain {
            predicate_id: pushed.predicate.predicate_id.as_str().to_owned(),
            expression: pushed.predicate.expression.clone(),
            fidelity: pushed.fidelity.clone(),
        })
        .collect::<Vec<_>>();
    let inexact_predicates = pushed_predicates
        .iter()
        .filter(|predicate| predicate.fidelity == PushdownFidelity::Inexact)
        .cloned()
        .collect();
    let unsupported_predicates = scan
        .unsupported_predicates
        .iter()
        .map(|predicate| PredicateExplain {
            predicate_id: predicate.predicate_id.as_str().to_owned(),
            expression: predicate.expression.clone(),
            fidelity: PushdownFidelity::Unsupported,
        })
        .collect();

    ExplainData {
        resource_id: scan.request.resource_id.clone(),
        projected_fields: scan.request.projection.clone().unwrap_or_default(),
        projection_pushed,
        limit: scan.request.limit,
        limit_pushed,
        pushed_predicates,
        inexact_predicates,
        unsupported_predicates,
        partitions: scan
            .partitions
            .iter()
            .map(|partition| PartitionExplain {
                partition_id: partition.partition_id.as_str().to_owned(),
                scope_kind: format!("{:?}", partition.scope.kind()),
                metadata: partition.metadata.clone(),
            })
            .collect(),
        estimates: EstimateExplain {
            support: estimate_support,
            rows: scan.estimated_rows,
            bytes: scan.estimated_bytes,
        },
        delivery_guarantee: scan.delivery_guarantee.clone(),
        boundedness: boundedness.clone(),
        operator_chain: operator_chain.to_vec(),
    }
}

fn execute_batch(
    batch: &RecordBatch,
    plan: &EnginePlan,
    remaining_limit: &mut Option<u64>,
) -> Result<RecordBatch> {
    let filtered = apply_residual_filters(batch, &plan.residual_predicates)?;
    let limited = match remaining_limit {
        Some(remaining) => {
            let take = (*remaining).min(filtered.num_rows() as u64) as usize;
            *remaining -= take as u64;
            filtered.slice(0, take)
        }
        None => filtered,
    };
    apply_projection(&limited, plan.final_projection.as_deref())
}

fn apply_residual_filters(
    batch: &RecordBatch,
    predicates: &[ScanPredicate],
) -> Result<RecordBatch> {
    if predicates.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut keep = vec![true; batch.num_rows()];
    for predicate in predicates {
        let parsed = ParsedPredicate::parse(&predicate.expression)?;
        for (row, keep_row) in keep.iter_mut().enumerate() {
            *keep_row &= evaluate_predicate(batch, &parsed, row)?;
        }
    }

    let mask = BooleanArray::from(keep);
    filter_record_batch(batch, &mask).map_err(FirnError::from)
}

fn apply_projection(batch: &RecordBatch, projection: Option<&[String]>) -> Result<RecordBatch> {
    let Some(projection) = projection else {
        return Ok(batch.clone());
    };
    if projection.is_empty() {
        return Ok(batch.clone());
    }

    let indices = projection
        .iter()
        .map(|name| {
            batch.schema().index_of(name).map_err(|_| {
                FirnError::data(format!(
                    "projected field {name:?} is not present in resource batch"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    batch.project(&indices).map_err(FirnError::from)
}

fn apply_contract_exec(batch: RecordBatch, program: &ValidationProgram) -> Result<RecordBatch> {
    for field in batch.schema().fields() {
        let field_name = field.name();
        let covered = program
            .column_programs
            .iter()
            .any(|column| column.source_name == *field_name || column.output_name == *field_name);
        if !covered {
            return Err(FirnError::contract(format!(
                "validation program does not cover field {field_name:?}"
            )));
        }
    }
    Ok(batch)
}

fn apply_normalize_exec(batch: RecordBatch, program: &ValidationProgram) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| normalize_field(field.as_ref(), program))
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(FirnError::from)
}

fn normalize_field(field: &Field, program: &ValidationProgram) -> Result<Field> {
    let Some(column) = program
        .column_programs
        .iter()
        .find(|column| column.source_name == *field.name() || column.output_name == *field.name())
    else {
        return Err(FirnError::contract(format!(
            "validation program does not cover field {:?}",
            field.name()
        )));
    };
    Ok(with_source_name(field.clone(), column.source_name.clone()).with_name(&column.output_name))
}

fn schema_artifact(schema: &Schema) -> SchemaArtifact {
    SchemaArtifact {
        fields: schema
            .fields()
            .iter()
            .map(|field| SchemaFieldArtifact {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
    }
}

fn delivery_guarantee(disposition: WriteDisposition) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

fn predicate_operator(expression: &str) -> Option<String> {
    ParsedPredicate::split(expression).map(|(_, operator, _)| operator.to_owned())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedPredicate {
    column: String,
    operator: ComparisonOperator,
    literal: Literal,
}

impl ParsedPredicate {
    fn parse(expression: &str) -> Result<Self> {
        let Some((column, operator, literal)) = Self::split(expression) else {
            return Err(FirnError::contract(format!(
                "unsupported predicate expression {expression:?}; MVP predicates use '<column> <op> <literal>'"
            )));
        };
        Ok(Self {
            column: column.to_owned(),
            operator: ComparisonOperator::parse(operator)?,
            literal: Literal::parse(literal),
        })
    }

    fn split(expression: &str) -> Option<(&str, &str, &str)> {
        for operator in [">=", "<=", "!=", "=", ">", "<"] {
            if let Some(index) = expression.find(operator) {
                let (column, rest) = expression.split_at(index);
                let literal = &rest[operator.len()..];
                let column = column.trim();
                let literal = literal.trim();
                if !column.is_empty() && !literal.is_empty() {
                    return Some((column, operator, literal));
                }
            }
        }
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComparisonOperator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

impl ComparisonOperator {
    fn parse(operator: &str) -> Result<Self> {
        match operator {
            "=" => Ok(Self::Eq),
            "!=" => Ok(Self::NotEq),
            ">" => Ok(Self::Gt),
            ">=" => Ok(Self::GtEq),
            "<" => Ok(Self::Lt),
            "<=" => Ok(Self::LtEq),
            other => Err(FirnError::contract(format!(
                "unsupported predicate operator {other:?}"
            ))),
        }
    }

    fn compare_ord<T: Ord>(&self, left: T, right: T) -> bool {
        match self {
            Self::Eq => left == right,
            Self::NotEq => left != right,
            Self::Gt => left > right,
            Self::GtEq => left >= right,
            Self::Lt => left < right,
            Self::LtEq => left <= right,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Literal {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
}

impl Literal {
    fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        if let Some(unquoted) = trimmed
            .strip_prefix('\'')
            .and_then(|value| value.strip_suffix('\''))
        {
            return Self::String(unquoted.to_owned());
        }
        if let Some(unquoted) = trimmed
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        {
            return Self::String(unquoted.to_owned());
        }
        if trimmed.eq_ignore_ascii_case("true") {
            return Self::Bool(true);
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return Self::Bool(false);
        }
        if let Ok(value) = trimmed.parse::<i64>() {
            return Self::I64(value);
        }
        if let Ok(value) = trimmed.parse::<u64>() {
            return Self::U64(value);
        }
        Self::String(trimmed.to_owned())
    }
}

fn evaluate_predicate(
    batch: &RecordBatch,
    predicate: &ParsedPredicate,
    row: usize,
) -> Result<bool> {
    let index = batch.schema().index_of(&predicate.column).map_err(|_| {
        FirnError::data(format!(
            "predicate field {:?} is not present in resource batch",
            predicate.column
        ))
    })?;
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(false);
    }

    match array.data_type() {
        DataType::Int32 => compare_i64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Arrow Int32 array downcast"),
            row,
            predicate,
        ),
        DataType::Int64 => compare_i64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Arrow Int64 array downcast"),
            row,
            predicate,
        ),
        DataType::UInt32 => compare_u64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("Arrow UInt32 array downcast"),
            row,
            predicate,
        ),
        DataType::UInt64 => compare_u64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Arrow UInt64 array downcast"),
            row,
            predicate,
        ),
        DataType::Utf8 => compare_string(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Arrow Utf8 array downcast"),
            row,
            predicate,
        ),
        DataType::Boolean => compare_bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Arrow Boolean array downcast"),
            row,
            predicate,
        ),
        other => Err(FirnError::contract(format!(
            "predicate field {:?} has unsupported MVP filter type {other}",
            predicate.column
        ))),
    }
}

trait IntValueArray {
    fn value_i64(&self, row: usize) -> i64;
}

impl IntValueArray for Int32Array {
    fn value_i64(&self, row: usize) -> i64 {
        i64::from(self.value(row))
    }
}

impl IntValueArray for Int64Array {
    fn value_i64(&self, row: usize) -> i64 {
        self.value(row)
    }
}

fn compare_i64<T>(array: &T, row: usize, predicate: &ParsedPredicate) -> Result<bool>
where
    T: IntValueArray,
{
    let Literal::I64(right) = predicate.literal else {
        return Err(FirnError::contract(format!(
            "predicate {:?} requires a signed integer literal",
            predicate.column
        )));
    };
    Ok(predicate.operator.compare_ord(array.value_i64(row), right))
}

trait UIntValueArray {
    fn value_u64(&self, row: usize) -> u64;
}

impl UIntValueArray for UInt32Array {
    fn value_u64(&self, row: usize) -> u64 {
        u64::from(self.value(row))
    }
}

impl UIntValueArray for UInt64Array {
    fn value_u64(&self, row: usize) -> u64 {
        self.value(row)
    }
}

fn compare_u64<T>(array: &T, row: usize, predicate: &ParsedPredicate) -> Result<bool>
where
    T: UIntValueArray,
{
    let right = match predicate.literal {
        Literal::U64(value) => value,
        Literal::I64(value) if value >= 0 => value as u64,
        _ => {
            return Err(FirnError::contract(format!(
                "predicate {:?} requires an unsigned integer literal",
                predicate.column
            )));
        }
    };
    Ok(predicate.operator.compare_ord(array.value_u64(row), right))
}

fn compare_string(array: &StringArray, row: usize, predicate: &ParsedPredicate) -> Result<bool> {
    let Literal::String(ref right) = predicate.literal else {
        return Err(FirnError::contract(format!(
            "predicate {:?} requires a string literal",
            predicate.column
        )));
    };
    Ok(predicate
        .operator
        .compare_ord(array.value(row), right.as_str()))
}

fn compare_bool(array: &BooleanArray, row: usize, predicate: &ParsedPredicate) -> Result<bool> {
    let Literal::Bool(right) = predicate.literal else {
        return Err(FirnError::contract(format!(
            "predicate {:?} requires a boolean literal",
            predicate.column
        )));
    };
    match predicate.operator {
        ComparisonOperator::Eq | ComparisonOperator::NotEq => {
            Ok(predicate.operator.compare_ord(array.value(row), right))
        }
        _ => Err(FirnError::contract(format!(
            "predicate {:?} uses an unsupported boolean operator",
            predicate.column
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow_array::{ArrayRef, BooleanArray, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use firn_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
    use firn_kernel::{
        BackpressureSupport, Batch, BatchHeader, BatchStats, BatchStream, CapabilitySupport,
        ContractRef, EstimateSupport, FilterCapabilities, FreshnessSpec, IncrementalShape,
        PartitionId, PartitioningCapabilities, PredicateId, ResourceDescriptor, SchemaHash,
        SchemaSource, ScopeKey, TrustLevel,
    };
    use futures_executor::block_on;
    use futures_util::stream;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn tier_a_resource_runs_engine_projection_filter_limit_into_package() {
        let resource = MockResource::tier_a(sample_batches());
        let input = plan_input(
            vec!["id > 1", "active = true"],
            Some(vec!["name".to_owned()]),
            Some(1),
            PlanBoundedness::Bounded,
        );
        let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

        assert_eq!(plan.explain.pushed_predicates, Vec::new());
        assert_eq!(plan.explain.unsupported_predicates.len(), 2);

        let temp = TempDir::new().unwrap();
        let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

        assert_eq!(output.manifest.lifecycle.status, PackageStatus::Packaged);
        assert_eq!(output.profile.output_rows, 1);
        assert_eq!(output.segments.len(), 1);

        let reader = firn_package::PackageReader::open(temp.path()).unwrap();
        let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).name(), "name");
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "two");
    }

    #[test]
    fn tier_b_negotiates_pushdown_fidelity_without_io() {
        let resource = MockResource::tier_b(sample_batches());
        let input = plan_input(
            vec!["id > 1", "active = true", "name != 'missing'"],
            Some(vec!["name".to_owned()]),
            Some(10),
            PlanBoundedness::Bounded,
        );
        let plan = Planner::new().plan_tier_b(&resource, input).unwrap();

        assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
        assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
        assert_eq!(plan.scan.pushed_predicates.len(), 2);
        assert_eq!(
            plan.scan.pushed_predicates[0].fidelity,
            PushdownFidelity::Exact
        );
        assert_eq!(
            datafusion_filter_pushdown(&plan.scan.pushed_predicates[0].fidelity),
            datafusion::logical_expr::TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            plan.scan.pushed_predicates[1].fidelity,
            PushdownFidelity::Inexact
        );
        assert_eq!(plan.scan.unsupported_predicates.len(), 1);
        assert_eq!(plan.residual_predicates.len(), 2);
        assert!(plan.explain.projection_pushed);
        assert!(plan.explain.limit_pushed);
        assert_eq!(plan.explain.inexact_predicates.len(), 1);
        assert_eq!(plan.explain.unsupported_predicates.len(), 1);
        assert_eq!(plan.explain.partitions.len(), 2);
        assert_eq!(plan.explain.estimates.rows, Some(3));
        assert_eq!(
            plan.explain.delivery_guarantee,
            DeliveryGuarantee::EffectivelyOncePerKey
        );
    }

    #[test]
    fn inexact_and_unsupported_predicates_are_reapplied_during_execution() {
        let resource = MockResource::tier_b(sample_batches());
        let input = plan_input(
            vec!["id > 1", "active = true", "name != 'three'"],
            Some(vec!["name".to_owned()]),
            None,
            PlanBoundedness::Bounded,
        );
        let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
        let temp = TempDir::new().unwrap();
        let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

        assert_eq!(output.profile.output_rows, 2);
        let reader = firn_package::PackageReader::open(temp.path()).unwrap();
        for segment in output.segments {
            let batches = reader.read_segment(&segment.segment_id).unwrap();
            let names = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "two");
        }
    }

    #[test]
    fn illegal_unbounded_live_plan_is_rejected() {
        let resource = MockResource::tier_a(sample_batches());
        let input = plan_input(
            vec![],
            None,
            None,
            PlanBoundedness::UnboundedLive {
                checkpoint_cadence_ms: None,
                package_rotation_rows: None,
                watermark: None,
            },
        );
        let error = Planner::new().plan_tier_a(&resource, input).unwrap_err();

        assert_eq!(error.kind, firn_kernel::ErrorKind::Contract);
        assert!(error.message.contains("unbounded live plans are illegal"));
    }

    #[test]
    fn explain_and_operator_chain_carry_contract_package_details() {
        let resource = MockResource::tier_a(sample_batches());
        let input = plan_input(
            vec!["active = true"],
            Some(vec!["id".to_owned(), "name".to_owned()]),
            Some(2),
            PlanBoundedness::UnboundedDrain,
        );
        let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
        let explain_json = serde_json::to_value(&plan.explain).unwrap();

        assert!(explain_json.get("pushed_predicates").is_some());
        assert!(explain_json.get("inexact_predicates").is_some());
        assert!(explain_json.get("unsupported_predicates").is_some());
        assert!(explain_json.get("partitions").is_some());
        assert!(explain_json.get("estimates").is_some());
        assert!(explain_json.get("delivery_guarantee").is_some());
        assert!(plan.operator_chain.iter().any(|operator| {
            matches!(
                operator,
                OperatorNode::ContractExec {
                    normalizer_version,
                    ..
                } if normalizer_version == firn_contract::NORMALIZER_NAMECASE_V1
            )
        }));
        assert!(plan.operator_chain.iter().any(|operator| {
            matches!(
                operator,
                OperatorNode::PackageSink { package_id } if package_id == "pkg-engine-test"
            )
        }));
    }

    #[derive(Clone)]
    struct MockResource {
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        batches: Vec<Batch>,
        tier_b: bool,
        negotiate_count: Arc<AtomicUsize>,
        open_count: Arc<AtomicUsize>,
    }

    impl MockResource {
        fn tier_a(batches: Vec<Batch>) -> Self {
            Self::new(batches, false)
        }

        fn tier_b(batches: Vec<Batch>) -> Self {
            Self::new(batches, true)
        }

        fn new(batches: Vec<Batch>, tier_b: bool) -> Self {
            Self {
                descriptor: descriptor(),
                schema: sample_schema(),
                batches,
                tier_b,
                negotiate_count: Arc::new(AtomicUsize::new(0)),
                open_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl ResourceStream for MockResource {
        fn descriptor(&self) -> &ResourceDescriptor {
            &self.descriptor
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
            let count = if self.tier_b { 2 } else { 1 };
            (0..count)
                .map(|index| {
                    Ok(PartitionPlan {
                        partition_id: PartitionId::new(format!("part-{index}"))?,
                        scope: ScopeKey::Partition {
                            partition_id: PartitionId::new(format!("part-{index}"))?,
                        },
                        start_position: None,
                        metadata: BTreeMap::from([("ordinal".to_owned(), index.to_string())]),
                    })
                })
                .collect()
        }

        fn open(
            &self,
            partition: PartitionPlan,
        ) -> firn_kernel::BoxFuture<'_, Result<BatchStream>> {
            self.open_count.fetch_add(1, Ordering::SeqCst);
            let batches = self
                .batches
                .iter()
                .filter(|batch| batch.header.partition_id == partition.partition_id)
                .cloned()
                .collect::<Vec<_>>();
            Box::pin(async move {
                Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream)
            })
        }
    }

    impl QueryableResource for MockResource {
        fn capabilities(&self) -> &ResourceCapabilities {
            static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> =
                std::sync::OnceLock::new();
            CAPABILITIES.get_or_init(|| ResourceCapabilities {
                projection: CapabilitySupport::Supported,
                filters: FilterCapabilities {
                    default_fidelity: PushdownFidelity::Inexact,
                    supported_operators: vec![">".to_owned(), "=".to_owned()],
                },
                limits: CapabilitySupport::Supported,
                ordering: CapabilitySupport::Unsupported,
                partitioning: PartitioningCapabilities {
                    parallel_partitions: true,
                    supported_scopes: vec![firn_kernel::ScopeKind::Partition],
                },
                incremental: IncrementalShape::Cursor,
                replay: firn_kernel::ReplaySupport::ExactRecordedBatches,
                idempotent_reads: true,
                backpressure: BackpressureSupport::Pausable,
                estimates: EstimateSupport::RowsAndBytes,
            })
        }

        fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
            self.negotiate_count.fetch_add(1, Ordering::SeqCst);
            let mut plan = negotiate_scan_plan(
                request.resource_id.clone(),
                request.clone(),
                self.capabilities(),
                self.plan_partitions(request)?,
                Some(3),
                Some(256),
                DeliveryGuarantee::EffectivelyOncePerKey,
            )?;
            for pushed in &mut plan.pushed_predicates {
                if pushed.predicate.expression == "id > 1" {
                    pushed.fidelity = PushdownFidelity::Exact;
                }
            }
            Ok(plan)
        }
    }

    fn plan_input(
        filters: Vec<&str>,
        projection: Option<Vec<String>>,
        limit: Option<u64>,
        boundedness: PlanBoundedness,
    ) -> EnginePlanInput {
        let observed = ObservedSchema::from_arrow(sample_schema().as_ref());
        let validation_program =
            compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
                .unwrap();
        EnginePlanInput {
            request: ScanRequest {
                resource_id: ResourceId::new("orders").unwrap(),
                projection,
                filters: filters
                    .into_iter()
                    .enumerate()
                    .map(|(index, expression)| ScanPredicate {
                        predicate_id: PredicateId::new(format!("p{index}")).unwrap(),
                        expression: expression.to_owned(),
                    })
                    .collect(),
                limit,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            },
            validation_program,
            boundedness,
            package_id: "pkg-engine-test".to_owned(),
        }
    }

    fn descriptor() -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("orders").unwrap(),
            schema_source: SchemaSource::Discovered {
                schema_hash: Some(SchemaHash::new("schema-v1").unwrap()),
            },
            primary_key: vec!["id".to_owned()],
            merge_key: vec!["id".to_owned()],
            cursor: None,
            write_disposition: WriteDisposition::Merge,
            contract: Some(ContractRef::new("contract-orders").unwrap()),
            state_scope: ScopeKey::Resource,
            freshness: Some(FreshnessSpec { max_age_ms: 60_000 }),
            trust_level: TrustLevel::Governed,
        }
    }

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    fn sample_batches() -> Vec<Batch> {
        vec![
            batch_for_partition(
                "batch-0",
                "part-0",
                vec![1, 2, 3],
                vec!["one", "two", "three"],
                vec![false, true, true],
            ),
            batch_for_partition(
                "batch-1",
                "part-1",
                vec![1, 2, 3],
                vec!["one", "two", "three"],
                vec![false, true, true],
            ),
        ]
    }

    fn batch_for_partition(
        batch_id: &str,
        partition_id: &str,
        ids: Vec<i32>,
        names: Vec<&str>,
        active: Vec<bool>,
    ) -> Batch {
        let schema = sample_schema();
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(BooleanArray::from(active)) as ArrayRef,
            ],
        )
        .unwrap();

        Batch {
            header: BatchHeader {
                batch_id: BatchId::new(batch_id).unwrap(),
                resource_id: ResourceId::new("orders").unwrap(),
                partition_id: PartitionId::new(partition_id).unwrap(),
                observed_schema_hash: SchemaHash::new("schema-v1").unwrap(),
                row_count: record_batch.num_rows() as u64,
                byte_count: record_batch.get_array_memory_size() as u64,
                source_position: None,
                watermarks: Vec::new(),
                stats: BatchStats::default(),
                cdc: None,
            },
            payload: firn_kernel::BatchPayload::RecordBatch(record_batch),
        }
    }
}
