use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, UInt64Array};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use cdf_contract::{CompiledExpressionPlan, Expression, ExpressionUse, PlannedExpression};
use cdf_kernel::{
    CdfError, Result, STATISTICS_MODEL_VERSION, StatisticsArrowType, StatisticsCompleteness,
    TypedScalar, canonical_arrow_schema_hash,
};
use cdf_memory::MemoryCoordinator;
use cdf_package::{
    PackageReader, STATISTICS_PROFILE_FILE, StatisticsProfileGrain, StatisticsProfileRow,
    VerifiedPackage, VerifiedSegmentObject, VerifiedStatisticsProfileWindow,
};
use datafusion::{
    common::{Column, DFSchema, ScalarValue},
    physical_expr::{execution_props::ExecutionProps, planner::create_physical_expr},
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
};
use half::f16;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsPruningContainerGrain {
    Segment,
    Package,
}

impl From<StatisticsProfileGrain> for StatisticsPruningContainerGrain {
    fn from(value: StatisticsProfileGrain) -> Self {
        match value {
            StatisticsProfileGrain::Segment => Self::Segment,
            StatisticsProfileGrain::Package => Self::Package,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsPruningOutcome {
    Pruned,
    RetainedMayMatch,
    RetainedConservatively,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatisticsPruningDecision {
    pub grain: StatisticsPruningContainerGrain,
    pub container_ordinal: u64,
    pub container_id: String,
    pub row_count: u64,
    pub byte_count: u64,
    pub outcome: StatisticsPruningOutcome,
    pub conservative_fields: Box<[Box<str>]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatisticsPruningReport {
    pub statistics_model_version: u16,
    pub schema_hash: String,
    pub predicate: Expression,
    pub container_count: u64,
    pub pruned_count: u64,
    pub decisions: Box<[StatisticsPruningDecision]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatisticsPruningLimits {
    maximum_containers_per_window: usize,
    maximum_window_bytes: u64,
}

impl StatisticsPruningLimits {
    pub fn new(maximum_containers_per_window: usize, maximum_window_bytes: u64) -> Result<Self> {
        if maximum_containers_per_window == 0 || maximum_window_bytes == 0 {
            return Err(CdfError::contract(
                "statistics pruning container and memory windows must be nonzero",
            ));
        }
        Ok(Self {
            maximum_containers_per_window,
            maximum_window_bytes,
        })
    }

    pub const fn maximum_containers_per_window(self) -> usize {
        self.maximum_containers_per_window
    }

    pub const fn maximum_window_bytes(self) -> u64 {
        self.maximum_window_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsPruningEvidence {
    VerifiedProfile,
    MissingProfile,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatisticsPruningSummary {
    pub statistics_model_version: u16,
    pub evidence_generation: String,
    pub evidence: StatisticsPruningEvidence,
    pub schema_hash: String,
    pub predicate: Expression,
    pub segment_count: u64,
    pub pruned_count: u64,
    pub pruned_rows: u64,
    pub pruned_bytes: u64,
    pub retained_count: u64,
    pub retained_rows: u64,
    pub retained_bytes: u64,
    pub conservative_count: u64,
}

/// One canonical package segment after a verified statistics-pruning decision.
///
/// Pruned segments expose only recorded evidence. Retained segments carry the verified capability
/// a replay/backfill/query/merge consumer may open; the planner never opens segment payloads.
pub enum VerifiedPackageSegmentPruning {
    Pruned {
        decision: StatisticsPruningDecision,
    },
    Retained {
        decision: StatisticsPruningDecision,
        segment: VerifiedSegmentObject<()>,
    },
}

/// Streams sound segment decisions for one verified package.
///
/// The package profile and DataFusion arrays share one caller-sized `cdf-memory` reservation.
/// Missing profile evidence retains every segment. Decisions are delivered incrementally so the
/// planner's memory is independent of package cardinality.
pub fn for_each_verified_package_segment_pruning(
    reader: &PackageReader,
    verified: &VerifiedPackage,
    compiled: &CompiledExpressionPlan,
    predicate_index: usize,
    memory: Arc<dyn MemoryCoordinator>,
    limits: StatisticsPruningLimits,
    visitor: &mut dyn FnMut(VerifiedPackageSegmentPruning) -> Result<()>,
) -> Result<StatisticsPruningSummary> {
    compiled.validate_recorded()?;
    let planned = compiled.predicates.get(predicate_index).ok_or_else(|| {
        CdfError::contract(format!(
            "statistics pruning predicate index {predicate_index} is absent from the recorded compiled expression plan"
        ))
    })?;
    if planned.use_kind != ExpressionUse::Filter {
        return Err(CdfError::contract(
            "statistics pruning requires a recorded filter expression",
        ));
    }
    let schema = reader.runtime_arrow_schema_verified(verified)?;
    let schema_hash = canonical_arrow_schema_hash(schema.as_ref())?.to_string();
    let mut profile_present = false;
    reader.for_each_identity_file(&mut |entry| {
        profile_present |= entry.path == STATISTICS_PROFILE_FILE;
        Ok(())
    })?;
    let evidence = if profile_present {
        StatisticsPruningEvidence::VerifiedProfile
    } else {
        StatisticsPruningEvidence::MissingProfile
    };
    let mut summary = StatisticsPruningSummary {
        statistics_model_version: STATISTICS_MODEL_VERSION,
        evidence_generation: verified.package_hash().to_owned(),
        evidence,
        schema_hash: schema_hash.clone(),
        predicate: planned.optimized.clone(),
        segment_count: 0,
        pruned_count: 0,
        pruned_rows: 0,
        pruned_bytes: 0,
        retained_count: 0,
        retained_rows: 0,
        retained_bytes: 0,
        conservative_count: 0,
    };
    let mut segments = reader.verified_canonical_segment_object_stream_with(verified)?;

    if !profile_present {
        let conservative_fields = planned
            .optimized
            .column_dependencies()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        for segment in &mut segments {
            let segment = segment?;
            let decision = StatisticsPruningDecision {
                grain: StatisticsPruningContainerGrain::Segment,
                container_ordinal: summary.segment_count,
                container_id: segment.entry.segment_id.as_str().to_owned(),
                row_count: segment.entry.row_count,
                byte_count: segment.entry.byte_count,
                outcome: StatisticsPruningOutcome::RetainedConservatively,
                conservative_fields: conservative_fields.clone(),
            };
            record_segment_decision(&mut summary, &decision)?;
            visitor(VerifiedPackageSegmentPruning::Retained { decision, segment })?;
        }
        return Ok(summary);
    }

    reader.for_each_verified_statistics_profile_window(
        verified,
        memory,
        limits.maximum_containers_per_window,
        limits.maximum_window_bytes,
        &mut |window| {
            validate_pruning_window_memory(window)?;
            let report =
                evaluate_verified_statistics_pruning(compiled, predicate_index, window)?;
            if summary.segment_count == 0 {
                summary.schema_hash = report.schema_hash.clone();
            } else if summary.schema_hash != report.schema_hash {
                return Err(CdfError::data(
                    "statistics pruning profile schema hash changed between windows",
                ));
            }
            for mut decision in report.decisions.into_vec() {
                if decision.grain == StatisticsPruningContainerGrain::Package {
                    continue;
                }
                let segment = segments.next().transpose()?.ok_or_else(|| {
                    CdfError::data("statistics profile contains more segments than the manifest")
                })?;
                if decision.container_ordinal != summary.segment_count
                    || decision.container_id != segment.entry.segment_id.as_str()
                    || decision.row_count != segment.entry.row_count
                {
                    return Err(CdfError::data(
                        "statistics pruning decision does not align with canonical manifest segment authority",
                    ));
                }
                decision.byte_count = segment.entry.byte_count;
                record_segment_decision(&mut summary, &decision)?;
                match decision.outcome {
                    StatisticsPruningOutcome::Pruned => {
                        visitor(VerifiedPackageSegmentPruning::Pruned { decision })?
                    }
                    StatisticsPruningOutcome::RetainedMayMatch
                    | StatisticsPruningOutcome::RetainedConservatively => {
                        visitor(VerifiedPackageSegmentPruning::Retained { decision, segment })?
                    }
                }
            }
            Ok(())
        },
    )?;
    if segments.next().transpose()?.is_some() {
        return Err(CdfError::data(
            "statistics profile omitted canonical manifest segments",
        ));
    }
    Ok(summary)
}

fn validate_pruning_window_memory(window: &VerifiedStatisticsProfileWindow) -> Result<()> {
    let container_count = window
        .rows()
        .windows(2)
        .filter(|pair| {
            pair[0].grain != pair[1].grain || pair[0].container_ordinal != pair[1].container_ordinal
        })
        .count()
        .saturating_add(usize::from(!window.rows().is_empty()));
    let cells = container_count
        .checked_mul(window.schema().fields().len())
        .ok_or_else(|| CdfError::data("statistics pruning window cell count overflow"))?;
    let transient = u64::try_from(cells)
        .map_err(|_| CdfError::data("statistics pruning window cell count exceeds u64"))?
        .checked_mul(192)
        .ok_or_else(|| CdfError::data("statistics pruning transient memory overflow"))?;
    let required = window
        .retained_bytes()
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(transient))
        .ok_or_else(|| CdfError::data("statistics pruning working set overflow"))?;
    if required > window.reserved_bytes() {
        return Err(CdfError::data(format!(
            "statistics pruning window requires {required} bytes under its conservative Arrow working-set model but the configured window holds {}; increase the pruning memory window or reduce containers per window",
            window.reserved_bytes()
        )));
    }
    Ok(())
}

fn record_segment_decision(
    summary: &mut StatisticsPruningSummary,
    decision: &StatisticsPruningDecision,
) -> Result<()> {
    summary.segment_count = summary
        .segment_count
        .checked_add(1)
        .ok_or_else(|| CdfError::data("statistics pruning segment count overflow"))?;
    match decision.outcome {
        StatisticsPruningOutcome::Pruned => {
            summary.pruned_count = summary
                .pruned_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("statistics pruning pruned count overflow"))?;
            summary.pruned_rows = summary
                .pruned_rows
                .checked_add(decision.row_count)
                .ok_or_else(|| CdfError::data("statistics pruning pruned rows overflow"))?;
            summary.pruned_bytes = summary
                .pruned_bytes
                .checked_add(decision.byte_count)
                .ok_or_else(|| CdfError::data("statistics pruning pruned bytes overflow"))?;
        }
        StatisticsPruningOutcome::RetainedMayMatch
        | StatisticsPruningOutcome::RetainedConservatively => {
            summary.retained_count = summary
                .retained_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("statistics pruning retained count overflow"))?;
            summary.retained_rows = summary
                .retained_rows
                .checked_add(decision.row_count)
                .ok_or_else(|| CdfError::data("statistics pruning retained rows overflow"))?;
            summary.retained_bytes = summary
                .retained_bytes
                .checked_add(decision.byte_count)
                .ok_or_else(|| CdfError::data("statistics pruning retained bytes overflow"))?;
            if decision.outcome == StatisticsPruningOutcome::RetainedConservatively {
                summary.conservative_count =
                    summary.conservative_count.checked_add(1).ok_or_else(|| {
                        CdfError::data("statistics pruning conservative count overflow")
                    })?;
            }
        }
    }
    Ok(())
}

/// Evaluates one sealed, bounded, verified profile window against one predicate bound to the
/// recorded compiled plan. DataFusion supplies only the pruning proof: CDF statistics remain the
/// evidence authority, and the returned report contains no DataFusion type.
///
/// Missing or incomplete field evidence becomes a typed NULL statistic and can only retain data.
/// The package layer constructs the sealed window only after validating the complete profile, so
/// provisional visitor rows cannot authorize a payload skip.
fn evaluate_verified_statistics_pruning(
    compiled: &CompiledExpressionPlan,
    predicate_index: usize,
    window: &VerifiedStatisticsProfileWindow,
) -> Result<StatisticsPruningReport> {
    evaluate_statistics_rows(
        compiled,
        predicate_index,
        Arc::clone(window.schema()),
        window.schema_hash(),
        window.rows(),
    )
}

fn evaluate_statistics_rows(
    compiled: &CompiledExpressionPlan,
    predicate_index: usize,
    schema: SchemaRef,
    expected_schema_hash: &str,
    rows: &[StatisticsProfileRow],
) -> Result<StatisticsPruningReport> {
    compiled.validate_recorded()?;
    let planned = compiled.predicates.get(predicate_index).ok_or_else(|| {
        CdfError::contract(format!(
            "statistics pruning predicate index {predicate_index} is absent from the recorded compiled expression plan"
        ))
    })?;
    if planned.use_kind != ExpressionUse::Filter {
        return Err(CdfError::contract(
            "statistics pruning requires a recorded filter expression",
        ));
    }
    if expected_schema_hash.trim().is_empty() {
        return Err(CdfError::contract(
            "statistics pruning requires a nonempty schema hash",
        ));
    }

    let containers =
        PruningContainer::from_profile_rows(schema.as_ref(), expected_schema_hash, rows)?;
    if containers.is_empty() {
        return Ok(StatisticsPruningReport {
            statistics_model_version: STATISTICS_MODEL_VERSION,
            schema_hash: expected_schema_hash.to_owned(),
            predicate: planned.optimized.clone(),
            container_count: 0,
            pruned_count: 0,
            decisions: Box::new([]),
        });
    }
    let Some(logical) = crate::expression::lower_recorded_filter_for_pruning(
        &planned.optimized.root,
        schema.as_ref(),
    )?
    else {
        return conservative_report(planned, expected_schema_hash, containers);
    };
    let statistics = CdfPruningStatistics::try_new(Arc::clone(&schema), &containers)?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(datafusion_error)?;
    let physical = create_physical_expr(&logical, &df_schema, &ExecutionProps::new())
        .map_err(datafusion_error)?;
    let predicate = PruningPredicate::try_new(physical, schema).map_err(datafusion_error)?;
    let retain = predicate.prune(&statistics).map_err(datafusion_error)?;
    if retain.len() != containers.len() {
        return Err(CdfError::internal(
            "DataFusion pruning result cardinality differs from CDF evidence",
        ));
    }

    let referenced_fields = planned.optimized.column_dependencies();
    let mut pruned_count = 0_u64;
    let decisions = containers
        .into_iter()
        .zip(retain)
        .map(|(container, retain)| {
            let conservative_fields = referenced_fields
                .iter()
                .filter_map(|name| {
                    let index = statistics.schema.index_of(name).ok()?;
                    (!container.columns[index].available_for_pruning).then(|| name.as_str().into())
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let outcome = if !retain {
                pruned_count += 1;
                StatisticsPruningOutcome::Pruned
            } else if conservative_fields.is_empty() {
                StatisticsPruningOutcome::RetainedMayMatch
            } else {
                StatisticsPruningOutcome::RetainedConservatively
            };
            StatisticsPruningDecision {
                grain: container.grain,
                container_ordinal: container.ordinal,
                container_id: container.id.into(),
                row_count: container.row_count,
                byte_count: 0,
                outcome,
                conservative_fields,
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let container_count = u64::try_from(decisions.len())
        .map_err(|_| CdfError::data("statistics pruning container count exceeds u64"))?;

    Ok(StatisticsPruningReport {
        statistics_model_version: STATISTICS_MODEL_VERSION,
        schema_hash: expected_schema_hash.to_owned(),
        predicate: planned.optimized.clone(),
        container_count,
        pruned_count,
        decisions,
    })
}

struct PruningColumn {
    minimum: Option<ScalarValue>,
    maximum: Option<ScalarValue>,
    null_count: Option<u64>,
    available_for_pruning: bool,
}

struct PruningContainer {
    grain: StatisticsPruningContainerGrain,
    ordinal: u64,
    id: Box<str>,
    row_count: u64,
    columns: Box<[PruningColumn]>,
}

impl PruningContainer {
    fn from_profile_rows(
        schema: &arrow_schema::Schema,
        expected_schema_hash: &str,
        rows: &[StatisticsProfileRow],
    ) -> Result<Vec<Self>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let field_count = schema.fields().len();
        if field_count == 0 {
            return Err(CdfError::contract(
                "statistics pruning does not admit a zero-field schema",
            ));
        }

        let mut containers = Vec::new();
        let mut current_key = None;
        let mut current_id = None::<Box<str>>;
        let mut current_row_count = None;
        let mut columns = Vec::with_capacity(field_count);
        let mut last_finished_key = None;

        for row in rows {
            if row.schema_hash != expected_schema_hash {
                return Err(CdfError::data(format!(
                    "statistics pruning expected schema hash {expected_schema_hash} but container {} records {}",
                    row.container_id, row.schema_hash
                )));
            }
            let key = (row.grain, row.container_ordinal);
            if current_key.is_some_and(|current| current != key) {
                let finished_key = current_key
                    .take()
                    .ok_or_else(|| CdfError::internal("statistics pruning key disappeared"))?;
                if last_finished_key
                    .is_some_and(|previous| !profile_key_is_after(previous, finished_key))
                {
                    return Err(CdfError::data(
                        "statistics pruning containers are not in canonical order",
                    ));
                }
                containers.push(finish_container(
                    finished_key,
                    current_id.take().ok_or_else(|| {
                        CdfError::internal("statistics pruning container id disappeared")
                    })?,
                    current_row_count.take().ok_or_else(|| {
                        CdfError::internal("statistics pruning row count disappeared")
                    })?,
                    std::mem::take(&mut columns),
                    field_count,
                )?);
                last_finished_key = Some(finished_key);
            }
            if current_key.is_none() {
                current_key = Some(key);
                current_id = Some(row.container_id.as_str().into());
                current_row_count = Some(row.row_count);
            }
            if current_id.as_deref() != Some(row.container_id.as_str())
                || current_row_count != Some(row.row_count)
            {
                return Err(CdfError::data(
                    "statistics pruning container identity or row count changed between fields",
                ));
            }
            let expected_ordinal = u32::try_from(columns.len())
                .map_err(|_| CdfError::data("statistics pruning field count exceeds u32"))?;
            if row.field_ordinal != expected_ordinal {
                return Err(CdfError::data(
                    "statistics pruning fields are not contiguous within a container",
                ));
            }
            let field = schema.fields().get(columns.len()).ok_or_else(|| {
                CdfError::data("statistics pruning row references a field outside the schema")
            })?;
            if row.field_path.len() != 1 || row.field_path[0].as_ref() != field.name() {
                return Err(CdfError::data(format!(
                    "statistics pruning field ordinal {} names {:?}, expected {:?}",
                    row.field_ordinal,
                    row.field_path,
                    field.name()
                )));
            }
            let expected_type = StatisticsArrowType::from_arrow_data_type(field.data_type())?;
            if row.data_type != expected_type {
                return Err(CdfError::data(format!(
                    "statistics pruning field {:?} type differs from the runtime schema",
                    field.name()
                )));
            }
            row.data_type.validate_bounds(
                row.row_count,
                row.null_count,
                &row.completeness,
                row.minimum.as_ref(),
                row.maximum.as_ref(),
            )?;
            let (minimum, maximum, available_for_pruning) =
                if matches!(row.completeness, StatisticsCompleteness::Complete) {
                    let minimum = row
                        .minimum
                        .as_ref()
                        .and_then(|value| scalar_value(field.data_type(), value));
                    let maximum = row
                        .maximum
                        .as_ref()
                        .and_then(|value| scalar_value(field.data_type(), value));
                    let all_null = row.null_count == row.row_count;
                    if all_null {
                        (None, None, true)
                    } else if minimum.is_some() && maximum.is_some() {
                        (minimum, maximum, true)
                    } else {
                        // One unusable bound invalidates the pair. Supplying only the other bound
                        // can still let a pruning rewrite prove false, so both become unknown.
                        (None, None, false)
                    }
                } else {
                    (None, None, false)
                };
            columns.push(PruningColumn {
                minimum,
                maximum,
                null_count: available_for_pruning.then_some(row.null_count),
                available_for_pruning,
            });
        }

        containers.push(finish_container(
            current_key
                .filter(|key| {
                    last_finished_key.is_none_or(|previous| profile_key_is_after(previous, *key))
                })
                .ok_or_else(|| {
                    CdfError::data("statistics pruning containers are not in canonical order")
                })?,
            current_id.ok_or_else(|| {
                CdfError::internal("nonempty statistics pruning rows omitted a container id")
            })?,
            current_row_count.ok_or_else(|| {
                CdfError::internal("nonempty statistics pruning rows omitted a row count")
            })?,
            columns,
            field_count,
        )?);
        Ok(containers)
    }
}

fn profile_key_is_after(
    previous: (StatisticsProfileGrain, u64),
    candidate: (StatisticsProfileGrain, u64),
) -> bool {
    match (previous.0, candidate.0) {
        (StatisticsProfileGrain::Segment, StatisticsProfileGrain::Segment)
        | (StatisticsProfileGrain::Package, StatisticsProfileGrain::Package) => {
            candidate.1 > previous.1
        }
        (StatisticsProfileGrain::Segment, StatisticsProfileGrain::Package) => true,
        (StatisticsProfileGrain::Package, StatisticsProfileGrain::Segment) => false,
    }
}

fn finish_container(
    key: (StatisticsProfileGrain, u64),
    id: Box<str>,
    row_count: u64,
    columns: Vec<PruningColumn>,
    expected_fields: usize,
) -> Result<PruningContainer> {
    if columns.len() != expected_fields {
        return Err(CdfError::data(format!(
            "statistics pruning container {id:?} has {} fields, expected {expected_fields}",
            columns.len()
        )));
    }
    Ok(PruningContainer {
        grain: key.0.into(),
        ordinal: key.1,
        id,
        row_count,
        columns: columns.into_boxed_slice(),
    })
}

struct PruningFieldArrays {
    minimum: ArrayRef,
    maximum: ArrayRef,
    null_counts: ArrayRef,
}

struct CdfPruningStatistics {
    schema: SchemaRef,
    row_counts: ArrayRef,
    fields: Box<[PruningFieldArrays]>,
    container_count: usize,
}

impl CdfPruningStatistics {
    fn try_new(schema: SchemaRef, containers: &[PruningContainer]) -> Result<Self> {
        let row_counts = Arc::new(UInt64Array::from_iter_values(
            containers.iter().map(|container| container.row_count),
        )) as ArrayRef;
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let null =
                    ScalarValue::try_new_null(field.data_type()).map_err(datafusion_error)?;
                let minimum = ScalarValue::iter_to_array(containers.iter().map(|container| {
                    container.columns[index]
                        .minimum
                        .clone()
                        .unwrap_or_else(|| null.clone())
                }))
                .map_err(datafusion_error)?;
                let maximum = ScalarValue::iter_to_array(containers.iter().map(|container| {
                    container.columns[index]
                        .maximum
                        .clone()
                        .unwrap_or_else(|| null.clone())
                }))
                .map_err(datafusion_error)?;
                let null_counts = Arc::new(UInt64Array::from(
                    containers
                        .iter()
                        .map(|container| container.columns[index].null_count)
                        .collect::<Vec<_>>(),
                )) as ArrayRef;
                Ok(PruningFieldArrays {
                    minimum,
                    maximum,
                    null_counts,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        Ok(Self {
            schema,
            row_counts,
            fields,
            container_count: containers.len(),
        })
    }
}

impl PruningStatistics for CdfPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        self.fields
            .get(index)
            .map(|field| Arc::clone(&field.minimum))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        self.fields
            .get(index)
            .map(|field| Arc::clone(&field.maximum))
    }

    fn num_containers(&self) -> usize {
        self.container_count
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        self.fields
            .get(index)
            .map(|field| Arc::clone(&field.null_counts))
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        Some(Arc::clone(&self.row_counts))
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

fn conservative_report(
    planned: &PlannedExpression,
    expected_schema_hash: &str,
    containers: Vec<PruningContainer>,
) -> Result<StatisticsPruningReport> {
    let conservative_fields = planned
        .optimized
        .column_dependencies()
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let decisions = containers
        .into_iter()
        .map(|container| StatisticsPruningDecision {
            grain: container.grain,
            container_ordinal: container.ordinal,
            container_id: container.id.into(),
            row_count: container.row_count,
            byte_count: 0,
            outcome: StatisticsPruningOutcome::RetainedConservatively,
            conservative_fields: conservative_fields.clone(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let container_count = u64::try_from(decisions.len())
        .map_err(|_| CdfError::data("statistics pruning container count exceeds u64"))?;
    Ok(StatisticsPruningReport {
        statistics_model_version: STATISTICS_MODEL_VERSION,
        schema_hash: expected_schema_hash.to_owned(),
        predicate: planned.optimized.clone(),
        container_count,
        pruned_count: 0,
        decisions,
    })
}

fn scalar_value(data_type: &DataType, value: &TypedScalar) -> Option<ScalarValue> {
    match (data_type, value) {
        (DataType::Boolean, TypedScalar::Boolean(value)) => {
            Some(ScalarValue::Boolean(Some(*value)))
        }
        (DataType::Int8, TypedScalar::Signed(value)) => {
            Some(ScalarValue::Int8(Some(i8::try_from(*value).ok()?)))
        }
        (DataType::Int16, TypedScalar::Signed(value)) => {
            Some(ScalarValue::Int16(Some(i16::try_from(*value).ok()?)))
        }
        (DataType::Int32, TypedScalar::Signed(value)) => {
            Some(ScalarValue::Int32(Some(i32::try_from(*value).ok()?)))
        }
        (DataType::Int64, TypedScalar::Signed(value)) => Some(ScalarValue::Int64(Some(*value))),
        (DataType::UInt8, TypedScalar::Unsigned(value)) => {
            Some(ScalarValue::UInt8(Some(u8::try_from(*value).ok()?)))
        }
        (DataType::UInt16, TypedScalar::Unsigned(value)) => {
            Some(ScalarValue::UInt16(Some(u16::try_from(*value).ok()?)))
        }
        (DataType::UInt32, TypedScalar::Unsigned(value)) => {
            Some(ScalarValue::UInt32(Some(u32::try_from(*value).ok()?)))
        }
        (DataType::UInt64, TypedScalar::Unsigned(value)) => Some(ScalarValue::UInt64(Some(*value))),
        (DataType::Float16, TypedScalar::Float16Bits(bits)) => {
            let value = f16::from_bits(*bits);
            value
                .is_finite()
                .then_some(ScalarValue::Float16(Some(value)))
        }
        (DataType::Float32, TypedScalar::Float32Bits(bits)) => {
            let value = f32::from_bits(*bits);
            value
                .is_finite()
                .then_some(ScalarValue::Float32(Some(value)))
        }
        (DataType::Float64, TypedScalar::Float64Bits(bits)) => {
            let value = f64::from_bits(*bits);
            value
                .is_finite()
                .then_some(ScalarValue::Float64(Some(value)))
        }
        (DataType::Decimal32(precision, scale), TypedScalar::Decimal32(value)) => {
            Some(ScalarValue::Decimal32(Some(*value), *precision, *scale))
        }
        (DataType::Decimal64(precision, scale), TypedScalar::Decimal64(value)) => {
            Some(ScalarValue::Decimal64(Some(*value), *precision, *scale))
        }
        (DataType::Decimal128(precision, scale), TypedScalar::Decimal128(value)) => {
            Some(ScalarValue::Decimal128(Some(*value), *precision, *scale))
        }
        (DataType::Decimal256(precision, scale), TypedScalar::Decimal256(value)) => {
            Some(ScalarValue::Decimal256(
                Some(datafusion::arrow::datatypes::i256::from_be_bytes(*value)),
                *precision,
                *scale,
            ))
        }
        (DataType::Date32, TypedScalar::Signed(value)) => {
            Some(ScalarValue::Date32(Some(i32::try_from(*value).ok()?)))
        }
        (DataType::Date64, TypedScalar::Signed(value)) => Some(ScalarValue::Date64(Some(*value))),
        (DataType::Time32(TimeUnit::Second), TypedScalar::Signed(value)) => {
            Some(ScalarValue::Time32Second(Some(i32::try_from(*value).ok()?)))
        }
        (DataType::Time32(TimeUnit::Millisecond), TypedScalar::Signed(value)) => Some(
            ScalarValue::Time32Millisecond(Some(i32::try_from(*value).ok()?)),
        ),
        (DataType::Time64(TimeUnit::Microsecond), TypedScalar::Signed(value)) => {
            Some(ScalarValue::Time64Microsecond(Some(*value)))
        }
        (DataType::Time64(TimeUnit::Nanosecond), TypedScalar::Signed(value)) => {
            Some(ScalarValue::Time64Nanosecond(Some(*value)))
        }
        (DataType::Timestamp(unit, timezone), TypedScalar::Signed(value)) => {
            let timezone = timezone.clone();
            Some(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(*value), timezone),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(*value), timezone),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(*value), timezone),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(*value), timezone),
            })
        }
        (DataType::Duration(TimeUnit::Second), TypedScalar::Signed(value)) => {
            Some(ScalarValue::DurationSecond(Some(*value)))
        }
        (DataType::Duration(TimeUnit::Millisecond), TypedScalar::Signed(value)) => {
            Some(ScalarValue::DurationMillisecond(Some(*value)))
        }
        (DataType::Duration(TimeUnit::Microsecond), TypedScalar::Signed(value)) => {
            Some(ScalarValue::DurationMicrosecond(Some(*value)))
        }
        (DataType::Duration(TimeUnit::Nanosecond), TypedScalar::Signed(value)) => {
            Some(ScalarValue::DurationNanosecond(Some(*value)))
        }
        (DataType::Utf8, TypedScalar::Utf8(value)) => {
            Some(ScalarValue::Utf8(Some(value.to_string())))
        }
        (DataType::LargeUtf8, TypedScalar::Utf8(value)) => {
            Some(ScalarValue::LargeUtf8(Some(value.to_string())))
        }
        (DataType::Utf8View, TypedScalar::Utf8(value)) => {
            Some(ScalarValue::Utf8View(Some(value.to_string())))
        }
        (DataType::Binary, TypedScalar::Binary(value)) => {
            Some(ScalarValue::Binary(Some(value.to_vec())))
        }
        (DataType::LargeBinary, TypedScalar::Binary(value)) => {
            Some(ScalarValue::LargeBinary(Some(value.to_vec())))
        }
        (DataType::BinaryView, TypedScalar::Binary(value)) => {
            Some(ScalarValue::BinaryView(Some(value.to_vec())))
        }
        (DataType::FixedSizeBinary(width), TypedScalar::Binary(value))
            if usize::try_from(*width).ok() == Some(value.len()) =>
        {
            Some(ScalarValue::FixedSizeBinary(*width, Some(value.to_vec())))
        }
        _ => None,
    }
}

fn datafusion_error(error: impl std::fmt::Display) -> CdfError {
    CdfError::contract(format!("DataFusion statistics pruning failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::{Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use cdf_contract::{CompiledExpressionPlan, Expression, ExpressionNode, ExpressionUse};
    use cdf_kernel::{
        BatchStats, IncompleteStatisticsReason, SegmentId, StatisticsArrowType,
        StatisticsCompleteness, TypedScalar,
    };
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_package::{
        PackageBuilder, PackageBuilderResources, PackageReader, StatisticsProfileGrain,
        StatisticsProfileRow, VerifiedPackage,
    };

    use super::{
        StatisticsPruningEvidence, StatisticsPruningLimits, StatisticsPruningOutcome,
        VerifiedPackageSegmentPruning, evaluate_statistics_rows,
        for_each_verified_package_segment_pruning,
    };

    const SCHEMA_HASH: &str = "sha256:statistics-pruning-fixture";

    struct TestBounds {
        row_count: u64,
        null_count: u64,
        minimum: Option<TypedScalar>,
        maximum: Option<TypedScalar>,
        completeness: StatisticsCompleteness,
    }

    fn complete(
        row_count: u64,
        null_count: u64,
        minimum: Option<TypedScalar>,
        maximum: Option<TypedScalar>,
    ) -> TestBounds {
        TestBounds {
            row_count,
            null_count,
            minimum,
            maximum,
            completeness: StatisticsCompleteness::Complete,
        }
    }

    fn incomplete(
        row_count: u64,
        null_count: u64,
        reason: IncompleteStatisticsReason,
    ) -> TestBounds {
        TestBounds {
            row_count,
            null_count,
            minimum: None,
            maximum: None,
            completeness: StatisticsCompleteness::Incomplete { reason },
        }
    }

    fn row(
        container: u64,
        field_ordinal: u32,
        field: &Field,
        bounds: TestBounds,
    ) -> StatisticsProfileRow {
        StatisticsProfileRow {
            grain: StatisticsProfileGrain::Segment,
            container_ordinal: container,
            container_id: format!("segment-{container}"),
            schema_hash: SCHEMA_HASH.to_owned(),
            field_ordinal,
            field_path: vec![field.name().as_str().into()].into_boxed_slice(),
            data_type: StatisticsArrowType::from_arrow_data_type(field.data_type()).unwrap(),
            row_count: bounds.row_count,
            null_count: bounds.null_count,
            completeness: bounds.completeness,
            minimum: bounds.minimum,
            maximum: bounds.maximum,
        }
    }

    fn compiled(expression: Expression, schema: &Schema) -> CompiledExpressionPlan {
        let planned =
            crate::expression::plan_expression(expression, ExpressionUse::Filter, schema).unwrap();
        CompiledExpressionPlan::current(vec![planned], Vec::new(), Vec::new(), Vec::new()).unwrap()
    }

    fn planned(source: &str, schema: &Schema) -> CompiledExpressionPlan {
        compiled(Expression::parse_comparison(source).unwrap(), schema)
    }

    fn package_with_segment_statistics(
        root: &std::path::Path,
        with_profile: bool,
    ) -> (PackageReader, VerifiedPackage, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batches = [0..5, 5..10]
            .into_iter()
            .map(|values| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from_iter_values(values))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let builder = PackageBuilder::create(
            root,
            "pkg-pruning-consumer",
            PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        builder.write_runtime_arrow_schema(schema.as_ref()).unwrap();
        let stats = batches
            .iter()
            .map(|batch| BatchStats::compute(batch).unwrap())
            .collect::<Vec<_>>();
        if with_profile {
            let mut aggregate = stats[0].clone();
            aggregate.merge(&stats[1]).unwrap();
            let mut profile = builder.begin_statistics_profile().unwrap();
            for (ordinal, stats) in stats.iter().enumerate() {
                profile
                    .write_stats(
                        StatisticsProfileGrain::Segment,
                        u64::try_from(ordinal).unwrap(),
                        &format!("segment-{ordinal}"),
                        SCHEMA_HASH,
                        stats,
                    )
                    .unwrap();
            }
            profile
                .write_stats(
                    StatisticsProfileGrain::Package,
                    0,
                    "pkg-pruning-consumer",
                    SCHEMA_HASH,
                    &aggregate,
                )
                .unwrap();
            profile.finish().unwrap();
        }
        for (ordinal, batch) in batches.into_iter().enumerate() {
            let start = u64::try_from(ordinal * 5).unwrap();
            let canonical =
                cdf_package_contract::append_package_row_ord(vec![batch], start).unwrap();
            builder
                .write_segment(
                    SegmentId::new(format!("segment-{ordinal}")).unwrap(),
                    start,
                    &canonical,
                )
                .unwrap();
        }
        let (_, verified) = builder.finish_verified().unwrap();
        (
            PackageReader::open(root).unwrap(),
            verified,
            Arc::clone(&schema),
        )
    }

    #[test]
    fn typed_bounds_prune_only_impossible_containers() {
        let field = Field::new("id", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let rows = vec![
            row(
                0,
                0,
                &field,
                complete(
                    100,
                    0,
                    Some(TypedScalar::Signed(0)),
                    Some(TypedScalar::Signed(4)),
                ),
            ),
            row(
                1,
                0,
                &field,
                complete(
                    100,
                    0,
                    Some(TypedScalar::Signed(5)),
                    Some(TypedScalar::Signed(20)),
                ),
            ),
        ];
        let report = evaluate_statistics_rows(
            &planned("id >= 5", schema.as_ref()),
            0,
            schema,
            SCHEMA_HASH,
            &rows,
        )
        .unwrap();

        assert_eq!(report.container_count, 2);
        assert_eq!(report.pruned_count, 1);
        assert_eq!(
            report.decisions[0].outcome,
            StatisticsPruningOutcome::Pruned
        );
        assert_eq!(
            report.decisions[1].outcome,
            StatisticsPruningOutcome::RetainedMayMatch
        );
    }

    #[test]
    fn integer_pruning_never_skips_a_matching_value() {
        let field = Field::new("id", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let mut state = 0x7f4a_7c15_d0e1_9b31_u64;
        for container in 0..2_000_u64 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let first = i32::try_from(state % 2_001).unwrap() - 1_000;
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let second = i32::try_from(state % 2_001).unwrap() - 1_000;
            let minimum = first.min(second);
            let maximum = first.max(second);
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let threshold = i32::try_from(state % 2_001).unwrap() - 1_000;
            let rows = [row(
                container,
                0,
                &field,
                complete(
                    u64::try_from(i64::from(maximum) - i64::from(minimum) + 1).unwrap(),
                    0,
                    Some(TypedScalar::Signed(i64::from(minimum))),
                    Some(TypedScalar::Signed(i64::from(maximum))),
                ),
            )];
            let report = evaluate_statistics_rows(
                &planned(&format!("id >= {threshold}"), schema.as_ref()),
                0,
                Arc::clone(&schema),
                SCHEMA_HASH,
                &rows,
            )
            .unwrap();
            if report.decisions[0].outcome == StatisticsPruningOutcome::Pruned {
                assert!(
                    (minimum..=maximum).all(|value| value < threshold),
                    "pruned [{minimum}, {maximum}] for id >= {threshold}"
                );
            }
        }
    }

    #[test]
    fn incomplete_and_all_null_evidence_are_conservative_and_null_sound() {
        let field = Field::new("score", DataType::Float64, true);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let rows = vec![
            row(
                0,
                0,
                &field,
                incomplete(10, 0, IncompleteStatisticsReason::NanObserved),
            ),
            row(1, 0, &field, complete(10, 10, None, None)),
            row(
                2,
                0,
                &field,
                complete(
                    10,
                    0,
                    Some(TypedScalar::Float64Bits(f64::NAN.to_bits())),
                    Some(TypedScalar::Float64Bits(f64::NAN.to_bits())),
                ),
            ),
        ];
        let report = evaluate_statistics_rows(
            &planned("score > 5", schema.as_ref()),
            0,
            Arc::clone(&schema),
            SCHEMA_HASH,
            &rows,
        )
        .unwrap();

        assert_eq!(
            report.decisions[0].outcome,
            StatisticsPruningOutcome::RetainedConservatively
        );
        assert_eq!(
            report.decisions[0]
                .conservative_fields
                .iter()
                .map(Box::as_ref)
                .collect::<Vec<_>>(),
            ["score"]
        );
        assert_eq!(
            report.decisions[1].outcome,
            StatisticsPruningOutcome::Pruned
        );
        assert_eq!(
            report.decisions[2].outcome,
            StatisticsPruningOutcome::RetainedConservatively
        );

        let is_null = compiled(
            Expression::call(
                "is_null",
                vec![ExpressionNode::Column {
                    name: "score".to_owned(),
                }],
            ),
            schema.as_ref(),
        );
        let null_report =
            evaluate_statistics_rows(&is_null, 0, schema, SCHEMA_HASH, &rows).unwrap();
        assert_eq!(
            null_report.decisions[0].outcome,
            StatisticsPruningOutcome::RetainedConservatively
        );
        assert_eq!(
            null_report.decisions[1].outcome,
            StatisticsPruningOutcome::RetainedMayMatch
        );
        assert_eq!(
            null_report.decisions[2].outcome,
            StatisticsPruningOutcome::RetainedConservatively
        );
    }

    #[test]
    fn unsupported_decimal_and_timezone_predicates_retain_conservatively() {
        let amount = Field::new("amount", DataType::Decimal128(20, 2), true);
        let occurred = Field::new(
            "occurred",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        let schema = Arc::new(Schema::new(vec![amount.clone(), occurred.clone()]));
        let rows = vec![
            row(
                0,
                0,
                &amount,
                complete(
                    5,
                    0,
                    Some(TypedScalar::Decimal128(100)),
                    Some(TypedScalar::Decimal128(499)),
                ),
            ),
            row(
                0,
                1,
                &occurred,
                complete(
                    5,
                    0,
                    Some(TypedScalar::Signed(1_000_000)),
                    Some(TypedScalar::Signed(2_000_000)),
                ),
            ),
        ];
        let amount = crate::expression::record_exact_source_expression(
            Expression::parse_comparison("amount >= 5").unwrap(),
        )
        .unwrap();
        let amount =
            CompiledExpressionPlan::current(vec![amount], Vec::new(), Vec::new(), Vec::new())
                .unwrap();
        let amount_report =
            evaluate_statistics_rows(&amount, 0, Arc::clone(&schema), SCHEMA_HASH, &rows).unwrap();
        assert_eq!(
            amount_report.decisions[0].outcome,
            StatisticsPruningOutcome::RetainedConservatively
        );

        let timestamp = crate::expression::record_exact_source_expression(
            Expression::parse_comparison("occurred > '1970-01-01T00:00:02Z'").unwrap(),
        )
        .unwrap();
        let timestamp =
            CompiledExpressionPlan::current(vec![timestamp], Vec::new(), Vec::new(), Vec::new())
                .unwrap();
        let timestamp_report =
            evaluate_statistics_rows(&timestamp, 0, schema, SCHEMA_HASH, &rows).unwrap();
        assert_eq!(
            timestamp_report.decisions[0].outcome,
            StatisticsPruningOutcome::RetainedConservatively
        );
    }

    #[test]
    fn stale_or_structurally_incomplete_profile_rows_fail_before_pruning() {
        let field = Field::new("id", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let mut tampered_plan = planned("id = 1", schema.as_ref());
        tampered_plan.content_sha256 = "sha256:tampered".to_owned();
        assert!(
            evaluate_statistics_rows(
                &tampered_plan,
                0,
                Arc::clone(&schema),
                SCHEMA_HASH,
                &[row(
                    0,
                    0,
                    &field,
                    complete(
                        1,
                        0,
                        Some(TypedScalar::Signed(1)),
                        Some(TypedScalar::Signed(1)),
                    ),
                )],
            )
            .unwrap_err()
            .to_string()
            .contains("content digest")
        );

        let mut stale = row(
            0,
            0,
            &field,
            complete(
                1,
                0,
                Some(TypedScalar::Signed(1)),
                Some(TypedScalar::Signed(1)),
            ),
        );
        stale.schema_hash = "sha256:stale".to_owned();
        assert!(
            evaluate_statistics_rows(
                &planned("id = 1", schema.as_ref()),
                0,
                Arc::clone(&schema),
                SCHEMA_HASH,
                &[stale],
            )
            .unwrap_err()
            .to_string()
            .contains("expected schema hash")
        );

        let two_fields = Arc::new(Schema::new(vec![
            field.clone(),
            Field::new("other", DataType::Int64, true),
        ]));
        assert!(
            evaluate_statistics_rows(
                &planned("id = 1", two_fields.as_ref()),
                0,
                two_fields,
                SCHEMA_HASH,
                &[row(
                    0,
                    0,
                    &field,
                    complete(
                        1,
                        0,
                        Some(TypedScalar::Signed(1)),
                        Some(TypedScalar::Signed(1)),
                    ),
                )],
            )
            .unwrap_err()
            .to_string()
            .contains("has 1 fields, expected 2")
        );
    }

    #[test]
    fn verified_package_consumer_skips_payloads_and_matches_unpruned_rows() {
        let temp = tempfile::tempdir().unwrap();
        let (reader, verified, schema) = package_with_segment_statistics(temp.path(), true);
        let compiled = planned("id >= 5", schema.as_ref());
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(8 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let mut selected = Vec::new();
        let mut skipped = Vec::new();
        let summary = for_each_verified_package_segment_pruning(
            &reader,
            &verified,
            &compiled,
            0,
            Arc::clone(&memory),
            StatisticsPruningLimits::new(1, 1024 * 1024).unwrap(),
            &mut |item| {
                match item {
                    VerifiedPackageSegmentPruning::Pruned { decision } => {
                        skipped.push(decision.container_id);
                    }
                    VerifiedPackageSegmentPruning::Retained { segment, .. } => {
                        let segment = segment.read(Arc::clone(&memory), 1024 * 1024)?;
                        for batch in segment.batches {
                            let ids = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            selected.extend(ids.iter().flatten().filter(|value| *value >= 5));
                        }
                    }
                }
                Ok(())
            },
        )
        .unwrap();

        let mut unpruned = Vec::new();
        for segment in reader
            .verified_canonical_segment_object_stream_with(&verified)
            .unwrap()
        {
            let segment = segment
                .unwrap()
                .read(Arc::clone(&memory), 1024 * 1024)
                .unwrap();
            for batch in segment.batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                unpruned.extend(ids.iter().flatten().filter(|value| *value >= 5));
            }
        }

        assert_eq!(summary.evidence, StatisticsPruningEvidence::VerifiedProfile);
        assert_eq!(summary.evidence_generation, verified.package_hash());
        assert_eq!(summary.segment_count, 2);
        assert_eq!(summary.pruned_count, 1);
        assert_eq!(summary.retained_count, 1);
        assert_eq!(summary.conservative_count, 0);
        assert!(summary.pruned_bytes > 0);
        assert!(summary.retained_bytes > 0);
        assert_eq!(skipped, ["segment-0"]);
        assert_eq!(selected, unpruned);
        assert_eq!(selected, [5, 6, 7, 8, 9]);
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn missing_profile_streams_every_segment_as_conservative_retention() {
        let temp = tempfile::tempdir().unwrap();
        let (reader, verified, schema) = package_with_segment_statistics(temp.path(), false);
        let compiled = planned("id >= 5", schema.as_ref());
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(2 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let mut retained = Vec::new();
        let summary = for_each_verified_package_segment_pruning(
            &reader,
            &verified,
            &compiled,
            0,
            memory,
            StatisticsPruningLimits::new(4, 1024 * 1024).unwrap(),
            &mut |item| {
                let VerifiedPackageSegmentPruning::Retained { decision, segment } = item else {
                    panic!("missing profile must never authorize a skip");
                };
                assert_eq!(
                    decision.outcome,
                    StatisticsPruningOutcome::RetainedConservatively
                );
                retained.push(segment.entry.segment_id.to_string());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(summary.evidence, StatisticsPruningEvidence::MissingProfile);
        assert_eq!(summary.pruned_count, 0);
        assert_eq!(summary.retained_count, 2);
        assert_eq!(summary.conservative_count, 2);
        assert_eq!(retained, ["segment-0", "segment-1"]);
    }
}
