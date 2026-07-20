use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_schema::Schema;
use cdf_contract::{
    CanonicalArrowField, CompiledExpressionPlan, ContractPolicy, FieldCoercionDecision,
    IdentifierPolicy, ResidualProgram, RowDispositionRule, SchemaChangeKind, SchemaCoercionPlan,
    SchemaVerdictRule, TypePolicy, ValidationProgram, VerdictAction, plan_schema_reconciliation,
    reconcile_schema,
};
use cdf_kernel::{
    CdfError, DeliveryGuarantee, DiscoveryExecutorBudgetEvidence, EffectiveSchemaCatalogEntry,
    EffectiveSchemaEvidence, EstimateSupport, ExecutionExtent, PartitionId,
    ProcessedObservationPosition, PushdownFidelity, ResourceId, ResourceStream, Result,
    RunPhaseMetric, ScanPlan, ScanPredicate, ScanRequest, SchemaHash,
    SchemaObservationFieldQuarantine, SchemaObservationPolicy, SegmentId, SourcePosition,
    TerminalSchemaObservationQuarantine, WriteDisposition, source_name,
};
use cdf_package::VerifiedPackage;
use cdf_package_contract::{PackageManifest, SegmentEntry};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePlanInput {
    pub request: ScanRequest,
    pub validation_program: ValidationProgram,
    pub execution_extent: ExecutionExtent,
    pub package_id: String,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePlan {
    pub scan: ScanPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_source_execution: Option<cdf_runtime::CompiledSourceExecutionPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_schedule: Option<cdf_runtime::CanonicalPartitionSchedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_graph: Option<cdf_runtime::CompiledOperatorGraph>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_stream_policy: Option<cdf_runtime::CompiledStreamPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_schema_evidence: Option<EffectiveSchemaPlanEvidence>,
    pub final_projection: Option<Vec<String>>,
    pub residual_predicates: Vec<ScanPredicate>,
    /// Parsed, resolved, optimized, and frozen expressions consumed by execution and replay.
    pub compiled_expression_plan: CompiledExpressionPlan,
    /// The sole schema-admission program consumed by extraction and replay.
    pub compiled_schema_admission: CompiledSchemaAdmissionPlan,
    pub execution_extent: ExecutionExtent,
    pub write_disposition: WriteDisposition,
    pub validation_program: ValidationProgram,
    pub schema_authority: EngineSchemaAuthority,
    pub output_schema: CompiledArrowSchema,
    pub operator_chain: Vec<OperatorNode>,
    pub explain: ExplainData,
    pub package_id: String,
}

impl EnginePlan {
    pub fn validate_execution_extent_for_execution(&self) -> Result<()> {
        if self.execution_extent != self.explain.execution_extent {
            return Err(CdfError::contract(
                "plan execution extent does not match its recorded explain extent",
            ));
        }
        self.execution_extent.validate_for_execution()
    }

    pub fn validate_compiled_expression_plan(&self) -> Result<()> {
        let compiled = &self.compiled_expression_plan;
        compiled.validate_program_binding(&self.validation_program)?;
        compiled.validate_predicate_bindings(self.scan.request.filters.iter().map(
            |predicate| {
                (
                    predicate.expression.as_str(),
                    &predicate.canonical_expression,
                    self.scan.pushed_predicates.iter().any(|pushed| {
                        pushed.predicate.predicate_id == predicate.predicate_id
                            && pushed.fidelity == PushdownFidelity::Exact
                    }),
                )
            },
        ))?;
        compiled.validate_residual_bindings(self.residual_predicates.iter().map(|predicate| {
            (
                predicate.expression.as_str(),
                &predicate.canonical_expression,
            )
        }))?;
        Ok(())
    }

    pub fn validate_partition_schedule(&self) -> Result<()> {
        if self.partition_schedule != self.explain.partition_schedule {
            return Err(CdfError::data(
                "engine partition schedule does not match its recorded explain schedule",
            ));
        }
        match (&self.partition_schedule, &self.compiled_source_execution) {
            (Some(schedule), Some(source)) => {
                let compiler_source = self.compiled_schema_admission.source.as_ref().ok_or_else(|| {
                    CdfError::data(
                        "partition schedule requires the compiler-owned schema-admission source binding",
                    )
                })?;
                source.validate_compiler_binding(compiler_source)?;
                schedule.validate_against_scan(&self.scan, source)?;
            }
            (None, None) => {
                return Err(CdfError::data(
                    "executable engine plan requires compiled source and partition-schedule authority",
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(CdfError::data(
                    "partition schedule and compiled source execution plan must be present together",
                ));
            }
        }
        Ok(())
    }

    pub fn rebind_validation_program(
        &mut self,
        program: ValidationProgram,
        expression_schema: &Schema,
    ) -> Result<()> {
        crate::planning::rebind_validation_program(self, program, expression_schema)
    }

    pub fn validate_compiled_schema_admission<R>(&self, resource: &R) -> Result<()>
    where
        R: ResourceStream + ?Sized,
    {
        self.compiled_schema_admission.validate(
            &self.schema_authority,
            &self.validation_program,
            resource,
        )
    }

    /// Validates the serialized source ceiling against the independently resolved resource.
    pub fn validate_compiled_source_resource<R>(&self, resource: &R) -> Result<()>
    where
        R: ResourceStream + ?Sized,
    {
        if self.compiled_stream_policy != self.explain.compiled_stream_policy {
            return Err(CdfError::data(
                "compiled stream policy does not match its recorded explain evidence",
            ));
        }
        let source = self.compiled_source_execution.as_ref().ok_or_else(|| {
            CdfError::data("executable engine plan requires compiled source authority")
        })?;
        match (&self.execution_extent, self.compiled_stream_policy.as_ref()) {
            (ExecutionExtent::Bounded { .. }, None) => {}
            (ExecutionExtent::Drain { .. }, Some(policy))
                if policy.compiled_source_plan_hash == source.compiled_source_plan_hash()
                    && policy.execution_extent == self.execution_extent =>
            {
                policy.validate_against_execution_plan(source)?;
            }
            (ExecutionExtent::Drain { .. }, _) => {
                return Err(CdfError::data(
                    "executable drain plan requires one source-bound compiled stream policy",
                ));
            }
            _ => {
                return Err(CdfError::data(
                    "bounded plan cannot carry unbounded stream-policy evidence",
                ));
            }
        }
        if let Some(graph) = &self.operator_graph {
            graph
                .validate_plan_join(&self.execution_extent, self.compiled_stream_policy.as_ref())?;
        } else if matches!(self.execution_extent, ExecutionExtent::Drain { .. }) {
            return Err(CdfError::data(
                "executable drain plan requires a compiled operator graph",
            ));
        }
        match (
            self.compiled_source_execution.as_ref(),
            resource.compiled_source_plan_hash(),
        ) {
            (Some(compiled), Some(resolved))
                if compiled.compiled_source_plan_hash() == resolved =>
            {
                Ok(())
            }
            (None, None) => Err(CdfError::data(
                "executable engine plan and resolved resource require compiler source bindings",
            )),
            (Some(_), Some(_)) => Err(CdfError::data(
                "resolved source does not match the compiler source artifact recorded by the engine plan",
            )),
            (Some(_), None) => Err(CdfError::data(
                "compiled engine source has no independently resolved source binding",
            )),
            (None, Some(_)) => Err(CdfError::data(
                "resolved source has a compiler binding but the engine plan omitted it",
            )),
        }?;
        let execution = source.execution_capabilities();
        if !execution.bounded && !execution.pausable {
            resource
                .replay_retention()
                .ok_or_else(|| {
                    CdfError::data(
                        "unbounded non-pausable source execution requires a byte/time/unit-bounded replay-retention authority; configure replay-retention byte, age, and unit-count knobs or use a pausable source",
                    )
                })?
                .status()?
                .validate()?;
        }
        Ok(())
    }

    /// Binds every engine-owned source authority from one validated compiler output.
    pub fn bind_compiled_source(
        mut self,
        source: &cdf_runtime::CompiledSourcePlan,
    ) -> Result<Self> {
        let compiled_policy =
            cdf_runtime::CompiledStreamPolicy::compile(&self.execution_extent, source)?;
        let stream_policy = (!self.execution_extent.is_bounded()).then_some(compiled_policy);
        self.compiled_schema_admission
            .bind_source(source, &self.scan.request.resource_id)?;
        let compiled_source_execution = cdf_runtime::CompiledSourceExecutionPlan::compile(source)?;
        let schedule = cdf_runtime::CanonicalPartitionSchedule::compile(
            &compiled_source_execution,
            &self.scan,
        )?;
        self.explain.partition_schedule = Some(schedule.clone());
        self.explain.compiled_stream_policy = stream_policy.clone();
        self.partition_schedule = Some(schedule);
        self.compiled_source_execution = Some(compiled_source_execution);
        self.compiled_stream_policy = stream_policy;
        Ok(self)
    }

    pub fn bind_operator_graph(
        mut self,
        source: &cdf_runtime::CompiledSourcePlan,
        destination: &cdf_runtime::DestinationRuntimeCapabilities,
    ) -> Result<Self> {
        let bound_source = self.compiled_source_execution.as_ref().ok_or_else(|| {
            CdfError::contract("bind the compiled source before compiling the operator graph")
        })?;
        if bound_source.compiled_source_plan_hash() != cdf_runtime::artifact_hash(source)? {
            return Err(CdfError::contract(
                "operator graph source differs from the source already bound to the engine plan",
            ));
        }
        if let Some(policy) = &self.compiled_stream_policy {
            policy.validate_against_source(source)?;
        }
        let graph = crate::compile_operator_graph(&self, source, destination)?;
        self.operator_graph = Some(graph);
        Ok(self)
    }

    /// Selects a deterministic subset of already-planned partitions and atomically rebinds every
    /// scheduler-owned view of that scan.
    ///
    /// Incremental orchestration may choose which immutable planned partitions remain, but it may
    /// not edit schedules directly or leave explain evidence stale.
    pub fn select_partitions(mut self, selected: &BTreeSet<PartitionId>) -> Result<Self> {
        let planned = self
            .scan
            .partitions
            .iter()
            .map(|partition| partition.partition_id.clone())
            .collect::<BTreeSet<_>>();
        if !selected.is_subset(&planned) {
            return Err(CdfError::contract(
                "partition selection contains an id absent from the compiled scan",
            ));
        }
        self.scan
            .partitions
            .retain(|partition| selected.contains(&partition.partition_id));
        self.explain.partitions.retain(|partition| {
            self.scan
                .partitions
                .iter()
                .any(|planned| planned.partition_id.as_str() == partition.partition_id)
        });
        if let Some(source) = &self.compiled_source_execution {
            let schedule = cdf_runtime::CanonicalPartitionSchedule::compile(source, &self.scan)?;
            self.explain.partition_schedule = Some(schedule.clone());
            self.partition_schedule = Some(schedule);
        }
        Ok(self)
    }

    /// Advances an already-selected drain plan past one committed partition prefix.
    ///
    /// This is deliberately narrower than [`Self::select_partitions`]: epoch settlement may only
    /// move forward through the exact canonical prefix returned by execution. Keeping one owned
    /// plan and draining that prefix avoids cloning and re-filtering the entire remaining scan for
    /// every epoch.
    pub fn advance_committed_partition_prefix(&mut self, consumed: usize) -> Result<()> {
        if consumed == 0 || consumed > self.scan.partitions.len() {
            return Err(CdfError::contract(
                "committed drain prefix must consume at least one and no more than the remaining planned partitions",
            ));
        }
        if self.explain.partitions.len() != self.scan.partitions.len()
            || !self
                .scan
                .partitions
                .iter()
                .zip(&self.explain.partitions)
                .all(|(planned, explained)| planned.partition_id.as_str() == explained.partition_id)
        {
            return Err(CdfError::data(
                "engine scan and explain partition authorities diverged before drain advancement",
            ));
        }
        self.scan.partitions.drain(..consumed);
        self.explain.partitions.drain(..consumed);
        if let Some(source) = &self.compiled_source_execution {
            let schedule = cdf_runtime::CanonicalPartitionSchedule::compile(source, &self.scan)?;
            self.explain.partition_schedule = Some(schedule.clone());
            self.partition_schedule = Some(schedule);
        }
        Ok(())
    }

    /// Advances one settled drain epoch, retaining a partially consumed head partition when the
    /// canonical closure occurred inside that partition.
    pub fn advance_committed_drain_frontier(
        &mut self,
        consumed: usize,
        resume: Option<&DrainPartitionResume>,
    ) -> Result<()> {
        if consumed > self.scan.partitions.len() {
            return Err(CdfError::contract(
                "committed drain prefix exceeds the remaining planned partitions",
            ));
        }
        if consumed > 0 {
            self.advance_committed_partition_prefix(consumed)?;
        }
        if let Some(resume) = resume {
            let partition = self.scan.partitions.first_mut().ok_or_else(|| {
                CdfError::data("drain continuation references an absent remaining partition")
            })?;
            if partition.partition_id != resume.partition_id {
                return Err(CdfError::data(
                    "drain continuation does not match the canonical remaining partition",
                ));
            }
            resume.start_position.validate()?;
            partition.start_position = Some(resume.start_position.clone());
            if let Some(source) = &self.compiled_source_execution {
                let schedule =
                    cdf_runtime::CanonicalPartitionSchedule::compile(source, &self.scan)?;
                self.explain.partition_schedule = Some(schedule.clone());
                self.partition_schedule = Some(schedule);
            }
        } else if consumed == 0 {
            return Err(CdfError::contract(
                "drain epoch must consume a partition prefix or provide one continuation",
            ));
        }
        Ok(())
    }

    /// Rebinds this invocation to the checkpoint committed before source contact. Source-local
    /// partition/task semantics remain behind `ResourceStream`; the engine owns recompiling the
    /// canonical schedule and explain join after that mutation.
    pub fn rebind_initial_committed_frontier(
        &mut self,
        resource: &dyn ResourceStream,
        frontier: &SourcePosition,
    ) -> Result<()> {
        resource.rebind_scan_for_resume(&mut self.scan, frontier)?;
        if self.explain.partitions.len() == self.scan.partitions.len() {
            for (explained, planned) in self
                .explain
                .partitions
                .iter_mut()
                .zip(&self.scan.partitions)
            {
                if explained.partition_id != planned.partition_id.as_str() {
                    return Err(CdfError::data(
                        "engine scan and explain partition authorities diverged during resume binding",
                    ));
                }
            }
        } else if self.scan.planned_task_set.is_none() {
            return Err(CdfError::data(
                "engine scan and explain partition counts diverged during resume binding",
            ));
        }
        if let Some(source) = &self.compiled_source_execution {
            let schedule = cdf_runtime::CanonicalPartitionSchedule::compile(source, &self.scan)?;
            self.explain.partition_schedule = Some(schedule.clone());
            self.partition_schedule = Some(schedule);
        }
        Ok(())
    }

    /// Rebinds the physical package sink for one finite drain epoch while
    /// preserving every compiled source, expression, schema, and graph
    /// authority. Package identity is epoch-local; the logical scan plan is
    /// not recompiled or reinterpreted.
    pub fn rebind_package_id(mut self, package_id: impl Into<String>) -> Result<Self> {
        let package_id = package_id.into();
        if package_id.trim().is_empty() {
            return Err(CdfError::contract("epoch package id cannot be empty"));
        }
        rebind_package_sink(&mut self.operator_chain, &package_id)?;
        rebind_package_sink(&mut self.explain.operator_chain, &package_id)?;
        self.package_id = package_id;
        Ok(self)
    }

    pub fn effective_schema_evidence(&self) -> Option<&EffectiveSchemaPlanEvidence> {
        self.effective_schema_evidence.as_ref()
    }

    pub fn output_arrow_schema(&self) -> Result<Arc<Schema>> {
        self.output_schema.to_arrow()
    }

    pub fn schema_authority(&self) -> &EngineSchemaAuthority {
        &self.schema_authority
    }

    pub fn effective_schema_hash(&self) -> &SchemaHash {
        &self.schema_authority.effective_schema_hash
    }

    pub fn segmentation_policy(&self) -> Result<&crate::CanonicalSegmentationPolicy> {
        let mut policies = self
            .operator_chain
            .iter()
            .filter_map(|operator| match operator {
                OperatorNode::PackageSink { segmentation, .. } => Some(segmentation),
                _ => None,
            });
        let policy = policies
            .next()
            .ok_or_else(|| CdfError::data("engine plan has no package segmentation policy"))?;
        if policies.next().is_some() {
            return Err(CdfError::data(
                "engine plan has multiple package segmentation policies",
            ));
        }
        policy.validate()?;
        Ok(policy)
    }
}

fn rebind_package_sink(operators: &mut [OperatorNode], package_id: &str) -> Result<()> {
    let mut sinks = operators.iter_mut().filter_map(|operator| match operator {
        OperatorNode::PackageSink { package_id, .. } => Some(package_id),
        _ => None,
    });
    let sink = sinks
        .next()
        .ok_or_else(|| CdfError::data("engine plan has no package sink to bind to an epoch"))?;
    *sink = package_id.to_owned();
    if sinks.next().is_some() {
        return Err(CdfError::data(
            "engine plan has multiple package sinks and cannot bind one epoch identity",
        ));
    }
    Ok(())
}

pub const COMPILED_SCHEMA_ADMISSION_VERSION: u16 = 3;
pub const SCHEMA_ADMISSION_CACHE_KEY_FIELDS: [&str; 5] = [
    "source_generation",
    "source_driver_and_codec",
    "canonical_options",
    "normalizer_version",
    "contract_program",
];

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledSchemaAdmissionPlan {
    pub version: u16,
    pub baseline_schema_hash: SchemaHash,
    pub effective_schema_hash: SchemaHash,
    pub resource_schema_hash: SchemaHash,
    pub baseline_projection: Option<Vec<String>>,
    pub baseline_projected_schema_hashes: Vec<SchemaHash>,
    pub constraint_schema: CompiledArrowSchema,
    pub normalizer_version: String,
    pub identifier_policy: IdentifierPolicy,
    pub type_policy: TypePolicy,
    pub schema_verdicts: Vec<SchemaVerdictRule>,
    pub residual: Option<ResidualProgram>,
    pub row_dispositions: Vec<RowDispositionRule>,
    pub control_critical_fields: Vec<String>,
    pub cache_key_fields: Vec<String>,
    pub contract_program_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<cdf_runtime::CompiledSourceCompilerBinding>,
}

impl CompiledSchemaAdmissionPlan {
    pub(crate) fn compile(
        authority: &EngineSchemaAuthority,
        resource_schema: &Schema,
        constraint_schema: &Schema,
        validation_program: &ValidationProgram,
        type_policy: TypePolicy,
    ) -> Result<Self> {
        let mut control_critical_fields = validation_program
            .residual
            .iter()
            .flat_map(|residual| &residual.fields)
            .filter(|field| field.control_critical)
            .map(|field| field.source_name.clone())
            .collect::<Vec<_>>();
        control_critical_fields.sort();
        control_critical_fields.dedup();
        let plan = Self {
            version: COMPILED_SCHEMA_ADMISSION_VERSION,
            baseline_schema_hash: authority.baseline_schema_hash.clone(),
            effective_schema_hash: authority.effective_schema_hash.clone(),
            resource_schema_hash: cdf_kernel::canonical_arrow_schema_hash(resource_schema)?,
            baseline_projection: None,
            baseline_projected_schema_hashes: Vec::new(),
            constraint_schema: CompiledArrowSchema::from_arrow(constraint_schema)?,
            normalizer_version: validation_program.normalizer_version.clone(),
            identifier_policy: validation_program.identifier_policy.clone(),
            type_policy,
            schema_verdicts: validation_program.schema_verdicts.clone(),
            residual: validation_program.residual.clone(),
            row_dispositions: validation_program.row_dispositions.clone(),
            control_critical_fields,
            cache_key_fields: SCHEMA_ADMISSION_CACHE_KEY_FIELDS
                .into_iter()
                .map(str::to_owned)
                .collect(),
            contract_program_hash: cdf_runtime::artifact_hash(validation_program)?,
            source: None,
        };
        plan.validate_intrinsic(validation_program)?;
        Ok(plan)
    }

    pub(crate) fn bind_baseline_schema_catalog(
        &mut self,
        catalog: &[EffectiveSchemaCatalogEntry],
        resource_schema: &Schema,
        projection: Option<&[String]>,
    ) -> Result<()> {
        let mut hashes = catalog
            .iter()
            .map(|entry| {
                let projected = project_baseline_observation(
                    entry.schema.as_ref(),
                    resource_schema,
                    projection,
                )?;
                cdf_kernel::canonical_arrow_schema_hash(&projected)
            })
            .collect::<Result<Vec<_>>>()?;
        hashes.sort();
        hashes.dedup();
        self.baseline_projection = projection.map(<[String]>::to_vec);
        self.baseline_projected_schema_hashes = hashes;
        Ok(())
    }

    pub fn bind_source(
        &mut self,
        source: &cdf_runtime::CompiledSourcePlan,
        resource_id: &ResourceId,
    ) -> Result<()> {
        source.validate()?;
        if &source.descriptor.resource_id != resource_id {
            return Err(CdfError::contract(format!(
                "compiled schema-admission source belongs to `{}` but engine plan belongs to `{resource_id}`",
                source.descriptor.resource_id
            )));
        }
        let source_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&source.schema)?;
        if source_schema_hash != self.resource_schema_hash {
            return Err(CdfError::data(format!(
                "compiled source schema {source_schema_hash} does not match schema-admission resource schema {}",
                self.resource_schema_hash
            )));
        }
        if source.type_policy_allowances.coerce_types != self.type_policy.coerce_types
            || source.type_policy_allowances.allow_lossy_mapping
                != self.type_policy.allow_lossy_mapping
        {
            return Err(CdfError::data(
                "compiled source type allowances do not match the schema-admission program",
            ));
        }
        self.source = Some(cdf_runtime::CompiledSourceCompilerBinding::compile(source)?);
        Ok(())
    }

    pub fn instantiate(
        &self,
        observed: &Schema,
        observed_schema_hash: &SchemaHash,
    ) -> Result<SchemaCoercionPlan> {
        let actual = cdf_kernel::canonical_arrow_schema_hash(observed)?;
        if &actual != observed_schema_hash {
            return Err(CdfError::data(format!(
                "physical schema hash {observed_schema_hash} does not match observed Arrow schema {actual}",
            )));
        }
        let constraint = self.constraint_schema.to_arrow()?;
        let plan = reconcile_schema(observed, constraint.as_ref(), &self.type_policy)?.plan;
        self.validate_materialized(observed, &plan)?;
        Ok(plan)
    }

    pub(crate) fn instantiate_or_quarantine(
        &self,
        observation_id: &str,
        observed: &Schema,
        observed_schema_hash: &SchemaHash,
    ) -> Result<CompiledSchemaAdmissionOutcome> {
        let actual = cdf_kernel::canonical_arrow_schema_hash(observed)?;
        if &actual != observed_schema_hash {
            return Err(CdfError::data(format!(
                "physical schema hash {observed_schema_hash} does not match observed Arrow schema {actual}",
            )));
        }
        let constraint = self.constraint_schema.to_arrow()?;
        let report = plan_schema_reconciliation(observed, constraint.as_ref(), &self.type_policy)?;
        if let Some(quarantine) = self.control_critical_missing_quarantine(
            observation_id,
            observed_schema_hash,
            observed,
            constraint.as_ref(),
            &report.plan,
        )? {
            return Ok(CompiledSchemaAdmissionOutcome::Quarantined(Box::new(
                quarantine,
            )));
        }
        if report.errors.is_empty() {
            self.validate_materialized(observed, &report.plan)?;
            return Ok(CompiledSchemaAdmissionOutcome::Admitted(report.plan));
        }
        let narrowing_verdict = self
            .schema_verdicts
            .iter()
            .find(|rule| rule.change == SchemaChangeKind::TypeNarrowing)
            .map(|rule| &rule.verdict);
        if narrowing_verdict != Some(&VerdictAction::Quarantine) {
            return report
                .into_result()
                .map(|result| CompiledSchemaAdmissionOutcome::Admitted(result.plan));
        }
        let mut fields = report
            .errors
            .iter()
            .map(|error| {
                let observed_field = observed
                    .fields()
                    .iter()
                    .find(|field| {
                        source_name(field.as_ref()).unwrap_or_else(|| field.name())
                            == error.source_name
                    })
                    .map(|field| CanonicalArrowField::from_arrow(field.as_ref()))
                    .transpose()?;
                let effective_field = constraint
                    .fields()
                    .iter()
                    .find(|field| {
                        source_name(field.as_ref()).unwrap_or_else(|| field.name())
                            == error.source_name
                    })
                    .map(|field| CanonicalArrowField::from_arrow(field.as_ref()))
                    .transpose()?;
                SchemaObservationFieldQuarantine::new_field_path(
                    vec![error.source_name.clone()],
                    observed_field,
                    effective_field,
                    error.message.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if fields.is_empty() {
            fields.push(SchemaObservationFieldQuarantine::whole_schema(
                "physical schema is incompatible with the fixed admission schema",
            )?);
        }
        let evolve = self.schema_verdicts.iter().any(|rule| {
            rule.change == SchemaChangeKind::TypeWidening && rule.verdict == VerdictAction::Admit
        });
        let (rule_id, policy, remediation) = if evolve {
            (
                "schema-observation:incompatible",
                SchemaObservationPolicy::Evolve,
                "publish a compatible source type, declare an allowed coercion, or repin the schema after review",
            )
        } else {
            (
                "schema-observation:freeze-deviation",
                SchemaObservationPolicy::Freeze,
                "restore the pinned schema for this input, explicitly repin after review, or change the resource contract to evolve",
            )
        };
        Ok(CompiledSchemaAdmissionOutcome::Quarantined(Box::new(
            TerminalSchemaObservationQuarantine::new(
                observation_id,
                observed_schema_hash.clone(),
                rule_id,
                "schema_observation_quarantined",
                policy,
                remediation,
                fields,
            )?,
        )))
    }

    /// Validates coercion evidence for a batch that a source codec already materialized.
    ///
    /// The typed physical observation is the authority for every relation decision. Display
    /// strings in the serialized plan remain diagnostics and are never trusted as type evidence.
    pub fn validate_materialized(
        &self,
        observed: &Schema,
        plan: &SchemaCoercionPlan,
    ) -> Result<()> {
        let constraint = self.constraint_schema.to_arrow()?;
        let observed_hash = cdf_kernel::canonical_arrow_schema_hash(observed)?;
        let formed_pinned_baseline = self
            .baseline_projected_schema_hashes
            .binary_search(&observed_hash)
            .is_ok();
        let report = plan_schema_reconciliation(observed, constraint.as_ref(), &self.type_policy)?;
        if !report.errors.is_empty() || report.plan != *plan {
            return Err(CdfError::data(
                "materialized schema-admission evidence does not match the typed physical observation and compiled constraint",
            ));
        }
        for field in &plan.fields {
            match field.decision {
                FieldCoercionDecision::Preserved | FieldCoercionDecision::Rebound => {}
                FieldCoercionDecision::Missing
                    if !self.control_critical_fields.contains(&field.source_name) => {}
                FieldCoercionDecision::Missing => {
                    return Err(CdfError::data(format!(
                        "control-critical field {:?} is missing from the physical observation",
                        field.source_name
                    )));
                }
                FieldCoercionDecision::Widened
                    if formed_pinned_baseline
                        || self.schema_verdict(SchemaChangeKind::TypeWidening)?
                            == &VerdictAction::Admit => {}
                FieldCoercionDecision::Extra
                    if matches!(
                        self.schema_verdict(SchemaChangeKind::UnknownField)?,
                        VerdictAction::Admit | VerdictAction::AdmitAsVariant
                    ) => {}
                FieldCoercionDecision::Widened => {
                    return Err(CdfError::data(format!(
                        "field {:?} requires a width coercion that the compiled schema-admission verdict does not admit",
                        field.source_name
                    )));
                }
                FieldCoercionDecision::Extra => {
                    return Err(CdfError::data(format!(
                        "unknown field {:?} is rejected by the compiled schema-admission verdict",
                        field.source_name
                    )));
                }
                FieldCoercionDecision::CoercedByPolicy if self.type_policy.coerce_types => {}
                FieldCoercionDecision::LossyAllowed if self.type_policy.allow_lossy_mapping => {}
                FieldCoercionDecision::CoercedByPolicy => {
                    return Err(CdfError::contract(format!(
                        "field {:?} carries a parse coercion that the compiled schema-admission program does not allow",
                        field.source_name
                    )));
                }
                FieldCoercionDecision::LossyAllowed => {
                    return Err(CdfError::contract(format!(
                        "field {:?} carries a lossy coercion that the compiled schema-admission program does not allow",
                        field.source_name
                    )));
                }
                FieldCoercionDecision::LossyRejected | FieldCoercionDecision::Unsupported => {
                    return Err(CdfError::data(format!(
                        "field {:?} carries a non-materializable schema-admission decision",
                        field.source_name
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn validate_quarantined_observation(
        &self,
        quarantine: &TerminalSchemaObservationQuarantine,
        physical_observation: &PhysicalObservationEvidence,
    ) -> Result<()> {
        let PhysicalObservationEvidence::ArrowSchema { schema } = physical_observation else {
            return Err(CdfError::data(
                "schema quarantine requires an exact physical Arrow schema observation",
            ));
        };
        let observed = schema.to_arrow()?;
        if physical_observation.identity_hash()? != *quarantine.physical_schema_hash() {
            return Err(CdfError::data(format!(
                "schema quarantine {:?} does not bind its physical Arrow schema",
                quarantine.observation_id()
            )));
        }
        let outcome = self.instantiate_or_quarantine(
            quarantine.observation_id(),
            observed.as_ref(),
            quarantine.physical_schema_hash(),
        )?;
        let CompiledSchemaAdmissionOutcome::Quarantined(expected) = outcome else {
            return Err(CdfError::data(format!(
                "schema quarantine {:?} conflicts with the compiled admission verdict",
                quarantine.observation_id()
            )));
        };
        if expected.observation_id() != quarantine.observation_id()
            || expected.physical_schema_hash() != quarantine.physical_schema_hash()
            || expected.rule_id() != quarantine.rule_id()
            || expected.error_code() != quarantine.error_code()
            || expected.policy() != quarantine.policy()
            || expected.remediation() != quarantine.remediation()
            || expected.fields() != quarantine.fields()
        {
            return Err(CdfError::data(format!(
                "schema quarantine {:?} does not match the compiled admission action",
                quarantine.observation_id()
            )));
        }
        Ok(())
    }

    pub fn validate_admitted_observation(
        &self,
        observation_id: &str,
        physical_observation: &PhysicalObservationEvidence,
        recorded: &SchemaCoercionPlan,
    ) -> Result<()> {
        match physical_observation {
            PhysicalObservationEvidence::MaterializedOutput {
                physical_schema,
                output_schema,
                nullable_residual_fields,
                ..
            } => {
                let physical = physical_schema.to_arrow()?;
                self.validate_materialized(physical.as_ref(), recorded)?;
                let output = output_schema.to_arrow()?;
                let constraint = self.constraint_schema.to_arrow()?;
                let nullable_residual_fields = nullable_residual_fields
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                if output.metadata() != constraint.metadata()
                    || output.fields().len() != constraint.fields().len()
                {
                    return Err(CdfError::data(format!(
                        "materialized schema observation {observation_id:?} does not carry the compiled effective output schema"
                    )));
                }
                let mut actual_nullable_residual_fields = BTreeSet::new();
                for (output, constraint) in output.fields().iter().zip(constraint.fields()) {
                    let output_source =
                        source_name(output.as_ref()).unwrap_or_else(|| output.name());
                    let constraint_source =
                        source_name(constraint.as_ref()).unwrap_or_else(|| constraint.name());
                    let nullable_matches = output.is_nullable() == constraint.is_nullable()
                        || (output.is_nullable()
                            && !constraint.is_nullable()
                            && nullable_residual_fields.contains(constraint_source));
                    if output.is_nullable() && !constraint.is_nullable() {
                        actual_nullable_residual_fields.insert(constraint_source);
                    }
                    if output.name() != constraint.name()
                        || output_source != constraint_source
                        || output.data_type() != constraint.data_type()
                        || output.metadata() != constraint.metadata()
                        || !nullable_matches
                    {
                        return Err(CdfError::data(format!(
                            "materialized schema observation {observation_id:?} output field {:?} does not match compiled field {:?}",
                            output.name(),
                            constraint.name()
                        )));
                    }
                }
                if actual_nullable_residual_fields != nullable_residual_fields {
                    return Err(CdfError::data(format!(
                        "materialized schema observation {observation_id:?} nullable residual identities do not exactly match its output-schema delta"
                    )));
                }
                Ok(())
            }
            PhysicalObservationEvidence::ArrowSchema { schema } => {
                let observed = schema.to_arrow()?;
                let physical_hash = physical_observation.identity_hash()?;
                match self.instantiate_or_quarantine(
                    observation_id,
                    observed.as_ref(),
                    &physical_hash,
                )? {
                    CompiledSchemaAdmissionOutcome::Admitted(expected) if expected == *recorded => {
                        Ok(())
                    }
                    CompiledSchemaAdmissionOutcome::Admitted(_) => Err(CdfError::data(format!(
                        "admitted schema observation {observation_id:?} does not match the compiled coercion verdict"
                    ))),
                    CompiledSchemaAdmissionOutcome::Quarantined(_) => Err(CdfError::data(format!(
                        "admitted schema observation {observation_id:?} conflicts with the compiled quarantine verdict"
                    ))),
                }
            }
        }
    }

    fn control_critical_missing_quarantine(
        &self,
        observation_id: &str,
        physical_schema_hash: &SchemaHash,
        observed: &Schema,
        constraint: &Schema,
        plan: &SchemaCoercionPlan,
    ) -> Result<Option<TerminalSchemaObservationQuarantine>> {
        let fields = plan
            .fields
            .iter()
            .filter(|field| {
                field.decision == FieldCoercionDecision::Missing
                    && self.control_critical_fields.contains(&field.source_name)
            })
            .map(|decision| {
                let observed_field = observed
                    .fields()
                    .iter()
                    .find(|field| {
                        source_name(field.as_ref()).unwrap_or_else(|| field.name())
                            == decision.source_name
                    })
                    .map(|field| CanonicalArrowField::from_arrow(field.as_ref()))
                    .transpose()?;
                let effective_field = constraint
                    .fields()
                    .iter()
                    .find(|field| {
                        source_name(field.as_ref()).unwrap_or_else(|| field.name())
                            == decision.source_name
                    })
                    .map(|field| CanonicalArrowField::from_arrow(field.as_ref()))
                    .transpose()?;
                SchemaObservationFieldQuarantine::new_field_path(
                    vec![decision.source_name.clone()],
                    observed_field,
                    effective_field,
                    "control-critical field is missing from the physical observation",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if fields.is_empty() {
            return Ok(None);
        }
        Ok(Some(TerminalSchemaObservationQuarantine::new(
            observation_id,
            physical_schema_hash.clone(),
            "schema-observation:control-critical-missing",
            "schema_control_field_missing",
            SchemaObservationPolicy::Freeze,
            "restore the required cursor/key field or publish the partition to quarantine for correction",
            fields,
        )?))
    }

    pub(crate) fn captures_unknown_fields(&self) -> Result<bool> {
        Ok(match self.schema_verdict(SchemaChangeKind::UnknownField)? {
            VerdictAction::AdmitAsVariant => true,
            VerdictAction::Admit => self
                .residual
                .as_ref()
                .and_then(|residual| residual.capture.as_ref())
                .is_some(),
            _ => false,
        })
    }

    fn schema_verdict(&self, change: SchemaChangeKind) -> Result<&VerdictAction> {
        self.schema_verdicts
            .iter()
            .find(|rule| rule.change == change)
            .map(|rule| &rule.verdict)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "compiled schema-admission program omitted its {change:?} verdict"
                ))
            })
    }

    pub fn validate_recorded(&self, validation_program: &ValidationProgram) -> Result<()> {
        self.validate_intrinsic(validation_program)
    }

    pub(crate) fn validate_preobserved_evidence(
        &self,
        evidence: &EffectiveSchemaPlanEvidence,
    ) -> Result<()> {
        if evidence.authority.baseline.schema_hash() != &self.baseline_schema_hash
            || evidence.authority.effective_schema_hash != self.effective_schema_hash
        {
            return Err(CdfError::data(
                "preobserved schema evidence does not match the compiled admission epoch",
            ));
        }
        validate_physical_observation_catalog(&evidence.physical_observation_catalog)?;
        let mut admitted = BTreeSet::new();
        let mut expected_physical_observations = BTreeSet::new();
        for observation in &evidence.observations {
            if observation.observation_id.is_empty()
                || !admitted.insert(observation.observation_id.as_str())
            {
                return Err(CdfError::data(
                    "preobserved schema evidence contains an empty or duplicate admitted observation",
                ));
            }
            let physical_hash = observation.physical_schema_hash.as_str();
            expected_physical_observations.insert(physical_hash);
            let physical = evidence
                .physical_observation_catalog
                .get(physical_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "preobserved schema evidence {:?} references an absent physical observation",
                        observation.observation_id
                    ))
                })?;
            self.validate_admitted_observation(
                &observation.observation_id,
                physical,
                &observation.coercion_plan,
            )?;
        }
        let mut quarantined = BTreeSet::new();
        for quarantine in &evidence.terminal_quarantines {
            quarantine.validate()?;
            if !quarantined.insert(quarantine.observation_id())
                || admitted.contains(quarantine.observation_id())
            {
                return Err(CdfError::data(
                    "preobserved schema evidence contains a duplicate admitted/quarantined observation",
                ));
            }
            let physical_hash = quarantine.physical_schema_hash().as_str();
            expected_physical_observations.insert(physical_hash);
            let physical = evidence
                .physical_observation_catalog
                .get(physical_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "preobserved quarantine {:?} omitted its physical Arrow schema",
                        quarantine.observation_id()
                    ))
                })?;
            if physical.identity_hash()? != *quarantine.physical_schema_hash() {
                return Err(CdfError::data(format!(
                    "preobserved quarantine {:?} does not bind its physical Arrow schema",
                    quarantine.observation_id()
                )));
            }
            self.validate_quarantined_observation(quarantine, physical)?;
        }
        if expected_physical_observations
            != evidence
                .physical_observation_catalog
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        {
            return Err(CdfError::data(
                "preobserved schema evidence physical-observation catalog is incomplete",
            ));
        }
        Ok(())
    }

    fn validate<R>(
        &self,
        authority: &EngineSchemaAuthority,
        validation_program: &ValidationProgram,
        resource: &R,
    ) -> Result<()>
    where
        R: ResourceStream + ?Sized,
    {
        self.validate_intrinsic(validation_program)?;
        if self.baseline_schema_hash != authority.baseline_schema_hash
            || self.effective_schema_hash != authority.effective_schema_hash
        {
            return Err(CdfError::data(
                "compiled schema-admission identities do not match engine schema authority",
            ));
        }
        let resource_schema_hash =
            cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref())?;
        if self.resource_schema_hash != resource_schema_hash {
            return Err(CdfError::data(format!(
                "compiled schema-admission resource schema {} does not match execution schema {resource_schema_hash}",
                self.resource_schema_hash
            )));
        }
        let mut expected = resource
            .baseline_observation_schema_catalog()
            .iter()
            .map(|entry| {
                let projected = project_baseline_observation(
                    entry.schema.as_ref(),
                    resource.schema().as_ref(),
                    self.baseline_projection.as_deref(),
                )?;
                cdf_kernel::canonical_arrow_schema_hash(&projected)
            })
            .collect::<Result<Vec<_>>>()?;
        expected.sort();
        expected.dedup();
        if self.baseline_projected_schema_hashes != expected {
            return Err(CdfError::data(
                "compiled schema-admission baseline observations do not match the execution resource",
            ));
        }
        let mut expected =
            ContractPolicy::for_trust(resource.descriptor().trust_level.clone()).types;
        let allowances = resource.type_policy_allowances();
        expected.coerce_types = allowances.coerce_types;
        expected.allow_lossy_mapping = allowances.allow_lossy_mapping;
        if self.type_policy != expected {
            return Err(CdfError::data(
                "compiled schema-admission type policy does not match execution resource allowances",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_intrinsic(&self, validation_program: &ValidationProgram) -> Result<()> {
        if self.version != COMPILED_SCHEMA_ADMISSION_VERSION {
            return Err(CdfError::data(format!(
                "unsupported compiled schema-admission version {}; expected {COMPILED_SCHEMA_ADMISSION_VERSION}",
                self.version
            )));
        }
        self.constraint_schema.to_arrow()?;
        if self
            .baseline_projection
            .as_ref()
            .is_some_and(|projection| projection.is_empty())
        {
            return Err(CdfError::data(
                "compiled schema-admission baseline projection cannot be empty",
            ));
        }
        if self
            .baseline_projected_schema_hashes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(CdfError::data(
                "compiled schema-admission baseline observation hashes are not sorted and unique",
            ));
        }
        if self.normalizer_version != validation_program.normalizer_version
            || self.identifier_policy != validation_program.identifier_policy
            || self.schema_verdicts != validation_program.schema_verdicts
            || self.residual != validation_program.residual
            || self.row_dispositions != validation_program.row_dispositions
            || self.contract_program_hash != cdf_runtime::artifact_hash(validation_program)?
        {
            return Err(CdfError::data(
                "compiled schema-admission program does not match the validation program",
            ));
        }
        let mut expected_control_fields = validation_program
            .residual
            .iter()
            .flat_map(|residual| &residual.fields)
            .filter(|field| field.control_critical)
            .map(|field| field.source_name.clone())
            .collect::<Vec<_>>();
        expected_control_fields.sort();
        expected_control_fields.dedup();
        if self.control_critical_fields != expected_control_fields
            || self.cache_key_fields
                != SCHEMA_ADMISSION_CACHE_KEY_FIELDS
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
        {
            return Err(CdfError::data(
                "compiled schema-admission control fields or cache-key shape are invalid",
            ));
        }
        if let Some(source) = &self.source {
            source.validate()?;
        }
        Ok(())
    }
}

fn project_baseline_observation(
    physical: &Schema,
    resource_schema: &Schema,
    projection: Option<&[String]>,
) -> Result<Schema> {
    let Some(projection) = projection else {
        return Ok(physical.clone());
    };
    let fields = projection
        .iter()
        .map(|logical_name| {
            let resource_field = resource_schema.field_with_name(logical_name).map_err(|_| {
                CdfError::contract(format!(
                    "compiled baseline projection field {logical_name:?} is absent from the resource schema"
                ))
            })?;
            let physical_name = source_name(resource_field).unwrap_or_else(|| resource_field.name());
            physical
                .fields()
                .iter()
                .find(|field| {
                    field.name() == physical_name
                        || source_name(field.as_ref()) == Some(physical_name)
                })
                .cloned()
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "baseline physical schema omitted projected source field {physical_name:?} for resource field {logical_name:?}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        physical.metadata().clone(),
    ))
}

pub(crate) enum CompiledSchemaAdmissionOutcome {
    Admitted(SchemaCoercionPlan),
    Quarantined(Box<TerminalSchemaObservationQuarantine>),
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamAdmissionObservationEvidence {
    pub observation_id: String,
    pub physical_observation_hash: String,
    pub coercion_plan: SchemaCoercionPlan,
    pub completion: StreamAdmissionCompletion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamAdmissionCompletion {
    Partial {
        attempted_position: Option<SourcePosition>,
        observed_rows: u64,
        partition_binding: String,
    },
    Complete {
        source_position: SourcePosition,
    },
    CompleteUnpositioned {
        partition_binding: String,
    },
}

impl StreamAdmissionObservationEvidence {
    pub fn new(
        observation_id: impl Into<String>,
        physical_observation_hash: SchemaHash,
        coercion_plan: SchemaCoercionPlan,
        completion: StreamAdmissionCompletion,
    ) -> Result<Self> {
        let observation_id = observation_id.into();
        if observation_id.is_empty() {
            return Err(CdfError::contract(
                "stream-admission observation id must not be empty",
            ));
        }
        Ok(Self {
            observation_id,
            physical_observation_hash: physical_observation_hash.to_string(),
            coercion_plan,
            completion,
        })
    }

    pub fn bind_source_position(&mut self, source_position: SourcePosition) -> Result<()> {
        if matches!(
            &self.completion,
            StreamAdmissionCompletion::Complete {
                source_position: existing
            } if existing != &source_position
        ) {
            return Err(CdfError::data(format!(
                "stream-admission observation {:?} carries conflicting source positions",
                self.observation_id
            )));
        }
        self.completion = StreamAdmissionCompletion::Complete { source_position };
        Ok(())
    }

    pub fn bind_partial_attempt(
        &mut self,
        attempted_position: SourcePosition,
        observed_rows: u64,
        partition_binding: String,
    ) -> Result<()> {
        if observed_rows == 0 || partition_binding.is_empty() {
            return Err(CdfError::data(format!(
                "partial stream-admission observation {:?} must cover at least one observed row",
                self.observation_id
            )));
        }
        match &self.completion {
            StreamAdmissionCompletion::Partial {
                attempted_position: None,
                observed_rows: 0,
                partition_binding: existing_binding,
            } if existing_binding.is_empty() => {
                self.completion = StreamAdmissionCompletion::Partial {
                    attempted_position: Some(attempted_position),
                    observed_rows,
                    partition_binding,
                };
                Ok(())
            }
            StreamAdmissionCompletion::Partial {
                attempted_position: Some(existing_position),
                observed_rows: existing_rows,
                partition_binding: existing_binding,
            } if existing_position == &attempted_position
                && *existing_rows == observed_rows
                && existing_binding == &partition_binding =>
            {
                Ok(())
            }
            _ => Err(CdfError::data(format!(
                "stream-admission observation {:?} carries conflicting partial-attempt evidence",
                self.observation_id
            ))),
        }
    }

    pub fn bind_unpositioned_completion(&mut self, partition_binding: String) -> Result<()> {
        if partition_binding.is_empty() {
            return Err(CdfError::data(format!(
                "unpositioned stream-admission observation {:?} requires a planned partition binding",
                self.observation_id
            )));
        }
        self.completion = StreamAdmissionCompletion::CompleteUnpositioned { partition_binding };
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PhysicalObservationEvidence {
    ArrowSchema {
        schema: CompiledArrowSchema,
    },
    MaterializedOutput {
        physical_schema: CompiledArrowSchema,
        output_schema: CompiledArrowSchema,
        nullable_residual_fields: Vec<String>,
    },
}

impl PhysicalObservationEvidence {
    pub fn arrow_schema(schema: &Schema) -> Result<Self> {
        Ok(Self::ArrowSchema {
            schema: CompiledArrowSchema::from_arrow(schema)?,
        })
    }

    pub fn materialized_output(
        physical_schema: &Schema,
        output_schema: &Schema,
        nullable_residual_fields: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        let mut nullable_residual_fields = nullable_residual_fields.into_iter().collect::<Vec<_>>();
        nullable_residual_fields.sort();
        nullable_residual_fields.dedup();
        Ok(Self::MaterializedOutput {
            physical_schema: CompiledArrowSchema::from_arrow(physical_schema)?,
            output_schema: CompiledArrowSchema::from_arrow(output_schema)?,
            nullable_residual_fields,
        })
    }

    pub fn identity_hash(&self) -> Result<SchemaHash> {
        self.validate()?;
        match self {
            Self::ArrowSchema { schema } => Ok(schema.arrow_schema_hash.clone()),
            Self::MaterializedOutput { .. } => SchemaHash::new(cdf_runtime::artifact_hash(self)?),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::ArrowSchema { schema } => {
                schema.to_arrow()?;
            }
            Self::MaterializedOutput {
                physical_schema,
                output_schema,
                nullable_residual_fields,
            } => {
                physical_schema.to_arrow()?;
                output_schema.to_arrow()?;
                if nullable_residual_fields.iter().any(String::is_empty) {
                    return Err(CdfError::data(
                        "materialized output residual-field identity must not be empty",
                    ));
                }
                let mut canonical = nullable_residual_fields.clone();
                canonical.sort();
                canonical.dedup();
                if &canonical != nullable_residual_fields {
                    return Err(CdfError::data(
                        "materialized output residual-field identities must be sorted and unique",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledStreamAdmissionEvidence {
    pub compiled_admission_hash: String,
    pub baseline_schema_hash: String,
    pub effective_schema_hash: String,
    pub physical_observation_catalog: BTreeMap<String, PhysicalObservationEvidence>,
    pub observations: Vec<StreamAdmissionObservationEvidence>,
}

impl CompiledStreamAdmissionEvidence {
    pub fn new(
        admission: &CompiledSchemaAdmissionPlan,
        physical_observation_catalog: BTreeMap<String, PhysicalObservationEvidence>,
        observations: Vec<StreamAdmissionObservationEvidence>,
    ) -> Result<Self> {
        let evidence = Self {
            compiled_admission_hash: cdf_runtime::artifact_hash(admission)?,
            baseline_schema_hash: admission.baseline_schema_hash.to_string(),
            effective_schema_hash: admission.effective_schema_hash.to_string(),
            physical_observation_catalog,
            observations,
        };
        evidence.validate(admission)?;
        Ok(evidence)
    }

    pub fn validate(&self, admission: &CompiledSchemaAdmissionPlan) -> Result<()> {
        if self.compiled_admission_hash != cdf_runtime::artifact_hash(admission)?
            || self.baseline_schema_hash != admission.baseline_schema_hash.as_str()
            || self.effective_schema_hash != admission.effective_schema_hash.as_str()
        {
            return Err(CdfError::data(
                "stream-admission evidence does not match the recorded compiled admission plan",
            ));
        }
        validate_physical_observation_catalog(&self.physical_observation_catalog)?;
        let mut observation_ids = std::collections::BTreeSet::new();
        let mut referenced_physical_observations = BTreeSet::new();
        for observation in &self.observations {
            if observation.observation_id.is_empty()
                || !observation_ids.insert(observation.observation_id.as_str())
            {
                return Err(CdfError::data(
                    "stream-admission evidence contains an empty or duplicate observation id",
                ));
            }
            if let StreamAdmissionCompletion::Partial {
                attempted_position,
                observed_rows,
                partition_binding,
            } = &observation.completion
                && (attempted_position.is_none()
                    || *observed_rows == 0
                    || partition_binding.is_empty())
            {
                return Err(CdfError::data(format!(
                    "partial stream-admission observation {:?} omits its exact attempted position, observed row count, or planned partition binding",
                    observation.observation_id
                )));
            }
            if matches!(
                &observation.completion,
                StreamAdmissionCompletion::CompleteUnpositioned { partition_binding }
                    if partition_binding.is_empty()
            ) {
                return Err(CdfError::data(format!(
                    "unpositioned stream-admission observation {:?} omits its planned partition binding",
                    observation.observation_id
                )));
            }
            let physical = self
                .physical_observation_catalog
                .get(&observation.physical_observation_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "stream-admission observation {:?} references an absent physical observation",
                        observation.observation_id
                    ))
                })?;
            referenced_physical_observations.insert(observation.physical_observation_hash.as_str());
            admission.validate_admitted_observation(
                &observation.observation_id,
                physical,
                &observation.coercion_plan,
            )?;
        }
        if referenced_physical_observations
            != self
                .physical_observation_catalog
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        {
            return Err(CdfError::data(
                "stream-admission evidence physical-observation catalog is not an exact referenced set",
            ));
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaQuarantineObservationEvidence {
    pub observation_id: String,
    pub quarantine_hash: String,
    pub physical_observation_hash: String,
}

impl SchemaQuarantineObservationEvidence {
    pub fn new(
        quarantine: &TerminalSchemaObservationQuarantine,
        physical_observation_hash: SchemaHash,
    ) -> Result<Self> {
        quarantine.validate()?;
        let evidence = Self {
            observation_id: quarantine.observation_id().to_owned(),
            quarantine_hash: cdf_runtime::artifact_hash(quarantine)?,
            physical_observation_hash: physical_observation_hash.to_string(),
        };
        evidence.validate(quarantine)?;
        Ok(evidence)
    }

    pub fn validate(&self, quarantine: &TerminalSchemaObservationQuarantine) -> Result<()> {
        if self.observation_id != quarantine.observation_id()
            || self.quarantine_hash != cdf_runtime::artifact_hash(quarantine)?
            || self.physical_observation_hash != quarantine.physical_schema_hash().as_str()
            || quarantine.source_position().is_none()
        {
            return Err(CdfError::data(format!(
                "schema-quarantine observation {:?} does not bind its quarantine, physical schema, and processed position",
                self.observation_id
            )));
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledSchemaQuarantineEvidence {
    pub compiled_admission_hash: String,
    pub baseline_schema_hash: String,
    pub effective_schema_hash: String,
    pub physical_observation_catalog: BTreeMap<String, PhysicalObservationEvidence>,
    pub observations: Vec<SchemaQuarantineObservationEvidence>,
}

impl CompiledSchemaQuarantineEvidence {
    pub fn new(
        admission: &CompiledSchemaAdmissionPlan,
        physical_observation_catalog: BTreeMap<String, PhysicalObservationEvidence>,
        observations: Vec<SchemaQuarantineObservationEvidence>,
    ) -> Result<Self> {
        let evidence = Self {
            compiled_admission_hash: cdf_runtime::artifact_hash(admission)?,
            baseline_schema_hash: admission.baseline_schema_hash.to_string(),
            effective_schema_hash: admission.effective_schema_hash.to_string(),
            physical_observation_catalog,
            observations,
        };
        evidence.validate_admission(admission)?;
        Ok(evidence)
    }

    pub fn validate_admission(&self, admission: &CompiledSchemaAdmissionPlan) -> Result<()> {
        if self.compiled_admission_hash != cdf_runtime::artifact_hash(admission)?
            || self.baseline_schema_hash != admission.baseline_schema_hash.as_str()
            || self.effective_schema_hash != admission.effective_schema_hash.as_str()
        {
            return Err(CdfError::data(
                "schema-quarantine evidence does not match the recorded compiled admission plan",
            ));
        }
        validate_physical_observation_catalog(&self.physical_observation_catalog)?;
        let mut ids = BTreeSet::new();
        let mut referenced_physical_observations = BTreeSet::new();
        if self.observations.iter().any(|observation| {
            observation.observation_id.is_empty()
                || !ids.insert(observation.observation_id.as_str())
        }) {
            return Err(CdfError::data(
                "schema-quarantine evidence contains an empty or duplicate observation id",
            ));
        }
        for observation in &self.observations {
            if !self
                .physical_observation_catalog
                .contains_key(&observation.physical_observation_hash)
            {
                return Err(CdfError::data(
                    "schema-quarantine evidence references an absent physical observation",
                ));
            }
            referenced_physical_observations.insert(observation.physical_observation_hash.as_str());
        }
        if referenced_physical_observations
            != self
                .physical_observation_catalog
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        {
            return Err(CdfError::data(
                "schema-quarantine evidence physical-observation catalog is not an exact referenced set",
            ));
        }
        Ok(())
    }
}

fn validate_physical_observation_catalog(
    catalog: &BTreeMap<String, PhysicalObservationEvidence>,
) -> Result<()> {
    for (identity, observation) in catalog {
        if identity != observation.identity_hash()?.as_str() {
            return Err(CdfError::data(format!(
                "physical-observation catalog key {identity:?} does not match its evidence identity"
            )));
        }
    }
    Ok(())
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineSchemaAuthority {
    pub version: u16,
    pub baseline_schema_hash: SchemaHash,
    pub effective_schema_hash: SchemaHash,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledArrowSchema {
    pub version: u16,
    pub arrow_schema_hash: SchemaHash,
    pub fields: Vec<CanonicalArrowField>,
    pub metadata: BTreeMap<String, String>,
}

impl CompiledArrowSchema {
    pub fn from_arrow(schema: &Schema) -> Result<Self> {
        let arrow_schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema)?;
        let fields = schema
            .fields()
            .iter()
            .map(|field| CanonicalArrowField::from_arrow(field))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            version: 1,
            arrow_schema_hash,
            fields,
            metadata: schema
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }

    pub fn to_arrow(&self) -> Result<Arc<Schema>> {
        if self.version != 1 {
            return Err(CdfError::data(format!(
                "unsupported compiled Arrow schema version {}",
                self.version
            )));
        }
        let fields = self
            .fields
            .iter()
            .map(CanonicalArrowField::to_arrow)
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.metadata.clone().into_iter().collect(),
        ));
        let actual = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
        if actual != self.arrow_schema_hash {
            return Err(CdfError::data(format!(
                "compiled Arrow schema hash mismatch: plan records {}, materialized {}",
                self.arrow_schema_hash, actual
            )));
        }
        Ok(schema)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaPlanEvidence {
    pub authority: EffectiveSchemaEvidence,
    pub effective_arrow_schema_hash: SchemaHash,
    pub observations: Vec<EffectiveSchemaObservationCoercion>,
    pub physical_observation_catalog: BTreeMap<String, PhysicalObservationEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_executor_budget: Option<DiscoveryExecutorBudgetEvidence>,
    pub observation_bindings: BTreeMap<String, String>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaObservationCoercion {
    pub observation_id: String,
    pub physical_schema_hash: SchemaHash,
    pub coercion_plan: SchemaCoercionPlan,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperatorNode {
    CdfResourceAdapter {
        adapter_kind: String,
        resource_id: ResourceId,
    },
    CdfNativeScan {
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
        segmentation: crate::CanonicalSegmentationPolicy,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_schedule: Option<cdf_runtime::CanonicalPartitionSchedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_stream_policy: Option<cdf_runtime::CompiledStreamPolicy>,
    pub estimates: EstimateExplain,
    pub delivery_guarantee: DeliveryGuarantee,
    pub execution_extent: ExecutionExtent,
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
    pub verification: VerifiedPackage,
    pub segments: Vec<SegmentEntry>,
    pub profile: ExecutionProfile,
    pub lineage: LineageSummary,
    pub terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
}

pub const ENGINE_EXECUTION_EVIDENCE_VERSION: u16 = 2;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineExecutionEvidence {
    version: u16,
    processed_observations: Vec<ProcessedObservationPosition>,
    source_retries: Vec<cdf_runtime::SourceRetryEvidence>,
    checkpoint_eligible: bool,
}

impl EngineExecutionEvidence {
    pub fn new(
        mut processed_observations: Vec<ProcessedObservationPosition>,
        mut source_retries: Vec<cdf_runtime::SourceRetryEvidence>,
        partition_schedule: Option<&cdf_runtime::CanonicalPartitionSchedule>,
        checkpoint_eligible: bool,
    ) -> cdf_kernel::Result<Self> {
        processed_observations
            .sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        if let Some(repeated) = processed_observations
            .windows(2)
            .find(|pair| pair[0].observation_id == pair[1].observation_id)
        {
            return Err(cdf_kernel::CdfError::data(format!(
                "processed schema observation {:?} is assigned to more than one partition",
                repeated[0].observation_id
            )));
        }
        source_retries.sort_by_key(cdf_runtime::SourceRetryEvidence::partition_ordinal);
        let retry_schedule_is_valid = match partition_schedule {
            Some(schedule) => source_retries
                .iter()
                .all(|evidence| evidence.validate_against_schedule(schedule).is_ok()),
            None => source_retries.is_empty(),
        };
        if !retry_schedule_is_valid
            || source_retries.windows(2).any(|pair| {
                pair[0].partition_ordinal() == pair[1].partition_ordinal()
                    || pair[0].partition_id() == pair[1].partition_id()
            })
        {
            return Err(cdf_kernel::CdfError::data(
                "source retry evidence requires unique partitions and ordered nonempty attempt history",
            ));
        }
        Ok(Self {
            version: ENGINE_EXECUTION_EVIDENCE_VERSION,
            processed_observations,
            source_retries,
            checkpoint_eligible,
        })
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn processed_observations(&self) -> &[ProcessedObservationPosition] {
        &self.processed_observations
    }

    pub fn source_retries(&self) -> &[cdf_runtime::SourceRetryEvidence] {
        &self.source_retries
    }

    pub fn checkpoint_eligible(&self) -> bool {
        self.checkpoint_eligible
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineRunOutputWithSegmentPositions {
    pub output: EngineRunOutput,
    pub segment_positions: Vec<EngineSegmentPosition>,
    pub phase_metrics: Vec<RunPhaseMetric>,
    pub source_frontier: cdf_runtime::SourceFrontierReport,
    pub drain_epoch: Option<EngineDrainEpoch>,
    pub(crate) execution_evidence: EngineExecutionEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineDrainEpoch {
    pub closure: cdf_runtime::DrainEpochClosure,
    pub consumed_partition_count: usize,
    pub resume_partition: Option<Box<DrainPartitionResume>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineDrainEpochOutcome {
    Package(Box<EngineRunOutputWithSegmentPositions>),
    FinishedNoOp {
        source_frontier: cdf_runtime::SourceFrontierReport,
    },
}

impl EngineDrainEpochOutcome {
    pub fn into_package(self) -> Result<EngineRunOutputWithSegmentPositions> {
        match self {
            Self::Package(output) => Ok(*output),
            Self::FinishedNoOp { .. } => {
                Err(CdfError::data("drain execution finished without a package"))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainPartitionResume {
    pub partition_id: PartitionId,
    pub start_position: SourcePosition,
}

impl EngineRunOutputWithSegmentPositions {
    pub fn new(
        output: EngineRunOutput,
        segment_positions: Vec<EngineSegmentPosition>,
        execution_evidence: EngineExecutionEvidence,
    ) -> Self {
        Self {
            output,
            segment_positions,
            phase_metrics: Vec::new(),
            source_frontier: cdf_runtime::SourceFrontierReport::default(),
            drain_epoch: None,
            execution_evidence,
        }
    }

    pub fn execution_evidence(&self) -> &EngineExecutionEvidence {
        &self.execution_evidence
    }
}

#[derive(Clone, Default)]
pub struct EngineExecutionOptions {
    pub(crate) phase_metrics: bool,
    pub(crate) services: Option<cdf_runtime::ExecutionServices>,
    pub(crate) unfused_transform: bool,
    pub(crate) statistics_profile: bool,
    pub(crate) scheduler: Option<cdf_runtime::RuntimeSchedulerResolution>,
    pub(crate) cancellation: cdf_runtime::RunCancellation,
    pub(crate) retry_journal: cdf_runtime::SourceRetryJournal,
}

impl EngineExecutionOptions {
    pub const fn with_phase_metrics(mut self, enabled: bool) -> Self {
        self.phase_metrics = enabled;
        self
    }

    pub fn with_execution_services(mut self, services: cdf_runtime::ExecutionServices) -> Self {
        self.services = Some(services);
        self
    }

    pub const fn with_unfused_transform_for_conformance(mut self, enabled: bool) -> Self {
        self.unfused_transform = enabled;
        self
    }

    pub const fn with_statistics_profile(mut self, enabled: bool) -> Self {
        self.statistics_profile = enabled;
        self
    }

    pub fn with_scheduler_resolution(
        mut self,
        scheduler: cdf_runtime::RuntimeSchedulerResolution,
    ) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_cancellation(mut self, cancellation: cdf_runtime::RunCancellation) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn source_retry_evidence(&self) -> cdf_runtime::SourceRetryEvidenceView {
        self.retry_journal.evidence_view()
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub struct EnginePackageDraft<'a> {
    pub segments: &'a [SegmentEntry],
    pub profile: &'a ExecutionProfile,
    pub lineage: &'a LineageSummary,
    pub segment_positions: &'a [EngineSegmentPosition],
    pub(crate) execution_evidence: &'a EngineExecutionEvidence,
}

impl<'a> EnginePackageDraft<'a> {
    pub fn execution_evidence(&self) -> &'a EngineExecutionEvidence {
        self.execution_evidence
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineSegmentPosition {
    pub segment_id: SegmentId,
    pub partition_ordinal: u32,
    pub output_position: Option<SourcePosition>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionProfile {
    pub output_rows: u64,
    pub output_bytes: u64,
    pub output_batches: u64,
    // Keep the aggregate JSON artifact stable until typed Parquet replaces it in one reviewed
    // identity boundary; this in-memory typed evidence is not a second package representation.
    #[serde(default, skip_serializing)]
    pub statistics: cdf_kernel::BatchStats,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageSummary {
    pub input_partitions: Vec<cdf_kernel::PartitionId>,
    pub input_rows: u64,
    pub input_observations: Vec<LineageInputObservation>,
    pub output_segments: Vec<SegmentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageInputObservation {
    pub observation_id: String,
    pub partition_id: cdf_kernel::PartitionId,
    pub observed_rows: u64,
    pub output_position: Option<SourcePosition>,
}

pub const PREVIEW_POLICY_BALANCED_STRATIFIED_V1: &str = "preview-balanced-stratified-v1";
pub const DEFAULT_PREVIEW_MAX_ROWS: u64 = 500;
pub const DEFAULT_PREVIEW_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_PREVIEW_MAX_BATCHES: u64 = 64;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePreviewLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_batches: u64,
}

impl Default for EnginePreviewLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_PREVIEW_MAX_ROWS,
            max_bytes: DEFAULT_PREVIEW_MAX_BYTES,
            max_batches: DEFAULT_PREVIEW_MAX_BATCHES,
        }
    }
}

impl EnginePreviewLimits {
    pub fn new(max_rows: u64, max_bytes: u64, max_batches: u64) -> cdf_kernel::Result<Self> {
        if max_rows == 0 || max_bytes == 0 || max_batches == 0 {
            return Err(cdf_kernel::CdfError::contract(
                "preview row, decoded-byte, and batch limits must be positive",
            ));
        }
        Ok(Self {
            max_rows,
            max_bytes,
            max_batches,
        })
    }

    pub fn with_max_rows(mut self, max_rows: u64) -> cdf_kernel::Result<Self> {
        if max_rows == 0 {
            return Err(cdf_kernel::CdfError::contract(
                "preview row limit must be positive",
            ));
        }
        self.max_rows = max_rows;
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePreviewSelectedPartition {
    pub partition_id: String,
    pub canonical_location: String,
    pub score_sha256: String,
    pub bounded_identity_sha256: String,
    pub batch_quota: u64,
    pub inspected_batches: u64,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePreviewSelectionEvidence {
    pub policy: String,
    pub selector: String,
    pub candidate_count: u64,
    pub selected: Vec<EnginePreviewSelectedPartition>,
    pub selected_but_uninspected_partition_ids: Vec<String>,
    pub partially_inspected_partition_ids: Vec<String>,
    pub payload_uninspected_partition_ids: Vec<String>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePreviewOutput {
    pub resource_id: ResourceId,
    pub first_partition_id: Option<String>,
    pub first_batch_id: Option<String>,
    pub planned_partition_count: u64,
    pub payload_eligible_partition_count: u64,
    pub selected_partition_count: u64,
    pub payload_opened_partition_count: u64,
    pub attested_partition_count: u64,
    pub inspected_partition_count: u64,
    pub partially_inspected_partition_count: u64,
    pub payload_uninspected_partition_count: u64,
    pub inspected_batch_count: u64,
    pub row_count: u64,
    pub byte_count: u64,
    pub output_byte_count: u64,
    pub quarantined_row_count: u64,
    pub residual_row_count: u64,
    pub terminal_quarantine_count: u64,
    pub fields: Vec<String>,
    pub limits: EnginePreviewLimits,
    pub selection: EnginePreviewSelectionEvidence,
    pub truncated: bool,
}
