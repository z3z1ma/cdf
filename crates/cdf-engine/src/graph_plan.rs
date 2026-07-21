use arrow_schema::DataType;
use cdf_contract::{ColumnProgramStep, RedactionDecision};
use cdf_kernel::{
    CdfError, EventTimeDomain, ExecutionExtent, OperatorWatermarkBehavior, Result, WatermarkPolicy,
    source_name,
};
use cdf_runtime::{
    CompiledOperatorGraph, CompiledSourcePlan, DestinationIngressMode,
    DestinationRuntimeCapabilities, DestinationWriterModel, GraphEdgeDescriptor, GraphEdgeTransfer,
    GraphExecutorClass, GraphNodeDescriptor, GraphNodeKind, GraphOrdering, SourceExecutorClass,
};

use crate::{CanonicalSegmentationPolicy, EnginePlan};

const OPERATOR_GRAPH_VERSION: &str = "p3-graph-v1";
const ENGINE_KERNEL_VERSION: &str = "engine-kernel-v1";
const PACKAGE_WRITER_VERSION: &str = "package-writer-v1";
const COMMIT_GATE_VERSION: &str = "commit-gate-v1";
const CONTROL_WORKING_SET_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy)]
struct WorkingSet {
    minimum_bytes: u64,
    maximum_bytes: u64,
}

impl WorkingSet {
    const fn new(minimum_bytes: u64, maximum_bytes: u64) -> Self {
        Self {
            minimum_bytes,
            maximum_bytes,
        }
    }
}

pub fn compile_operator_graph(
    plan: &EnginePlan,
    source: &CompiledSourcePlan,
    destination: &DestinationRuntimeCapabilities,
) -> Result<CompiledOperatorGraph> {
    source.validate()?;
    destination.validate()?;
    validate_watermark_semantics(plan, source)?;
    let segmentation_policy = plan.segmentation_policy()?;
    let compiled_stream_policy_hash = match &plan.execution_extent {
        ExecutionExtent::Bounded { .. } => None,
        ExecutionExtent::Drain { .. } => Some(
            plan.compiled_stream_policy
                .as_ref()
                .ok_or_else(|| {
                    CdfError::contract(
                        "drain operator graph requires a source-bound compiled stream policy",
                    )
                })?
                .semantic_hash
                .clone(),
        ),
        ExecutionExtent::Resident { .. } => {
            return Err(CdfError::contract(
                "resident operator graphs are not enabled",
            ));
        }
    };

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    nodes.push(source_node(source)?);
    if !source.execution_capabilities.canonical_order {
        nodes.push(engine_node(
            "canonical_reorder",
            GraphNodeKind::StatefulBarrier,
            WorkingSet::new(
                source.execution_capabilities.minimum_decode_bytes,
                source.execution_capabilities.maximum_decode_bytes,
            ),
            true,
            GraphOrdering::Canonical,
            None,
        ));
    }
    nodes.push(engine_node(
        "reconcile",
        GraphNodeKind::Reconcile,
        WorkingSet::new(
            source.execution_capabilities.minimum_decode_bytes,
            source.execution_capabilities.maximum_decode_bytes,
        ),
        false,
        GraphOrdering::PartitionLocal,
        Some("fused_transform_v1"),
    ));
    nodes.push(engine_node(
        "transform",
        GraphNodeKind::Transform,
        WorkingSet::new(
            source.execution_capabilities.minimum_decode_bytes,
            source.execution_capabilities.maximum_decode_bytes,
        ),
        false,
        GraphOrdering::PartitionLocal,
        Some("fused_transform_v1"),
    ));
    if source.execution_capabilities.canonical_order {
        edge(
            &mut edges,
            "source",
            "reconcile",
            GraphOrdering::PartitionLocal,
            GraphEdgeTransfer::Accounted,
        );
    } else {
        edge(
            &mut edges,
            "source",
            "canonical_reorder",
            GraphOrdering::Unordered,
            GraphEdgeTransfer::Accounted,
        );
        edge(
            &mut edges,
            "canonical_reorder",
            "reconcile",
            GraphOrdering::PartitionLocal,
            GraphEdgeTransfer::Accounted,
        );
    }
    edge(
        &mut edges,
        "reconcile",
        "transform",
        GraphOrdering::PartitionLocal,
        GraphEdgeTransfer::Fused,
    );

    let mut prior = "transform";
    if plan.validation_program.has_exact_row_dedup_rule()
        || plan.validation_program.has_keyed_dedup_rule()
    {
        nodes.push(engine_node(
            "package_dedup",
            GraphNodeKind::StatefulBarrier,
            WorkingSet::new(
                segmentation_policy.microbatch_minimum_bytes,
                segmentation_policy.maximum_bytes,
            ),
            true,
            GraphOrdering::Canonical,
            None,
        ));
        edge(
            &mut edges,
            prior,
            "package_dedup",
            GraphOrdering::Canonical,
            GraphEdgeTransfer::Accounted,
        );
        prior = "package_dedup";
    }

    nodes.push(engine_node(
        "segment_assembly",
        GraphNodeKind::SegmentAssembly,
        WorkingSet::new(
            segmentation_policy.microbatch_minimum_bytes,
            segmentation_policy.maximum_bytes,
        ),
        false,
        GraphOrdering::Canonical,
        None,
    ));
    edge(
        &mut edges,
        prior,
        "segment_assembly",
        GraphOrdering::Canonical,
        GraphEdgeTransfer::Accounted,
    );

    nodes.push(io_node(
        "segment_persist",
        GraphNodeKind::SegmentPersist,
        WorkingSet::new(
            segmentation_policy.target_bytes,
            segmentation_policy.maximum_bytes,
        ),
        true,
        GraphOrdering::Canonical,
        PACKAGE_WRITER_VERSION,
    ));
    edge(
        &mut edges,
        "segment_assembly",
        "segment_persist",
        GraphOrdering::Canonical,
        GraphEdgeTransfer::Accounted,
    );

    if destination.ingress_mode == DestinationIngressMode::StagedDurableSegments {
        nodes.push(destination_node(
            "staged_ingress",
            destination,
            segmentation_policy,
        )?);
        edge(
            &mut edges,
            "segment_persist",
            "staged_ingress",
            GraphOrdering::Canonical,
            GraphEdgeTransfer::Durable,
        );
        prior = "staged_ingress";
    } else {
        prior = "segment_persist";
    }

    nodes.push(io_node(
        "package_finalize",
        GraphNodeKind::PackageFinalize,
        WorkingSet::new(CONTROL_WORKING_SET_BYTES, segmentation_policy.maximum_bytes),
        true,
        GraphOrdering::Canonical,
        PACKAGE_WRITER_VERSION,
    ));
    edge(
        &mut edges,
        prior,
        "package_finalize",
        GraphOrdering::Canonical,
        if prior == "segment_persist" {
            GraphEdgeTransfer::Durable
        } else {
            GraphEdgeTransfer::Accounted
        },
    );

    nodes.push(destination_node(
        "destination_bind",
        destination,
        segmentation_policy,
    )?);
    edge(
        &mut edges,
        "package_finalize",
        "destination_bind",
        GraphOrdering::Canonical,
        GraphEdgeTransfer::Durable,
    );
    nodes.push(io_node(
        "commit_gate",
        GraphNodeKind::CommitGate,
        WorkingSet::new(CONTROL_WORKING_SET_BYTES, CONTROL_WORKING_SET_BYTES),
        false,
        GraphOrdering::Canonical,
        COMMIT_GATE_VERSION,
    ));
    edge(
        &mut edges,
        "destination_bind",
        "commit_gate",
        GraphOrdering::Canonical,
        GraphEdgeTransfer::Accounted,
    );
    let graph = CompiledOperatorGraph::new(
        OPERATOR_GRAPH_VERSION,
        plan.execution_extent.clone(),
        compiled_stream_policy_hash
            .as_ref()
            .map(|_| source.compiled_source_plan_hash())
            .transpose()?,
        compiled_stream_policy_hash,
        nodes,
        edges,
    )?;
    graph.validate_execution_extent(&plan.execution_extent)?;
    Ok(graph)
}

fn validate_watermark_semantics(plan: &EnginePlan, source: &CompiledSourcePlan) -> Result<()> {
    let ExecutionExtent::Drain { policy, .. } = &plan.execution_extent else {
        return Ok(());
    };
    let WatermarkPolicy::Enabled {
        event_time_field,
        domain,
        ..
    } = &policy.watermark
    else {
        return Ok(());
    };
    if plan.final_projection.as_ref().is_some_and(|projection| {
        !projection
            .iter()
            .any(|field| field == event_time_field.as_ref())
    }) {
        return Err(CdfError::contract(format!(
            "watermark event-time field `{event_time_field}` is removed by the final projection; retain it or disable watermarks"
        )));
    }
    let output_schema = plan.output_arrow_schema()?;
    let output_field = output_schema.field_with_name(event_time_field).map_err(|_| {
        CdfError::contract(format!(
            "watermark event-time field `{event_time_field}` is absent from the compiled output schema; name an existing typed output field or disable watermarks"
        ))
    })?;
    validate_event_time_domain(event_time_field, domain, output_field.data_type())?;

    let column = plan
        .validation_program
        .column_programs
        .iter()
        .find(|column| column.output_name == event_time_field.as_ref())
        .ok_or_else(|| {
            CdfError::contract(format!(
                "watermark event-time field `{event_time_field}` has no compiled column program"
            ))
        })?;
    if column.redaction != RedactionDecision::Preserve {
        return Err(CdfError::contract(format!(
            "watermark event-time field `{event_time_field}` is redacted by the compiled column program; preserve that field or disable watermarks"
        )));
    }
    if column
        .steps
        .iter()
        .any(|step| matches!(step, ColumnProgramStep::ApplyTransform(_)))
    {
        return Err(CdfError::contract(format!(
            "watermark event-time field `{event_time_field}` is changed by a transform without a named monotone watermark mapping; remove the transform, disable watermarks, or add an explicit source mapping"
        )));
    }
    let source_field = source
        .schema
        .fields()
        .iter()
        .find(|field| {
            field.name() == &column.source_name
                || field.name() == event_time_field.as_ref()
                || source_name(field.as_ref()) == Some(column.source_name.as_str())
        })
        .ok_or_else(|| {
            CdfError::contract(format!(
                "watermark event-time field `{event_time_field}` has no authoritative source field `{}`",
                column.source_name
            ))
        })?;
    validate_event_time_domain(event_time_field, domain, source_field.data_type())?;
    Ok(())
}

fn validate_event_time_domain(
    field: &str,
    domain: &EventTimeDomain,
    data_type: &DataType,
) -> Result<()> {
    if !domain.matches_arrow_type(data_type) {
        return Err(CdfError::contract(format!(
            "watermark event-time field `{field}` declares domain {domain:?} but its compiled Arrow type is {data_type}; change the watermark domain to match the field or choose another field"
        )));
    }
    Ok(())
}

fn source_node(source: &CompiledSourcePlan) -> Result<GraphNodeDescriptor> {
    let capabilities = &source.execution_capabilities;
    let (executor, blocking_lane) = match capabilities.executor_class {
        SourceExecutorClass::Io => (GraphExecutorClass::Io, None),
        SourceExecutorClass::Cpu => (GraphExecutorClass::Cpu, None),
        SourceExecutorClass::BlockingLane => (
            GraphExecutorClass::BlockingLane,
            Some(
                capabilities
                    .blocking_lane
                    .as_ref()
                    .ok_or_else(|| CdfError::contract("blocking source omitted its lane"))?
                    .lane_id
                    .clone(),
            ),
        ),
    };
    Ok(GraphNodeDescriptor {
        node_id: "source".to_owned(),
        kind: GraphNodeKind::Source,
        implementation_version: source.driver.driver_version.clone(),
        executor,
        blocking_lane,
        minimum_working_set_bytes: capabilities
            .minimum_poll_bytes
            .saturating_add(capabilities.minimum_decode_bytes),
        maximum_working_set_bytes: capabilities
            .maximum_poll_bytes
            .saturating_add(capabilities.maximum_decode_bytes),
        maximum_concurrency: capabilities.maximum_concurrency,
        spillable: capabilities.spillable,
        ordering: if capabilities.canonical_order {
            GraphOrdering::PartitionLocal
        } else {
            GraphOrdering::Unordered
        },
        execution_extent_hash: None,
        watermark_behavior: source
            .stream_capabilities
            .as_ref()
            .map_or(OperatorWatermarkBehavior::Preserve, |capabilities| {
                capabilities.watermark_behavior.clone()
            }),
        fusion_group: None,
        durable_output: false,
    })
}

fn engine_node(
    id: &str,
    kind: GraphNodeKind,
    working_set: WorkingSet,
    spillable: bool,
    ordering: GraphOrdering,
    fusion_group: Option<&str>,
) -> GraphNodeDescriptor {
    GraphNodeDescriptor {
        node_id: id.to_owned(),
        kind,
        implementation_version: ENGINE_KERNEL_VERSION.to_owned(),
        executor: GraphExecutorClass::Cpu,
        blocking_lane: None,
        minimum_working_set_bytes: working_set.minimum_bytes,
        maximum_working_set_bytes: working_set.maximum_bytes,
        maximum_concurrency: u16::MAX,
        spillable,
        ordering,
        execution_extent_hash: None,
        watermark_behavior: OperatorWatermarkBehavior::Preserve,
        fusion_group: fusion_group.map(str::to_owned),
        durable_output: false,
    }
}

fn io_node(
    id: &str,
    kind: GraphNodeKind,
    working_set: WorkingSet,
    durable_output: bool,
    ordering: GraphOrdering,
    version: &str,
) -> GraphNodeDescriptor {
    GraphNodeDescriptor {
        node_id: id.to_owned(),
        kind,
        implementation_version: version.to_owned(),
        executor: GraphExecutorClass::Io,
        blocking_lane: None,
        minimum_working_set_bytes: working_set.minimum_bytes,
        maximum_working_set_bytes: working_set.maximum_bytes,
        maximum_concurrency: u16::MAX,
        spillable: false,
        ordering,
        execution_extent_hash: None,
        watermark_behavior: OperatorWatermarkBehavior::Preserve,
        fusion_group: None,
        durable_output,
    }
}

fn destination_node(
    id: &str,
    destination: &DestinationRuntimeCapabilities,
    policy: &CanonicalSegmentationPolicy,
) -> Result<GraphNodeDescriptor> {
    let declared_lane = if id == "staged_ingress" {
        destination.staged_ingress_lane.as_deref()
    } else {
        destination.final_binding_lane.as_deref()
    };
    let (executor, lane) = match declared_lane {
        Some(lane) => (GraphExecutorClass::BlockingLane, Some(lane.to_owned())),
        None => (GraphExecutorClass::Io, None),
    };
    let maximum_bytes = destination
        .max_in_flight_bytes
        .unwrap_or(policy.maximum_bytes);
    if maximum_bytes < policy.microbatch_minimum_bytes {
        return Err(CdfError::contract(format!(
            "destination in-flight byte bound {maximum_bytes} is below the graph microbatch minimum {}",
            policy.microbatch_minimum_bytes
        )));
    }
    let maximum_concurrency = match destination.writer_model {
        DestinationWriterModel::SingleWriter => 1,
        DestinationWriterModel::ConcurrentSegments => {
            destination.max_in_flight_segments.unwrap_or(u16::MAX)
        }
    };
    Ok(GraphNodeDescriptor {
        node_id: id.to_owned(),
        kind: if id == "staged_ingress" {
            GraphNodeKind::StagedIngress
        } else {
            GraphNodeKind::DestinationBind
        },
        implementation_version: destination
            .bulk_evidence_version
            .clone()
            .unwrap_or_else(|| "destination-driver-v1".to_owned()),
        executor,
        blocking_lane: lane,
        minimum_working_set_bytes: policy.microbatch_minimum_bytes,
        maximum_working_set_bytes: maximum_bytes,
        maximum_concurrency,
        spillable: false,
        ordering: GraphOrdering::Canonical,
        execution_extent_hash: None,
        watermark_behavior: OperatorWatermarkBehavior::Preserve,
        fusion_group: None,
        durable_output: false,
    })
}

fn edge(
    edges: &mut Vec<GraphEdgeDescriptor>,
    producer: &str,
    consumer: &str,
    ordering: GraphOrdering,
    transfer: GraphEdgeTransfer,
) {
    edges.push(GraphEdgeDescriptor {
        edge_id: format!("{producer}_to_{consumer}"),
        producer: producer.to_owned(),
        consumer: consumer.to_owned(),
        ordering,
        transfer,
    });
}
