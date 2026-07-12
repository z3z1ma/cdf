use cdf_kernel::{CdfError, Result};
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

pub fn compile_operator_graph(
    plan: &EnginePlan,
    source: &CompiledSourcePlan,
    destination: &DestinationRuntimeCapabilities,
) -> Result<CompiledOperatorGraph> {
    source.validate()?;
    destination.validate()?;
    let policy = plan.segmentation_policy()?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    nodes.push(source_node(source)?);
    if !source.execution_capabilities.canonical_order {
        nodes.push(engine_node(
            "canonical_reorder",
            GraphNodeKind::StatefulBarrier,
            source.execution_capabilities.minimum_decode_bytes,
            source.execution_capabilities.maximum_decode_bytes,
            true,
            GraphOrdering::Canonical,
            None,
        ));
    }
    nodes.push(engine_node(
        "reconcile",
        GraphNodeKind::Reconcile,
        source.execution_capabilities.minimum_decode_bytes,
        source.execution_capabilities.maximum_decode_bytes,
        false,
        GraphOrdering::PartitionLocal,
        Some("fused_transform_v1"),
    ));
    nodes.push(engine_node(
        "transform",
        GraphNodeKind::Transform,
        source.execution_capabilities.minimum_decode_bytes,
        source.execution_capabilities.maximum_decode_bytes,
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
            policy.microbatch_minimum_bytes,
            policy.maximum_bytes,
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
        policy.microbatch_minimum_bytes,
        policy.maximum_bytes,
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
        policy.target_bytes,
        policy.maximum_bytes,
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
        nodes.push(destination_node("staged_ingress", destination, policy)?);
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
        CONTROL_WORKING_SET_BYTES,
        policy.maximum_bytes,
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

    nodes.push(destination_node("destination_bind", destination, policy)?);
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
        CONTROL_WORKING_SET_BYTES,
        CONTROL_WORKING_SET_BYTES,
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
    CompiledOperatorGraph::new(OPERATOR_GRAPH_VERSION, nodes, edges)
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
        fusion_group: None,
        durable_output: false,
    })
}

fn engine_node(
    id: &str,
    kind: GraphNodeKind,
    minimum_bytes: u64,
    maximum_bytes: u64,
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
        minimum_working_set_bytes: minimum_bytes,
        maximum_working_set_bytes: maximum_bytes,
        maximum_concurrency: u16::MAX,
        spillable,
        ordering,
        fusion_group: fusion_group.map(str::to_owned),
        durable_output: false,
    }
}

fn io_node(
    id: &str,
    kind: GraphNodeKind,
    minimum_bytes: u64,
    maximum_bytes: u64,
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
        minimum_working_set_bytes: minimum_bytes,
        maximum_working_set_bytes: maximum_bytes,
        maximum_concurrency: u16::MAX,
        spillable: false,
        ordering,
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
