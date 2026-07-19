use std::{collections::BTreeSet, sync::Arc};

use cdf_kernel::{
    CdfError, ExecutionExtent, OperatorWatermarkBehavior, PartitionId, Result, SchemaHash,
    SourcePosition, WatermarkAuthority, WatermarkPolicy,
};
use cdf_memory::{
    AccountedBatch, AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease,
    ReservationRequest, reserve,
};
use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt, future::Either};
use serde::{Deserialize, Serialize};

use crate::{
    DestinationIngressMode, DestinationRuntimeCapabilities, DestinationWriterModel,
    RunCancellation, artifact_hash,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeKind {
    Source,
    Reconcile,
    Transform,
    StatefulBarrier,
    SegmentAssembly,
    SegmentPersist,
    StagedIngress,
    PackageFinalize,
    DestinationBind,
    CommitGate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphExecutorClass {
    Io,
    Cpu,
    BlockingLane,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphOrdering {
    Unordered,
    PartitionLocal,
    Canonical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphEdgeTransfer {
    Fused,
    Accounted,
    Durable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNodeDescriptor {
    pub node_id: String,
    pub kind: GraphNodeKind,
    pub implementation_version: String,
    pub executor: GraphExecutorClass,
    pub blocking_lane: Option<String>,
    pub minimum_working_set_bytes: u64,
    pub maximum_working_set_bytes: u64,
    pub maximum_concurrency: u16,
    pub spillable: bool,
    pub ordering: GraphOrdering,
    /// `None` is the canonical reference to bounded-v1. Drain nodes carry the hash of the one
    /// graph-level extent instead of duplicating arbitrarily large source-frontier payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_extent_hash: Option<String>,
    #[serde(
        default = "preserved_watermark_behavior",
        skip_serializing_if = "watermark_behavior_is_preserve"
    )]
    pub watermark_behavior: OperatorWatermarkBehavior,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fusion_group: Option<String>,
    pub durable_output: bool,
}

impl GraphNodeDescriptor {
    pub fn validate(&self) -> Result<()> {
        validate_token("graph node id", &self.node_id)?;
        validate_token(
            "graph node implementation version",
            &self.implementation_version,
        )?;
        if self.minimum_working_set_bytes == 0
            || self.maximum_working_set_bytes < self.minimum_working_set_bytes
            || self.maximum_concurrency == 0
        {
            return Err(CdfError::contract(format!(
                "graph node `{}` requires nonzero ordered working-set and concurrency bounds",
                self.node_id
            )));
        }
        match (self.executor, self.blocking_lane.as_deref()) {
            (GraphExecutorClass::BlockingLane, Some(lane)) => {
                validate_token("graph blocking lane", lane)?;
            }
            (GraphExecutorClass::BlockingLane, None) => {
                return Err(CdfError::contract(format!(
                    "blocking graph node `{}` requires a declared lane",
                    self.node_id
                )));
            }
            (_, None) => {}
            (_, Some(_)) => {
                return Err(CdfError::contract(format!(
                    "nonblocking graph node `{}` cannot declare a blocking lane",
                    self.node_id
                )));
            }
        }
        if let Some(group) = &self.fusion_group {
            validate_token("graph fusion group", group)?;
            if self.durable_output || self.kind == GraphNodeKind::StatefulBarrier {
                return Err(CdfError::contract(format!(
                    "durable or stateful graph node `{}` cannot belong to a fusion group",
                    self.node_id
                )));
            }
        }
        if let Some(hash) = &self.execution_extent_hash {
            crate::validate_artifact_hash("graph node execution extent", hash)?;
        }
        self.watermark_behavior.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdgeDescriptor {
    pub edge_id: String,
    pub producer: String,
    pub consumer: String,
    pub ordering: GraphOrdering,
    pub transfer: GraphEdgeTransfer,
}

impl GraphEdgeDescriptor {
    pub fn validate(&self) -> Result<()> {
        validate_token("graph edge id", &self.edge_id)?;
        validate_token("graph edge producer", &self.producer)?;
        validate_token("graph edge consumer", &self.consumer)?;
        if self.producer == self.consumer {
            return Err(CdfError::contract(format!(
                "graph edge `{}` cannot connect a node to itself",
                self.edge_id
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledOperatorGraph {
    pub graph_version: String,
    #[serde(
        default = "ExecutionExtent::bounded",
        skip_serializing_if = "ExecutionExtent::is_bounded"
    )]
    pub execution_extent: ExecutionExtent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_source_plan_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_stream_policy_hash: Option<String>,
    pub nodes: Vec<GraphNodeDescriptor>,
    pub edges: Vec<GraphEdgeDescriptor>,
    pub semantic_hash: String,
}

#[derive(Serialize)]
struct GraphIdentity<'a> {
    graph_version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_extent: Option<&'a ExecutionExtent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compiled_source_plan_hash: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compiled_stream_policy_hash: Option<&'a String>,
    nodes: &'a [GraphNodeDescriptor],
    edges: &'a [GraphEdgeDescriptor],
}

impl CompiledOperatorGraph {
    pub fn new(
        graph_version: impl Into<String>,
        execution_extent: ExecutionExtent,
        compiled_source_plan_hash: Option<String>,
        compiled_stream_policy_hash: Option<String>,
        mut nodes: Vec<GraphNodeDescriptor>,
        mut edges: Vec<GraphEdgeDescriptor>,
    ) -> Result<Self> {
        let graph_version = graph_version.into();
        validate_token("operator graph version", &graph_version)?;
        execution_extent.validate_for_plan()?;
        validate_authority_hashes_for_extent(
            &execution_extent,
            compiled_source_plan_hash.as_deref(),
            compiled_stream_policy_hash.as_deref(),
        )?;
        let execution_extent_hash = (!execution_extent.is_bounded())
            .then(|| artifact_hash(&execution_extent))
            .transpose()?;
        for node in &mut nodes {
            node.execution_extent_hash = execution_extent_hash.clone();
        }
        validate_graph(&nodes, &edges)?;
        nodes = canonical_node_order(&nodes, &edges)?;
        edges.sort_by(|left, right| left.edge_id.cmp(&right.edge_id));
        let semantic_hash = artifact_hash(&GraphIdentity {
            graph_version: &graph_version,
            execution_extent: (!execution_extent.is_bounded()).then_some(&execution_extent),
            compiled_source_plan_hash: compiled_source_plan_hash.as_ref(),
            compiled_stream_policy_hash: compiled_stream_policy_hash.as_ref(),
            nodes: &nodes,
            edges: &edges,
        })?;
        Ok(Self {
            graph_version,
            execution_extent,
            compiled_source_plan_hash,
            compiled_stream_policy_hash,
            nodes,
            edges,
            semantic_hash,
        })
    }

    pub fn validate(&self) -> Result<()> {
        validate_token("operator graph version", &self.graph_version)?;
        self.execution_extent.validate_for_plan()?;
        validate_authority_hashes_for_extent(
            &self.execution_extent,
            self.compiled_source_plan_hash.as_deref(),
            self.compiled_stream_policy_hash.as_deref(),
        )?;
        validate_graph(&self.nodes, &self.edges)?;
        self.validate_node_extent_references()?;
        if canonical_node_order(&self.nodes, &self.edges)? != self.nodes {
            return Err(CdfError::contract(
                "compiled operator graph nodes are not in canonical topological order",
            ));
        }
        if self
            .edges
            .windows(2)
            .any(|pair| pair[0].edge_id >= pair[1].edge_id)
        {
            return Err(CdfError::contract(
                "compiled operator graph edges are not in canonical id order",
            ));
        }
        let expected = artifact_hash(&GraphIdentity {
            graph_version: &self.graph_version,
            execution_extent: (!self.execution_extent.is_bounded())
                .then_some(&self.execution_extent),
            compiled_source_plan_hash: self.compiled_source_plan_hash.as_ref(),
            compiled_stream_policy_hash: self.compiled_stream_policy_hash.as_ref(),
            nodes: &self.nodes,
            edges: &self.edges,
        })?;
        if expected != self.semantic_hash {
            return Err(CdfError::contract(
                "compiled operator graph semantic hash does not match its nodes and edges",
            ));
        }
        Ok(())
    }

    pub fn validate_destination_join(
        &self,
        destination: &DestinationRuntimeCapabilities,
    ) -> Result<()> {
        self.validate()?;
        destination.validate()?;
        let staged = self
            .nodes
            .iter()
            .filter(|node| node.kind == GraphNodeKind::StagedIngress)
            .collect::<Vec<_>>();
        let binds = self
            .nodes
            .iter()
            .filter(|node| node.kind == GraphNodeKind::DestinationBind)
            .collect::<Vec<_>>();
        if binds.len() != 1 {
            return Err(CdfError::contract(
                "compiled operator graph requires exactly one destination binding node",
            ));
        }
        match destination.ingress_mode {
            DestinationIngressMode::FinalizedPackageOnly if !staged.is_empty() => {
                return Err(CdfError::contract(
                    "compiled graph stages segments but the resolved destination is finalized-only; rebuild the plan for the selected destination",
                ));
            }
            DestinationIngressMode::StagedDurableSegments if staged.len() != 1 => {
                return Err(CdfError::contract(
                    "compiled graph omits its single staged-ingress node; rebuild the plan for the selected destination",
                ));
            }
            _ => {}
        }
        for (label, node, declared_lane) in [
            (
                "staged ingress",
                staged.first().copied(),
                destination.staged_ingress_lane.as_deref(),
            ),
            (
                "final binding",
                binds.first().copied(),
                destination.final_binding_lane.as_deref(),
            ),
        ] {
            let Some(node) = node else {
                continue;
            };
            match (declared_lane, node.executor, node.blocking_lane.as_deref()) {
                (Some(expected), GraphExecutorClass::BlockingLane, Some(actual))
                    if expected == actual => {}
                (None, GraphExecutorClass::Io, None) => {}
                _ => {
                    return Err(CdfError::contract(format!(
                        "compiled graph {label} executor/lane no longer matches the resolved destination; rebuild the plan"
                    )));
                }
            }
            if let Some(maximum_bytes) = destination.max_in_flight_bytes
                && node.maximum_working_set_bytes != maximum_bytes
            {
                return Err(CdfError::contract(format!(
                    "compiled graph {label} byte bound {} differs from resolved destination bound {maximum_bytes}; rebuild the plan",
                    node.maximum_working_set_bytes
                )));
            }
            let expected_concurrency = match destination.writer_model {
                DestinationWriterModel::SingleWriter => 1,
                DestinationWriterModel::ConcurrentSegments => {
                    destination.max_in_flight_segments.unwrap_or(u16::MAX)
                }
            };
            if node.maximum_concurrency != expected_concurrency {
                return Err(CdfError::contract(format!(
                    "compiled graph {label} concurrency {} differs from resolved destination concurrency {expected_concurrency}; rebuild the plan",
                    node.maximum_concurrency
                )));
            }
        }
        Ok(())
    }

    pub fn validate_execution_extent(&self, extent: &ExecutionExtent) -> Result<()> {
        self.validate()?;
        extent.validate_for_plan()?;
        if &self.execution_extent != extent {
            return Err(CdfError::contract(
                "operator graph execution extent differs from the compiled plan extent",
            ));
        }
        let ExecutionExtent::Drain { policy, .. } = extent else {
            return Ok(());
        };
        let WatermarkPolicy::Enabled { authority, .. } = &policy.watermark else {
            return Ok(());
        };
        for node in &self.nodes {
            match &node.watermark_behavior {
                OperatorWatermarkBehavior::Preserve => {}
                OperatorWatermarkBehavior::Transform { mapping_id }
                    if matches!(
                        authority,
                        WatermarkAuthority::Derived {
                            mapping_id: expected
                        } if expected == mapping_id
                    ) => {}
                OperatorWatermarkBehavior::Transform { mapping_id } => {
                    return Err(CdfError::contract(format!(
                        "operator `{}` transforms watermarks with mapping `{mapping_id}` but the stream policy records a different authority mapping",
                        node.node_id
                    )));
                }
                OperatorWatermarkBehavior::Drop => {
                    return Err(CdfError::contract(format!(
                        "operator `{}` drops the declared watermark; retain the event-time authority, declare its monotone transform, or disable watermarks",
                        node.node_id
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn validate_plan_join(
        &self,
        extent: &ExecutionExtent,
        policy: Option<&crate::CompiledStreamPolicy>,
    ) -> Result<()> {
        self.validate_execution_extent(extent)?;
        match (
            extent,
            policy,
            self.compiled_source_plan_hash.as_deref(),
            self.compiled_stream_policy_hash.as_deref(),
        ) {
            (ExecutionExtent::Bounded { .. }, None, None, None) => Ok(()),
            (ExecutionExtent::Drain { .. }, Some(policy), Some(source_hash), Some(policy_hash))
                if policy_hash == policy.semantic_hash
                    && source_hash == policy.compiled_source_plan_hash
                    && policy.execution_extent == *extent =>
            {
                policy.validate_intrinsic()
            }
            (ExecutionExtent::Drain { .. }, _, _, _) => Err(CdfError::contract(
                "drain operator graph does not bind the plan's exact compiled stream policy",
            )),
            _ => Err(CdfError::contract(
                "operator graph stream-policy binding is invalid for its execution extent",
            )),
        }
    }

    fn validate_node_extent_references(&self) -> Result<()> {
        let expected = (!self.execution_extent.is_bounded())
            .then(|| artifact_hash(&self.execution_extent))
            .transpose()?;
        if let Some(node) = self
            .nodes
            .iter()
            .find(|node| node.execution_extent_hash != expected)
        {
            return Err(CdfError::contract(format!(
                "operator `{}` does not reference the graph's exact execution extent",
                node.node_id
            )));
        }
        Ok(())
    }
}

fn validate_authority_hashes_for_extent(
    extent: &ExecutionExtent,
    source_hash: Option<&str>,
    policy_hash: Option<&str>,
) -> Result<()> {
    match (extent, source_hash, policy_hash) {
        (ExecutionExtent::Bounded { .. }, None, None) => Ok(()),
        (ExecutionExtent::Drain { .. }, Some(source_hash), Some(policy_hash)) => {
            crate::validate_artifact_hash("compiled source plan", source_hash)?;
            crate::validate_artifact_hash("compiled stream policy", policy_hash)
        }
        (ExecutionExtent::Drain { .. }, _, _) => Err(CdfError::contract(
            "drain operator graph requires compiled source-plan and stream-policy hashes",
        )),
        (ExecutionExtent::Bounded { .. }, _, _) => Err(CdfError::contract(
            "bounded operator graph cannot declare source-plan or stream-policy hashes",
        )),
        (ExecutionExtent::Resident { .. }, _, _) => Err(CdfError::contract(
            "resident operator graphs are not enabled",
        )),
    }
}

fn preserved_watermark_behavior() -> OperatorWatermarkBehavior {
    OperatorWatermarkBehavior::Preserve
}

fn watermark_behavior_is_preserve(behavior: &OperatorWatermarkBehavior) -> bool {
    *behavior == OperatorWatermarkBehavior::Preserve
}

fn validate_graph(nodes: &[GraphNodeDescriptor], edges: &[GraphEdgeDescriptor]) -> Result<()> {
    if nodes.is_empty() {
        return Err(CdfError::contract(
            "compiled operator graph requires at least one semantic node",
        ));
    }
    let mut node_ids = BTreeSet::new();
    for node in nodes {
        node.validate()?;
        if !node_ids.insert(node.node_id.as_str()) {
            return Err(CdfError::contract(format!(
                "compiled operator graph repeats node id `{}`",
                node.node_id
            )));
        }
    }
    let mut edge_ids = BTreeSet::new();
    for edge in edges {
        edge.validate()?;
        if !edge_ids.insert(edge.edge_id.as_str()) {
            return Err(CdfError::contract(format!(
                "compiled operator graph repeats edge id `{}`",
                edge.edge_id
            )));
        }
        if !node_ids.contains(edge.producer.as_str()) || !node_ids.contains(edge.consumer.as_str())
        {
            return Err(CdfError::contract(format!(
                "graph edge `{}` references an unknown producer or consumer",
                edge.edge_id
            )));
        }
        let producer = nodes
            .iter()
            .find(|node| node.node_id == edge.producer)
            .expect("producer membership checked");
        let consumer = nodes
            .iter()
            .find(|node| node.node_id == edge.consumer)
            .expect("consumer membership checked");
        if (edge.transfer == GraphEdgeTransfer::Durable) != producer.durable_output {
            return Err(CdfError::contract(format!(
                "graph edge `{}` durable transfer disagrees with producer `{}`",
                edge.edge_id, producer.node_id
            )));
        }
        match edge.transfer {
            GraphEdgeTransfer::Fused
                if producer.fusion_group.is_none()
                    || producer.fusion_group != consumer.fusion_group =>
            {
                return Err(CdfError::contract(format!(
                    "fused graph edge `{}` requires the same declared fusion group on both nodes",
                    edge.edge_id
                )));
            }
            GraphEdgeTransfer::Accounted | GraphEdgeTransfer::Durable
                if producer.fusion_group.is_some()
                    && producer.fusion_group == consumer.fusion_group =>
            {
                return Err(CdfError::contract(format!(
                    "graph edge `{}` cannot transfer ownership inside fusion group {:?}",
                    edge.edge_id, producer.fusion_group
                )));
            }
            _ => {}
        }
    }
    ensure_acyclic(nodes, edges)
}

fn ensure_acyclic(nodes: &[GraphNodeDescriptor], edges: &[GraphEdgeDescriptor]) -> Result<()> {
    let mut remaining = nodes
        .iter()
        .map(|node| {
            let incoming = edges
                .iter()
                .filter(|edge| edge.consumer == node.node_id)
                .count();
            (node.node_id.as_str(), incoming)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut ready = remaining
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<Vec<_>>();
    let mut visited = 0usize;
    while let Some(node) = ready.pop() {
        if remaining.remove(node).is_none() {
            continue;
        }
        visited += 1;
        for edge in edges.iter().filter(|edge| edge.producer == node) {
            if let Some(count) = remaining.get_mut(edge.consumer.as_str()) {
                *count -= 1;
                if *count == 0 {
                    ready.push(edge.consumer.as_str());
                }
            }
        }
    }
    if visited != nodes.len() {
        return Err(CdfError::contract(
            "compiled operator graph must be acyclic",
        ));
    }
    Ok(())
}

fn canonical_node_order(
    nodes: &[GraphNodeDescriptor],
    edges: &[GraphEdgeDescriptor],
) -> Result<Vec<GraphNodeDescriptor>> {
    let by_id = nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut incoming = nodes
        .iter()
        .map(|node| {
            (
                node.node_id.as_str(),
                edges
                    .iter()
                    .filter(|edge| edge.consumer == node.node_id)
                    .count(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut ready = incoming
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(node_id) = ready.pop_first() {
        if incoming.remove(node_id).is_none() {
            continue;
        }
        ordered.push(
            (*by_id
                .get(node_id)
                .ok_or_else(|| CdfError::internal("canonical graph node disappeared"))?)
            .clone(),
        );
        for edge in edges.iter().filter(|edge| edge.producer == node_id) {
            if let Some(count) = incoming.get_mut(edge.consumer.as_str()) {
                *count -= 1;
                if *count == 0 {
                    ready.insert(edge.consumer.as_str());
                }
            }
        }
    }
    if ordered.len() != nodes.len() {
        return Err(CdfError::contract(
            "compiled operator graph must be acyclic",
        ));
    }
    Ok(ordered)
}

#[derive(Clone, Debug)]
pub enum AccountedGraphPayload {
    Arrow(AccountedBatch),
    Bytes(AccountedBytes),
}

impl AccountedGraphPayload {
    pub fn accounted_bytes(&self) -> u64 {
        match self {
            Self::Arrow(batch) => batch.lease().bytes(),
            Self::Bytes(bytes) => bytes.lease().bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphOutcome {
    pub outcome_id: String,
    pub fact_count: u64,
    pub encoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct AccountedGraphOutcomes<O> {
    outcomes: Vec<O>,
    encoded_bytes: u64,
    lease: Option<MemoryLease>,
}

impl<O> Default for AccountedGraphOutcomes<O> {
    fn default() -> Self {
        Self {
            outcomes: Vec::new(),
            encoded_bytes: 0,
            lease: None,
        }
    }
}

impl<O> AccountedGraphOutcomes<O> {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn new(outcomes: Vec<O>, encoded_bytes: u64, lease: MemoryLease) -> Result<Self> {
        if outcomes.is_empty() {
            return Err(CdfError::contract(
                "an accounted outcome set cannot be empty; use AccountedGraphOutcomes::none",
            ));
        }
        if encoded_bytes == 0 {
            return Err(CdfError::contract(
                "nonempty graph outcomes require a nonzero encoded byte count",
            ));
        }
        if lease.bytes() < encoded_bytes {
            return Err(CdfError::data(format!(
                "graph outcomes require {encoded_bytes} accounted bytes but lease holds {}",
                lease.bytes()
            )));
        }
        lease.reconcile(encoded_bytes)?;
        Ok(Self {
            outcomes,
            encoded_bytes,
            lease: Some(lease),
        })
    }

    pub fn outcomes(&self) -> &[O] {
        &self.outcomes
    }

    pub fn accounted_bytes(&self) -> u64 {
        self.lease.as_ref().map_or(0, MemoryLease::bytes)
    }

    fn validate(&self) -> Result<()> {
        match (&self.outcomes[..], &self.lease) {
            ([], None) => Ok(()),
            ([], Some(_)) | (_, None) => Err(CdfError::internal(
                "graph outcome metadata and its memory lease must have the same lifetime",
            )),
            (outcomes, Some(lease)) => {
                if outcomes.is_empty()
                    || self.encoded_bytes == 0
                    || self.encoded_bytes != lease.bytes()
                {
                    return Err(CdfError::internal(
                        "graph outcome memory lease no longer matches encoded outcome bytes",
                    ));
                }
                Ok(())
            }
        }
    }
}

impl GraphOutcome {
    pub fn new(outcome_id: impl Into<String>, fact_count: u64, encoded_bytes: u64) -> Result<Self> {
        let outcome_id = outcome_id.into();
        validate_token("graph outcome id", &outcome_id)?;
        if fact_count == 0 || encoded_bytes == 0 {
            return Err(CdfError::contract(
                "graph outcomes require nonzero fact and encoded-byte counts",
            ));
        }
        Ok(Self {
            outcome_id,
            fact_count,
            encoded_bytes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphSchemaAuthority {
    pub observed_schema_hash: SchemaHash,
    pub effective_schema_hash: SchemaHash,
    pub coercion_plan_hash: Option<SchemaHash>,
}

#[derive(Clone, Debug)]
pub struct GraphDataEnvelope<O> {
    pub partition_ordinal: u64,
    pub partition_id: PartitionId,
    pub local_sequence: u64,
    pub source_position: Option<SourcePosition>,
    pub schema_authority: GraphSchemaAuthority,
    pub outcomes: AccountedGraphOutcomes<O>,
    pub payload: AccountedGraphPayload,
}

impl<O> GraphDataEnvelope<O> {
    pub fn validate(&self, maximum_outcomes: usize, maximum_outcome_bytes: u64) -> Result<()> {
        self.outcomes.validate()?;
        if self.outcomes.outcomes().len() > maximum_outcomes {
            return Err(CdfError::data(format!(
                "graph envelope carries {} outcomes above edge bound {maximum_outcomes}",
                self.outcomes.outcomes().len()
            )));
        }
        let outcome_bytes = self.outcomes.accounted_bytes();
        if outcome_bytes > maximum_outcome_bytes {
            return Err(CdfError::data(format!(
                "graph envelope carries {outcome_bytes} outcome bytes above edge bound {maximum_outcome_bytes}"
            )));
        }
        Ok(())
    }

    pub fn accounted_bytes(&self) -> u64 {
        self.payload
            .accounted_bytes()
            .saturating_add(self.outcomes.accounted_bytes())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphEdgeRuntimeConfig {
    pub edge_id: String,
    pub maximum_items: usize,
    pub maximum_outcomes_per_item: usize,
    pub maximum_outcome_bytes_per_item: u64,
}

impl GraphEdgeRuntimeConfig {
    pub fn validate(&self) -> Result<()> {
        validate_token("graph runtime edge id", &self.edge_id)?;
        if self.maximum_items == 0
            || self.maximum_outcomes_per_item == 0
            || self.maximum_outcome_bytes_per_item == 0
        {
            return Err(CdfError::contract(
                "graph runtime edge bounds must be nonzero",
            ));
        }
        Ok(())
    }
}

pub struct GraphEdgeSender<O> {
    sender: mpsc::Sender<GraphDataEnvelope<O>>,
    config: Arc<GraphEdgeRuntimeConfig>,
    cancellation: RunCancellation,
}

pub struct GraphEdgeReceiver<O> {
    receiver: mpsc::Receiver<GraphDataEnvelope<O>>,
    cancellation: RunCancellation,
}

pub fn graph_edge<O>(
    config: GraphEdgeRuntimeConfig,
    cancellation: RunCancellation,
) -> Result<(GraphEdgeSender<O>, GraphEdgeReceiver<O>)> {
    config.validate()?;
    let (sender, receiver) = mpsc::channel(config.maximum_items);
    Ok((
        GraphEdgeSender {
            sender,
            config: Arc::new(config),
            cancellation: cancellation.clone(),
        },
        GraphEdgeReceiver {
            receiver,
            cancellation,
        },
    ))
}

impl<O> GraphEdgeSender<O> {
    pub async fn send(&mut self, envelope: GraphDataEnvelope<O>) -> Result<()> {
        self.cancellation.check()?;
        envelope.validate(
            self.config.maximum_outcomes_per_item,
            self.config.maximum_outcome_bytes_per_item,
        )?;
        let send = self.sender.send(envelope);
        let cancelled = self.cancellation.cancelled();
        futures_util::pin_mut!(send, cancelled);
        match futures_util::future::select(send, cancelled).await {
            Either::Left((result, _)) => {
                result.map_err(|_| {
                    CdfError::internal(format!(
                        "graph edge `{}` closed before accepting an accounted envelope",
                        self.config.edge_id
                    ))
                })?;
                self.cancellation.check()
            }
            Either::Right(((), _)) => self.cancellation.check(),
        }
    }
}

impl<O> GraphEdgeReceiver<O> {
    pub async fn receive(&mut self) -> Result<Option<GraphDataEnvelope<O>>> {
        self.cancellation.check()?;
        let receive = self.receiver.next();
        let cancelled = self.cancellation.cancelled();
        futures_util::pin_mut!(receive, cancelled);
        match futures_util::future::select(receive, cancelled).await {
            Either::Left((item, _)) => {
                self.cancellation.check()?;
                Ok(item)
            }
            Either::Right(((), _)) => {
                self.cancellation.check()?;
                unreachable!("cancelled future completes only after cancellation")
            }
        }
    }
}

pub async fn account_graph_batch(
    memory: Arc<dyn MemoryCoordinator>,
    consumer_name: impl Into<String>,
    batch: arrow_array::RecordBatch,
) -> Result<AccountedGraphPayload> {
    let bytes = cdf_memory::record_batch_retained_bytes(&batch)?;
    let request =
        ReservationRequest::new(ConsumerKey::new(consumer_name, MemoryClass::Queue)?, bytes)?;
    let lease = reserve(memory, request).await?;
    Ok(AccountedGraphPayload::Arrow(AccountedBatch::new(
        batch, lease,
    )?))
}

pub async fn account_graph_outcomes<O>(
    memory: Arc<dyn MemoryCoordinator>,
    consumer_name: impl Into<String>,
    outcomes: Vec<O>,
    encoded_bytes: u64,
) -> Result<AccountedGraphOutcomes<O>> {
    if outcomes.is_empty() {
        if encoded_bytes != 0 {
            return Err(CdfError::contract(
                "empty graph outcomes cannot declare encoded bytes",
            ));
        }
        return Ok(AccountedGraphOutcomes::none());
    }
    let request = ReservationRequest::new(
        ConsumerKey::new(consumer_name, MemoryClass::Control)?,
        encoded_bytes,
    )?;
    let lease = reserve(memory, request).await?;
    AccountedGraphOutcomes::new(outcomes, encoded_bytes, lease)
}

fn validate_token(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(CdfError::contract(format!(
            "{label} must contain 1..=128 safe ASCII token bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        DrainTermination, EpochClosureTrigger, PartitionId, PartitionWatermarkAggregation,
        SafeFrontierPolicy, SchemaHash, SourcePosition, StreamEpochPolicy, WatermarkAuthority,
        WatermarkPolicy,
    };
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use futures_util::{FutureExt, pin_mut};

    use super::*;

    fn schema_authority() -> GraphSchemaAuthority {
        let hash = SchemaHash::new(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        GraphSchemaAuthority {
            observed_schema_hash: hash.clone(),
            effective_schema_hash: hash,
            coercion_plan_hash: None,
        }
    }

    fn node(id: &str, kind: GraphNodeKind, durable_output: bool) -> GraphNodeDescriptor {
        GraphNodeDescriptor {
            node_id: id.to_owned(),
            kind,
            implementation_version: "v1".to_owned(),
            executor: GraphExecutorClass::Cpu,
            blocking_lane: None,
            minimum_working_set_bytes: 1,
            maximum_working_set_bytes: 1024,
            maximum_concurrency: 1,
            spillable: false,
            ordering: GraphOrdering::Canonical,
            execution_extent_hash: None,
            watermark_behavior: OperatorWatermarkBehavior::Preserve,
            fusion_group: None,
            durable_output,
        }
    }

    #[test]
    fn graph_identity_excludes_runtime_edge_pressure_configuration() {
        let graph = CompiledOperatorGraph::new(
            "graph-v1",
            ExecutionExtent::bounded(),
            None,
            None,
            vec![
                node("mock_source", GraphNodeKind::Source, false),
                GraphNodeDescriptor {
                    implementation_version: "external-operator-v7".to_owned(),
                    maximum_concurrency: 4,
                    ..node("external_transform", GraphNodeKind::Transform, false)
                },
                node("mock_destination", GraphNodeKind::DestinationBind, false),
            ],
            vec![
                GraphEdgeDescriptor {
                    edge_id: "source_to_transform".to_owned(),
                    producer: "mock_source".to_owned(),
                    consumer: "external_transform".to_owned(),
                    ordering: GraphOrdering::Canonical,
                    transfer: GraphEdgeTransfer::Accounted,
                },
                GraphEdgeDescriptor {
                    edge_id: "transform_to_destination".to_owned(),
                    producer: "external_transform".to_owned(),
                    consumer: "mock_destination".to_owned(),
                    ordering: GraphOrdering::Canonical,
                    transfer: GraphEdgeTransfer::Accounted,
                },
            ],
        )
        .unwrap();
        graph.validate().unwrap();
        let encoded = serde_json::to_value(&graph).unwrap();
        assert!(encoded.get("runtime_capacity").is_none());
        assert!(encoded.get("execution_extent").is_none());
        assert!(encoded.get("compiled_source_plan_hash").is_none());
        assert!(encoded.get("compiled_stream_policy_hash").is_none());
        assert!(encoded["nodes"].as_array().unwrap().iter().all(|node| {
            node.get("execution_extent_hash").is_none() && node.get("watermark_behavior").is_none()
        }));
        assert!(graph.semantic_hash.starts_with("sha256:"));
        let shuffled = CompiledOperatorGraph::new(
            "graph-v1",
            ExecutionExtent::bounded(),
            None,
            None,
            graph.nodes.iter().cloned().rev().collect(),
            graph.edges.iter().cloned().rev().collect(),
        )
        .unwrap();
        assert_eq!(shuffled, graph);
    }

    #[test]
    fn graph_rejects_cycles_and_durable_boundary_disagreement() {
        let error = CompiledOperatorGraph::new(
            "graph-v1",
            ExecutionExtent::bounded(),
            None,
            None,
            vec![
                node("a", GraphNodeKind::Source, true),
                node("b", GraphNodeKind::DestinationBind, false),
            ],
            vec![GraphEdgeDescriptor {
                edge_id: "a_to_b".to_owned(),
                producer: "a".to_owned(),
                consumer: "b".to_owned(),
                ordering: GraphOrdering::Canonical,
                transfer: GraphEdgeTransfer::Accounted,
            }],
        )
        .unwrap_err();
        assert!(error.message.contains("durable transfer disagrees"));

        let error = CompiledOperatorGraph::new(
            "graph-v1",
            ExecutionExtent::bounded(),
            None,
            None,
            vec![
                node("a", GraphNodeKind::Source, false),
                node("b", GraphNodeKind::DestinationBind, false),
            ],
            vec![
                GraphEdgeDescriptor {
                    edge_id: "a_to_b".to_owned(),
                    producer: "a".to_owned(),
                    consumer: "b".to_owned(),
                    ordering: GraphOrdering::Canonical,
                    transfer: GraphEdgeTransfer::Accounted,
                },
                GraphEdgeDescriptor {
                    edge_id: "b_to_a".to_owned(),
                    producer: "b".to_owned(),
                    consumer: "a".to_owned(),
                    ordering: GraphOrdering::Canonical,
                    transfer: GraphEdgeTransfer::Accounted,
                },
            ],
        )
        .unwrap_err();
        assert!(error.message.contains("acyclic"));
    }

    #[test]
    fn enabled_watermark_fails_at_the_exact_dropping_operator() {
        let extent = ExecutionExtent::Drain {
            version: cdf_kernel::EXECUTION_EXTENT_VERSION,
            policy: StreamEpochPolicy {
                version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
                checkpoint_cadence: EpochClosureTrigger::Rows { count: 100 },
                package_rotation: EpochClosureTrigger::Rows { count: 100 },
                watermark: WatermarkPolicy::Enabled {
                    event_time_field: "event_time".into(),
                    domain: cdf_kernel::EventTimeDomain::SignedInteger,
                    authority: WatermarkAuthority::Source,
                    partition_aggregation: PartitionWatermarkAggregation::MinimumAll,
                },
                late_data: cdf_kernel::LateDataAction::Quarantine,
                safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
            },
            termination: DrainTermination::Records { count: 1_000 },
        };
        let source = node("source", GraphNodeKind::Source, false);
        let mut transform = node("transform", GraphNodeKind::Transform, false);
        transform.watermark_behavior = OperatorWatermarkBehavior::Drop;
        let sink = node("sink", GraphNodeKind::DestinationBind, false);
        let graph = CompiledOperatorGraph::new(
            "graph-v1",
            extent.clone(),
            Some(format!("sha256:{}", "cd".repeat(32))),
            Some(format!("sha256:{}", "ab".repeat(32))),
            vec![source, transform, sink],
            vec![
                GraphEdgeDescriptor {
                    edge_id: "source_to_transform".to_owned(),
                    producer: "source".to_owned(),
                    consumer: "transform".to_owned(),
                    ordering: GraphOrdering::Canonical,
                    transfer: GraphEdgeTransfer::Accounted,
                },
                GraphEdgeDescriptor {
                    edge_id: "transform_to_sink".to_owned(),
                    producer: "transform".to_owned(),
                    consumer: "sink".to_owned(),
                    ordering: GraphOrdering::Canonical,
                    transfer: GraphEdgeTransfer::Accounted,
                },
            ],
        )
        .unwrap();
        let error = graph.validate_execution_extent(&extent).unwrap_err();
        assert!(error.message.contains("operator `transform` drops"));
    }

    #[test]
    fn accounted_edge_backpressures_on_global_bytes_and_releases_on_drop() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let bytes = cdf_memory::record_batch_retained_bytes(&batch).unwrap();
        let coordinator: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(bytes, BTreeMap::new()).unwrap());
        let first = futures_executor::block_on(account_graph_batch(
            Arc::clone(&coordinator),
            "edge",
            batch.clone(),
        ))
        .unwrap();
        let waiting = account_graph_batch(Arc::clone(&coordinator), "edge", batch);
        pin_mut!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        drop(first);
        assert!(waiting.as_mut().now_or_never().unwrap().is_ok());
        assert_eq!(coordinator.snapshot().peak_bytes, bytes);
    }

    #[test]
    fn external_typed_outcomes_share_the_neutral_accounting_contract() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct ExternalOutcome {
            code: u16,
        }

        let coordinator: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
        let outcomes = futures_executor::block_on(account_graph_outcomes(
            Arc::clone(&coordinator),
            "external_outcomes",
            vec![ExternalOutcome { code: 7 }],
            32,
        ))
        .unwrap();
        assert_eq!(outcomes.outcomes(), &[ExternalOutcome { code: 7 }]);
        assert_eq!(outcomes.accounted_bytes(), 32);
        assert_eq!(coordinator.snapshot().current_bytes, 32);
        drop(outcomes);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn cancellation_rejects_envelopes_without_leaking_the_lease() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let bytes = cdf_memory::record_batch_retained_bytes(&batch).unwrap();
        let coordinator: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(bytes, BTreeMap::new()).unwrap());
        let payload = futures_executor::block_on(account_graph_batch(
            Arc::clone(&coordinator),
            "cancelled_edge",
            batch,
        ))
        .unwrap();
        let cancellation = RunCancellation::default();
        let (mut sender, _receiver) = graph_edge(
            GraphEdgeRuntimeConfig {
                edge_id: "cancelled".to_owned(),
                maximum_items: 1,
                maximum_outcomes_per_item: 1,
                maximum_outcome_bytes_per_item: 1,
            },
            cancellation.clone(),
        )
        .unwrap();
        cancellation.cancel();
        let envelope = GraphDataEnvelope {
            partition_ordinal: 0,
            partition_id: PartitionId::new("p0").unwrap(),
            local_sequence: 0,
            source_position: Some(SourcePosition::PageToken(cdf_kernel::PageToken {
                version: 1,
                token: "p0:0".to_owned(),
            })),
            schema_authority: schema_authority(),
            outcomes: AccountedGraphOutcomes::<()>::none(),
            payload,
        };
        let error = futures_executor::block_on(sender.send(envelope)).unwrap_err();
        assert!(error.message.contains("cancelled"));
        drop(sender);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn cancellation_wakes_a_sender_blocked_by_a_slow_consumer() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let first_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let second_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2]))]).unwrap();
        let bytes = cdf_memory::record_batch_retained_bytes(&first_batch).unwrap();
        let coordinator: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(bytes * 2, BTreeMap::new()).unwrap());
        let first = futures_executor::block_on(account_graph_batch(
            Arc::clone(&coordinator),
            "slow_edge",
            first_batch,
        ))
        .unwrap();
        let second = futures_executor::block_on(account_graph_batch(
            Arc::clone(&coordinator),
            "slow_edge",
            second_batch,
        ))
        .unwrap();
        let cancellation = RunCancellation::default();
        let (mut sender, receiver) = graph_edge(
            GraphEdgeRuntimeConfig {
                edge_id: "slow".to_owned(),
                maximum_items: 1,
                maximum_outcomes_per_item: 1,
                maximum_outcome_bytes_per_item: 1,
            },
            cancellation.clone(),
        )
        .unwrap();
        let envelope = |sequence, payload| GraphDataEnvelope {
            partition_ordinal: 0,
            partition_id: PartitionId::new("p0").unwrap(),
            local_sequence: sequence,
            source_position: Some(SourcePosition::PageToken(cdf_kernel::PageToken {
                version: 1,
                token: format!("p0:{sequence}"),
            })),
            schema_authority: schema_authority(),
            outcomes: AccountedGraphOutcomes::<()>::none(),
            payload,
        };
        futures_executor::block_on(sender.send(envelope(0, first))).unwrap();
        {
            let blocked = sender.send(envelope(1, second));
            futures_util::pin_mut!(blocked);
            assert!(blocked.as_mut().now_or_never().is_none());
            cancellation.cancel();
            let error = blocked.as_mut().now_or_never().unwrap().unwrap_err();
            assert!(error.message.contains("cancelled"));
        }
        assert_eq!(coordinator.snapshot().current_bytes, bytes * 2);
        drop(sender);
        drop(receiver);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    #[ignore = "performance lab benchmark; run explicitly in release mode"]
    fn accounted_edge_overhead_benchmark() {
        let item_count = std::env::var("CDF_A5_EDGE_BENCH_ITEMS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(200_000);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let bytes = cdf_memory::record_batch_retained_bytes(&batch).unwrap();
        let coordinator: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(bytes, BTreeMap::new()).unwrap());
        let payload = futures_executor::block_on(account_graph_batch(
            Arc::clone(&coordinator),
            "edge_benchmark",
            batch,
        ))
        .unwrap();
        let template = GraphDataEnvelope {
            partition_ordinal: 0,
            partition_id: PartitionId::new("p0").unwrap(),
            local_sequence: 0,
            source_position: Some(SourcePosition::PageToken(cdf_kernel::PageToken {
                version: 1,
                token: "p0:0".to_owned(),
            })),
            schema_authority: schema_authority(),
            outcomes: AccountedGraphOutcomes::<()>::none(),
            payload,
        };

        let direct_started = std::time::Instant::now();
        for _ in 0..item_count {
            drop(std::hint::black_box(template.clone()));
        }
        let direct = direct_started.elapsed();

        let cancellation = RunCancellation::default();
        let (mut sender, mut receiver) = graph_edge(
            GraphEdgeRuntimeConfig {
                edge_id: "benchmark".to_owned(),
                maximum_items: 64,
                maximum_outcomes_per_item: 1,
                maximum_outcome_bytes_per_item: 1,
            },
            cancellation,
        )
        .unwrap();
        let edge_started = std::time::Instant::now();
        let received = futures_executor::block_on(async {
            let producer = async {
                for sequence in 0..item_count {
                    let mut envelope = template.clone();
                    envelope.local_sequence = sequence;
                    sender.send(envelope).await?;
                }
                Ok::<_, CdfError>(())
            };
            let consumer = async {
                let mut received = 0u64;
                while received < item_count {
                    receiver
                        .receive()
                        .await?
                        .ok_or_else(|| CdfError::internal("benchmark graph edge closed early"))?;
                    received += 1;
                }
                Ok::<_, CdfError>(received)
            };
            let ((), received) = futures_util::try_join!(producer, consumer)?;
            Ok::<_, CdfError>(received)
        })
        .unwrap();
        let edge = edge_started.elapsed();
        assert_eq!(received, item_count);
        let direct_ns = direct.as_nanos() as f64 / item_count as f64;
        let edge_ns = edge.as_nanos() as f64 / item_count as f64;
        eprintln!(
            "accounted-edge items={item_count} direct_ns_per_item={direct_ns:.2} edge_ns_per_item={edge_ns:.2} incremental_ns_per_item={:.2}",
            edge_ns - direct_ns
        );
    }
}
