use std::{collections::BTreeSet, sync::Arc};

use cdf_kernel::{CdfError, PartitionId, Result, SchemaHash, SourcePosition};
use cdf_memory::{
    AccountedBatch, AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease,
    ReservationRequest, reserve,
};
use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{RunCancellation, artifact_hash};

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
    pub nodes: Vec<GraphNodeDescriptor>,
    pub edges: Vec<GraphEdgeDescriptor>,
    pub semantic_hash: String,
}

#[derive(Serialize)]
struct GraphIdentity<'a> {
    graph_version: &'a str,
    nodes: &'a [GraphNodeDescriptor],
    edges: &'a [GraphEdgeDescriptor],
}

impl CompiledOperatorGraph {
    pub fn new(
        graph_version: impl Into<String>,
        mut nodes: Vec<GraphNodeDescriptor>,
        mut edges: Vec<GraphEdgeDescriptor>,
    ) -> Result<Self> {
        let graph_version = graph_version.into();
        validate_token("operator graph version", &graph_version)?;
        validate_graph(&nodes, &edges)?;
        nodes = canonical_node_order(&nodes, &edges)?;
        edges.sort_by(|left, right| left.edge_id.cmp(&right.edge_id));
        let semantic_hash = artifact_hash(&GraphIdentity {
            graph_version: &graph_version,
            nodes: &nodes,
            edges: &edges,
        })?;
        Ok(Self {
            graph_version,
            nodes,
            edges,
            semantic_hash,
        })
    }

    pub fn validate(&self) -> Result<()> {
        validate_token("operator graph version", &self.graph_version)?;
        validate_graph(&self.nodes, &self.edges)?;
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

#[derive(Clone, Debug, Default)]
pub struct AccountedGraphOutcomes {
    outcomes: Vec<GraphOutcome>,
    lease: Option<MemoryLease>,
}

impl AccountedGraphOutcomes {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn new(outcomes: Vec<GraphOutcome>, lease: MemoryLease) -> Result<Self> {
        if outcomes.is_empty() {
            return Err(CdfError::contract(
                "an accounted outcome set cannot be empty; use AccountedGraphOutcomes::none",
            ));
        }
        let observed = outcome_bytes(&outcomes)?;
        if lease.bytes() < observed {
            return Err(CdfError::data(format!(
                "graph outcomes require {observed} accounted bytes but lease holds {}",
                lease.bytes()
            )));
        }
        lease.reconcile(observed)?;
        Ok(Self {
            outcomes,
            lease: Some(lease),
        })
    }

    pub fn outcomes(&self) -> &[GraphOutcome] {
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
                let observed = outcome_bytes(outcomes)?;
                if observed != lease.bytes() {
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

#[derive(Clone, Debug)]
pub struct GraphDataEnvelope {
    pub partition_ordinal: u64,
    pub partition_id: PartitionId,
    pub local_sequence: u64,
    pub source_position: SourcePosition,
    pub schema_hash: SchemaHash,
    pub coercion_authority: Option<String>,
    pub outcomes: AccountedGraphOutcomes,
    pub payload: AccountedGraphPayload,
}

impl GraphDataEnvelope {
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
        if let Some(authority) = &self.coercion_authority {
            validate_token("graph coercion authority", authority)?;
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

pub struct GraphEdgeSender {
    sender: mpsc::Sender<GraphDataEnvelope>,
    config: Arc<GraphEdgeRuntimeConfig>,
    cancellation: RunCancellation,
}

pub struct GraphEdgeReceiver {
    receiver: mpsc::Receiver<GraphDataEnvelope>,
    cancellation: RunCancellation,
}

pub fn graph_edge(
    config: GraphEdgeRuntimeConfig,
    cancellation: RunCancellation,
) -> Result<(GraphEdgeSender, GraphEdgeReceiver)> {
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

impl GraphEdgeSender {
    pub async fn send(&mut self, envelope: GraphDataEnvelope) -> Result<()> {
        self.cancellation.check()?;
        envelope.validate(
            self.config.maximum_outcomes_per_item,
            self.config.maximum_outcome_bytes_per_item,
        )?;
        self.sender.send(envelope).await.map_err(|_| {
            CdfError::internal(format!(
                "graph edge `{}` closed before accepting an accounted envelope",
                self.config.edge_id
            ))
        })?;
        self.cancellation.check()
    }
}

impl GraphEdgeReceiver {
    pub async fn receive(&mut self) -> Result<Option<GraphDataEnvelope>> {
        self.cancellation.check()?;
        let item = self.receiver.next().await;
        self.cancellation.check()?;
        Ok(item)
    }
}

pub async fn account_graph_batch(
    memory: Arc<dyn MemoryCoordinator>,
    consumer_name: impl Into<String>,
    batch: arrow_array::RecordBatch,
) -> Result<AccountedGraphPayload> {
    let bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| CdfError::data("Arrow graph payload size exceeds u64"))?;
    let request =
        ReservationRequest::new(ConsumerKey::new(consumer_name, MemoryClass::Queue)?, bytes)?;
    let lease = reserve(memory, request).await?;
    Ok(AccountedGraphPayload::Arrow(AccountedBatch::new(
        batch, lease,
    )?))
}

pub async fn account_graph_outcomes(
    memory: Arc<dyn MemoryCoordinator>,
    consumer_name: impl Into<String>,
    outcomes: Vec<GraphOutcome>,
) -> Result<AccountedGraphOutcomes> {
    if outcomes.is_empty() {
        return Ok(AccountedGraphOutcomes::none());
    }
    let bytes = outcome_bytes(&outcomes)?;
    let request = ReservationRequest::new(
        ConsumerKey::new(consumer_name, MemoryClass::Control)?,
        bytes,
    )?;
    let lease = reserve(memory, request).await?;
    AccountedGraphOutcomes::new(outcomes, lease)
}

fn outcome_bytes(outcomes: &[GraphOutcome]) -> Result<u64> {
    outcomes.iter().try_fold(0u64, |total, outcome| {
        total
            .checked_add(outcome.encoded_bytes)
            .ok_or_else(|| CdfError::data("graph outcome byte accounting overflowed"))
    })
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
    use cdf_kernel::{PartitionId, SchemaHash, SourcePosition};
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use futures_util::{FutureExt, pin_mut};

    use super::*;

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
            fusion_group: None,
            durable_output,
        }
    }

    #[test]
    fn graph_identity_excludes_runtime_edge_pressure_configuration() {
        let graph = CompiledOperatorGraph::new(
            "graph-v1",
            vec![
                node("mock_source", GraphNodeKind::Source, false),
                node("mock_destination", GraphNodeKind::DestinationBind, false),
            ],
            vec![GraphEdgeDescriptor {
                edge_id: "source_to_destination".to_owned(),
                producer: "mock_source".to_owned(),
                consumer: "mock_destination".to_owned(),
                ordering: GraphOrdering::Canonical,
                transfer: GraphEdgeTransfer::Accounted,
            }],
        )
        .unwrap();
        graph.validate().unwrap();
        let encoded = serde_json::to_value(&graph).unwrap();
        assert!(encoded.get("runtime_capacity").is_none());
        assert!(graph.semantic_hash.starts_with("sha256:"));
    }

    #[test]
    fn graph_rejects_cycles_and_durable_boundary_disagreement() {
        let error = CompiledOperatorGraph::new(
            "graph-v1",
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
    fn accounted_edge_backpressures_on_global_bytes_and_releases_on_drop() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
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
    fn cancellation_rejects_envelopes_without_leaking_the_lease() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
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
            source_position: SourcePosition::PageToken(cdf_kernel::PageToken {
                version: 1,
                token: "p0:0".to_owned(),
            }),
            schema_hash: SchemaHash::new(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap(),
            coercion_authority: None,
            outcomes: AccountedGraphOutcomes::none(),
            payload,
        };
        let error = futures_executor::block_on(sender.send(envelope)).unwrap_err();
        assert!(error.message.contains("cancelled"));
        drop(sender);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }
}
