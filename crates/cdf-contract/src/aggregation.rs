use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Result, source_name, with_null_origin, with_source_name};
use serde::{Deserialize, Serialize};

use crate::{RuleOutcome, is_lossless_type_widening};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateSchemaCandidate {
    pub location: String,
    pub schema: Schema,
}

impl AggregateSchemaCandidate {
    pub fn new(location: impl Into<String>, schema: Schema) -> Self {
        Self {
            location: location.into(),
            schema,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateSchemaJoin {
    pub schema: Schema,
    pub files: Vec<AggregateFileSchemaVerdict>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateSchemaJoinReport {
    pub schema: Schema,
    pub files: Vec<AggregateFileSchemaVerdict>,
    pub incompatibilities: Vec<AggregateSchemaIncompatibility>,
}

impl AggregateSchemaJoinReport {
    pub fn is_compatible(&self) -> bool {
        self.incompatibilities.is_empty()
    }

    pub fn into_result(self) -> Result<AggregateSchemaJoin> {
        if self.incompatibilities.is_empty() {
            return Ok(AggregateSchemaJoin {
                schema: self.schema,
                files: self.files,
            });
        }

        let summary = self
            .incompatibilities
            .iter()
            .map(|incompatibility| {
                format!(
                    "{} at {}: {}",
                    incompatibility.location,
                    display_field_path(&incompatibility.field_path),
                    incompatibility.reason
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        Err(CdfError::contract(format!(
            "aggregate schema join found {} incompatible per-file field verdicts: {summary}",
            self.incompatibilities.len()
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateFileSchemaVerdict {
    pub location: String,
    pub schema_metadata_variance: Vec<AggregateMetadataVariance>,
    pub fields: Vec<AggregateFieldSchemaVerdict>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateFieldSchemaVerdict {
    pub field_path: Vec<String>,
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_type: Option<String>,
    pub effective_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_nullable: Option<bool>,
    pub effective_nullable: bool,
    pub decision: AggregateFieldDecision,
    pub outcome: RuleOutcome,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_variance: Vec<AggregateMetadataVariance>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFieldDecision {
    Preserved,
    Widened,
    MissingNull,
    Incompatible,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateMetadataVariance {
    pub key: String,
    pub observed_value: String,
    pub candidate_values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateSchemaIncompatibility {
    pub location: String,
    pub field_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_type: Option<String>,
    pub effective_type: String,
    pub reason: String,
}

pub fn aggregate_arrow_schemas(
    candidates: &[AggregateSchemaCandidate],
) -> Result<AggregateSchemaJoin> {
    plan_aggregate_arrow_schema_join(candidates)?.into_result()
}

pub fn plan_aggregate_arrow_schema_join(
    candidates: &[AggregateSchemaCandidate],
) -> Result<AggregateSchemaJoinReport> {
    if candidates.is_empty() {
        return Err(CdfError::contract(
            "aggregate schema join requires at least one physical Arrow schema candidate",
        ));
    }

    let mut candidates = candidates.iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.location.cmp(&right.location));
    for pair in candidates.windows(2) {
        if pair[0].location == pair[1].location {
            return Err(CdfError::contract(format!(
                "aggregate schema join contains duplicate canonical candidate location {:?}",
                pair[0].location
            )));
        }
    }

    let locations = candidates
        .iter()
        .map(|candidate| candidate.location.as_str())
        .collect::<Vec<_>>();
    let candidate_fields = candidates
        .iter()
        .map(|candidate| {
            candidate
                .schema
                .fields()
                .iter()
                .map(|field| field.as_ref())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let nodes = join_field_collection(&candidate_fields, &locations, &[])?;
    let (schema_metadata, schema_conflicts) = merge_metadata(
        candidates
            .iter()
            .map(|candidate| candidate.schema.metadata()),
    );
    let schema = Schema::new_with_metadata(
        nodes
            .iter()
            .map(|node| node.field.clone())
            .collect::<Vec<_>>(),
        schema_metadata,
    );

    let mut files = Vec::with_capacity(candidates.len());
    let mut incompatibilities = Vec::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        let mut fields = Vec::new();
        let candidate_by_name = unique_fields_by_name(
            candidate_fields[candidate_index].iter().copied(),
            &candidate.location,
            &[],
        )?;
        for node in &nodes {
            let observed = candidate_by_name
                .get(node.source_name.as_str())
                .copied()
                .map(ObservedNode::Present)
                .unwrap_or(ObservedNode::Missing);
            append_file_field_verdicts(
                &candidate.location,
                node,
                observed,
                &mut fields,
                &mut incompatibilities,
            );
        }
        files.push(AggregateFileSchemaVerdict {
            location: candidate.location.clone(),
            schema_metadata_variance: metadata_variance_for_candidate(
                candidate.schema.metadata(),
                &schema_conflicts,
            ),
            fields,
        });
    }

    Ok(AggregateSchemaJoinReport {
        schema,
        files,
        incompatibilities,
    })
}

#[derive(Clone, Debug)]
struct JoinedFieldNode {
    source_name: String,
    field: Field,
    present_nullable: bool,
    metadata_conflicts: BTreeMap<String, Vec<String>>,
    children: Vec<JoinedFieldNode>,
}

fn join_field_collection(
    candidates: &[Vec<&Field>],
    locations: &[&str],
    parent_path: &[String],
) -> Result<Vec<JoinedFieldNode>> {
    let mut candidate_maps = Vec::with_capacity(candidates.len());
    let mut field_order = Vec::new();
    let mut seen_order = BTreeSet::new();

    for (index, fields) in candidates.iter().enumerate() {
        for field in fields {
            let source_identity = field_source_identity(field);
            if seen_order.insert(source_identity.to_owned()) {
                field_order.push(source_identity.to_owned());
            }
        }
        let fields = unique_fields_by_name(fields.iter().copied(), locations[index], parent_path)?;
        candidate_maps.push(fields);
    }

    field_order
        .into_iter()
        .map(|source_name| {
            let fields = candidate_maps
                .iter()
                .map(|candidate| candidate.get(&source_name).copied())
                .collect::<Vec<_>>();
            join_field_node(&source_name, &fields, locations, parent_path)
        })
        .collect()
}

fn unique_fields_by_name<'a>(
    fields: impl IntoIterator<Item = &'a Field>,
    location: &str,
    parent_path: &[String],
) -> Result<BTreeMap<String, &'a Field>> {
    let mut by_name = BTreeMap::new();
    for field in fields {
        let source_name = field_source_identity(field).to_owned();
        if by_name.insert(source_name.clone(), field).is_some() {
            let mut path = parent_path.to_vec();
            path.push(source_name);
            return Err(CdfError::contract(format!(
                "aggregate schema candidate `{location}` contains duplicate unnormalized source field {}",
                display_field_path(&path)
            )));
        }
    }
    Ok(by_name)
}

fn join_field_node(
    source_name: &str,
    fields: &[Option<&Field>],
    locations: &[&str],
    parent_path: &[String],
) -> Result<JoinedFieldNode> {
    let first = fields
        .iter()
        .flatten()
        .next()
        .copied()
        .expect("field order is derived from at least one present field");
    let mut path = parent_path.to_vec();
    path.push(source_name.to_owned());
    let (data_type, children) = join_data_type(fields, locations, &path)?;
    let missing_locations = fields
        .iter()
        .zip(locations)
        .filter_map(|(field, location)| field.is_none().then_some(*location))
        .collect::<Vec<_>>();
    let present_nullable = first.is_nullable();
    let nullable = present_nullable || !missing_locations.is_empty();
    let (mut metadata, metadata_conflicts) =
        merge_metadata(fields.iter().flatten().map(|field| field.metadata()));
    metadata.retain(|key, _| !is_reserved_metadata(key));
    let mut field = Field::new(source_name, data_type, nullable).with_metadata(metadata);
    field = with_source_name(field, source_name);
    if !missing_locations.is_empty() {
        let null_origin = serde_json::json!({
            "kind": "missing_in_candidates",
            "locations": missing_locations,
        })
        .to_string();
        field = with_null_origin(field, null_origin);
    }

    Ok(JoinedFieldNode {
        source_name: source_name.to_owned(),
        field,
        present_nullable,
        metadata_conflicts,
        children,
    })
}

fn join_data_type(
    fields: &[Option<&Field>],
    locations: &[&str],
    field_path: &[String],
) -> Result<(DataType, Vec<JoinedFieldNode>)> {
    let first = fields
        .iter()
        .flatten()
        .next()
        .copied()
        .expect("joined field has a present physical field");
    match first.data_type() {
        DataType::Struct(_) => {
            let child_candidates = fields
                .iter()
                .map(|field| match field.map(Field::data_type) {
                    Some(DataType::Struct(children)) => {
                        children.iter().map(|child| child.as_ref()).collect()
                    }
                    _ => Vec::new(),
                })
                .collect::<Vec<Vec<&Field>>>();
            let children = join_field_collection(&child_candidates, locations, field_path)?;
            let data_type = DataType::Struct(
                children
                    .iter()
                    .map(|child| Arc::new(child.field.clone()))
                    .collect::<Vec<_>>()
                    .into(),
            );
            Ok((data_type, children))
        }
        DataType::List(_) => join_list_type(fields, locations, field_path, ListKind::List),
        DataType::LargeList(_) => {
            join_list_type(fields, locations, field_path, ListKind::LargeList)
        }
        DataType::ListView(_) => join_list_type(fields, locations, field_path, ListKind::ListView),
        DataType::LargeListView(_) => {
            join_list_type(fields, locations, field_path, ListKind::LargeListView)
        }
        DataType::FixedSizeList(_, size) => {
            join_fixed_size_list_type(fields, locations, field_path, *size)
        }
        DataType::Map(_, keys_sorted) => join_map_type(fields, locations, field_path, *keys_sorted),
        first_type => {
            let mut aggregate = first_type.clone();
            for candidate in fields.iter().flatten().skip(1) {
                let candidate_type = candidate.data_type();
                if aggregate == *candidate_type {
                    continue;
                }
                if is_lossless_type_widening(&aggregate, candidate_type) {
                    aggregate = candidate_type.clone();
                } else if !is_lossless_type_widening(candidate_type, &aggregate) {
                    // Keep the canonical first compatible type. The total verdict pass below
                    // records every incompatible candidate without inventing a fallback lattice.
                }
            }
            Ok((aggregate, Vec::new()))
        }
    }
}

#[derive(Clone, Copy)]
enum ListKind {
    List,
    LargeList,
    ListView,
    LargeListView,
}

fn join_list_type(
    fields: &[Option<&Field>],
    locations: &[&str],
    field_path: &[String],
    kind: ListKind,
) -> Result<(DataType, Vec<JoinedFieldNode>)> {
    let items = fields
        .iter()
        .map(|field| match (kind, field.map(Field::data_type)) {
            (ListKind::List, Some(DataType::List(item)))
            | (ListKind::LargeList, Some(DataType::LargeList(item)))
            | (ListKind::ListView, Some(DataType::ListView(item)))
            | (ListKind::LargeListView, Some(DataType::LargeListView(item))) => Some(item.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let item_name = field_source_identity(
        items
            .iter()
            .flatten()
            .next()
            .expect("first list field has an item"),
    )
    .to_owned();
    let item = join_field_node(&item_name, &items, locations, field_path)?;
    let data_type = match kind {
        ListKind::List => DataType::List(Arc::new(item.field.clone())),
        ListKind::LargeList => DataType::LargeList(Arc::new(item.field.clone())),
        ListKind::ListView => DataType::ListView(Arc::new(item.field.clone())),
        ListKind::LargeListView => DataType::LargeListView(Arc::new(item.field.clone())),
    };
    Ok((data_type, vec![item]))
}

fn join_fixed_size_list_type(
    fields: &[Option<&Field>],
    locations: &[&str],
    field_path: &[String],
    size: i32,
) -> Result<(DataType, Vec<JoinedFieldNode>)> {
    let items = fields
        .iter()
        .map(|field| match field.map(Field::data_type) {
            Some(DataType::FixedSizeList(item, candidate_size)) if *candidate_size == size => {
                Some(item.as_ref())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let item_name = field_source_identity(
        items
            .iter()
            .flatten()
            .next()
            .expect("first fixed-size list field has an item"),
    )
    .to_owned();
    let item = join_field_node(&item_name, &items, locations, field_path)?;
    Ok((
        DataType::FixedSizeList(Arc::new(item.field.clone()), size),
        vec![item],
    ))
}

fn join_map_type(
    fields: &[Option<&Field>],
    locations: &[&str],
    field_path: &[String],
    keys_sorted: bool,
) -> Result<(DataType, Vec<JoinedFieldNode>)> {
    let entries = fields
        .iter()
        .map(|field| match field.map(Field::data_type) {
            Some(DataType::Map(entries, candidate_sorted)) if *candidate_sorted == keys_sorted => {
                Some(entries.as_ref())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let entry_name = field_source_identity(
        entries
            .iter()
            .flatten()
            .next()
            .expect("first map field has an entries field"),
    )
    .to_owned();
    let entry = join_field_node(&entry_name, &entries, locations, field_path)?;
    Ok((
        DataType::Map(Arc::new(entry.field.clone()), keys_sorted),
        vec![entry],
    ))
}

fn merge_metadata<'a>(
    metadata: impl IntoIterator<Item = &'a HashMap<String, String>>,
) -> (HashMap<String, String>, BTreeMap<String, Vec<String>>) {
    let mut values = BTreeMap::<String, BTreeSet<String>>::new();
    for metadata in metadata {
        for (key, value) in metadata {
            if !is_reserved_metadata(key) {
                values.entry(key.clone()).or_default().insert(value.clone());
            }
        }
    }

    let mut retained = HashMap::new();
    let mut conflicts = BTreeMap::new();
    for (key, values) in values {
        if values.len() == 1 {
            retained.insert(key, values.into_iter().next().expect("one metadata value"));
        } else {
            conflicts.insert(key, values.into_iter().collect());
        }
    }
    (retained, conflicts)
}

fn is_reserved_metadata(key: &str) -> bool {
    key.starts_with("cdf:")
}

fn metadata_variance_for_candidate(
    metadata: &HashMap<String, String>,
    conflicts: &BTreeMap<String, Vec<String>>,
) -> Vec<AggregateMetadataVariance> {
    conflicts
        .iter()
        .filter_map(|(key, candidate_values)| {
            metadata
                .get(key)
                .map(|observed_value| AggregateMetadataVariance {
                    key: key.clone(),
                    observed_value: observed_value.clone(),
                    candidate_values: candidate_values.clone(),
                })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum ObservedNode<'a> {
    Present(&'a Field),
    Missing,
    ParentIncompatible,
}

fn append_file_field_verdicts(
    location: &str,
    node: &JoinedFieldNode,
    observed: ObservedNode<'_>,
    verdicts: &mut Vec<AggregateFieldSchemaVerdict>,
    incompatibilities: &mut Vec<AggregateSchemaIncompatibility>,
) {
    let mut field_path = Vec::new();
    append_file_field_verdicts_at_path(
        location,
        node,
        observed,
        &mut field_path,
        verdicts,
        incompatibilities,
    );
}

fn append_file_field_verdicts_at_path(
    location: &str,
    node: &JoinedFieldNode,
    observed: ObservedNode<'_>,
    parent_path: &mut Vec<String>,
    verdicts: &mut Vec<AggregateFieldSchemaVerdict>,
    incompatibilities: &mut Vec<AggregateSchemaIncompatibility>,
) {
    parent_path.push(node.source_name.clone());
    let (decision, outcome, reason, observed_type, observed_nullable, metadata_variance) =
        field_verdict(node, observed);
    let verdict = AggregateFieldSchemaVerdict {
        field_path: parent_path.clone(),
        source_name: node.source_name.clone(),
        observed_type: observed_type.clone(),
        effective_type: node.field.data_type().to_string(),
        observed_nullable,
        effective_nullable: node.field.is_nullable(),
        decision,
        outcome,
        reason: reason.clone(),
        metadata_variance,
    };
    if decision == AggregateFieldDecision::Incompatible {
        incompatibilities.push(AggregateSchemaIncompatibility {
            location: location.to_owned(),
            field_path: parent_path.clone(),
            observed_type,
            effective_type: node.field.data_type().to_string(),
            reason,
        });
    }
    verdicts.push(verdict);

    for child in &node.children {
        let child_observed = observed_child(node, child, observed);
        append_file_field_verdicts_at_path(
            location,
            child,
            child_observed,
            parent_path,
            verdicts,
            incompatibilities,
        );
    }
    parent_path.pop();
}

fn field_verdict(
    node: &JoinedFieldNode,
    observed: ObservedNode<'_>,
) -> (
    AggregateFieldDecision,
    RuleOutcome,
    String,
    Option<String>,
    Option<bool>,
    Vec<AggregateMetadataVariance>,
) {
    match observed {
        ObservedNode::Missing => (
            AggregateFieldDecision::MissingNull,
            RuleOutcome::Coerced,
            "field is absent in this candidate and materializes as a typed null".to_owned(),
            None,
            None,
            Vec::new(),
        ),
        ObservedNode::ParentIncompatible => (
            AggregateFieldDecision::Incompatible,
            RuleOutcome::Fatal,
            "parent container is incompatible with the aggregate field shape".to_owned(),
            None,
            None,
            Vec::new(),
        ),
        ObservedNode::Present(field) => {
            let metadata_variance =
                metadata_variance_for_candidate(field.metadata(), &node.metadata_conflicts);
            let observed_type = Some(field.data_type().to_string());
            let observed_nullable = Some(field.is_nullable());
            if field.is_nullable() != node.present_nullable {
                return (
                    AggregateFieldDecision::Incompatible,
                    RuleOutcome::Fatal,
                    format!(
                        "present-field nullability {} conflicts with canonical present-field nullability {}",
                        field.is_nullable(),
                        node.present_nullable
                    ),
                    observed_type,
                    observed_nullable,
                    metadata_variance,
                );
            }
            if !can_widen_data_type(field.data_type(), node) {
                return (
                    AggregateFieldDecision::Incompatible,
                    RuleOutcome::Fatal,
                    format!(
                        "no ratified lossless widening from {} to {}",
                        field.data_type(),
                        node.field.data_type()
                    ),
                    observed_type,
                    observed_nullable,
                    metadata_variance,
                );
            }
            let widened = field.data_type() != node.field.data_type()
                || field.is_nullable() != node.field.is_nullable();
            if widened {
                (
                    AggregateFieldDecision::Widened,
                    RuleOutcome::Coerced,
                    format!(
                        "lossless aggregate widening from {} to {}",
                        field.data_type(),
                        node.field.data_type()
                    ),
                    observed_type,
                    observed_nullable,
                    metadata_variance,
                )
            } else {
                (
                    AggregateFieldDecision::Preserved,
                    RuleOutcome::Pass,
                    "physical field already satisfies the aggregate schema".to_owned(),
                    observed_type,
                    observed_nullable,
                    metadata_variance,
                )
            }
        }
    }
}

fn observed_child<'a>(
    parent: &JoinedFieldNode,
    child: &JoinedFieldNode,
    observed: ObservedNode<'a>,
) -> ObservedNode<'a> {
    let ObservedNode::Present(field) = observed else {
        return observed;
    };
    match (parent.field.data_type(), field.data_type()) {
        (DataType::Struct(_), DataType::Struct(observed_children)) => observed_children
            .iter()
            .find(|observed| field_source_identity(observed) == child.source_name)
            .map(|observed| ObservedNode::Present(observed.as_ref()))
            .unwrap_or(ObservedNode::Missing),
        (DataType::List(_), DataType::List(observed_child))
        | (DataType::LargeList(_), DataType::LargeList(observed_child))
        | (DataType::ListView(_), DataType::ListView(observed_child))
        | (DataType::LargeListView(_), DataType::LargeListView(observed_child)) => {
            ObservedNode::Present(observed_child.as_ref())
        }
        (
            DataType::FixedSizeList(_, aggregate_size),
            DataType::FixedSizeList(observed_child, observed_size),
        ) if aggregate_size == observed_size => ObservedNode::Present(observed_child.as_ref()),
        (DataType::Map(_, aggregate_sorted), DataType::Map(observed_entries, observed_sorted))
            if aggregate_sorted == observed_sorted =>
        {
            ObservedNode::Present(observed_entries.as_ref())
        }
        _ => ObservedNode::ParentIncompatible,
    }
}

fn can_widen_data_type(observed: &DataType, node: &JoinedFieldNode) -> bool {
    let aggregate = node.field.data_type();
    match (observed, aggregate) {
        (DataType::Struct(observed_fields), DataType::Struct(_)) => {
            let observed_names = observed_fields
                .iter()
                .map(|field| field_source_identity(field))
                .collect::<Vec<_>>();
            let aggregate_projection = node
                .children
                .iter()
                .filter(|child| observed_names.contains(&child.source_name.as_str()))
                .map(|child| child.source_name.as_str())
                .collect::<Vec<_>>();
            if observed_names != aggregate_projection {
                return false;
            }
            observed_fields.iter().all(|field| {
                node.children
                    .iter()
                    .find(|child| child.source_name == field_source_identity(field))
                    .is_some_and(|child| field_can_widen(field.as_ref(), child))
            })
        }
        (DataType::List(observed), DataType::List(_))
        | (DataType::LargeList(observed), DataType::LargeList(_))
        | (DataType::ListView(observed), DataType::ListView(_))
        | (DataType::LargeListView(observed), DataType::LargeListView(_)) => node
            .children
            .first()
            .is_some_and(|child| field_can_widen(observed.as_ref(), child)),
        (
            DataType::FixedSizeList(observed, observed_size),
            DataType::FixedSizeList(_, aggregate_size),
        ) => {
            observed_size == aggregate_size
                && node
                    .children
                    .first()
                    .is_some_and(|child| field_can_widen(observed.as_ref(), child))
        }
        (DataType::Map(observed, observed_sorted), DataType::Map(_, aggregate_sorted)) => {
            observed_sorted == aggregate_sorted
                && node
                    .children
                    .first()
                    .is_some_and(|child| field_can_widen(observed.as_ref(), child))
        }
        _ => observed == aggregate || is_lossless_type_widening(observed, aggregate),
    }
}

fn field_can_widen(observed: &Field, node: &JoinedFieldNode) -> bool {
    field_source_identity(observed) == node.source_name
        && observed.is_nullable() == node.present_nullable
        && can_widen_data_type(observed.data_type(), node)
}

fn field_source_identity(field: &Field) -> &str {
    source_name(field).unwrap_or_else(|| field.name())
}

fn display_field_path(path: &[String]) -> String {
    path.join(".")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{null_origin, source_name, with_source_name};
    use proptest::prelude::*;

    use super::*;

    fn candidate(location: &str, fields: Vec<Field>) -> AggregateSchemaCandidate {
        AggregateSchemaCandidate::new(location, Schema::new(fields))
    }

    fn verdict<'a>(
        join: &'a AggregateSchemaJoin,
        location: &str,
        path: &[&str],
    ) -> &'a AggregateFieldSchemaVerdict {
        join.files
            .iter()
            .find(|file| file.location == location)
            .unwrap()
            .fields
            .iter()
            .find(|field| {
                field
                    .field_path
                    .iter()
                    .map(String::as_str)
                    .eq(path.iter().copied())
            })
            .unwrap()
    }

    #[test]
    fn aggregate_join_widens_unions_and_materializes_missing_null_evidence() {
        let join = aggregate_arrow_schemas(&[
            candidate(
                "b.parquet",
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("note", DataType::Utf8, false),
                ],
            ),
            candidate("a.parquet", vec![Field::new("id", DataType::Int32, false)]),
        ])
        .unwrap();

        assert_eq!(join.schema.field(0).name(), "id");
        assert_eq!(join.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(join.schema.field(1).name(), "note");
        assert!(join.schema.field(1).is_nullable());
        assert_eq!(source_name(join.schema.field(1)), Some("note"));
        let origin: serde_json::Value =
            serde_json::from_str(null_origin(join.schema.field(1)).unwrap()).unwrap();
        assert_eq!(origin["locations"], serde_json::json!(["a.parquet"]));
        assert_eq!(
            verdict(&join, "a.parquet", &["id"]).decision,
            AggregateFieldDecision::Widened
        );
        assert_eq!(
            verdict(&join, "a.parquet", &["note"]).decision,
            AggregateFieldDecision::MissingNull
        );
        assert_eq!(
            verdict(&join, "b.parquet", &["note"]).decision,
            AggregateFieldDecision::Widened
        );
    }

    #[test]
    fn aggregate_join_matches_top_level_and_nested_fields_by_source_name_metadata() {
        let left = with_source_name(
            Field::new_struct(
                "normalized_payload_a",
                vec![with_source_name(
                    Field::new("normalized_count_a", DataType::Int32, false),
                    "Count",
                )],
                false,
            ),
            "Payload",
        );
        let right = with_source_name(
            Field::new_struct(
                "normalized_payload_b",
                vec![
                    with_source_name(
                        Field::new("normalized_count_b", DataType::Int64, false),
                        "Count",
                    ),
                    with_source_name(Field::new("normalized_tag_b", DataType::Utf8, false), "Tag"),
                ],
                false,
            ),
            "Payload",
        );

        let join = aggregate_arrow_schemas(&[
            candidate("a.parquet", vec![left]),
            candidate("b.parquet", vec![right]),
        ])
        .unwrap();

        assert_eq!(join.schema.fields().len(), 1);
        assert_eq!(join.schema.field(0).name(), "Payload");
        assert_eq!(source_name(join.schema.field(0)), Some("Payload"));
        let DataType::Struct(children) = join.schema.field(0).data_type() else {
            panic!("Payload must remain a struct");
        };
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].name(), "Count");
        assert_eq!(children[0].data_type(), &DataType::Int64);
        assert_eq!(source_name(&children[0]), Some("Count"));
        assert_eq!(children[1].name(), "Tag");
        assert!(children[1].is_nullable());
        assert_eq!(
            verdict(&join, "a.parquet", &["Payload", "Count"]).decision,
            AggregateFieldDecision::Widened
        );
        assert_eq!(
            verdict(&join, "a.parquet", &["Payload", "Tag"]).decision,
            AggregateFieldDecision::MissingNull
        );
    }

    #[test]
    fn aggregate_join_uses_active_decimal_date_and_float_widenings() {
        let join = aggregate_arrow_schemas(&[
            candidate(
                "a",
                vec![
                    Field::new("amount", DataType::Int32, false),
                    Field::new("day", DataType::Date32, false),
                    Field::new("score", DataType::Float32, false),
                ],
            ),
            candidate(
                "b",
                vec![
                    Field::new("amount", DataType::Decimal128(12, 2), false),
                    Field::new(
                        "day",
                        DataType::Timestamp(
                            arrow_schema::TimeUnit::Microsecond,
                            Some("UTC".into()),
                        ),
                        false,
                    ),
                    Field::new("score", DataType::Float64, false),
                ],
            ),
        ])
        .unwrap();

        assert_eq!(
            join.schema.field_with_name("amount").unwrap().data_type(),
            &DataType::Decimal128(12, 2)
        );
        assert!(matches!(
            join.schema.field_with_name("day").unwrap().data_type(),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, timezone)
                if timezone.as_deref() == Some("UTC")
        ));
        assert_eq!(
            join.schema.field_with_name("score").unwrap().data_type(),
            &DataType::Float64
        );
        assert!(
            join.files[0]
                .fields
                .iter()
                .all(|field| field.decision == AggregateFieldDecision::Widened)
        );
    }

    #[test]
    fn aggregate_join_recurses_through_struct_list_and_map_children() {
        let struct_a = Field::new_struct(
            "payload",
            vec![Field::new("count", DataType::Int32, false)],
            false,
        );
        let struct_b = Field::new_struct(
            "payload",
            vec![
                Field::new("count", DataType::Int64, false),
                Field::new("tag", DataType::Utf8, false),
            ],
            false,
        );
        let list_a = Field::new_list("items", Field::new("item", DataType::Int16, false), false);
        let list_b = Field::new_list("items", Field::new("item", DataType::Int64, false), false);
        let map_entries_a = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::UInt8, false),
            ],
            false,
        );
        let map_entries_b = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::UInt64, false),
            ],
            false,
        );
        let map_a = Field::new(
            "labels",
            DataType::Map(Arc::new(map_entries_a), false),
            false,
        );
        let map_b = Field::new(
            "labels",
            DataType::Map(Arc::new(map_entries_b), false),
            false,
        );

        let join = aggregate_arrow_schemas(&[
            candidate("a.arrow", vec![struct_a, list_a, map_a]),
            candidate("b.arrow", vec![struct_b, list_b, map_b]),
        ])
        .unwrap();

        let DataType::Struct(payload) = join.schema.field(0).data_type() else {
            panic!("payload must remain struct");
        };
        assert_eq!(payload[0].data_type(), &DataType::Int64);
        assert!(payload[1].is_nullable());
        let DataType::List(item) = join.schema.field(1).data_type() else {
            panic!("items must remain list");
        };
        assert_eq!(item.data_type(), &DataType::Int64);
        let DataType::Map(entries, false) = join.schema.field(2).data_type() else {
            panic!("labels must remain unsorted map");
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("map entries must remain struct");
        };
        assert_eq!(entries[1].data_type(), &DataType::UInt64);
        assert_eq!(
            verdict(&join, "a.arrow", &["payload", "tag"]).decision,
            AggregateFieldDecision::MissingNull
        );
    }

    #[test]
    fn aggregate_join_rejects_unratified_types_container_shapes_order_and_nullability() {
        let cases = vec![
            (DataType::Int32, DataType::UInt32),
            (DataType::Int64, DataType::Float64),
            (DataType::Float16, DataType::Float32),
            (DataType::Decimal128(10, 2), DataType::Decimal128(12, 2)),
            (
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            ),
        ];
        for (left, right) in cases {
            let report = plan_aggregate_arrow_schema_join(&[
                candidate("a", vec![Field::new("value", left, false)]),
                candidate("b", vec![Field::new("value", right, false)]),
            ])
            .unwrap();
            assert!(!report.is_compatible());
        }

        let nullability = plan_aggregate_arrow_schema_join(&[
            candidate("a", vec![Field::new("value", DataType::Int64, false)]),
            candidate("b", vec![Field::new("value", DataType::Int64, true)]),
        ])
        .unwrap();
        assert!(!nullability.is_compatible());

        let list_width = plan_aggregate_arrow_schema_join(&[
            candidate(
                "a",
                vec![Field::new_list(
                    "items",
                    Field::new("item", DataType::Int64, false),
                    false,
                )],
            ),
            candidate(
                "b",
                vec![Field::new_large_list(
                    "items",
                    Field::new("item", DataType::Int64, false),
                    false,
                )],
            ),
        ])
        .unwrap();
        assert!(!list_width.is_compatible());

        let map_sortedness = plan_aggregate_arrow_schema_join(&[
            candidate("a", vec![map_field(false)]),
            candidate("b", vec![map_field(true)]),
        ])
        .unwrap();
        assert!(!map_sortedness.is_compatible());

        let struct_order = plan_aggregate_arrow_schema_join(&[
            candidate("a", vec![ordered_struct(&["first", "second"])]),
            candidate("b", vec![ordered_struct(&["second", "first"])]),
        ])
        .unwrap();
        assert!(!struct_order.is_compatible());
    }

    fn map_field(sorted: bool) -> Field {
        Field::new(
            "map",
            DataType::Map(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ],
                    false,
                )),
                sorted,
            ),
            false,
        )
    }

    fn ordered_struct(order: &[&str]) -> Field {
        Field::new_struct(
            "payload",
            order
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, false))
                .collect::<Vec<_>>(),
            false,
        )
    }

    #[test]
    fn aggregate_join_regenerates_reserved_metadata_and_records_variance() {
        let left = Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([
            ("stable".to_owned(), "same".to_owned()),
            ("one_sided".to_owned(), "retained".to_owned()),
            ("owner".to_owned(), "left".to_owned()),
            ("cdf:source_name".to_owned(), "id".to_owned()),
            ("cdf:semantic".to_owned(), "spoofed".to_owned()),
        ]));
        let right = Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([
            ("stable".to_owned(), "same".to_owned()),
            ("owner".to_owned(), "right".to_owned()),
            ("cdf:physical_type".to_owned(), "spoofed".to_owned()),
        ]));
        let left_schema = Schema::new_with_metadata(
            vec![left],
            HashMap::from([("system".to_owned(), "left".to_owned())]),
        );
        let right_schema = Schema::new_with_metadata(
            vec![right],
            HashMap::from([("system".to_owned(), "right".to_owned())]),
        );

        let join = aggregate_arrow_schemas(&[
            AggregateSchemaCandidate::new("a", left_schema),
            AggregateSchemaCandidate::new("b", right_schema),
        ])
        .unwrap();
        let field = join.schema.field(0);
        assert_eq!(field.metadata().get("stable"), Some(&"same".to_owned()));
        assert_eq!(
            field.metadata().get("one_sided"),
            Some(&"retained".to_owned())
        );
        assert!(!field.metadata().contains_key("owner"));
        assert_eq!(source_name(field), Some("id"));
        assert!(!field.metadata().contains_key("cdf:semantic"));
        assert!(!field.metadata().contains_key("cdf:physical_type"));
        assert!(!join.schema.metadata().contains_key("system"));
        for file in &join.files {
            assert_eq!(file.schema_metadata_variance.len(), 1);
            let field = file
                .fields
                .iter()
                .find(|field| field.field_path == ["id"])
                .unwrap();
            assert_eq!(field.metadata_variance.len(), 1);
            assert_eq!(field.metadata_variance[0].key, "owner");
            assert_eq!(
                field.metadata_variance[0].candidate_values,
                ["left", "right"]
            );
        }
    }

    #[test]
    fn aggregate_join_rejects_source_identity_collisions() {
        let error = plan_aggregate_arrow_schema_join(&[candidate(
            "a",
            vec![
                with_source_name(
                    Field::new("normalized_id", DataType::Int64, false),
                    "SourceID",
                ),
                with_source_name(
                    Field::new("normalized_id_copy", DataType::Utf8, false),
                    "SourceID",
                ),
            ],
        )])
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate unnormalized source field SourceID")
        );
    }

    #[test]
    fn aggregate_join_is_canonical_after_location_sorting() {
        let mut first = candidate(
            "a",
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ],
        );
        first.schema = first
            .schema
            .with_metadata(HashMap::from([("version".to_owned(), "one".to_owned())]));
        let second = candidate(
            "b",
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("extra", DataType::Boolean, false),
            ],
        );
        let forward = aggregate_arrow_schemas(&[first.clone(), second.clone()]).unwrap();
        let reverse = aggregate_arrow_schemas(&[second, first]).unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(
            serde_json::to_string(&forward.files).unwrap(),
            serde_json::to_string(&reverse.files).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&forward.files).unwrap()[0]["fields"][0]["decision"],
            "widened"
        );
    }

    #[test]
    fn aggregate_verdict_serialization_has_a_canonical_fixture() {
        let join = aggregate_arrow_schemas(&[candidate(
            "a.arrow",
            vec![Field::new("id", DataType::Int64, false)],
        )])
        .unwrap();

        assert_eq!(
            serde_json::to_value(&join.files).unwrap(),
            serde_json::json!([{
                "location": "a.arrow",
                "schema_metadata_variance": [],
                "fields": [{
                    "field_path": ["id"],
                    "source_name": "id",
                    "observed_type": "Int64",
                    "effective_type": "Int64",
                    "observed_nullable": false,
                    "effective_nullable": false,
                    "decision": "preserved",
                    "outcome": "pass",
                    "reason": "physical field already satisfies the aggregate schema"
                }]
            }])
        );
    }

    fn signed_type(index: usize) -> DataType {
        [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ][index]
            .clone()
    }

    proptest! {
        #[test]
        fn aggregate_signed_widening_composes_and_input_permutation_is_irrelevant(
            a in 0_usize..4,
            b in 0_usize..4,
            c in 0_usize..4,
        ) {
            let candidates = vec![
                candidate("c", vec![Field::new("id", signed_type(c), false)]),
                candidate("a", vec![Field::new("id", signed_type(a), false)]),
                candidate("b", vec![Field::new("id", signed_type(b), false)]),
            ];
            let mut reversed = candidates.clone();
            reversed.reverse();
            let forward = aggregate_arrow_schemas(&candidates).unwrap();
            let reverse = aggregate_arrow_schemas(&reversed).unwrap();
            prop_assert_eq!(&forward, &reverse);
            prop_assert_eq!(forward.schema.field(0).data_type(), &signed_type(a.max(b).max(c)));
            prop_assert!(forward.files.iter().all(|file| file.fields.len() == 1));
        }
    }
}
