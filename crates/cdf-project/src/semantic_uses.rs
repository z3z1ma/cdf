use std::collections::BTreeMap;

use arrow_schema::{DataType, Field};
use cdf_declarative::CompiledResource;
use cdf_kernel::{CanonicalArrowField, Result};
use cdf_semantic::{ResolvedSemantic, SemanticAuthority, SemanticCatalog};

#[derive(Clone, Debug)]
pub(crate) struct CompiledField {
    pub resource_id: String,
    pub field_ordinal: u32,
    pub field_path: String,
    pub field: CanonicalArrowField,
    pub semantic: Option<ResolvedSemantic>,
}

pub(crate) fn semantic_pins_for_resources(
    resources: &[CompiledResource],
    catalog: &SemanticCatalog,
) -> Result<BTreeMap<String, String>> {
    let mut pins = BTreeMap::new();
    for field in compiled_fields(resources, catalog)? {
        if let Some(semantic) = field.semantic {
            pins.insert(
                semantic.reference().to_string(),
                semantic.definition_hash().to_owned(),
            );
        }
    }
    Ok(pins)
}

pub(crate) fn compiled_fields(
    resources: &[CompiledResource],
    catalog: &SemanticCatalog,
) -> Result<Vec<CompiledField>> {
    let mut resources = resources.iter().collect::<Vec<_>>();
    resources.sort_by_key(|resource| resource.descriptor().resource_id.as_str());
    let mut uses = Vec::new();
    for resource in resources {
        let mut ordinal = 0_u32;
        for field in resource.schema().fields() {
            visit_field(
                catalog,
                resource.descriptor().resource_id.as_str(),
                field,
                field.name(),
                &mut ordinal,
                &mut uses,
            )?;
        }
    }
    Ok(uses)
}

fn visit_field(
    catalog: &SemanticCatalog,
    resource_id: &str,
    field: &Field,
    field_path: &str,
    ordinal: &mut u32,
    uses: &mut Vec<CompiledField>,
) -> Result<()> {
    let current_ordinal = *ordinal;
    *ordinal = ordinal
        .checked_add(1)
        .ok_or_else(|| cdf_kernel::CdfError::internal("semantic field ordinal exceeded u32"))?;
    let semantic = catalog.resolve_field(field, SemanticAuthority::Compiled)?;
    uses.push(CompiledField {
        resource_id: resource_id.to_owned(),
        field_ordinal: current_ordinal,
        field_path: field_path.to_owned(),
        field: CanonicalArrowField::from_arrow(field)?,
        semantic,
    });
    visit_children(
        catalog,
        resource_id,
        field.data_type(),
        field_path,
        ordinal,
        uses,
    )
}

fn visit_children(
    catalog: &SemanticCatalog,
    resource_id: &str,
    data_type: &DataType,
    parent_path: &str,
    ordinal: &mut u32,
    uses: &mut Vec<CompiledField>,
) -> Result<()> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            visit_nested(catalog, resource_id, field, parent_path, ordinal, uses)?
        }
        DataType::Struct(fields) => {
            for field in fields {
                visit_nested(catalog, resource_id, field, parent_path, ordinal, uses)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                visit_nested(catalog, resource_id, field, parent_path, ordinal, uses)?;
            }
        }
        DataType::RunEndEncoded(run_ends, values) => {
            visit_nested(catalog, resource_id, run_ends, parent_path, ordinal, uses)?;
            visit_nested(catalog, resource_id, values, parent_path, ordinal, uses)?;
        }
        _ => {}
    }
    Ok(())
}

fn visit_nested(
    catalog: &SemanticCatalog,
    resource_id: &str,
    field: &Field,
    parent_path: &str,
    ordinal: &mut u32,
    uses: &mut Vec<CompiledField>,
) -> Result<()> {
    visit_field(
        catalog,
        resource_id,
        field,
        &format!("{parent_path}.{}", field.name()),
        ordinal,
        uses,
    )
}
