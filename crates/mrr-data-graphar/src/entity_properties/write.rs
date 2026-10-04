//! Native publication, isolated staging and pre-commit physical budgets.
use super::projection::{label, strings};
use super::{
    GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt, GraphArEntityPropertyTable,
};
use crate::{GraphArChunkLayout, inventory_graphar_directory};
use arrow_array::Array;
use graphar_rs::{
    builder::{Vertex, VerticesBuilder},
    info::{GraphInfo, InfoVersion},
};
use meta_relational_reasoning::SemanticSnapshot;
use std::{fs, path::Path};

/// Persist the exact declared table set, including empty types and isolated
/// entities. Canonical IDs, never `GraphAr` row indices, remain logical identity.
/// # Errors
/// Refuses invalid input/budgets before native work and never returns a receipt
/// for partial staging or a failed commit. Destination must be absent.
pub fn write_graphar_entity_properties(
    output: &Path,
    projection: &GraphArEntityPropertyProjection,
    semantic: &SemanticSnapshot,
    tables: &[GraphArEntityPropertyTable],
    layout: GraphArChunkLayout,
    limits: GraphArEntityPropertyLimits,
) -> Result<GraphArEntityPropertyReceipt, Error> {
    let ordered = projection.validate_tables(tables, limits)?;
    match fs::symlink_metadata(output) {
        Ok(_) => return Err(Error::OutputExists),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let staging = tempfile::Builder::new()
        .prefix(".mrr-entity-properties-")
        .tempdir_in(parent)?;
    let absolute = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let infos = projection.native_infos(layout.vertex_chunk_size(), limits)?;
    let prefix = format!(
        "{}/",
        staging
            .path()
            .to_str()
            .ok_or(Error::Shape("non-UTF8 path"))?
    );
    let tables = tables
        .iter()
        .map(|table| (table.schema.id(), table))
        .collect::<std::collections::BTreeMap<_, _>>();
    for (schema, info) in projection.catalog.entities().iter().zip(&infos) {
        let input = tables[&schema.id()];
        let mut builder = VerticesBuilder::try_new(info, &prefix, 0)?;
        for (_, row) in &ordered[&schema.id()] {
            let mut vertex = Vertex::new();
            for (column, field) in input.batch.schema().fields().iter().enumerate() {
                let values = strings(&input.batch, column)?;
                if !values.is_null(*row) {
                    vertex.add_property_string(field.name(), values.value(*row));
                }
            }
            builder.add_vertex(vertex)?;
        }
        builder.dump()?;
        info.save(
            staging
                .path()
                .join(format!("{}.vertex.yaml", label(schema))),
        )?;
    }
    GraphInfo::builder("mrr_entity_properties_v1")
        .vertex_infos(infos)
        .prefix(format!(
            "{}/",
            absolute.to_str().ok_or(Error::Shape("non-UTF8 path"))?
        ))
        .version(InfoVersion::new(1)?)
        .try_build()?
        .save(staging.path().join(crate::query_source::GRAPH_INFO_FILE))?;
    let inventory = inventory_graphar_directory(staging.path(), limits.inventory)?;
    // Keep the TempDir guard active across rename so failed publication removes staging.
    fs::rename(staging.path(), output)?;
    Ok(GraphArEntityPropertyReceipt {
        root: output.to_path_buf(),
        inventory,
        catalog: projection.catalog_digest(),
        generation: semantic.generation(),
        snapshot: *semantic.digest(),
        rows: ordered.values().map(Vec::len).sum(),
        layout,
    })
}
