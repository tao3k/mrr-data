//! Private verified copies and catalog/generation-scoped immutable Arrow reuse.
use super::projection::{label, table};
use super::{
    GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt, GraphArEntityPropertyTable,
};
use arrow_array::StringArray;
use graphar_rs::{
    info::{GraphInfo, InfoVersion},
    reader::read_vertex_string_batch,
};
use meta_relational_reasoning::{CatalogBoundQuery, EntityCatalogDigest, GenerationId};
use std::{path::Path, sync::Arc};

/// Native paths/handles are gone before return. Tables share immutable Arrow
/// buffers, and may only be reused under the captured semantic scope.
pub struct CapturedGraphArEntityProperties {
    tables: Vec<GraphArEntityPropertyTable>,
    catalog: EntityCatalogDigest,
    generation: GenerationId,
    snapshot: [u8; 32],
}
impl CapturedGraphArEntityProperties {
    /// # Errors
    /// Refuses generation, semantic snapshot or entity catalog substitution.
    pub fn tables(
        &self,
        query: &CatalogBoundQuery,
    ) -> Result<&[GraphArEntityPropertyTable], Error> {
        if self.catalog != query.entity_catalog_digest()
            || self.generation != query.generation()
            || &self.snapshot != query.snapshot_digest()
        {
            return Err(Error::Scope);
        }
        Ok(&self.tables)
    }
    /// Transfer the existing table vector under the same admitted scope.
    /// Backend callers use `ResourceHandle::try_transform` to retain its lease.
    /// # Errors
    /// Refuses the same semantic substitutions as `tables()`.
    pub fn into_tables(
        self,
        query: &CatalogBoundQuery,
    ) -> Result<Vec<GraphArEntityPropertyTable>, Error> {
        self.tables(query)?;
        Ok(self.tables)
    }
}
/// Capture only declared, verified files and reconstruct native metadata from
/// the admitted catalog. Source YAML never supplies native filesystem paths.
/// The caller authenticates receipt inventory in its publication/root protocol;
/// local receipt construction alone is not proof of that external authority.
/// # Errors
/// Refuses drift, corrupt files, identity/schema/nullability/row or byte limits.
pub fn capture_graphar_entity_properties(
    source: &Path,
    query: &CatalogBoundQuery,
    projection: &GraphArEntityPropertyProjection,
    receipt: &GraphArEntityPropertyReceipt,
    limits: GraphArEntityPropertyLimits,
) -> Result<CapturedGraphArEntityProperties, Error> {
    capture_checked(source, query, projection, receipt, limits, || Ok(()))
}
pub(crate) fn capture_checked(
    source: &Path,
    query: &CatalogBoundQuery,
    projection: &GraphArEntityPropertyProjection,
    receipt: &GraphArEntityPropertyReceipt,
    limits: GraphArEntityPropertyLimits,
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<CapturedGraphArEntityProperties, Error> {
    check()?;
    limits.validate()?;
    if &receipt.snapshot != query.snapshot_digest()
        || receipt.catalog != query.entity_catalog_digest()
        || projection.catalog_digest() != receipt.catalog
        || query.generation() != receipt.generation
    {
        return Err(Error::Scope);
    }
    if receipt.rows > limits.max_rows {
        return Err(Error::Budget("entity rows"));
    }
    receipt
        .inventory
        .canonical_bytes(limits.inventory)
        .map_err(crate::GraphArInventoryError::from)?;
    let infos = projection.native_infos(receipt.layout.vertex_chunk_size(), limits)?;
    let directory = tempfile::tempdir()?;
    crate::snapshot::check_entry(source, true)?;
    for file in receipt.inventory.files() {
        check()?;
        crate::snapshot::copy_verified_file(source, directory.path(), file, limits.inventory)?;
    }
    let info = GraphInfo::builder("mrr_entity_properties_v1")
        .vertex_infos(infos)
        .prefix(format!(
            "{}/",
            directory
                .path()
                .to_str()
                .ok_or(Error::Shape("non-UTF8 path"))?
        ))
        .version(InfoVersion::new(1)?)
        .try_build()?;
    let mut tables = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for schema in projection.catalog.entities() {
        check()?;
        let names = std::iter::once("entity_id".to_owned())
            .chain(schema.properties().iter().map(|p| p.name().to_owned()))
            .collect::<Vec<_>>();
        let batch = read_vertex_string_batch(&info, label(schema), &names, limits.max_rows - rows)?;
        check()?;
        rows = rows
            .checked_add(batch.row_count())
            .ok_or(Error::Budget("row count overflow"))?;
        let mut columns = Vec::with_capacity(names.len());
        for column in 0..names.len() {
            let mut values = Vec::with_capacity(batch.row_count());
            for row in 0..batch.row_count() {
                if batch.id(row) != i64::try_from(row).ok() {
                    return Err(Error::Shape("physical vertex index"));
                }
                let value = batch.value(row, column);
                if let Some(value) = value {
                    bytes = bytes
                        .checked_add(value.len())
                        .ok_or(Error::Budget("value bytes overflow"))?;
                }
                if bytes > limits.max_value_bytes {
                    return Err(Error::Budget("entity value bytes"));
                }
                values.push(value);
            }
            columns.push(Arc::new(StringArray::from(values)));
        }
        tables.push(table(schema, columns)?);
    }
    if rows != receipt.rows {
        return Err(Error::Shape("declared entity rows"));
    }
    projection.validate_tables(&tables, limits)?;
    check()?;
    drop(info);
    directory.close()?;
    Ok(CapturedGraphArEntityProperties {
        tables,
        catalog: receipt.catalog,
        generation: receipt.generation,
        snapshot: *query.snapshot_digest(),
    })
}
