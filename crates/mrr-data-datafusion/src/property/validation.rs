//! Physical table and catalog validation before planning.
use super::execution::{
    BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits, Result, invalid, strings,
    unsupported,
};
use crate::DataFusionQueryError;
use arrow_array::Array;
use meta_relational_reasoning::{
    CatalogBoundQuery, EntityCatalog, EntityId, RelationCatalog, ValueSchema,
};
use std::collections::BTreeSet;
pub(super) fn validate_tables(
    query: &CatalogBoundQuery,
    entities: &[EntityPropertyTable],
    relations: &[BinaryRelationTable],
    limits: PropertyQueryLimits,
) -> Result<()> {
    if [
        limits.max_input_rows,
        limits.max_input_bytes,
        limits.max_join_rows,
        limits.max_output_cells,
        limits.execution_memory_bytes,
    ]
    .contains(&0)
    {
        return Err(DataFusionQueryError::ResourceLimit("zero budget"));
    }
    let catalog = EntityCatalog::admit(entities.iter().map(|t| t.schema.clone()).collect())
        .map_err(|_| DataFusionQueryError::CatalogMismatch)?;
    let relation_catalog =
        RelationCatalog::admit(relations.iter().map(|t| t.schema.clone()).collect())
            .map_err(|_| DataFusionQueryError::CatalogMismatch)?;
    if catalog.digest() != query.entity_catalog_digest()
        || relation_catalog.digest() != query.catalog_digest()
    {
        return Err(DataFusionQueryError::CatalogMismatch);
    }
    let mut count = 0usize;
    let mut bytes = 0usize;
    for batch in entities
        .iter()
        .map(|t| &t.batch)
        .chain(relations.iter().map(|t| &t.batch))
    {
        count = count
            .checked_add(batch.num_rows())
            .ok_or(DataFusionQueryError::ResourceLimit("input rows"))?;
        bytes = bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or(DataFusionQueryError::ResourceLimit("input bytes"))?;
        if count > limits.max_input_rows || bytes > limits.max_input_bytes {
            return Err(DataFusionQueryError::ResourceLimit("input rows or bytes"));
        }
        if batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<BTreeSet<_>>()
            .len()
            != batch.num_columns()
        {
            return invalid("duplicate column names");
        }
    }
    let ids = validate_entities(entities)?;
    validate_relations(relations, &ids)
}
fn validate_entities(entities: &[EntityPropertyTable]) -> Result<BTreeSet<EntityId>> {
    let mut ids = BTreeSet::new();
    for table in entities {
        if table.batch.num_columns() != table.schema.properties().len() + 1 {
            return invalid("entity column count");
        }
        let identities = strings(table.batch.column(0).as_ref())?;
        for row in 0..identities.len() {
            if identities.is_null(row) {
                return invalid("null entity ID");
            }
            let id = identities.value(row).parse::<EntityId>().map_err(|_| {
                DataFusionQueryError::InvalidEntityIdentity(identities.value(row).to_owned())
            })?;
            if !ids.insert(id) {
                return invalid("duplicate entity ID");
            }
        }
        for (index, field) in table.schema.properties().iter().enumerate() {
            if field.schema() != &ValueSchema::String {
                return unsupported("only string properties supported");
            }
            if table.batch.schema().field(index + 1).name() != field.name() {
                return invalid("property column mismatch");
            }
            let values = strings(table.batch.column(index + 1).as_ref())?;
            if !field.nullable() && values.null_count() != 0 {
                return invalid("null non-nullable property");
            }
        }
    }
    Ok(ids)
}
fn validate_relations(relations: &[BinaryRelationTable], ids: &BTreeSet<EntityId>) -> Result<()> {
    for table in relations {
        let [source, target] = table.schema.fields() else {
            return Err(DataFusionQueryError::InvalidRelationSchema);
        };
        if table.batch.num_columns() != 2 {
            return invalid("binary edge column count");
        }
        for (index, field) in [source, target].iter().enumerate() {
            if field.schema() != &ValueSchema::Entity || field.nullable() {
                return Err(DataFusionQueryError::InvalidRelationSchema);
            }
            if table.batch.schema().field(index).name() != field.name() {
                return invalid("edge column mismatch");
            }
            let values = strings(table.batch.column(index).as_ref())?;
            validate_endpoints(values, ids)?;
        }
    }
    Ok(())
}

fn validate_endpoints(values: &arrow_array::StringArray, ids: &BTreeSet<EntityId>) -> Result<()> {
    for row in 0..values.len() {
        if values.is_null(row) {
            return invalid("null edge endpoint");
        }
        let id = values.value(row).parse::<EntityId>().map_err(|_| {
            DataFusionQueryError::InvalidEntityIdentity(values.value(row).to_owned())
        })?;
        if !ids.contains(&id) {
            return invalid("dangling edge endpoint");
        }
    }
    Ok(())
}
