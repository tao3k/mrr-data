//! Exact admitted catalog to declared native vertex labels and Arrow columns.
use super::{GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use graphar_rs::{
    info::{InfoVersion, VertexInfo},
    property::{Property, PropertyGroup, PropertyVec},
    types::{Cardinality, DataType as NativeType, FileType},
};
use meta_relational_reasoning::{
    EntityCatalog, EntityCatalogDigest, EntityId, EntitySchema, ValueSchema,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

/// Only the declared String/nullable slice is admitted. No IDs are derived from
/// user labels and no unsupported MRR scalar is silently stringified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphArEntityPropertyProjection {
    pub(super) catalog: EntityCatalog,
}
/// One exact logical type; this Arrow shape is accepted by the property executor.
#[derive(Clone, Debug)]
pub struct GraphArEntityPropertyTable {
    pub schema: EntitySchema,
    pub batch: RecordBatch,
}
impl GraphArEntityPropertyProjection {
    /// # Errors
    /// Rejects reserved identity names and unsupported property schemas.
    pub fn admit(catalog: &EntityCatalog) -> Result<Self, Error> {
        if catalog.entities().iter().any(|s| {
            s.properties()
                .iter()
                .any(|p| p.name() == "entity_id" || p.schema() != &ValueSchema::String)
        }) {
            return Err(Error::UnsupportedSchema);
        }
        Ok(Self {
            catalog: catalog.clone(),
        })
    }
    #[must_use]
    pub const fn catalog_digest(&self) -> EntityCatalogDigest {
        self.catalog.digest()
    }
    fn check_limits(&self, limits: GraphArEntityPropertyLimits) -> Result<(), Error> {
        limits.validate()?;
        if self.catalog.entities().len() > limits.max_types {
            return Err(Error::Budget("entity types"));
        }
        let properties = self
            .catalog
            .entities()
            .iter()
            .try_fold(0usize, |total, s| {
                total
                    .checked_add(s.properties().len())
                    .ok_or(Error::Budget("property count overflow"))
            })?;
        if properties > limits.max_properties {
            return Err(Error::Budget("property count"));
        }
        Ok(())
    }
    pub(super) fn native_infos(
        &self,
        chunk: usize,
        limits: GraphArEntityPropertyLimits,
    ) -> Result<Vec<VertexInfo>, Error> {
        self.check_limits(limits)?;
        self.catalog
            .entities()
            .iter()
            .map(|s| native_info(s, chunk))
            .collect()
    }
    pub(super) fn validate_tables(
        &self,
        tables: &[GraphArEntityPropertyTable],
        limits: GraphArEntityPropertyLimits,
    ) -> Result<BTreeMap<EntityId, Vec<(EntityId, usize)>>, Error> {
        self.check_limits(limits)?;
        if tables.len() != self.catalog.entities().len() {
            return Err(Error::Shape("type table set"));
        }
        let mut ids = BTreeSet::new();
        let mut ordered = BTreeMap::new();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for table in tables {
            if self.catalog.entity(table.schema.id()) != Some(&table.schema)
                || table.batch.schema().as_ref() != &arrow_schema(&table.schema)
            {
                return Err(Error::Shape("catalog or Arrow schema"));
            }
            rows = rows
                .checked_add(table.batch.num_rows())
                .ok_or(Error::Budget("row count overflow"))?;
            if rows > limits.max_rows {
                return Err(Error::Budget("entity rows"));
            }
            let mut indices = Vec::with_capacity(table.batch.num_rows());
            let key = strings(&table.batch, 0)?;
            for row in 0..table.batch.num_rows() {
                if key.is_null(row) {
                    return Err(Error::Shape("null entity identity"));
                }
                let id = EntityId::from_str(key.value(row))
                    .map_err(|_| Error::Shape("canonical entity identity"))?;
                if key.value(row) != id.to_string() || !ids.insert(id) {
                    return Err(Error::Shape("noncanonical or duplicate identity"));
                }
                for (column, field) in table.batch.schema().fields().iter().enumerate() {
                    let values = strings(&table.batch, column)?;
                    if values.is_null(row) && !field.is_nullable() {
                        return Err(Error::Shape("null required property"));
                    }
                    if !values.is_null(row) {
                        bytes = bytes
                            .checked_add(values.value(row).len())
                            .ok_or(Error::Budget("value bytes overflow"))?;
                    }
                    if bytes > limits.max_value_bytes {
                        return Err(Error::Budget("entity value bytes"));
                    }
                }
                indices.push((id, row));
            }
            indices.sort_unstable_by_key(|(id, _)| *id);
            if ordered.insert(table.schema.id(), indices).is_some() {
                return Err(Error::Shape("duplicate type table"));
            }
        }
        Ok(ordered)
    }
}
pub(super) fn strings(batch: &RecordBatch, index: usize) -> Result<&StringArray, Error> {
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or(Error::Shape("expected Utf8 column"))
}
pub(super) fn arrow_schema(schema: &EntitySchema) -> Schema {
    Schema::new(
        std::iter::once(Field::new("entity_id", DataType::Utf8, false))
            .chain(
                schema
                    .properties()
                    .iter()
                    .map(|p| Field::new(p.name(), DataType::Utf8, p.nullable())),
            )
            .collect::<Vec<_>>(),
    )
}
pub(super) fn label(schema: &EntitySchema) -> String {
    use std::fmt::Write as _;
    schema
        .id()
        .digest_bytes()
        .iter()
        .fold(String::from("mrr_entity_"), |mut label, byte| {
            write!(&mut label, "{byte:02x}").expect("write into String");
            label
        })
}
fn native_info(schema: &EntitySchema, chunk: usize) -> Result<VertexInfo, Error> {
    let mut properties = PropertyVec::new();
    properties.push(Property::new(
        "entity_id",
        NativeType::string(),
        true,
        false,
        Cardinality::Single,
    ));
    for p in schema.properties() {
        properties.push(Property::new(
            p.name(),
            NativeType::string(),
            false,
            p.nullable(),
            Cardinality::Single,
        ));
    }
    Ok(VertexInfo::builder(
        label(schema),
        i64::try_from(chunk).map_err(|_| Error::Budget("vertex chunk size"))?,
    )
    .push_property_group(PropertyGroup::new(
        properties,
        FileType::Parquet,
        "properties/",
    ))
    .prefix(format!("vertex/{}/", label(schema)))
    .version(InfoVersion::new(1)?)
    .try_build()?)
}
pub(super) fn table(
    schema: &EntitySchema,
    columns: Vec<Arc<StringArray>>,
) -> Result<GraphArEntityPropertyTable, Error> {
    Ok(GraphArEntityPropertyTable {
        schema: schema.clone(),
        batch: RecordBatch::try_new(
            Arc::new(arrow_schema(schema)),
            columns
                .into_iter()
                .map(|a| a as arrow_array::ArrayRef)
                .collect(),
        )?,
    })
}
