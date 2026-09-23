//! Catalog-bound property rows to immutable Arrow/CID snapshot blocks.
//! POO Flow owns row values; MRR owns type IDs and semantic generations.
//! This module owns only the physical representation and local materialization.

use anyhow::{Context, Result, ensure};
use arrow_array::{ArrayRef, RecordBatch, StringArray, builder::StringBuilder};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning::{
    EntityCatalog, EntityId, RelationCatalog, RelationId, SemanticSnapshot, ValueSchema,
};
use mrr_data_content::{AsyncContentStore, ContentBlock, ContentCodec};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, EntityDescriptor, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// One POO-owned entity identity and its named string properties.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyEntityRow {
    pub entity_id: EntityId,
    pub properties: BTreeMap<String, Option<String>>,
}

/// One directed, binary relation from the source projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PropertyRelationRow {
    pub source: EntityId,
    pub target: EntityId,
}

/// Every catalog type must be present, including types with zero rows.
/// Catalog IDs are supplied by MRR, never derived from POO labels here.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PropertySnapshotRows {
    pub entities: BTreeMap<EntityId, Vec<PropertyEntityRow>>,
    pub relations: BTreeMap<RelationId, Vec<PropertyRelationRow>>,
}

/// Bounded physical materialization, independent of query execution budgets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PropertySnapshotLimits {
    pub max_rows: usize,
    pub max_blocks: usize,
    pub max_block_bytes: usize,
    pub max_total_bytes: usize,
}

/// The caller supplies admitted semantic and catalog authority plus exact
/// coverage evidence. This function does not claim source completeness itself.
pub struct PropertySnapshotInput<'a> {
    pub semantic_snapshot: SemanticSnapshot,
    pub relation_catalog: &'a RelationCatalog,
    pub entity_catalog: &'a EntityCatalog,
    pub rows: &'a PropertySnapshotRows,
    pub coverage: CoverageDescriptor,
    pub coverage_bytes: &'a [u8],
    pub limits: PropertySnapshotLimits,
}

/// Root and measured local closure; remote publication uses the existing
/// `mrr_data_content::publish_snapshot` admission path.
#[derive(Debug)]
pub struct MaterializedPropertySnapshot {
    pub snapshot: SnapshotBlock,
    pub row_count: usize,
    pub total_bytes: usize,
}

/// Validate and encode every table before writing any local block. A failed
/// local store may leave immutable children but never yields a root receipt.
///
/// # Errors
/// Rejects missing/extra types, unsupported schemas, duplicate/dangling IDs,
/// bad property values, coverage drift, resource limits, encoding, or storage.
pub async fn materialize_property_snapshot(
    input: PropertySnapshotInput<'_>,
    local: &(impl AsyncContentStore + ?Sized),
) -> Result<MaterializedPropertySnapshot> {
    validate_input(&input)?;
    let limits = input.limits;
    let mut row_count = 0usize;
    let mut children = Vec::<Vec<u8>>::new();
    let (entity_descriptors, ids) = encode_entities(&input, &mut row_count, &mut children)?;
    let relation_descriptors = encode_relations(&input, &ids, &mut row_count, &mut children)?;
    children.push(input.coverage_bytes.to_vec());
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            input.semantic_snapshot,
            input.relation_catalog,
            input.entity_catalog,
            relation_descriptors,
            input.coverage,
        )
        .with_entities(entity_descriptors),
    )?;
    let snapshot = SnapshotBlock::encode(manifest)?;
    ensure!(
        snapshot.bytes().len() <= limits.max_block_bytes,
        "property snapshot root block budget"
    );
    let block_count = children
        .len()
        .checked_add(1)
        .context("block count overflow")?;
    ensure!(
        block_count <= limits.max_blocks,
        "property snapshot block budget"
    );
    let mut total_bytes = snapshot.bytes().len();
    for bytes in &children {
        ensure!(
            bytes.len() <= limits.max_block_bytes,
            "property snapshot block budget"
        );
        total_bytes = total_bytes
            .checked_add(bytes.len())
            .context("byte count overflow")?;
    }
    ensure!(
        total_bytes <= limits.max_total_bytes,
        "property snapshot byte budget"
    );
    for bytes in &children {
        let expected = raw_cid(bytes);
        let stored = local
            .store(ContentBlock::new(ContentCodec::Raw, bytes))
            .await
            .context("store property snapshot child")?;
        ensure!(
            stored == expected,
            "local store returned a different child CID"
        );
    }
    let stored = local
        .store(ContentBlock::new(ContentCodec::DagCbor, snapshot.bytes()))
        .await
        .context("store property snapshot root")?;
    ensure!(
        stored == *snapshot.cid(),
        "local store returned a different root CID"
    );
    Ok(MaterializedPropertySnapshot {
        snapshot,
        row_count,
        total_bytes,
    })
}

fn validate_input(input: &PropertySnapshotInput<'_>) -> Result<()> {
    let limits = input.limits;
    ensure!(
        limits.max_rows > 0
            && limits.max_blocks > 0
            && limits.max_block_bytes > 0
            && limits.max_total_bytes > 0,
        "zero property snapshot budget"
    );
    ensure!(!input.coverage_bytes.is_empty(), "empty coverage evidence");
    ensure!(
        input.coverage.declaration_cid() == &raw_cid(input.coverage_bytes),
        "coverage evidence CID mismatch"
    );
    let expected_entities = input
        .entity_catalog
        .entities()
        .iter()
        .map(meta_relational_reasoning::EntitySchema::id)
        .collect::<BTreeSet<_>>();
    let expected_relations = input
        .relation_catalog
        .relations()
        .iter()
        .map(meta_relational_reasoning::RelationSchema::id)
        .collect::<BTreeSet<_>>();
    ensure!(
        input.rows.entities.keys().copied().collect::<BTreeSet<_>>() == expected_entities,
        "entity table set differs from admitted catalog"
    );
    ensure!(
        input
            .rows
            .relations
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            == expected_relations,
        "relation table set differs from admitted catalog"
    );

    Ok(())
}

fn encode_entities(
    input: &PropertySnapshotInput<'_>,
    row_count: &mut usize,
    children: &mut Vec<Vec<u8>>,
) -> Result<(Vec<EntityDescriptor>, BTreeSet<EntityId>)> {
    let mut ids = BTreeSet::new();
    let mut descriptors = Vec::new();
    let mut source_value_bytes = 0usize;
    for schema in input.entity_catalog.entities() {
        let rows = &input.rows.entities[&schema.id()];
        add_rows(row_count, rows.len(), input.limits.max_rows)?;
        let mut ordered = rows.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|row| row.entity_id);
        for row in &ordered {
            ensure!(ids.insert(row.entity_id), "duplicate entity identity");
            ensure!(
                row.properties.len() == schema.properties().len(),
                "entity property set differs from admitted schema"
            );
            for property in schema.properties() {
                ensure!(
                    property.name() != "entity_id",
                    "reserved entity property name"
                );
                ensure!(
                    property.schema() == &ValueSchema::String,
                    "only string entity properties are supported"
                );
                let value = row
                    .properties
                    .get(property.name())
                    .context("missing catalog property")?;
                ensure!(
                    value.is_some() || property.nullable(),
                    "null required property"
                );
                if let Some(value) = value {
                    ensure!(
                        value.len() <= input.limits.max_block_bytes,
                        "source value block budget"
                    );
                    source_value_bytes = source_value_bytes
                        .checked_add(value.len())
                        .context("source value byte count overflow")?;
                    ensure!(
                        source_value_bytes <= input.limits.max_total_bytes,
                        "source value total budget"
                    );
                }
            }
        }
        let bytes = entity_ipc(schema, &ordered)?;
        let descriptor = batch(&bytes, rows.len())?;
        descriptors.push(EntityDescriptor::new(
            schema.clone(),
            rows.len() as u64,
            vec![descriptor],
        )?);
        children.push(bytes);
    }
    Ok((descriptors, ids))
}

fn encode_relations(
    input: &PropertySnapshotInput<'_>,
    ids: &BTreeSet<EntityId>,
    row_count: &mut usize,
    children: &mut Vec<Vec<u8>>,
) -> Result<Vec<RelationDescriptor>> {
    let mut descriptors = Vec::new();
    for schema in input.relation_catalog.relations() {
        let rows = &input.rows.relations[&schema.id()];
        add_rows(row_count, rows.len(), input.limits.max_rows)?;
        let [source_field, target_field] = schema.fields() else {
            anyhow::bail!("only binary relations are supported");
        };
        ensure!(
            schema.constraints().is_empty()
                && source_field.schema() == &ValueSchema::Entity
                && target_field.schema() == &ValueSchema::Entity
                && !source_field.nullable()
                && !target_field.nullable(),
            "unsupported relation schema"
        );
        let mut ordered = rows.clone();
        ordered.sort_unstable_by_key(|row| (row.source, row.target));
        for row in &ordered {
            ensure!(
                ids.contains(&row.source) && ids.contains(&row.target),
                "dangling relation endpoint"
            );
        }
        let bytes = relation_ipc(source_field.name(), target_field.name(), &ordered)?;
        let descriptor = batch(&bytes, rows.len())?;
        descriptors.push(RelationDescriptor::new(
            schema.id(),
            rows.len() as u64,
            vec![descriptor],
        )?);
        children.push(bytes);
    }
    Ok(descriptors)
}

fn add_rows(total: &mut usize, count: usize, max: usize) -> Result<()> {
    *total = total.checked_add(count).context("row count overflow")?;
    ensure!(*total <= max, "property snapshot row budget");
    Ok(())
}

fn batch(bytes: &[u8], row_count: usize) -> Result<BatchDescriptor> {
    Ok(BatchDescriptor::new(
        raw_cid(bytes),
        row_count as u64,
        bytes.len() as u64,
    )?)
}

fn string_column<'a>(values: impl Iterator<Item = Option<&'a str>>) -> StringArray {
    let mut builder = StringBuilder::new();
    for value in values {
        if let Some(value) = value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }
    builder.finish()
}

fn entity_ipc(
    schema: &meta_relational_reasoning::EntitySchema,
    rows: &[&PropertyEntityRow],
) -> Result<Vec<u8>> {
    let mut fields = vec![Field::new("entity_id", DataType::Utf8, false)];
    let ids = rows
        .iter()
        .map(|row| row.entity_id.to_string())
        .collect::<Vec<_>>();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(string_column(
        ids.iter().map(|id| Some(id.as_str())),
    ))];
    for property in schema.properties() {
        fields.push(Field::new(
            property.name(),
            DataType::Utf8,
            property.nullable(),
        ));
        columns.push(Arc::new(string_column(
            rows.iter()
                .map(|row| row.properties[property.name()].as_deref()),
        )));
    }
    ipc(fields, columns)
}

fn relation_ipc(source: &str, target: &str, rows: &[PropertyRelationRow]) -> Result<Vec<u8>> {
    let fields = vec![
        Field::new(source, DataType::Utf8, false),
        Field::new(target, DataType::Utf8, false),
    ];
    let sources = rows
        .iter()
        .map(|row| row.source.to_string())
        .collect::<Vec<_>>();
    let targets = rows
        .iter()
        .map(|row| row.target.to_string())
        .collect::<Vec<_>>();
    ipc(
        fields,
        vec![
            Arc::new(string_column(
                sources.iter().map(|value| Some(value.as_str())),
            )),
            Arc::new(string_column(
                targets.iter().map(|value| Some(value.as_str())),
            )),
        ],
    )
}

fn ipc(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

#[cfg(test)]
#[path = "../tests/unit/property_snapshot.rs"]
mod tests;
