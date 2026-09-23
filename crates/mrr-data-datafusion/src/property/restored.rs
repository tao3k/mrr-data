//! Decode only a verified physical closure before bounded property execution.
use std::{io::Cursor, sync::Arc};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::compute::concat_batches;
use meta_relational_reasoning::{
    CatalogBoundQuery, EntityCatalog, RelationCatalog, RelationSchema,
};
use mrr_data_content::RestoredSnapshot;
use mrr_data_core::{
    BatchDescriptor, CoverageKind, EntityDescriptor, PhysicalQueryOutput, RelationDescriptor,
    SnapshotManifest,
};

use super::{
    BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits, execute_property_path_query,
};
use crate::DataFusionQueryError;

type Result<T> = std::result::Result<T, DataFusionQueryError>;

/// Named physical inputs; the query and catalogs remain owned by MRR.
pub struct RestoredPropertyQuery<'a> {
    pub query: &'a CatalogBoundQuery,
    pub restored: &'a RestoredSnapshot,
    pub relation_catalog: &'a RelationCatalog,
    pub entity_catalog: &'a EntityCatalog,
    pub limits: PropertyQueryLimits,
}

/// Execute one bounded property path from a fully verified content closure.
/// The caller retains MRR query binding and semantic result admission authority.
///
/// # Errors
/// Rejects catalog or generation drift, incomplete coverage, missing or malformed
/// IPC children, row-count discrepancies, and all physical query limits.
pub async fn execute_restored_property_path_query(
    input: RestoredPropertyQuery<'_>,
) -> Result<PhysicalQueryOutput> {
    let manifest = input.restored.snapshot().manifest();
    validate_restored_binding(&input, manifest)?;
    check_declared_limits(manifest, input.limits)?;
    let entities = decode_entity_tables(input.restored, manifest)?;
    let relations = decode_relation_tables(input.restored, manifest, input.relation_catalog)?;
    execute_property_path_query(input.query, &entities, &relations, input.limits).await
}

fn validate_restored_binding(
    input: &RestoredPropertyQuery<'_>,
    manifest: &SnapshotManifest,
) -> Result<()> {
    manifest
        .verify_catalogs(input.relation_catalog, input.entity_catalog)
        .map_err(|_| DataFusionQueryError::CatalogMismatch)?;
    if input.relation_catalog.digest() != input.query.catalog_digest()
        || input.entity_catalog.digest() != input.query.entity_catalog_digest()
        || manifest.semantic_snapshot().generation() != input.query.generation()
        || manifest.semantic_snapshot().digest() != input.query.snapshot_digest()
    {
        return Err(DataFusionQueryError::CatalogMismatch);
    }
    if manifest.coverage().kind() != CoverageKind::Complete {
        return Err(DataFusionQueryError::RestoredSnapshot(
            "incomplete coverage",
        ));
    }
    Ok(())
}

fn check_declared_limits(manifest: &SnapshotManifest, limits: PropertyQueryLimits) -> Result<()> {
    let mut descriptors = manifest
        .entities()
        .iter()
        .flat_map(EntityDescriptor::batches)
        .chain(
            manifest
                .relations()
                .iter()
                .flat_map(RelationDescriptor::batches),
        );
    descriptors.try_fold((0_usize, 0_usize), |(rows, bytes), batch| {
        let next_rows = rows
            .checked_add(
                usize::try_from(batch.row_count())
                    .map_err(|_| DataFusionQueryError::ResourceLimit("declared input rows"))?,
            )
            .ok_or(DataFusionQueryError::ResourceLimit("declared input rows"))?;
        let next_bytes = bytes
            .checked_add(
                usize::try_from(batch.byte_length())
                    .map_err(|_| DataFusionQueryError::ResourceLimit("declared input bytes"))?,
            )
            .ok_or(DataFusionQueryError::ResourceLimit("declared input bytes"))?;
        if next_rows > limits.max_input_rows || next_bytes > limits.max_input_bytes {
            return Err(DataFusionQueryError::ResourceLimit(
                "declared input rows or bytes",
            ));
        }
        Ok((next_rows, next_bytes))
    })?;
    Ok(())
}

fn decode_entity_tables(
    restored: &RestoredSnapshot,
    manifest: &SnapshotManifest,
) -> Result<Vec<EntityPropertyTable>> {
    manifest
        .entities()
        .iter()
        .map(|entity| {
            let mut fields = vec![Field::new("entity_id", DataType::Utf8, false)];
            fields.extend(
                entity.schema().properties().iter().map(|property| {
                    Field::new(property.name(), DataType::Utf8, property.nullable())
                }),
            );
            Ok(EntityPropertyTable {
                schema: entity.schema().clone(),
                batch: decode_batches(restored, entity.batches(), Arc::new(Schema::new(fields)))?,
            })
        })
        .collect()
}

fn decode_relation_tables(
    restored: &RestoredSnapshot,
    manifest: &SnapshotManifest,
    relation_catalog: &RelationCatalog,
) -> Result<Vec<BinaryRelationTable>> {
    manifest
        .relations()
        .iter()
        .map(|descriptor| {
            let schema = relation_catalog
                .relations()
                .iter()
                .find(|schema| schema.id() == descriptor.relation_id())
                .ok_or(DataFusionQueryError::CatalogMismatch)?;
            Ok(BinaryRelationTable {
                schema: schema.clone(),
                batch: decode_batches(
                    restored,
                    descriptor.batches(),
                    relation_arrow_schema(schema),
                )?,
            })
        })
        .collect()
}

fn relation_arrow_schema(schema: &RelationSchema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), DataType::Utf8, field.nullable()))
            .collect::<Vec<_>>(),
    ))
}

fn decode_batches(
    restored: &RestoredSnapshot,
    descriptors: &[BatchDescriptor],
    expected_schema: SchemaRef,
) -> Result<RecordBatch> {
    let batches: Vec<_> = descriptors
        .iter()
        .map(|descriptor| decode_child(restored, descriptor))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(expected_schema));
    }
    if batches.iter().any(|batch| {
        batch.schema().fields().len() != expected_schema.fields().len()
            || batch
                .schema()
                .fields()
                .iter()
                .zip(expected_schema.fields())
                .any(|(actual, expected)| {
                    actual.name() != expected.name() || actual.data_type() != expected.data_type()
                })
    }) {
        return Err(DataFusionQueryError::InvalidArrowBatch(
            "catalog column shape",
        ));
    }
    concat_batches(&batches[0].schema(), &batches)
        .map_err(|error| DataFusionQueryError::ArrowIpc(error.to_string()))
}

fn decode_child(
    restored: &RestoredSnapshot,
    descriptor: &BatchDescriptor,
) -> Result<Vec<RecordBatch>> {
    let bytes = restored
        .children()
        .get(descriptor.cid())
        .ok_or(DataFusionQueryError::RestoredSnapshot("missing child"))?;
    if u64::try_from(bytes.len()).ok() != Some(descriptor.byte_length()) {
        return Err(DataFusionQueryError::RestoredSnapshot(
            "child byte length mismatch",
        ));
    }
    let reader = StreamReader::try_new(Cursor::new(bytes.as_slice()), None)
        .map_err(|error| DataFusionQueryError::ArrowIpc(error.to_string()))?;
    let batches = reader
        .map(|batch| batch.map_err(|error| DataFusionQueryError::ArrowIpc(error.to_string())))
        .collect::<Result<Vec<_>>>()?;
    let rows = batches.iter().try_fold(0_u64, |count, batch| {
        count
            .checked_add(batch.num_rows() as u64)
            .ok_or(DataFusionQueryError::RestoredSnapshot("row count overflow"))
    })?;
    if rows != descriptor.row_count() {
        return Err(DataFusionQueryError::RestoredSnapshot(
            "child row count mismatch",
        ));
    }
    Ok(batches)
}
