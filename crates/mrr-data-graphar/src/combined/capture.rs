use super::{CapturedCombinedGraphAr, CapturedGraphArRelation, CombinedGraphArLimits};
use crate::{
    BinaryEntityProjection, GraphArEntityPropertyError as Error, GraphArEntityPropertyProjection,
    GraphArEntityPropertyReceipt, GraphArReadLimits,
};
use meta_relational_reasoning::RelationCatalog;
use mrr_data_content::PreparedCombinedGraph;
use mrr_data_core::{BoundDataQuery, GraphDatasetInventory, dag_cbor_cid};
use std::path::Path;

/// Capture a prepared/cold-restored closure into private native working directories.
/// The caller authenticates the `SnapshotManifest` root before binding its MRR query.
/// YAML paths never control native readers; metadata is reconstructed from catalogs.
/// # Errors
/// Refuses root/catalog drift, corrupt content, schema, aggregate budgets or native errors.
pub fn capture_combined_graphar(
    closure: &PreparedCombinedGraph,
    query: &BoundDataQuery,
    relations: &RelationCatalog,
    properties: &GraphArEntityPropertyProjection,
    limits: CombinedGraphArLimits,
) -> Result<CapturedCombinedGraphAr, Error> {
    capture_checked(closure, query, relations, properties, limits, || Ok(()))
}
pub(super) fn capture_checked(
    closure: &PreparedCombinedGraph,
    query: &BoundDataQuery,
    relations: &RelationCatalog,
    properties: &GraphArEntityPropertyProjection,
    limits: CombinedGraphArLimits,
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<CapturedCombinedGraphAr, Error> {
    check()?;
    if closure.root() != query.snapshot_root()
        || relations.digest() != query.query().catalog_digest()
    {
        return Err(Error::Scope);
    }
    let dataset = closure.dataset();
    dataset
        .admit_query(query, limits.dataset)
        .map_err(|_| Error::Scope)?;
    let directory = tempfile::tempdir()?;
    let captured = capture_properties(closure, query, properties, limits, &mut check)?;
    let mut remaining_vertices = limits.topology.max_vertices();
    let mut remaining_edges = limits.topology.max_edges();
    let mut facts = Vec::with_capacity(dataset.relations().len());
    for (index, member) in dataset.relations().iter().enumerate() {
        check()?;
        let projection = BinaryEntityProjection::admit_catalog(relations, member.relation)
            .map_err(|_| Error::Scope)?;
        let source = directory.path().join(format!("relation-{index}"));
        materialize(closure, &member.inventory, &source, &mut check)?;
        let relation = crate::snapshot::capture_inventory(
            &source,
            &member.inventory,
            &projection,
            query.query().generation(),
            limits.dataset.inventory,
            GraphArReadLimits::new(remaining_vertices, remaining_edges),
        )?;
        check()?;
        remaining_vertices = remaining_vertices
            .checked_sub(relation.vertex_count())
            .ok_or(Error::Budget("aggregate topology vertices"))?;
        remaining_edges = remaining_edges
            .checked_sub(relation.facts().len())
            .ok_or(Error::Budget("aggregate topology facts"))?;
        facts.push(CapturedGraphArRelation {
            relation: member.relation,
            facts: relation.shared_facts(),
        });
        std::fs::remove_dir_all(source)?;
    }
    check()?;
    directory.close()?;
    Ok(CapturedCombinedGraphAr {
        snapshot: *query.snapshot_root(),
        descriptor: *query.graph_projection_manifest().ok_or(Error::Scope)?,
        properties: captured,
        relations: facts,
    })
}
pub(super) fn materialize(
    closure: &PreparedCombinedGraph,
    inventory: &GraphDatasetInventory,
    destination: &Path,
    check: &mut impl FnMut() -> Result<(), Error>,
) -> Result<(), Error> {
    std::fs::create_dir(destination)?;
    for file in inventory.files() {
        check()?;
        let bytes = closure.block(file.cid()).ok_or(Error::Integrity)?;
        // PreparedCombinedGraph has already checked all CIDs and declared lengths.
        let target = destination.join(file.path());
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(target, bytes)?;
    }
    Ok(())
}

pub(super) fn capture_properties(
    closure: &PreparedCombinedGraph,
    query: &BoundDataQuery,
    properties: &GraphArEntityPropertyProjection,
    limits: CombinedGraphArLimits,
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<crate::CapturedGraphArEntityProperties, Error> {
    let dataset = closure.dataset();
    let directory = tempfile::tempdir()?;
    let property_path = directory.path().join("properties");
    materialize(
        closure,
        dataset.properties().inventory(),
        &property_path,
        &mut check,
    )?;
    let property_bytes = dataset
        .properties()
        .canonical_bytes(limits.properties.inventory, limits.properties.max_rows)
        .map_err(crate::GraphArInventoryError::from)?;
    let receipt = GraphArEntityPropertyReceipt::decode_descriptor_checked(
        property_path.clone(),
        &dag_cbor_cid(&property_bytes),
        &property_bytes,
        properties,
        limits.properties,
    )?;
    let captured = crate::entity_properties::read::capture_checked(
        &property_path,
        query.query(),
        properties,
        &receipt,
        limits.properties,
        &mut check,
    )?;
    check()?;
    directory.close()?;
    Ok(captured)
}
