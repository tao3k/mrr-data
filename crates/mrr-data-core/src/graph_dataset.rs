//! Acyclic native dataset descriptor: properties plus every declared relation inventory.
use crate::{
    BoundDataQuery, GRAPHAR_DATASET_NAMESPACE, GraphDatasetInventory,
    GraphEntityPropertyDescriptor, GraphInventoryError as Error, GraphInventoryLimits,
    GraphProjectionDescriptor, GraphProjectionKind, dag_cbor_cid,
};
use cid::Cid;
use meta_relational_reasoning::{CatalogBoundQuery, EntityCatalog, RelationCatalog, RelationId};
use mrr_data_profile::GRAPHAR_DATASET_SCHEMA;
use serde::{Deserialize, Serialize};

/// Aggregate metadata and physical inventory bounds across the entire dataset.
#[derive(Clone, Copy, Debug)]
pub struct GraphDatasetLimits {
    pub inventory: GraphInventoryLimits,
    pub max_relations: usize,
    pub max_property_rows: usize,
}
/// One binary relation artifact. Logical paths are local to this artifact only.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphRelationMember {
    pub relation: RelationId,
    pub inventory: GraphDatasetInventory,
}
/// No snapshot root is embedded, so registration cannot produce a CID cycle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDatasetDescriptor {
    namespace: String,
    version: u64,
    relation_catalog: [u8; 32],
    properties: GraphEntityPropertyDescriptor,
    relations: Vec<GraphRelationMember>,
}
impl GraphDatasetDescriptor {
    /// # Errors
    /// Refuses semantic/catalog drift, missing/extra relations and aggregate budgets.
    pub fn admit(
        query: &CatalogBoundQuery,
        catalog: &RelationCatalog,
        properties: GraphEntityPropertyDescriptor,
        mut relations: Vec<GraphRelationMember>,
        limits: GraphDatasetLimits,
    ) -> Result<Self, Error> {
        relations.sort_unstable_by_key(|r| r.relation);
        let value = Self {
            namespace: GRAPHAR_DATASET_NAMESPACE.into(),
            version: GRAPHAR_DATASET_SCHEMA.version,
            relation_catalog: *catalog.digest().as_bytes(),
            properties,
            relations,
        };
        value.check_semantic(query)?;
        value.check_relations(catalog)?;
        value.canonical_bytes(limits)?;
        Ok(value)
    }
    fn check_semantic(&self, query: &CatalogBoundQuery) -> Result<(), Error> {
        let scope = self.properties.scope();
        if self.relation_catalog != *query.catalog_digest().as_bytes()
            || scope.catalog != *query.entity_catalog_digest().as_bytes()
            || scope.snapshot != *query.snapshot_digest()
            || scope.generation != query.generation()
        {
            return Err(Error::Integrity);
        }
        Ok(())
    }
    fn check_relations(&self, catalog: &RelationCatalog) -> Result<(), Error> {
        if self.relation_catalog != *catalog.digest().as_bytes()
            || self.relations.iter().map(|r| r.relation).ne(catalog
                .relations()
                .iter()
                .map(meta_relational_reasoning::RelationSchema::id))
        {
            return Err(Error::Integrity);
        }
        Ok(())
    }
    /// # Errors
    /// Rechecks catalogs and exact full relation coverage after decoding external metadata.
    pub fn verify_catalogs(
        &self,
        relations: &RelationCatalog,
        entities: &EntityCatalog,
    ) -> Result<(), Error> {
        self.check_relations(relations)?;
        if self.properties.scope().catalog != *entities.digest().as_bytes() {
            return Err(Error::Integrity);
        }
        Ok(())
    }
    /// # Errors
    /// Refuses another semantic or physical query/root profile before native I/O.
    pub fn admit_query(
        &self,
        query: &BoundDataQuery,
        limits: GraphDatasetLimits,
    ) -> Result<(), Error> {
        self.check_semantic(query.query())?;
        if query.graph_projection_kind() != Some(GraphProjectionKind::Dataset)
            || query.graph_projection_manifest()
                != Some(&dag_cbor_cid(&self.canonical_bytes(limits)?))
        {
            return Err(Error::Integrity);
        }
        Ok(())
    }
    /// # Errors
    /// Refuses malformed/duplicate members and global file/count/byte/encoding limits.
    pub fn canonical_bytes(&self, limits: GraphDatasetLimits) -> Result<Vec<u8>, Error> {
        if !GRAPHAR_DATASET_SCHEMA.accepts(&self.namespace, self.version) {
            return Err(Error::UnsupportedVersion);
        }
        if limits.max_relations == 0
            || self.relations.len() > limits.max_relations
            || self
                .relations
                .windows(2)
                .any(|r| r[0].relation >= r[1].relation)
        {
            return Err(Error::Limit);
        }
        self.properties
            .canonical_bytes(limits.inventory, limits.max_property_rows)?;
        let mut count = 0usize;
        let mut total = 0u64;
        for inventory in std::iter::once(self.properties.inventory())
            .chain(self.relations.iter().map(|r| &r.inventory))
        {
            inventory.canonical_bytes(limits.inventory)?;
            count = count
                .checked_add(inventory.files().len())
                .ok_or(Error::Limit)?;
            for file in inventory.files() {
                total = total.checked_add(file.byte_length()).ok_or(Error::Limit)?;
            }
        }
        if count > limits.inventory.max_files || total > limits.inventory.max_total_bytes {
            return Err(Error::Limit);
        }
        let bytes = serde_ipld_dagcbor::to_vec(self).map_err(|_| Error::Encode)?;
        if bytes.len() > limits.inventory.max_manifest_bytes {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    /// # Errors
    /// Authenticates bounded canonical bytes before returning any member inventories.
    pub fn decode_checked(
        bytes: &[u8],
        root: &Cid,
        limits: GraphDatasetLimits,
    ) -> Result<Self, Error> {
        if bytes.len() > limits.inventory.max_manifest_bytes {
            return Err(Error::Limit);
        }
        if dag_cbor_cid(bytes) != *root {
            return Err(Error::Integrity);
        }
        let value: Self = serde_ipld_dagcbor::from_slice(bytes).map_err(|_| Error::Decode)?;
        if value.canonical_bytes(limits)? != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(value)
    }
    /// # Errors
    /// Registers this exact acyclic dataset descriptor; invalid encoding/version refuses.
    pub fn snapshot_projection(
        &self,
        limits: GraphDatasetLimits,
    ) -> Result<GraphProjectionDescriptor, Error> {
        GraphProjectionDescriptor::dataset("1", dag_cbor_cid(&self.canonical_bytes(limits)?))
            .map_err(|_| Error::InvalidCid)
    }
    #[must_use]
    pub const fn properties(&self) -> &GraphEntityPropertyDescriptor {
        &self.properties
    }
    #[must_use]
    pub fn relations(&self) -> &[GraphRelationMember] {
        &self.relations
    }
}
