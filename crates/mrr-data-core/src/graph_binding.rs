//! Separate authenticated root for graph files and their semantic snapshot.
use crate::{
    BoundDataQuery, GraphDatasetInventory, GraphInventoryError, GraphInventoryLimits,
    admit_graph_projection_source, dag_cbor_cid, profile::validate_cid,
};
use cid::Cid;
use meta_relational_reasoning::{GenerationId, RelationId};
use serde::{Deserialize, Serialize};

const NAMESPACE: &str = "mrr.graphar.dataset-binding.v1";
const MAX_BYTES: usize = 4096;
/// Binds the complete inventory to one semantic snapshot and relation.
/// The Host must authenticate this root separately: the legacy snapshot's RAW
/// metadata CID alone does not authenticate an inventory of data chunks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDatasetBinding {
    namespace: String,
    version: u64,
    snapshot_root: Cid,
    generation: GenerationId,
    #[serde(with = "serde_bytes")]
    semantic_digest: Vec<u8>,
    #[serde(with = "serde_bytes")]
    relation_catalog_digest: Vec<u8>,
    #[serde(with = "serde_bytes")]
    entity_catalog_digest: Vec<u8>,
    relation: RelationId,
    inventory_root: Cid,
    metadata_root: Cid,
}
impl GraphDatasetBinding {
    /// Construct a publication candidate from an already bound query.
    /// # Errors
    /// Refuses inventory limits, missing source relation or metadata CID drift.
    pub fn admit(
        query: &BoundDataQuery,
        relation: RelationId,
        inventory: &GraphDatasetInventory,
        limits: GraphInventoryLimits,
    ) -> Result<Self, GraphInventoryError> {
        let inventory_root = dag_cbor_cid(&inventory.canonical_bytes(limits)?);
        let metadata_root = *inventory
            .files()
            .iter()
            .find(|f| f.path() == inventory.entry())
            .ok_or(GraphInventoryError::MissingEntry)?
            .cid();
        admit_graph_projection_source(query, relation, &metadata_root)
            .map_err(|_| GraphInventoryError::Integrity)?;
        let semantic = query.query();
        Ok(Self {
            namespace: NAMESPACE.into(),
            version: 1,
            snapshot_root: *query.snapshot_root(),
            generation: semantic.generation(),
            semantic_digest: semantic.snapshot_digest().to_vec(),
            relation_catalog_digest: semantic.catalog_digest().as_bytes().to_vec(),
            entity_catalog_digest: semantic.entity_catalog_digest().as_bytes().to_vec(),
            relation,
            inventory_root,
            metadata_root,
        })
    }
    /// Check every semantic identity and the complete inventory before native I/O.
    /// # Errors
    /// Refuses another snapshot, catalog, generation, relation or physical file set.
    pub fn admit_query(
        &self,
        query: &BoundDataQuery,
        inventory: &GraphDatasetInventory,
        limits: GraphInventoryLimits,
    ) -> Result<(), GraphInventoryError> {
        self.admit_query_scope(query)?;
        if dag_cbor_cid(&inventory.canonical_bytes(limits)?) != self.inventory_root {
            return Err(GraphInventoryError::Integrity);
        }
        let entry = inventory
            .files()
            .iter()
            .find(|f| f.path() == inventory.entry())
            .ok_or(GraphInventoryError::MissingEntry)?;
        if *entry.cid() != self.metadata_root {
            return Err(GraphInventoryError::Integrity);
        }
        Ok(())
    }
    /// Reauthorize an already captured inventory without reencoding its file list.
    /// # Errors
    /// Refuses snapshot, generation, catalogs, relation or metadata identity drift.
    pub fn admit_query_scope(&self, query: &BoundDataQuery) -> Result<(), GraphInventoryError> {
        self.validate()?;
        let semantic = query.query();
        if self.snapshot_root != *query.snapshot_root()
            || self.generation != semantic.generation()
            || self.semantic_digest.as_slice() != semantic.snapshot_digest().as_slice()
            || self.relation_catalog_digest.as_slice()
                != semantic.catalog_digest().as_bytes().as_slice()
            || self.entity_catalog_digest.as_slice()
                != semantic.entity_catalog_digest().as_bytes().as_slice()
        {
            return Err(GraphInventoryError::Integrity);
        }
        admit_graph_projection_source(query, self.relation, &self.metadata_root)
            .map_err(|_| GraphInventoryError::Integrity)
    }
    #[must_use]
    pub const fn generation(&self) -> GenerationId {
        self.generation
    }
    #[must_use]
    pub const fn relation(&self) -> RelationId {
        self.relation
    }
    #[must_use]
    pub const fn inventory_root(&self) -> &Cid {
        &self.inventory_root
    }
    /// Canonical bounded publication bytes. This operation does not publish them.
    /// # Errors
    /// Refuses invalid identities or encoding overflow.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, GraphInventoryError> {
        self.validate()?;
        let bytes = serde_ipld_dagcbor::to_vec(self).map_err(|_| GraphInventoryError::Encode)?;
        if bytes.len() > MAX_BYTES {
            return Err(GraphInventoryError::Limit);
        }
        Ok(bytes)
    }
    /// Verify against a Host-authenticated binding root before decoding.
    /// # Errors
    /// Refuses wrong roots, unknown fields/versions, malformed identities or noncanonical bytes.
    pub fn decode_checked(bytes: &[u8], trusted_root: &Cid) -> Result<Self, GraphInventoryError> {
        if bytes.len() > MAX_BYTES {
            return Err(GraphInventoryError::Limit);
        }
        if validate_cid(trusted_root, crate::DAG_CBOR_CODEC).is_err()
            || dag_cbor_cid(bytes) != *trusted_root
        {
            return Err(GraphInventoryError::Integrity);
        }
        let value: Self =
            serde_ipld_dagcbor::from_slice(bytes).map_err(|_| GraphInventoryError::Decode)?;
        if value.canonical_bytes()? != bytes {
            return Err(GraphInventoryError::NonCanonical);
        }
        Ok(value)
    }
    fn validate(&self) -> Result<(), GraphInventoryError> {
        if self.namespace != NAMESPACE || self.version != 1 {
            return Err(GraphInventoryError::UnsupportedVersion);
        }
        if self.semantic_digest.len() != 32
            || self.relation_catalog_digest.len() != 32
            || self.entity_catalog_digest.len() != 32
        {
            return Err(GraphInventoryError::Integrity);
        }
        for root in [&self.snapshot_root, &self.inventory_root] {
            validate_cid(root, crate::DAG_CBOR_CODEC)
                .map_err(|_| GraphInventoryError::InvalidCid)?;
        }
        validate_cid(&self.metadata_root, crate::RAW_CODEC)
            .map_err(|_| GraphInventoryError::InvalidCid)
    }
}
