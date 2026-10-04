//! Shared canonical property control metadata for native capture and closure transfer.
use crate::{
    GRAPHAR_ENTITY_PROPERTIES_NAMESPACE, GraphDatasetInventory, GraphInventoryError as Error,
    GraphInventoryLimits, dag_cbor_cid,
};
use cid::Cid;
use meta_relational_reasoning::GenerationId;
use serde::{Deserialize, Serialize};

/// Physical declarations; these bytes confer no semantic or publication authority.
#[derive(Clone, Copy, Debug)]
pub struct GraphEntityPropertyScope {
    pub catalog: [u8; 32],
    pub generation: GenerationId,
    pub snapshot: [u8; 32],
    pub rows: u64,
    pub vertex_chunk: u64,
}
/// Frozen property descriptor wire shape shared by storage and transfer owners.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphEntityPropertyDescriptor {
    namespace: String,
    version: u8,
    inventory: GraphDatasetInventory,
    catalog: [u8; 32],
    generation: GenerationId,
    snapshot: [u8; 32],
    rows: u64,
    vertex_chunk: u64,
}
impl GraphEntityPropertyDescriptor {
    /// # Errors
    /// Refuses malformed inventories, zero chunk width and declared row limits.
    pub fn admit(
        inventory: GraphDatasetInventory,
        scope: GraphEntityPropertyScope,
        limits: GraphInventoryLimits,
        max_rows: usize,
    ) -> Result<Self, Error> {
        let value = Self {
            namespace: GRAPHAR_ENTITY_PROPERTIES_NAMESPACE.into(),
            version: 1,
            inventory,
            catalog: scope.catalog,
            generation: scope.generation,
            snapshot: scope.snapshot,
            rows: scope.rows,
            vertex_chunk: scope.vertex_chunk,
        };
        value.canonical_bytes(limits, max_rows)?;
        Ok(value)
    }
    /// # Errors
    /// Refuses metadata/row/layout/encoding limits. This does not authenticate a root.
    pub fn canonical_bytes(
        &self,
        limits: GraphInventoryLimits,
        max_rows: usize,
    ) -> Result<Vec<u8>, Error> {
        self.validate(limits, max_rows)?;
        let bytes = serde_ipld_dagcbor::to_vec(self).map_err(|_| Error::Encode)?;
        if bytes.len() > limits.max_manifest_bytes {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    /// # Errors
    /// Checks byte limits and exact CID before decoding; rejects unknown/noncanonical declarations.
    pub fn decode_checked(
        bytes: &[u8],
        root: &Cid,
        limits: GraphInventoryLimits,
        max_rows: usize,
    ) -> Result<Self, Error> {
        if bytes.len() > limits.max_manifest_bytes {
            return Err(Error::Limit);
        }
        if dag_cbor_cid(bytes) != *root {
            return Err(Error::Integrity);
        }
        let value: Self = serde_ipld_dagcbor::from_slice(bytes).map_err(|_| Error::Decode)?;
        if value.canonical_bytes(limits, max_rows)? != bytes {
            return Err(Error::NonCanonical);
        }
        Ok(value)
    }
    fn validate(&self, limits: GraphInventoryLimits, max_rows: usize) -> Result<(), Error> {
        if self.namespace != GRAPHAR_ENTITY_PROPERTIES_NAMESPACE || self.version != 1 {
            return Err(Error::UnsupportedVersion);
        }
        self.inventory.canonical_bytes(limits)?;
        if max_rows == 0
            || self.rows > max_rows as u64
            || self.vertex_chunk == 0
            || usize::try_from(self.vertex_chunk).is_err()
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
    #[must_use]
    pub const fn inventory(&self) -> &GraphDatasetInventory {
        &self.inventory
    }
    #[must_use]
    pub const fn scope(&self) -> GraphEntityPropertyScope {
        GraphEntityPropertyScope {
            catalog: self.catalog,
            generation: self.generation,
            snapshot: self.snapshot,
            rows: self.rows,
            vertex_chunk: self.vertex_chunk,
        }
    }
}
