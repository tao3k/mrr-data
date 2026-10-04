//! Versioned MRR Data control-plane metadata, not a second `GraphAr` file format.
use super::{
    GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt,
};
use crate::GraphArChunkLayout;
use cid::Cid;
use meta_relational_reasoning::GenerationId;
use mrr_data_core::{GraphDatasetInventory, dag_cbor_cid};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const NAMESPACE: &str = mrr_data_core::GRAPHAR_ENTITY_PROPERTIES_NAMESPACE;
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    namespace: String,
    version: u8,
    inventory: GraphDatasetInventory,
    catalog: [u8; 32],
    generation: GenerationId,
    snapshot: [u8; 32],
    rows: u64,
    vertex_chunk: u64,
}
/// Immutable portable source descriptor and its exact DAG-CBOR content identity.
/// The local source path is never serialized. The caller authenticates this CID
/// in its root/publication protocol before accepting external data.
pub struct GraphArEntityPropertyBlock {
    cid: Cid,
    bytes: Vec<u8>,
}
impl GraphArEntityPropertyBlock {
    #[must_use]
    pub const fn cid(&self) -> &Cid {
        &self.cid
    }
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
impl GraphArEntityPropertyReceipt {
    /// Persist catalog/snapshot/generation scope, exact inventory and row/layout
    /// declarations so another process can reopen the same physical artifact.
    /// # Errors
    /// Rejects invalid/oversized control-plane declarations or encoding failures.
    pub fn descriptor(
        &self,
        limits: GraphArEntityPropertyLimits,
    ) -> Result<GraphArEntityPropertyBlock, Error> {
        limits.validate()?;
        self.inventory
            .canonical_bytes(limits.inventory)
            .map_err(crate::GraphArInventoryError::from)?;
        if self.rows > limits.max_rows {
            return Err(Error::Budget("entity rows"));
        }
        let wire = Wire {
            namespace: NAMESPACE.into(),
            version: 1,
            inventory: self.inventory.clone(),
            catalog: *self.catalog.as_bytes(),
            generation: self.generation,
            snapshot: self.snapshot,
            rows: u64::try_from(self.rows).map_err(|_| Error::Budget("row count overflow"))?,
            vertex_chunk: u64::try_from(self.layout.vertex_chunk_size())
                .map_err(|_| Error::Budget("vertex chunk size"))?,
        };
        let bytes = serde_ipld_dagcbor::to_vec(&wire)
            .map_err(|_| Error::Shape("property descriptor encoding"))?;
        if bytes.len() > limits.inventory.max_manifest_bytes {
            return Err(Error::Budget("property descriptor bytes"));
        }
        Ok(GraphArEntityPropertyBlock {
            cid: dag_cbor_cid(&bytes),
            bytes,
        })
    }
    /// Authenticate the descriptor's exact CID and canonical bytes before
    /// restoring a local receipt. Capture separately rechecks its MRR scope
    /// and every physical child CID; descriptor admission performs no native I/O.
    /// # Errors
    /// Refuses corrupt/noncanonical/unknown descriptors and row/inventory/layout limits.
    pub fn decode_descriptor_checked(
        source: PathBuf,
        root: &Cid,
        bytes: &[u8],
        projection: &GraphArEntityPropertyProjection,
        limits: GraphArEntityPropertyLimits,
    ) -> Result<Self, Error> {
        limits.validate()?;
        if bytes.len() > limits.inventory.max_manifest_bytes {
            return Err(Error::Budget("property descriptor bytes"));
        }
        if dag_cbor_cid(bytes) != *root {
            return Err(Error::Integrity);
        }
        let wire: Wire = serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|_| Error::Shape("property descriptor encoding"))?;
        if wire.namespace != NAMESPACE || wire.version != 1 {
            return Err(Error::Shape("property descriptor version"));
        }
        wire.inventory
            .canonical_bytes(limits.inventory)
            .map_err(crate::GraphArInventoryError::from)?;
        let rows = usize::try_from(wire.rows).map_err(|_| Error::Budget("entity rows"))?;
        if rows > limits.max_rows {
            return Err(Error::Budget("entity rows"));
        }
        let vertex =
            usize::try_from(wire.vertex_chunk).map_err(|_| Error::Budget("vertex chunk size"))?;
        let layout =
            GraphArChunkLayout::new(vertex, GraphArChunkLayout::default().edge_chunk_size())
                .map_err(|_| Error::Shape("vertex layout"))?;
        if wire.catalog != *projection.catalog_digest().as_bytes() {
            return Err(Error::Scope);
        }
        let canonical = serde_ipld_dagcbor::to_vec(&wire)
            .map_err(|_| Error::Shape("property descriptor encoding"))?;
        if canonical != bytes {
            return Err(Error::Integrity);
        }
        Ok(Self {
            root: source,
            inventory: wire.inventory,
            catalog: projection.catalog_digest(),
            generation: wire.generation,
            snapshot: wire.snapshot,
            rows,
            layout,
        })
    }
}
