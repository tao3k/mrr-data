//! Versioned MRR Data control-plane metadata, not a second `GraphAr` file format.
use super::{
    GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt,
};
use crate::GraphArChunkLayout;
use cid::Cid;
use mrr_data_core::{GraphEntityPropertyDescriptor, GraphEntityPropertyScope, dag_cbor_cid};
use std::path::PathBuf;

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
        let wire = GraphEntityPropertyDescriptor::admit(
            self.inventory.clone(),
            GraphEntityPropertyScope {
                catalog: *self.catalog.as_bytes(),
                generation: self.generation,
                snapshot: self.snapshot,
                rows: u64::try_from(self.rows).map_err(|_| Error::Budget("row count overflow"))?,
                vertex_chunk: u64::try_from(self.layout.vertex_chunk_size())
                    .map_err(|_| Error::Budget("vertex chunk size"))?,
            },
            limits.inventory,
            limits.max_rows,
        )
        .map_err(property_error)?;
        let bytes = wire
            .canonical_bytes(limits.inventory, limits.max_rows)
            .map_err(property_error)?;
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
        let wire = GraphEntityPropertyDescriptor::decode_checked(
            bytes,
            root,
            limits.inventory,
            limits.max_rows,
        )
        .map_err(property_error)?;
        let scope = wire.scope();
        if scope.catalog != *projection.catalog_digest().as_bytes() {
            return Err(Error::Scope);
        }
        let rows = usize::try_from(scope.rows).map_err(|_| Error::Budget("entity rows"))?;
        let vertex =
            usize::try_from(scope.vertex_chunk).map_err(|_| Error::Budget("vertex chunk size"))?;
        let layout =
            GraphArChunkLayout::new(vertex, GraphArChunkLayout::default().edge_chunk_size())
                .map_err(|_| Error::Shape("vertex layout"))?;
        Ok(Self {
            root: source,
            inventory: wire.inventory().clone(),
            catalog: projection.catalog_digest(),
            generation: scope.generation,
            snapshot: scope.snapshot,
            rows,
            layout,
        })
    }
}

fn property_error(error: mrr_data_core::GraphInventoryError) -> Error {
    match error {
        mrr_data_core::GraphInventoryError::Limit => Error::Budget("property descriptor"),
        mrr_data_core::GraphInventoryError::Integrity
        | mrr_data_core::GraphInventoryError::NonCanonical => Error::Integrity,
        error => Error::Inventory(crate::GraphArInventoryError::from(error)),
    }
}
