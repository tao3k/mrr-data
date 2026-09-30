//! Exact MRR Arrow row selection for the Cedar POO AES-SIV table model.

use cedar_poo_bridge::google_sdp::{SelectedTabularInput, WrappedKeyBinding};
use cid::Cid;
use meta_relational_reasoning::{
    EntityCatalog, Fact, RelationCatalog, RelationId, RelationSchema, Value,
};
use mrr_data_arrow::{ArrowRelationError, IpcImportLimits, ipc_to_facts};
use mrr_data_core::{DataError, SnapshotRowBinding, raw_cid};

use crate::{
    BoundGoogleDeidentifyPlan, CloudDataProtectionSelection, CurrentGovernance,
    GoogleSelectionMismatch, TokenAuthorizationClaim, TokenAuthorizationRequest,
    prepare_cloud_google_aes_siv_deidentify,
};

/// A failed physical selection never exposes the selected plaintext in its diagnostic.
#[derive(Debug)]
pub enum GoogleArrowSelectionError {
    Catalog(DataError),
    RelationUnavailable,
    ChildUnavailable,
    ChildLength,
    ChildCid,
    Decode(ArrowRelationError),
    RowCount,
    RowSource,
    InvalidRecipe,
    ValueField,
    ContextField,
    ValueMismatch,
    ContextMismatch,
    Authorization(GoogleSelectionMismatch),
}

/// One authenticated physical Arrow child, decoded once for many row
/// selections. It contains plaintext Facts and must remain in Host-controlled
/// memory. Authorization and current governance are never cached here.
pub struct VerifiedGoogleArrowChild<'a> {
    root: Cid,
    relation_id: RelationId,
    child_cid: Cid,
    relation: &'a RelationSchema,
    facts: Vec<Fact>,
}

impl<'a> VerifiedGoogleArrowChild<'a> {
    /// Authenticate one complete relation child against its snapshot and
    /// bounded MRR Arrow IPC profile. This is the batch-level work shared by
    /// subsequent requests for different rows of the same child.
    ///
    /// # Errors
    ///
    /// Returns a catalog, child, decode, or row-count mismatch.
    pub fn admit(
        row: SnapshotRowBinding<'_>,
        relations: &'a RelationCatalog,
        entities: &EntityCatalog,
        child_bytes: &[u8],
        limits: IpcImportLimits,
    ) -> Result<Self, GoogleArrowSelectionError> {
        let manifest = row.source().snapshot().manifest();
        manifest
            .verify_catalogs(relations, entities)
            .map_err(GoogleArrowSelectionError::Catalog)?;
        let relation = relations
            .relations()
            .iter()
            .find(|relation| relation.id() == row.relation_id())
            .ok_or(GoogleArrowSelectionError::RelationUnavailable)?;
        let child = manifest
            .relations()
            .iter()
            .find(|descriptor| descriptor.relation_id() == row.relation_id())
            .and_then(|descriptor| {
                descriptor
                    .batches()
                    .iter()
                    .find(|child| child.cid() == row.child_cid())
            })
            .ok_or(GoogleArrowSelectionError::ChildUnavailable)?;
        if u64::try_from(child_bytes.len()).ok() != Some(child.byte_length()) {
            return Err(GoogleArrowSelectionError::ChildLength);
        }
        if raw_cid(child_bytes) != *row.child_cid() {
            return Err(GoogleArrowSelectionError::ChildCid);
        }
        let facts = ipc_to_facts(relation, child_bytes, limits)
            .map_err(GoogleArrowSelectionError::Decode)?;
        if u64::try_from(facts.len()).ok() != Some(child.row_count()) {
            return Err(GoogleArrowSelectionError::RowCount);
        }
        Ok(Self {
            root: *row.source().root(),
            relation_id: row.relation_id(),
            child_cid: *row.child_cid(),
            relation,
            facts,
        })
    }

    /// Check a currently bound row and the exact two named UTF-8 cells of a
    /// proposed AES-SIV table selection. A different root, relation, or child
    /// cannot reuse this physical cache entry.
    ///
    /// # Errors
    ///
    /// Returns a source, recipe, field, or selected-cell mismatch.
    pub fn verify_row(
        &self,
        row: SnapshotRowBinding<'_>,
        selected: &SelectedTabularInput,
    ) -> Result<(), GoogleArrowSelectionError> {
        if row.source().root() != &self.root
            || row.relation_id() != self.relation_id
            || row.child_cid() != &self.child_cid
        {
            return Err(GoogleArrowSelectionError::RowSource);
        }
        if selected.dataset.is_empty()
            || selected.value_field.is_empty()
            || selected.context_field.is_empty()
            || selected.value_field == selected.context_field
        {
            return Err(GoogleArrowSelectionError::InvalidRecipe);
        }
        let fields = self.relation.fields();
        let unique_index = |name: &str| {
            let mut positions = fields
                .iter()
                .enumerate()
                .filter(|(_, field)| field.name() == name);
            let first = positions.next()?.0;
            positions.next().is_none().then_some(first)
        };
        let value_index =
            unique_index(&selected.value_field).ok_or(GoogleArrowSelectionError::ValueField)?;
        let context_index =
            unique_index(&selected.context_field).ok_or(GoogleArrowSelectionError::ContextField)?;
        let fact = self
            .facts
            .get(
                usize::try_from(row.row_index())
                    .map_err(|_| GoogleArrowSelectionError::RowCount)?,
            )
            .ok_or(GoogleArrowSelectionError::RowCount)?;
        let actual_value = match fact.values().get(value_index) {
            Some(Value::String(value)) if !value.is_empty() => value,
            _ => return Err(GoogleArrowSelectionError::ValueField),
        };
        let actual_context = match fact.values().get(context_index) {
            Some(Value::String(value)) if !value.is_empty() => value,
            _ => return Err(GoogleArrowSelectionError::ContextField),
        };
        if actual_value != &selected.value {
            return Err(GoogleArrowSelectionError::ValueMismatch);
        }
        if actual_context != &selected.context {
            return Err(GoogleArrowSelectionError::ContextMismatch);
        }
        Ok(())
    }
}

/// Verify the exact named UTF-8 cells against a checked snapshot row and the
/// caller's proposed Google selection. The manifest catalog digest, child
/// length/CID, complete Arrow schema, batch row count, and cell contents are
/// checked before the selection can be used as an execution instruction.
///
/// `limits` is owned by the Host and must bound untrusted IPC materialization.
/// This is a content check, not Cedar evaluation or provider execution.
///
/// # Errors
///
/// Returns a typed, plaintext-free error on any catalog, child, recipe, or
/// selected-cell mismatch.
pub fn verify_google_arrow_row(
    row: SnapshotRowBinding<'_>,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    child_bytes: &[u8],
    limits: IpcImportLimits,
    selected: &SelectedTabularInput,
) -> Result<(), GoogleArrowSelectionError> {
    VerifiedGoogleArrowChild::admit(row, relations, entities, child_bytes, limits)?
        .verify_row(row, selected)
}

/// The catalog, bytes, and resource budget for a single physical child.
/// The deploying Host authenticates the snapshot root and catalog source.
pub struct ArrowChildInput<'a> {
    pub relations: &'a RelationCatalog,
    pub entities: &'a EntityCatalog,
    pub bytes: &'a [u8],
    pub limits: IpcImportLimits,
}

/// One current Cloud authorization and Google request, ready to bind to a
/// physical Arrow source. Consuming this value creates at most one plan.
pub struct CloudGoogleArrowPreparation<'a> {
    pub cloud: CloudDataProtectionSelection<'a>,
    pub request: &'a TokenAuthorizationRequest<'a>,
    pub claim: &'a TokenAuthorizationClaim<'a>,
    pub current: CurrentGovernance<'a>,
    pub selected: &'a SelectedTabularInput,
    pub parent: String,
    pub key: WrappedKeyBinding,
}

impl CloudGoogleArrowPreparation<'_> {
    /// Check current authorization, then authenticate and select the actual
    /// Arrow child row before returning the Google request plan.
    ///
    /// # Errors
    ///
    /// Returns a current authorization or physical selection mismatch.
    pub fn from_arrow(
        self,
        child: &ArrowChildInput<'_>,
    ) -> Result<BoundGoogleDeidentifyPlan, GoogleArrowSelectionError> {
        self.prepare_with_row_check(|row, selected| {
            verify_google_arrow_row(
                row,
                child.relations,
                child.entities,
                child.bytes,
                child.limits,
                selected,
            )
        })
    }

    /// Use a previously verified physical child while checking the current
    /// claim, Cloud release, and Cedar gate relations on this call.
    ///
    /// # Errors
    ///
    /// Returns a current authorization or selected-row mismatch.
    pub fn from_verified_child(
        self,
        child: &VerifiedGoogleArrowChild<'_>,
    ) -> Result<BoundGoogleDeidentifyPlan, GoogleArrowSelectionError> {
        self.prepare_with_row_check(|row, selected| child.verify_row(row, selected))
    }

    fn prepare_with_row_check(
        self,
        check_row: impl FnOnce(
            SnapshotRowBinding<'_>,
            &SelectedTabularInput,
        ) -> Result<(), GoogleArrowSelectionError>,
    ) -> Result<BoundGoogleDeidentifyPlan, GoogleArrowSelectionError> {
        let row = self
            .request
            .input
            .row()
            .ok_or(GoogleArrowSelectionError::Authorization(
                GoogleSelectionMismatch::RowUnbound,
            ))?;
        let plan = prepare_cloud_google_aes_siv_deidentify(
            self.cloud,
            self.request,
            self.claim,
            self.current,
            self.selected.clone(),
            self.parent,
            self.key,
        )
        .map_err(GoogleArrowSelectionError::Authorization)?;
        check_row(row, self.selected)?;
        Ok(plan)
    }
}
