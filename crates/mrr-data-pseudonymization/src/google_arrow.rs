//! Exact MRR Arrow row selection for the Cedar POO AES-SIV table model.

use cedar_poo_bridge::google_sdp::{SelectedTabularInput, WrappedKeyBinding};
use meta_relational_reasoning::{EntityCatalog, RelationCatalog, Value};
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
    InvalidRecipe,
    ValueField,
    ContextField,
    ValueMismatch,
    ContextMismatch,
    Authorization(GoogleSelectionMismatch),
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
    if selected.dataset.is_empty()
        || selected.value_field.is_empty()
        || selected.context_field.is_empty()
        || selected.value_field == selected.context_field
    {
        return Err(GoogleArrowSelectionError::InvalidRecipe);
    }
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
    let facts =
        ipc_to_facts(relation, child_bytes, limits).map_err(GoogleArrowSelectionError::Decode)?;
    if u64::try_from(facts.len()).ok() != Some(child.row_count()) {
        return Err(GoogleArrowSelectionError::RowCount);
    }
    let fields = relation.fields();
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
    let fact = facts
        .get(usize::try_from(row.row_index()).map_err(|_| GoogleArrowSelectionError::RowCount)?)
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

/// Prepare the Cloud Google operation only after checking the actual Arrow
/// cells that the Cedar POO table model selects.
///
/// # Errors
///
/// Returns a physical-selection or authorization mismatch.
#[allow(clippy::too_many_arguments)]
pub fn prepare_cloud_google_aes_siv_from_arrow(
    cloud: CloudDataProtectionSelection<'_>,
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: CurrentGovernance<'_>,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    child_bytes: &[u8],
    limits: IpcImportLimits,
    selected: &SelectedTabularInput,
    parent: String,
    key: WrappedKeyBinding,
) -> Result<BoundGoogleDeidentifyPlan, GoogleArrowSelectionError> {
    let row = request
        .input
        .row()
        .ok_or(GoogleArrowSelectionError::Authorization(
            GoogleSelectionMismatch::RowUnbound,
        ))?;
    let plan = prepare_cloud_google_aes_siv_deidentify(
        cloud,
        request,
        claim,
        current,
        selected.clone(),
        parent,
        key,
    )
    .map_err(GoogleArrowSelectionError::Authorization)?;
    verify_google_arrow_row(row, relations, entities, child_bytes, limits, selected)?;
    Ok(plan)
}
