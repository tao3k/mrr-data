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
    GoogleSelectionMismatch, Mode, TokenAuthorizationClaim, TokenAuthorizationRequest,
    TokenProfile, prepare_cloud_google_aes_siv_deidentify,
};

/// The Host-authenticated tabular recipe corresponding to Lean's
/// `AesSivTableRecipe`. Provider wire fields are fixed separately from the
/// selected row value; this metadata does not authenticate a key or grant.
#[derive(Clone, Copy, Debug)]
pub struct AesSivTableRecipeBinding<'a> {
    pub dataset: &'a str,
    pub value_field: &'a str,
    pub context_field: &'a str,
    pub profile: TokenProfile<'a>,
    pub admitted_context: Option<&'a str>,
    pub surrogate_info_type: Option<&'a str>,
}

/// A proposed Google request differs from the admitted table recipe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableRecipeMismatch {
    InvalidRecipe,
    Dataset,
    ValueField,
    ContextField,
    Profile,
    AdmittedContext,
    SurrogateInfoType,
}

/// One declared row of a bounded table selection. Physical Arrow row
/// authentication and per-row authorization are separate required checks.
#[derive(Clone, Copy, Debug)]
pub struct GoogleTableBatchRow<'a> {
    pub ordinal: u64,
    pub fields: &'a [(&'a str, &'a str)],
}

/// The synthetic selected cells remain borrowed from the caller's table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GoogleTableBatchInput<'a> {
    pub ordinal: u64,
    pub value: &'a str,
    pub context: &'a str,
}

/// Mirror the SPEC's bounded table batch selection errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleTableBatchMismatch {
    Empty,
    InvalidBudget,
    TooManyRows,
    OrdinalOrder,
    InvalidRecipe,
    ValueField,
    ContextField,
    ContextNotAdmitted,
    TooManyBytes,
}

fn check_batch_budget(
    rows: usize,
    max_rows: usize,
    max_utf8_bytes: usize,
) -> Result<(), GoogleTableBatchMismatch> {
    if rows == 0 {
        return Err(GoogleTableBatchMismatch::Empty);
    }
    if max_rows == 0 || max_rows > 256 || max_utf8_bytes == 0 || max_utf8_bytes > 1_048_576 {
        return Err(GoogleTableBatchMismatch::InvalidBudget);
    }
    if rows > max_rows {
        return Err(GoogleTableBatchMismatch::TooManyRows);
    }
    Ok(())
}

fn advance_batch(
    previous: Option<u64>,
    ordinal: u64,
    used: usize,
    value: &str,
    context: &str,
    max_utf8_bytes: usize,
) -> Result<usize, GoogleTableBatchMismatch> {
    if previous.is_some_and(|prior| prior >= ordinal) {
        return Err(GoogleTableBatchMismatch::OrdinalOrder);
    }
    used.checked_add(value.len())
        .and_then(|bytes| bytes.checked_add(context.len()))
        .filter(|bytes| *bytes <= max_utf8_bytes)
        .ok_or(GoogleTableBatchMismatch::TooManyBytes)
}

impl AesSivTableRecipeBinding<'_> {
    /// Select an ordered table batch under the SPEC V1 row and byte ceilings.
    /// This pure check cannot authenticate a row or grant provider access.
    /// # Errors
    /// Returns a typed scope, row, ordering or budget mismatch.
    pub fn select_batch<'a>(
        &self,
        rows: &[GoogleTableBatchRow<'a>],
        max_rows: usize,
        max_utf8_bytes: usize,
    ) -> Result<Vec<GoogleTableBatchInput<'a>>, GoogleTableBatchMismatch> {
        check_batch_budget(rows.len(), max_rows, max_utf8_bytes)?;
        let mut selected = Vec::with_capacity(rows.len());
        let mut previous = None;
        let mut used = 0;
        for row in rows {
            if previous.is_some_and(|prior| prior >= row.ordinal) {
                return Err(GoogleTableBatchMismatch::OrdinalOrder);
            }
            if self.profile.mode != Mode::AesSiv
                || self.dataset.is_empty()
                || self.value_field.is_empty()
                || self.context_field.is_empty()
                || self.value_field == self.context_field
            {
                return Err(GoogleTableBatchMismatch::InvalidRecipe);
            }
            let unique = |name: &str| {
                let mut fields = row.fields.iter().filter(|(field, _)| *field == name);
                let value = fields.next()?.1;
                fields.next().is_none().then_some(value)
            };
            let value = unique(self.value_field)
                .filter(|value| !value.is_empty())
                .ok_or(GoogleTableBatchMismatch::ValueField)?;
            let context =
                unique(self.context_field).ok_or(GoogleTableBatchMismatch::ContextField)?;
            if context.is_empty()
                || self
                    .admitted_context
                    .is_some_and(|allowed| allowed != context)
            {
                return Err(GoogleTableBatchMismatch::ContextNotAdmitted);
            }
            used = advance_batch(previous, row.ordinal, used, value, context, max_utf8_bytes)?;
            previous = Some(row.ordinal);
            selected.push(GoogleTableBatchInput {
                ordinal: row.ordinal,
                value,
                context,
            });
        }
        Ok(selected)
    }
}

impl AesSivTableRecipeBinding<'_> {
    /// Check the Lean table recipe's field and context selection plus the
    /// Google surrogate annotation against one current bound operation.
    ///
    /// # Errors
    ///
    /// Returns a typed mismatch without exposing plaintext.
    pub fn check(
        &self,
        request: &TokenAuthorizationRequest<'_>,
        selected: &SelectedTabularInput,
    ) -> Result<(), TableRecipeMismatch> {
        if self.profile.mode != Mode::AesSiv
            || self.dataset.is_empty()
            || self.value_field.is_empty()
            || self.context_field.is_empty()
            || self.value_field == self.context_field
            || self.surrogate_info_type.is_some_and(str::is_empty)
        {
            return Err(TableRecipeMismatch::InvalidRecipe);
        }
        if self.dataset != request.dataset || self.dataset != selected.dataset {
            return Err(TableRecipeMismatch::Dataset);
        }
        if self.value_field != request.input.field() || self.value_field != selected.value_field {
            return Err(TableRecipeMismatch::ValueField);
        }
        if self.context_field != selected.context_field {
            return Err(TableRecipeMismatch::ContextField);
        }
        if &self.profile != request.input.profile()
            || self.profile
                != selected.token_profile(self.profile.lineage.tenant, self.profile.scope)
        {
            return Err(TableRecipeMismatch::Profile);
        }
        if self.admitted_context.is_some_and(|context| {
            context != request.input.context() || context != selected.context
        }) {
            return Err(TableRecipeMismatch::AdmittedContext);
        }
        if self.surrogate_info_type
            != selected
                .surrogate_info_type
                .as_ref()
                .map(|name| name.0.as_str())
        {
            return Err(TableRecipeMismatch::SurrogateInfoType);
        }
        Ok(())
    }
}

/// A failed physical selection never exposes the selected plaintext in its diagnostic.
#[derive(Debug)]
pub enum GoogleArrowSelectionError {
    Recipe(TableRecipeMismatch),
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
    pub recipe: AesSivTableRecipeBinding<'a>,
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
        self.recipe
            .check(self.request, self.selected)
            .map_err(GoogleArrowSelectionError::Recipe)?;
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

/// Batch planning retains one independently authorized, Arrow-checked plan
/// per row. It does not form a Google multi-row wire body or perform I/O.
#[derive(Debug)]
pub enum GoogleArrowBatchError {
    Selection(GoogleTableBatchMismatch),
    MixedScope,
    Row(GoogleArrowSelectionError),
}

/// Plan a bounded, ordered set of rows from one verified physical child.
/// Every row still checks its own current claim, Cloud release, Cedar gate,
/// recipe, and actual Arrow cells. No partial plan escapes on failure.
/// # Errors
/// Returns a batch shape, mixed scope, authorization, or row error.
pub fn prepare_cloud_google_arrow_batch(
    preparations: Vec<CloudGoogleArrowPreparation<'_>>,
    child: &VerifiedGoogleArrowChild<'_>,
    max_rows: usize,
    max_utf8_bytes: usize,
) -> Result<Vec<BoundGoogleDeidentifyPlan>, GoogleArrowBatchError> {
    check_batch_budget(preparations.len(), max_rows, max_utf8_bytes)
        .map_err(GoogleArrowBatchError::Selection)?;
    let first = &preparations[0];
    let recipe = first.recipe;
    let subject = first.request.subject;
    let purpose = first.request.purpose;
    let dataset = first.request.dataset;
    let policy_digest = *first.current.policy_digest;
    let epoch = first.current.epoch;
    let now = first.current.now;
    let receipt = first.cloud.receipt;
    let decisions = first.cloud.decisions;
    let parent = first.parent.clone();
    let mut previous = None;
    let mut used = 0;
    let mut plans = Vec::with_capacity(preparations.len());
    for preparation in preparations {
        if preparation.recipe.dataset != recipe.dataset
            || preparation.recipe.value_field != recipe.value_field
            || preparation.recipe.context_field != recipe.context_field
            || preparation.recipe.profile != recipe.profile
            || preparation.recipe.admitted_context != recipe.admitted_context
            || preparation.recipe.surrogate_info_type != recipe.surrogate_info_type
            || preparation.request.subject != subject
            || preparation.request.purpose != purpose
            || preparation.request.dataset != dataset
            || *preparation.current.policy_digest != policy_digest
            || preparation.current.epoch != epoch
            || preparation.current.now != now
            || preparation.cloud.receipt != receipt
            || preparation.cloud.decisions != decisions
            || preparation.parent != parent
        {
            return Err(GoogleArrowBatchError::MixedScope);
        }
        let row = preparation
            .request
            .input
            .row()
            .ok_or(GoogleArrowBatchError::Row(
                GoogleArrowSelectionError::Authorization(GoogleSelectionMismatch::RowUnbound),
            ))?;
        used = advance_batch(
            previous,
            row.row_index(),
            used,
            &preparation.selected.value,
            &preparation.selected.context,
            max_utf8_bytes,
        )
        .map_err(GoogleArrowBatchError::Selection)?;
        previous = Some(row.row_index());
        plans.push(
            preparation
                .from_verified_child(child)
                .map_err(GoogleArrowBatchError::Row)?,
        );
    }
    Ok(plans)
}
