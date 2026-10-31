//! Exact scope matching for Host-authenticated token authorization claims.
//!
//! A matching claim is not an authorization decision. The Host must obtain it
//! from its policy engine, authenticate its issuer, and atomically enforce any
//! stateful budget or revocation rule before performing the operation.

use cid::Cid;

use crate::{TokenInputBinding, TokenProfile};

/// The effect that a policy must authorize separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenAction {
    Deidentify,
    Reidentify,
}

/// One requested effect on a selected value in an immutable snapshot.
#[derive(Clone, Copy, Debug)]
pub struct TokenAuthorizationRequest<'a> {
    pub subject: &'a str,
    pub purpose: &'a str,
    pub dataset: &'a str,
    pub action: TokenAction,
    pub input: &'a TokenInputBinding<'a>,
}

/// A scoped claim produced and authenticated by the deploying Host.
///
/// Every field is a declaration until the Host verifies its source. In
/// particular, callers must not treat construction of this value as a grant.
#[derive(Clone, Copy, Debug)]
pub struct TokenAuthorizationClaim<'a> {
    pub subject: &'a str,
    pub purpose: &'a str,
    pub dataset: &'a str,
    pub action: TokenAction,
    pub root: &'a Cid,
    pub field: &'a str,
    pub value_digest: &'a [u8; 32],
    pub context: &'a str,
    pub profile: TokenProfile<'a>,
    pub policy_digest: &'a [u8; 32],
    /// State epoch; it is not a wall-clock timestamp.
    pub governance_epoch: i64,
    /// Exclusive Unix timestamp in seconds for this separate scoped claim.
    pub expires_at: u64,
}

/// A claim is stale, unscoped, or applies to a different operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaimMismatch {
    EmptyScope,
    Stale,
    DifferentOperation,
}

/// Current Host-authenticated policy and governance state at operation time.
#[derive(Clone, Copy, Debug)]
pub struct CurrentGovernance<'a> {
    pub policy_digest: &'a [u8; 32],
    /// State epoch; it is not a wall-clock timestamp.
    pub epoch: i64,
    /// Unix timestamp in seconds, supplied by the Host clock.
    pub now: u64,
}

impl TokenAuthorizationRequest<'_> {
    /// Match a Host-authenticated claim to the exact current operation.
    ///
    /// The Host supplies the current policy digest, governance epoch, and
    /// clock. A match alone does not authenticate the claim or execute Cedar.
    ///
    /// # Errors
    ///
    /// Returns [`ClaimMismatch`] if any scope or freshness condition fails.
    pub fn check_claim(
        &self,
        claim: &TokenAuthorizationClaim<'_>,
        current_policy_digest: &[u8; 32],
        current_governance_epoch: i64,
        now: u64,
    ) -> Result<(), ClaimMismatch> {
        if self.subject.is_empty()
            || self.purpose.is_empty()
            || self.dataset.is_empty()
            || self.input.field().is_empty()
            || self.input.context().is_empty()
        {
            return Err(ClaimMismatch::EmptyScope);
        }
        if claim.policy_digest != current_policy_digest
            || claim.governance_epoch != current_governance_epoch
            || now >= claim.expires_at
        {
            return Err(ClaimMismatch::Stale);
        }
        if self.subject != claim.subject
            || self.purpose != claim.purpose
            || self.dataset != claim.dataset
            || self.action != claim.action
            || self.input.source().root() != claim.root
            || self.input.field() != claim.field
            || self.input.value_digest() != claim.value_digest
            || self.input.context() != claim.context
            || self.input.profile() != &claim.profile
        {
            return Err(ClaimMismatch::DifferentOperation);
        }
        Ok(())
    }
}
