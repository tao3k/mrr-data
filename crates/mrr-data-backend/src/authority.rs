//! Generic durable authority generations. Semantic enrollment and commitments
//! are supplied by the Host; all profiles share this physical CAS boundary.
use crate::BackendError;
use cid::Cid;
use serde::{Deserialize, Serialize};
/// Retirement is terminal for one stable authority identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthorityStatus {
    Active,
    Retired,
}
/// Commitment binds the Host's exact key/policy/revocation snapshot. Generations
/// advance even if that snapshot returns to the same bytes, preventing ABA.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityState {
    pub generation: u64,
    pub commitment: Cid,
    pub status: AuthorityStatus,
}
/// Snapshot required by a content operation. Every enrolled authority in its
/// profile/namespace/scope is mandatory; callers cannot omit a guard to bypass it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityExpectation {
    pub authority_id: String,
    pub state: AuthorityState,
}
/// Host-authorized CAS proposal. No public method interprets this as semantic
/// permission to enroll or retire; embedding applications control access.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProposal {
    pub authority_id: String,
    pub expected: Option<AuthorityState>,
    pub replacement: Cid,
    pub status: AuthorityStatus,
}
/// Exact physical namespace tuple, never an interpolated SQL/path key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityKey {
    pub profile: String,
    pub namespace: String,
    pub scope: String,
    pub authority_id: String,
}
/// Owned CAS request; its next generation is the stable update/recovery identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityChange {
    pub key: AuthorityKey,
    pub proposal: AuthorityProposal,
}
impl AuthorityChange {
    /// Structural checks only; no semantic enrollment certificate is minted.
    /// # Errors
    /// Rejects malformed IDs/CIDs, exhausted generations and terminal retirement.
    pub fn next(&self) -> Result<AuthorityState, BackendError> {
        if self.key.authority_id != self.proposal.authority_id
            || [
                &self.key.profile,
                &self.key.namespace,
                &self.key.scope,
                &self.key.authority_id,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256)
        {
            return Err(BackendError::Limit);
        }
        mrr_data_content::ContentCodec::from_cid(&self.proposal.replacement)
            .map_err(|_| BackendError::Corrupt)?;
        if let Some(expected) = self.proposal.expected {
            if expected.generation == 0 {
                return Err(BackendError::AuthorityConflict);
            }
            mrr_data_content::ContentCodec::from_cid(&expected.commitment)
                .map_err(|_| BackendError::Corrupt)?;
            if expected.status == AuthorityStatus::Retired {
                return Err(BackendError::AuthorityRetired);
            }
        }
        let generation = self
            .proposal
            .expected
            .map_or(0, |s| s.generation)
            .checked_add(1)
            .ok_or(BackendError::Limit)?;
        Ok(AuthorityState {
            generation,
            commitment: self.proposal.replacement,
            status: self.proposal.status,
        })
    }
}
