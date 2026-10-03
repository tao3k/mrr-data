//! Atomic head/operation interface implemented by a Host's storage adapter.
//!
//! No storage provider is supplied here. Implementations authenticate observations
//! and meet the transaction and authority synchronization contract below.

use std::{convert::Infallible, future::Future, pin::Pin};

use crate::{
    ConditionalCommitError, ConditionalContentReceipt, ConditionalContentWrite, ContentRevision,
    PublishReceipt,
};

/// A confirmed exact operation, either newly committed or recovered without a write.
/// This is a Host-authenticated projection, not a cryptographic provider attestation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalContentCommitOutcome<'a> {
    Committed(ConditionalContentReceipt<'a>),
    Replayed(ConditionalContentReceipt<'a>),
}

/// Known refusals remain separate from an uncertain commit outcome.
/// `Unknown` requires recovery under the original exact operation identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionalCommitPortError<BackendError, ValidationError> {
    Protocol(ConditionalCommitError),
    Validation(ValidationError),
    /// The backend establishes that this attempt performed no new commit.
    BeforeCommit(BackendError),
    /// The backend cannot establish whether the exact operation committed.
    Unknown(BackendError),
}

/// Sendable adapter future with separate backend and domain validation errors.
pub type ConditionalCommitFuture<'a, T, E, V = Infallible> =
    Pin<Box<dyn Future<Output = Result<T, ConditionalCommitPortError<E, V>>> + Send + 'a>>;

/// Provider-neutral port for one atomic content-head and operation-ledger transaction.
///
/// Implementations compare the authenticated head and exact operation receipt,
/// enforce uniqueness of `(scope, operation_id)`, and call `validate_current`
/// only for a new Apply, inside the same protected transaction. An exact replay
/// skips validation and performs no write. Before a fresh commit, the callback's
/// current authority must remain valid through persistence: an adapter must
/// serialize authority changes or include their versions in the atomic comparison.
/// A callback followed by an unprotected write does not implement this contract.
///
/// Derive Apply/Replay with `ConditionalContentWrite::decide_commit`; derive
/// recovery with `ConditionalContentWrite::recover_receipt`.
/// Persist the exact next head and receipt together, with the backend's declared
/// durability policy. Return Committed only after its confirmation. An ambiguous
/// response is Unknown, never known refusal, success, or authenticated absence.
/// Recovery is a read of the original operation, not permission for a new effect.
/// Backend implementations and deployment conformance remain external obligations.
pub trait ConditionalContentCommitPort: Sync {
    type Error: Send;

    /// Commit or replay one exact write, refreshing domain authority for Apply.
    /// `None` publication is valid only for exact replay; a new write needs ACK.
    /// # Errors
    /// Returns protocol refusal, validation refusal, known backend refusal, or
    /// an explicitly uncertain outcome. Never retry Unknown under another ID.
    fn commit<'a, V, F>(
        &'a self,
        write: ConditionalContentWrite<'a>,
        physical: Option<&'a PublishReceipt>,
        validate_current: F,
    ) -> ConditionalCommitFuture<'a, ConditionalContentCommitOutcome<'a>, Self::Error, V>
    where
        V: Send + 'a,
        F: FnOnce(Option<ContentRevision>) -> Result<(), V> + Send + 'a;

    /// Recover the exact authenticated receipt, including after later head changes.
    /// `None` means a successful lookup found no row. An unreadable or uncertain
    /// ledger must be an error and cannot authorize retry under a different ID.
    /// # Errors
    /// Returns unavailable backend, malformed query, receipt mismatch or conflict.
    fn recover<'a>(
        &'a self,
        write: ConditionalContentWrite<'a>,
    ) -> ConditionalCommitFuture<'a, Option<ConditionalContentReceipt<'a>>, Self::Error>;
}
