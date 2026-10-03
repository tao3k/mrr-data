//! Owned exact records passed to blocking provider workers.
use crate::AuthorityExpectation;
use cid::Cid;
use mrr_data_content::{ConditionalContentWrite, ContentRevision};
use serde::{Deserialize, Serialize};
/// Complete revision/CID, retaining the protocol's full u64 revision range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoredRevision {
    pub revision: u64,
    pub root: Cid,
}
impl From<ContentRevision> for StoredRevision {
    fn from(r: ContentRevision) -> Self {
        Self {
            revision: r.revision,
            root: r.root,
        }
    }
}
impl From<StoredRevision> for ContentRevision {
    fn from(r: StoredRevision) -> Self {
        Self {
            revision: r.revision,
            root: r.root,
        }
    }
}
/// Profile and deployment namespace are independently configured. Scope and
/// operation remain opaque exact protocol IDs; none is interpolated into SQL.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoredWrite {
    pub profile: String,
    pub namespace: String,
    pub scope: String,
    pub operation_id: String,
    pub expected: Option<StoredRevision>,
    pub replacement: Cid,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorities: Vec<AuthorityExpectation>,
}
impl StoredWrite {
    #[must_use]
    pub fn content_write(&self) -> ConditionalContentWrite<'_> {
        ConditionalContentWrite {
            scope: &self.scope,
            operation_id: &self.operation_id,
            expected: self.expected.map(Into::into),
            replacement: self.replacement,
        }
    }
}
/// A provider outcome is converted back into the original borrowed port receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredOutcome {
    pub committed: StoredRevision,
    pub replayed: bool,
}
