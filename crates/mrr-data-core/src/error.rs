//! Typed fail-closed errors for snapshot identity and manifest admission.

use std::fmt;

use cid::Cid;
use meta_relational_reasoning::{RelationId, RevisionId};

/// Typed failures for canonical MRR Data snapshot admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataError {
    Encode(String),
    Decode(String),
    NonCanonicalEncoding,
    UnknownSchemaNamespace(String),
    UnknownSchemaVersion(u64),
    UnknownCidVersion(u64),
    UnknownManifestCodec(String),
    UnknownMultihash(String),
    UnknownChildCodec(String),
    UnknownPayloadFormat(String),
    UnknownArrowSchemaNamespace(String),
    UnknownArrowSchemaVersion(u64),
    UnknownGraphProjectionNamespace(String),
    UnknownGraphProjectionVersion(u64),
    InvalidCidProfile {
        cid: Box<Cid>,
        expected_codec: u64,
    },
    CidMismatch {
        expected: Box<Cid>,
        actual: Box<Cid>,
    },
    EmptyRelations,
    DuplicateRelation(RelationId),
    RelationCatalogMismatch,
    EntityCatalogMismatch,
    RelationSetMismatch,
    NonCanonicalRelationOrder,
    DuplicateChild(Box<Cid>),
    NonCanonicalLineageOrder,
    EmptyPayload(Box<Cid>),
    BatchRowsMismatch {
        relation: RelationId,
        declared: u64,
        actual: u64,
    },
    RowCountOverflow(RelationId),
    InvalidDigestLength {
        field: &'static str,
        actual: usize,
    },
    RevisionIdentityMismatch {
        declared: RevisionId,
        derived: RevisionId,
    },
    InvalidSource(String),
    SemanticSnapshotDigestMismatch,
    InvalidGraphArVersion,
}

impl fmt::Display for DataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DataError {}
