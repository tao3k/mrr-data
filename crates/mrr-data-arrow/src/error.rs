//! Typed fail-closed errors for Arrow projection and IPC import.

use meta_relational_reasoning::{FactId, RelationError, ValueSchema};

/// Fail-closed errors from schema projection or row reconstruction.
#[derive(Debug)]
pub enum ArrowRelationError {
    /// The schema is valid MRR but has no admitted lossless V1 Arrow mapping.
    UnsupportedSchema { field: String, schema: ValueSchema },
    /// A user field collides with the reserved semantic namespace.
    ReservedFieldName(String),
    /// A fact does not satisfy the owning relation contract.
    InvalidFact { fact: FactId, error: RelationError },
    /// A value does not satisfy its declared field shape.
    ValueMismatch { field: String },
    /// A reserved semantic column contains an invalid value.
    InvalidSemanticValue { column: &'static str, row: usize },
    /// An untrusted IPC resource exceeds its caller-owned import budget.
    ImportLimitExceeded {
        resource: &'static str,
        limit: usize,
        actual: usize,
    },
    /// The V1 IPC profile contains anything other than one complete fact batch.
    UnexpectedBatchCount(usize),
    /// The IPC file requests a feature excluded from the bounded V1 profile.
    UnsupportedIpcFeature(&'static str),
    /// The upstream Arrow decoder panicked while inspecting malformed IPC.
    MalformedIpc,
    /// The Arrow batch does not carry the expected MRR profile or relation.
    SchemaMismatch(&'static str),
    /// Arrow rejected a structurally invalid batch.
    Arrow(arrow_schema::ArrowError),
}

impl PartialEq for ArrowRelationError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::UnsupportedSchema {
                    field: left,
                    schema: left_schema,
                },
                Self::UnsupportedSchema {
                    field: right,
                    schema: right_schema,
                },
            ) => left == right && left_schema == right_schema,
            (Self::ReservedFieldName(left), Self::ReservedFieldName(right)) => left == right,
            (
                Self::InvalidFact {
                    fact: left_fact,
                    error: left_error,
                },
                Self::InvalidFact {
                    fact: right_fact,
                    error: right_error,
                },
            ) => left_fact == right_fact && left_error == right_error,
            (Self::ValueMismatch { field: left }, Self::ValueMismatch { field: right }) => {
                left == right
            }
            (
                Self::InvalidSemanticValue {
                    column: left_column,
                    row: left_row,
                },
                Self::InvalidSemanticValue {
                    column: right_column,
                    row: right_row,
                },
            ) => left_column == right_column && left_row == right_row,
            (
                Self::ImportLimitExceeded {
                    resource: left_resource,
                    limit: left_limit,
                    actual: left_actual,
                },
                Self::ImportLimitExceeded {
                    resource: right_resource,
                    limit: right_limit,
                    actual: right_actual,
                },
            ) => {
                left_resource == right_resource
                    && left_limit == right_limit
                    && left_actual == right_actual
            }
            (Self::UnexpectedBatchCount(left), Self::UnexpectedBatchCount(right)) => left == right,
            (Self::UnsupportedIpcFeature(left), Self::UnsupportedIpcFeature(right)) => {
                left == right
            }
            (Self::MalformedIpc, Self::MalformedIpc) => true,
            (Self::SchemaMismatch(left), Self::SchemaMismatch(right)) => left == right,
            (Self::Arrow(left), Self::Arrow(right)) => left.to_string() == right.to_string(),
            _ => false,
        }
    }
}

impl From<arrow_schema::ArrowError> for ArrowRelationError {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self::Arrow(error)
    }
}
