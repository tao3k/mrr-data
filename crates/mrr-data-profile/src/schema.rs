//! Stable contract identities; numeric versions belong to this Schema owner.
/// One admitted wire contract. Namespace never selects a format version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaIdentity {
    /// Stable owning contract name.
    pub namespace: &'static str,
    /// Independently admitted numeric wire format version.
    pub version: u64,
}
impl SchemaIdentity {
    /// Check both fields before admitting a serialized contract.
    #[must_use]
    pub fn accepts(self, namespace: &str, version: u64) -> bool {
        self.namespace == namespace && self.version == version
    }
}
/// Admitted Schema for `mrr.graphar.dataset`.
pub const GRAPHAR_DATASET_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.graphar.dataset",
    version: 1,
};
/// Admitted Schema for `mrr.graphar.dataset-binding`.
pub const GRAPHAR_DATASET_BINDING_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.graphar.dataset-binding",
    version: 1,
};
/// Admitted Schema for `mrr.graphar.file-inventory`.
pub const GRAPHAR_FILE_INVENTORY_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.graphar.file-inventory",
    version: 1,
};
/// Admitted Schema for `mrr.graphar.entity-properties`.
pub const GRAPHAR_ENTITY_PROPERTIES_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.graphar.entity-properties",
    version: 1,
};
/// Admitted Schema for `mrr.backend.key`.
pub const BACKEND_KEY_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.key",
    version: 2,
};
/// Admitted Schema for `mrr.backend.revision`.
pub const BACKEND_REVISION_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.revision",
    version: 2,
};
/// Admitted Schema for `mrr.backend.authority`.
pub const BACKEND_AUTHORITY_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.authority",
    version: 2,
};
/// Admitted Schema for `mrr.backend.expectation`.
pub const BACKEND_EXPECTATION_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.expectation",
    version: 2,
};
/// Admitted Schema for `mrr.backend.write`.
pub const BACKEND_WRITE_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.write",
    version: 2,
};
/// Admitted Schema for `mrr.backend.authority-key`.
pub const BACKEND_AUTHORITY_KEY_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.authority-key",
    version: 2,
};
/// Admitted Schema for `mrr.backend.authority-proposal`.
pub const BACKEND_AUTHORITY_PROPOSAL_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.authority-proposal",
    version: 2,
};
/// Admitted Schema for `mrr.backend.authority-change`.
pub const BACKEND_AUTHORITY_CHANGE_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.authority-change",
    version: 2,
};
/// Admitted Schema for `mrr.backend.completion`.
pub const BACKEND_COMPLETION_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.completion",
    version: 2,
};
/// Admitted Schema for `mrr.backend.authority-completion`.
pub const BACKEND_AUTHORITY_COMPLETION_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.backend.authority-completion",
    version: 2,
};
/// Admitted Schema for `mrr-data-backend.duckdb`.
pub const BACKEND_DUCKDB_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr-data-backend.duckdb",
    version: 2,
};
/// Admitted Schema for `mrr-data-backend.turso`.
pub const BACKEND_TURSO_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr-data-backend.turso",
    version: 2,
};

/// Admitted Schema for Arrow field value-schema metadata.
pub const ARROW_VALUE_SCHEMA: SchemaIdentity = SchemaIdentity {
    namespace: "mrr.value-schema",
    version: 1,
};
