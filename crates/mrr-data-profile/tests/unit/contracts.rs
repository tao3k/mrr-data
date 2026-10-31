use crate::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};

#[test]
fn namespaces_never_embed_schema_versions() {
    for namespace in [
        SNAPSHOT_SCHEMA_NAMESPACE,
        ARROW_FACT_SCHEMA_NAMESPACE,
        GRAPHAR_BINARY_ENTITY_NAMESPACE,
    ] {
        assert!(!namespace.contains("v1"));
        assert!(!namespace.ends_with(".1"));
    }
    assert_eq!(SNAPSHOT_SCHEMA_VERSION, 1);
    assert_eq!(ARROW_FACT_SCHEMA_VERSION, 1);
    assert_eq!(GRAPHAR_BINARY_ENTITY_VERSION, 1);
}

#[test]
fn all_graph_and_backend_schemas_admit_version_separately() {
    use crate::{
        ARROW_VALUE_SCHEMA, BACKEND_AUTHORITY_CHANGE_SCHEMA, BACKEND_AUTHORITY_COMPLETION_SCHEMA,
        BACKEND_AUTHORITY_KEY_SCHEMA, BACKEND_AUTHORITY_PROPOSAL_SCHEMA, BACKEND_AUTHORITY_SCHEMA,
        BACKEND_COMPLETION_SCHEMA, BACKEND_DUCKDB_SCHEMA, BACKEND_EXPECTATION_SCHEMA,
        BACKEND_KEY_SCHEMA, BACKEND_REVISION_SCHEMA, BACKEND_TURSO_SCHEMA, BACKEND_WRITE_SCHEMA,
        GRAPHAR_DATASET_BINDING_SCHEMA, GRAPHAR_DATASET_SCHEMA, GRAPHAR_ENTITY_PROPERTIES_SCHEMA,
        GRAPHAR_FILE_INVENTORY_SCHEMA,
    };
    for schema in [
        ARROW_VALUE_SCHEMA,
        GRAPHAR_DATASET_SCHEMA,
        GRAPHAR_DATASET_BINDING_SCHEMA,
        GRAPHAR_FILE_INVENTORY_SCHEMA,
        GRAPHAR_ENTITY_PROPERTIES_SCHEMA,
        BACKEND_KEY_SCHEMA,
        BACKEND_REVISION_SCHEMA,
        BACKEND_AUTHORITY_SCHEMA,
        BACKEND_EXPECTATION_SCHEMA,
        BACKEND_WRITE_SCHEMA,
        BACKEND_AUTHORITY_KEY_SCHEMA,
        BACKEND_AUTHORITY_PROPOSAL_SCHEMA,
        BACKEND_AUTHORITY_CHANGE_SCHEMA,
        BACKEND_COMPLETION_SCHEMA,
        BACKEND_AUTHORITY_COMPLETION_SCHEMA,
        BACKEND_DUCKDB_SCHEMA,
        BACKEND_TURSO_SCHEMA,
    ] {
        assert!(!schema.namespace.split(['.', '/', '-', '_']).any(|part| {
            part.strip_prefix('v').is_some_and(|version| {
                !version.is_empty() && version.bytes().all(|b| b.is_ascii_digit())
            })
        }));
        assert!(schema.accepts(schema.namespace, schema.version));
        assert!(!schema.accepts(schema.namespace, schema.version + 1));
        assert!(!schema.accepts("foreign", schema.version));
    }
}
