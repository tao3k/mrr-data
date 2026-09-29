use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};

use crate::{
    Mode, SelectedTokenInput, TokenLineage, TokenProfile, compatible_inputs, hmac_catalog_separated,
};

fn snapshot() -> SnapshotBlock {
    let generation = GenerationId::from_canonical_bytes("generation:token-fixture").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("test", "source", "revision").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:token-fixture").unwrap();
    let schema = RelationSchema::new(
        relation_id,
        "TokenFixture",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    let relations = RelationCatalog::admit(vec![schema]).unwrap();
    let entities = EntityCatalog::admit(vec![]).unwrap();
    let batch = BatchDescriptor::new(raw_cid(b"synthetic-arrow"), 1, 15).unwrap();
    let descriptor = RelationDescriptor::new(relation_id, 1, vec![batch]).unwrap();
    let coverage = CoverageDescriptor::new(CoverageKind::Unknown, raw_cid(b"coverage")).unwrap();
    let request =
        SnapshotManifestRequest::new(semantic, &relations, &entities, vec![descriptor], coverage);
    SnapshotBlock::encode(SnapshotManifest::admit(request).unwrap()).unwrap()
}

fn profile<'a>(mode: Mode, scope: &'a str, wrapping: &'a str) -> TokenProfile<'a> {
    TokenProfile {
        mode,
        scope,
        lineage: TokenLineage {
            tenant: "tenant-a",
            key_domain: "research-key",
            token_key_version: "key-1",
            transform_version: "normalization-1",
            wrapping_version: wrapping,
        },
    }
}

#[test]
fn actual_context_and_recipe_control_token_compatibility() {
    let source = snapshot();
    let digest = [23; 32];
    let first = SelectedTokenInput {
        field: "subject_id",
        value_digest: &digest,
        context: "tenant-a:study-1",
        profile: profile(Mode::AesSiv, "study-1", "wrapping-1"),
    }
    .bind_to(&source);
    let rewrapped = SelectedTokenInput {
        field: "subject_id",
        value_digest: &digest,
        context: "tenant-a:study-1",
        profile: profile(Mode::AesSiv, "study-1", "wrapping-2"),
    }
    .bind_to(&source);
    let changed_context = SelectedTokenInput {
        field: "subject_id",
        value_digest: &digest,
        context: "tenant-a:study-2",
        profile: profile(Mode::AesSiv, "study-1", "wrapping-1"),
    }
    .bind_to(&source);
    assert_eq!(first.source().root(), source.cid());
    assert!(compatible_inputs(&first, &rewrapped));
    assert!(!compatible_inputs(&first, &changed_context));
}

#[test]
fn hmac_scope_separation_is_independent_of_wrapper_rotation() {
    let catalog = [
        profile(Mode::HmacSha256, "study-1", "wrapping-1"),
        profile(Mode::HmacSha256, "study-2", "wrapping-2"),
    ];
    assert!(!hmac_catalog_separated(&catalog));
    assert!(hmac_catalog_separated(&catalog[..1]));
    let same_scope = [
        profile(Mode::HmacSha256, "study-1", "wrapping-1"),
        profile(Mode::HmacSha256, "study-1", "wrapping-2"),
    ];
    assert!(hmac_catalog_separated(&same_scope));
}
