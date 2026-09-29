use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, SnapshotRowBinding, raw_cid,
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

use crate::data_protection::{
    DataProtectionDecisions, DataProtectionMismatch, DataProtectionProfile,
    PseudonymizationInputBinding, ReleaseReceiptClaim,
};

#[test]
fn selected_input_stays_on_immutable_snapshot() {
    let source = snapshot();
    let digest = [9; 32];
    let input = PseudonymizationInputBinding::new(
        &source,
        "customer_id",
        &digest,
        "campaign-a",
        "aes-siv-profile",
    );
    assert_eq!(input.source().root(), source.cid());
    assert_eq!(input.field(), "customer_id");
    assert_eq!(input.value_digest(), &digest);
    assert_eq!(input.context(), "campaign-a");
    assert_eq!(input.profile(), &"aes-siv-profile");
    assert!(input.row().is_none());
    let relation = &source.manifest().relations()[0];
    let row = SnapshotRowBinding::new(
        &source,
        relation.relation_id(),
        relation.batches()[0].cid(),
        0,
    )
    .unwrap();
    let located = PseudonymizationInputBinding::at_row(
        row,
        "customer_id",
        &digest,
        "campaign-a",
        "aes-siv-profile",
    );
    assert_eq!(located.source().root(), source.cid());
    assert_eq!(
        located.row().unwrap().child_cid(),
        relation.batches()[0].cid()
    );
}

#[test]
fn cloud_profile_requires_exact_release_and_two_decisions() {
    let source = snapshot();
    let receipt = ReleaseReceiptClaim {
        artifact_digest: "sha256:candidate",
        source_commit: "commit-a",
        policy_root: "CustomerDataRelease",
        epoch: 7,
    };
    let profile = DataProtectionProfile::new(&source, "customer-campaign", receipt);
    let both = DataProtectionDecisions {
        policy_root: receipt.policy_root,
        dataset: "customer-campaign",
        artifact_digest: receipt.artifact_digest,
        epoch: 7,
        pipeline_release_allowed: true,
        transformation_allowed: true,
    };
    assert_eq!(profile.source().root(), source.cid());
    assert_eq!(profile.check(receipt, 7, both), Ok(()));
    assert_eq!(
        profile.check(receipt, 8, both),
        Err(DataProtectionMismatch::ReleaseReceipt)
    );
    assert_eq!(
        profile.check(
            ReleaseReceiptClaim {
                artifact_digest: "sha256:other",
                ..receipt
            },
            7,
            both
        ),
        Err(DataProtectionMismatch::ReleaseReceipt)
    );
    for wrong in [
        DataProtectionDecisions {
            policy_root: "ReleaseReady",
            ..both
        },
        DataProtectionDecisions {
            dataset: "other-dataset",
            ..both
        },
        DataProtectionDecisions {
            artifact_digest: "sha256:other",
            ..both
        },
        DataProtectionDecisions { epoch: 8, ..both },
    ] {
        assert_eq!(
            profile.check(receipt, 7, wrong),
            Err(DataProtectionMismatch::DecisionScope)
        );
    }
    assert_eq!(
        profile.check(
            receipt,
            7,
            DataProtectionDecisions {
                pipeline_release_allowed: false,
                ..both
            }
        ),
        Err(DataProtectionMismatch::PipelineDenied)
    );
    assert_eq!(
        profile.check(
            receipt,
            7,
            DataProtectionDecisions {
                transformation_allowed: false,
                ..both
            }
        ),
        Err(DataProtectionMismatch::TransformationDenied)
    );
}
