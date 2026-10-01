use cedar_poo_bridge::google_sdp::SelectedTabularInput;
use meta_relational_reasoning::{
    EntityCatalog, EntityId, EvidenceCompleteness, ExternalRevisionIdentity, Fact, FactId,
    FactProvenance, FactValidity, GenerationId, RelationAuthority, RelationCatalog,
    RelationContext, RelationField, RelationId, RelationSchema, RevisionBinding, SemanticSnapshot,
    Value, ValueSchema,
};
use mrr_data_arrow::{IpcImportLimits, facts_to_ipc};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, SnapshotRowBinding, raw_cid,
};

use crate::{GoogleArrowSelectionError, VerifiedGoogleArrowChild, verify_google_arrow_row};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "replay all SPEC table batch cases with exact outputs"
)]
fn spec_google_table_batch_v1_replay() {
    use crate::{
        AesSivTableRecipeBinding, GoogleTableBatchMismatch, GoogleTableBatchRow, Mode,
        TokenLineage, TokenProfile,
    };
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/google-table-batch-v1.json")).unwrap();
    assert_eq!(fixture["version"], "google-table-batch-v1");
    let mut admitted = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let profile = TokenProfile {
            mode: if case["mode"] == "aes-siv" {
                Mode::AesSiv
            } else {
                Mode::HmacSha256
            },
            scope: "study",
            lineage: TokenLineage {
                tenant: "tenant-a",
                key_domain: "study-key",
                token_key_version: "dek-1",
                transform_version: "recipe-1",
                wrapping_version: "kek-1",
            },
        };
        let recipe = AesSivTableRecipeBinding {
            dataset: "research",
            value_field: "patient_id",
            context_field: "study_context",
            profile,
            admitted_context: case["admitted_context"].as_str(),
            surrogate_info_type: None,
        };
        let owned: Vec<Vec<(String, String)>> = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                row["fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|field| {
                        (
                            field["name"].as_str().unwrap().to_owned(),
                            field["value"].as_str().unwrap().to_owned(),
                        )
                    })
                    .collect()
            })
            .collect();
        let borrowed: Vec<Vec<(&str, &str)>> = owned
            .iter()
            .map(|fields| {
                fields
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect()
            })
            .collect();
        let rows: Vec<_> = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .zip(&borrowed)
            .map(|(row, fields)| GoogleTableBatchRow {
                ordinal: row["ordinal"].as_u64().unwrap(),
                fields,
            })
            .collect();
        let result = recipe.select_batch(
            &rows,
            usize::try_from(case["max_rows"].as_u64().unwrap()).unwrap(),
            usize::try_from(case["max_utf8_bytes"].as_u64().unwrap()).unwrap(),
        );
        if case["result"]["allow"] == true {
            let selected = result.unwrap();
            admitted += 1;
            assert_eq!(
                selected.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
                case["result"]["ordinals"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_u64().unwrap())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                selected.iter().map(|row| row.value).collect::<Vec<_>>(),
                case["result"]["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                selected.iter().map(|row| row.context).collect::<Vec<_>>(),
                case["result"]["contexts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>()
            );
        } else {
            let expected = match case["result"]["error"].as_str().unwrap() {
                "empty" => GoogleTableBatchMismatch::Empty,
                "invalid-budget" => GoogleTableBatchMismatch::InvalidBudget,
                "too-many-rows" => GoogleTableBatchMismatch::TooManyRows,
                "ordinal-order" => GoogleTableBatchMismatch::OrdinalOrder,
                "invalid-recipe" => GoogleTableBatchMismatch::InvalidRecipe,
                "value-field" => GoogleTableBatchMismatch::ValueField,
                "context-field" => GoogleTableBatchMismatch::ContextField,
                "context-not-admitted" => GoogleTableBatchMismatch::ContextNotAdmitted,
                "too-many-bytes" => GoogleTableBatchMismatch::TooManyBytes,
                other => panic!("unexpected SPEC error {other}"),
            };
            assert_eq!(result.unwrap_err(), expected, "{}", case["name"]);
        }
    }
    assert_eq!(admitted, 2);
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 13);
}

fn fixture() -> (SnapshotBlock, RelationCatalog, EntityCatalog, Vec<u8>) {
    let generation = GenerationId::from_canonical_bytes("generation:arrow-selection").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("test", "source", "arrow-selection").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:arrow-selection").unwrap();
    let relation = RelationSchema::new(
        relation_id,
        "SelectedInput",
        vec![
            RelationField::new("patient_id", ValueSchema::String, false).unwrap(),
            RelationField::new("study_context", ValueSchema::String, false).unwrap(),
            RelationField::new("alias_context", ValueSchema::String, false).unwrap(),
        ],
        vec![],
    )
    .unwrap();
    let owner = EntityId::from_canonical_bytes("selection-owner").unwrap();
    let facts = [("patient-1", "study-a"), ("patient-2", "study-b")]
        .into_iter()
        .enumerate()
        .map(|(index, (value, context))| {
            Fact::new(
                FactId::from_canonical_bytes(format!("selection-fact-{index}")).unwrap(),
                relation_id,
                vec![
                    Value::String(value.into()),
                    Value::String(context.into()),
                    Value::String(context.into()),
                ],
                RelationContext::new(
                    generation,
                    RelationAuthority::Entity(owner),
                    FactProvenance::Source(owner),
                    EvidenceCompleteness::Complete,
                    FactValidity::Valid,
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let bytes = facts_to_ipc(&relation, &facts).unwrap();
    let relations = RelationCatalog::admit(vec![relation]).unwrap();
    let entities = EntityCatalog::admit(vec![]).unwrap();
    let child = BatchDescriptor::new(raw_cid(&bytes), 2, bytes.len() as u64).unwrap();
    let descriptor = RelationDescriptor::new(relation_id, 2, vec![child]).unwrap();
    let coverage = CoverageDescriptor::new(CoverageKind::Unknown, raw_cid(b"coverage")).unwrap();
    let manifest = SnapshotManifest::admit(SnapshotManifestRequest::new(
        semantic,
        &relations,
        &entities,
        vec![descriptor],
        coverage,
    ))
    .unwrap();
    (
        SnapshotBlock::encode(manifest).unwrap(),
        relations,
        entities,
        bytes,
    )
}

fn selected(value: &str, context: &str) -> SelectedTabularInput {
    SelectedTabularInput {
        dataset: "research".into(),
        value_field: "patient_id".into(),
        context_field: "study_context".into(),
        value: value.into(),
        context: context.into(),
        key_domain: "study-key".into(),
        token_key_version: "dek-1".into(),
        transform_version: "recipe-1".into(),
        wrapping_version: "kek-1".into(),
        surrogate_info_type: None,
    }
}

#[test]
fn arrow_selection_reads_the_child_local_row_and_rejects_drift() {
    let (snapshot, relations, entities, bytes) = fixture();
    let relation = &snapshot.manifest().relations()[0];
    let row = SnapshotRowBinding::new(
        &snapshot,
        relation.relation_id(),
        relation.batches()[0].cid(),
        1,
    )
    .unwrap();
    let limits = IpcImportLimits::new(bytes.len(), 2, 16);
    let verified = VerifiedGoogleArrowChild::admit(row, &relations, &entities, &bytes, limits)
        .expect("admit physical child once");
    let first_row = SnapshotRowBinding::new(
        &snapshot,
        relation.relation_id(),
        relation.batches()[0].cid(),
        0,
    )
    .unwrap();
    assert!(
        verified
            .verify_row(first_row, &selected("patient-1", "study-a"))
            .is_ok()
    );
    assert!(
        verified
            .verify_row(row, &selected("patient-2", "study-b"))
            .is_ok()
    );
    assert!(matches!(
        verified.verify_row(first_row, &selected("patient-2", "study-b")),
        Err(GoogleArrowSelectionError::ValueMismatch)
    ));
    let other_coverage =
        CoverageDescriptor::new(CoverageKind::Unknown, raw_cid(b"other-coverage")).unwrap();
    let same_child = BatchDescriptor::new(*row.child_cid(), 2, bytes.len() as u64).unwrap();
    let other_relation = RelationDescriptor::new(row.relation_id(), 2, vec![same_child]).unwrap();
    let other_manifest = SnapshotManifest::admit(SnapshotManifestRequest::new(
        snapshot.manifest().semantic_snapshot().clone(),
        &relations,
        &entities,
        vec![other_relation],
        other_coverage,
    ))
    .unwrap();
    let other_snapshot = SnapshotBlock::encode(other_manifest).unwrap();
    let other_row =
        SnapshotRowBinding::new(&other_snapshot, row.relation_id(), row.child_cid(), 1).unwrap();
    assert!(matches!(
        verified.verify_row(other_row, &selected("patient-2", "study-b")),
        Err(GoogleArrowSelectionError::RowSource)
    ));
    let verify = |bytes: &[u8], selected: &SelectedTabularInput, limits| {
        verify_google_arrow_row(row, &relations, &entities, bytes, limits, selected)
    };
    assert!(verify(&bytes, &selected("patient-2", "study-b"), limits).is_ok());
    assert!(matches!(
        verify(&bytes, &selected("patient-1", "study-b"), limits),
        Err(GoogleArrowSelectionError::ValueMismatch)
    ));
    assert!(matches!(
        verify(&bytes, &selected("patient-2", "study-a"), limits),
        Err(GoogleArrowSelectionError::ContextMismatch)
    ));
    let mut wrong_field = selected("patient-2", "study-b");
    wrong_field.value_field = "missing".into();
    assert!(matches!(
        verify(&bytes, &wrong_field, limits),
        Err(GoogleArrowSelectionError::ValueField)
    ));
    wrong_field.value_field = "study_context".into();
    assert!(matches!(
        verify(&bytes, &wrong_field, limits),
        Err(GoogleArrowSelectionError::InvalidRecipe)
    ));
    let mut tampered = bytes.clone();
    tampered[20] ^= 1;
    assert!(matches!(
        verify(&tampered, &selected("patient-2", "study-b"), limits),
        Err(GoogleArrowSelectionError::ChildCid)
    ));
    assert!(matches!(
        verify(
            &bytes[..bytes.len() - 1],
            &selected("patient-2", "study-b"),
            limits
        ),
        Err(GoogleArrowSelectionError::ChildLength)
    ));
    assert!(matches!(
        verify(
            &bytes,
            &selected("patient-2", "study-b"),
            IpcImportLimits::new(bytes.len() - 1, 2, 16)
        ),
        Err(GoogleArrowSelectionError::Decode(_))
    ));
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "complete cloud authorization and physical Arrow selection fixture"
)]
fn cloud_prepare_requires_authorization_then_actual_arrow_cells() {
    use cedar_poo_bridge::google_sdp::{GoogleSdpResponse, SurrogateInfoType, WrappedKeyBinding};
    use mrr_data_security::data_protection::{
        DataProtectionDecisions, DataProtectionProfile, ReleaseReceiptClaim,
    };
    use sha2::{Digest, Sha256};

    use crate::{
        AesSivTableRecipeBinding, ArrowChildInput, BoundGoogleDeidentifyBatchPlan,
        CloudDataProtectionSelection, CloudGoogleArrowPreparation, CloudPseudonymizationGate,
        CurrentGovernance, GoogleArrowBatchError, GoogleTableBatchMismatch, Mode,
        SelectedTokenInput, TableRecipeMismatch, TokenAction, TokenAuthorizationClaim,
        TokenAuthorizationRequest, TokenLineage, TokenProfile, prepare_cloud_google_arrow_batch,
    };

    let (snapshot, relations, entities, bytes) = fixture();
    let relation = &snapshot.manifest().relations()[0];
    let row = SnapshotRowBinding::new(
        &snapshot,
        relation.relation_id(),
        relation.batches()[0].cid(),
        1,
    )
    .unwrap();
    let digest: [u8; 32] = Sha256::digest(b"patient-2").into();
    let policy = [9; 32];
    let profile = TokenProfile {
        mode: Mode::AesSiv,
        scope: "study-b",
        lineage: TokenLineage {
            tenant: "hospital-a",
            key_domain: "study-key",
            token_key_version: "dek-1",
            transform_version: "recipe-1",
            wrapping_version: "kek-1",
        },
    };
    let input = SelectedTokenInput {
        field: "patient_id",
        value_digest: &digest,
        context: "study-b",
        profile,
    }
    .bind_to_row(row);
    let request = TokenAuthorizationRequest {
        subject: "data-protection-service",
        purpose: "research",
        dataset: "research",
        action: TokenAction::Deidentify,
        input: &input,
    };
    let claim = TokenAuthorizationClaim {
        subject: request.subject,
        purpose: request.purpose,
        dataset: request.dataset,
        action: request.action,
        root: snapshot.cid(),
        field: input.field(),
        value_digest: &digest,
        context: input.context(),
        profile,
        policy_digest: &policy,
        governance_epoch: 7,
        expires_at: 100,
    };
    let release = ReleaseReceiptClaim {
        artifact_digest: "sha256:release",
        source_commit: "commit-a",
        policy_root: "ResearchDataRelease",
        epoch: 7,
    };
    let data_profile = DataProtectionProfile::new(&snapshot, request.dataset, release);
    let cloud = CloudDataProtectionSelection {
        profile: &data_profile,
        receipt: release,
        current_epoch: 7,
        decisions: DataProtectionDecisions {
            policy_root: release.policy_root,
            dataset: request.dataset,
            artifact_digest: release.artifact_digest,
            epoch: 7,
            pipeline_release_allowed: true,
            transformation_allowed: true,
        },
        gate: CloudPseudonymizationGate {
            target_profile: profile,
            admitted_context: "study-b",
            artifact_digest: release.artifact_digest,
            key_authorized: true,
        },
    };
    let current = CurrentGovernance {
        policy_digest: &policy,
        epoch: 7,
        now: 99,
    };
    let selected = selected("patient-2", "study-b");
    let recipe = AesSivTableRecipeBinding {
        dataset: "research",
        value_field: "patient_id",
        context_field: "study_context",
        profile,
        admitted_context: Some("study-b"),
        surrogate_info_type: None,
    };
    let key = || WrappedKeyBinding {
        key_domain: "study-key".into(),
        token_key_version: "dek-1".into(),
        wrapping_version: "kek-1".into(),
        kms_key_name: "projects/p/locations/us/keyRings/r/cryptoKeys/k".into(),
        wrapped_key_base64: "a2V5".into(),
    };
    let preparation = |current| CloudGoogleArrowPreparation {
        cloud,
        recipe,
        request: &request,
        claim: &claim,
        current,
        selected: &selected,
        parent: "projects/p/locations/us".into(),
        key: key(),
    };
    let prepare = |bytes: &[u8], current| {
        preparation(current).from_arrow(&ArrowChildInput {
            relations: &relations,
            entities: &entities,
            bytes,
            limits: IpcImportLimits::new(bytes.len(), 2, 16),
        })
    };
    let plan = prepare(&bytes, current).unwrap();
    assert_eq!(plan.identity().row().unwrap().row_index, 1);
    let verified = VerifiedGoogleArrowChild::admit(
        row,
        &relations,
        &entities,
        &bytes,
        IpcImportLimits::new(bytes.len(), 2, 16),
    )
    .unwrap();
    let cached_prepare = |current| preparation(current).from_verified_child(&verified);
    assert_eq!(
        cached_prepare(current)
            .unwrap()
            .identity()
            .row()
            .unwrap()
            .row_index,
        1
    );
    let first_row = SnapshotRowBinding::new(
        &snapshot,
        relation.relation_id(),
        relation.batches()[0].cid(),
        0,
    )
    .unwrap();
    let first_digest: [u8; 32] = Sha256::digest(b"patient-1").into();
    let first_input = SelectedTokenInput {
        field: "patient_id",
        value_digest: &first_digest,
        context: "study-a",
        profile,
    }
    .bind_to_row(first_row);
    let first_request = TokenAuthorizationRequest {
        input: &first_input,
        ..request
    };
    let first_claim = TokenAuthorizationClaim {
        value_digest: &first_digest,
        context: "study-a",
        ..claim
    };
    let first_selected = SelectedTabularInput {
        value: "patient-1".into(),
        context: "study-a".into(),
        ..selected.clone()
    };
    let first_cloud = CloudDataProtectionSelection {
        gate: CloudPseudonymizationGate {
            admitted_context: "study-a",
            ..cloud.gate
        },
        ..cloud
    };
    let batch_recipe = AesSivTableRecipeBinding {
        admitted_context: None,
        ..recipe
    };
    let first_preparation = || CloudGoogleArrowPreparation {
        cloud: first_cloud,
        recipe: batch_recipe,
        request: &first_request,
        claim: &first_claim,
        current,
        selected: &first_selected,
        parent: "projects/p/locations/us".into(),
        key: key(),
    };
    let second_preparation = || CloudGoogleArrowPreparation {
        recipe: batch_recipe,
        ..preparation(current)
    };
    let batch = prepare_cloud_google_arrow_batch(
        vec![first_preparation(), second_preparation()],
        &verified,
        2,
        100,
    )
    .unwrap();
    assert_eq!(
        batch
            .iter()
            .map(|plan| plan.identity().row().unwrap().row_index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let wire = BoundGoogleDeidentifyBatchPlan::from_plans(batch).unwrap();
    let request_body: serde_json::Value =
        serde_json::from_slice(&wire.deidentify_body().unwrap().to_json_bytes().unwrap()).unwrap();
    assert_eq!(
        request_body["item"]["table"]["rows"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        request_body["item"]["table"]["rows"][1]["values"][1]["stringValue"],
        "study-b"
    );
    let response_body = serde_json::json!({
        "item": {"table": {
            "headers": [{"name": "patient_id"}, {"name": "study_context"}],
            "rows": [
                {"values": [
                    {"stringValue": "c3ludGhldGljLWNpcGhlcnRleHQtMQ=="},
                    {"stringValue": "study-a"}
                ]},
                {"values": [
                    {"stringValue": "c3ludGhldGljLWNpcGhlcnRleHQtMg=="},
                    {"stringValue": "study-b"}
                ]}
            ]
        }},
        "overview": {"transformationSummaries": [{
            "field": {"name": "patient_id"},
            "results": [{"code": "SUCCESS", "count": "2"}]
        }]}
    });
    let response =
        GoogleSdpResponse::from_json_bytes(&serde_json::to_vec(&response_body).unwrap()).unwrap();
    let outputs = wire.check_response(&response).unwrap();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].identity().row().unwrap().row_index, 0);
    assert_eq!(outputs[1].identity().row().unwrap().row_index, 1);
    let mut partial = response_body;
    partial["overview"]["transformationSummaries"][0]["results"][0]["count"] =
        serde_json::json!("1");
    let partial =
        GoogleSdpResponse::from_json_bytes(&serde_json::to_vec(&partial).unwrap()).unwrap();
    let wire = BoundGoogleDeidentifyBatchPlan::from_plans(
        prepare_cloud_google_arrow_batch(
            vec![first_preparation(), second_preparation()],
            &verified,
            2,
            100,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(wire.check_response(&partial).is_err());
    assert!(matches!(
        prepare_cloud_google_arrow_batch(
            vec![second_preparation(), second_preparation()],
            &verified,
            2,
            100
        ),
        Err(GoogleArrowBatchError::Selection(
            GoogleTableBatchMismatch::OrdinalOrder
        ))
    ));
    assert!(matches!(
        prepare_cloud_google_arrow_batch(
            vec![
                first_preparation(),
                CloudGoogleArrowPreparation {
                    current: CurrentGovernance {
                        epoch: 8,
                        ..current
                    },
                    ..second_preparation()
                }
            ],
            &verified,
            2,
            100
        ),
        Err(GoogleArrowBatchError::MixedScope)
    ));
    assert!(matches!(
        prepare_cloud_google_arrow_batch(
            vec![first_preparation(), second_preparation()],
            &verified,
            2,
            10
        ),
        Err(GoogleArrowBatchError::Selection(
            GoogleTableBatchMismatch::TooManyBytes
        ))
    ));
    assert!(matches!(
        prepare_cloud_google_arrow_batch(
            vec![first_preparation(), second_preparation()],
            &verified,
            1,
            100
        ),
        Err(GoogleArrowBatchError::Selection(
            GoogleTableBatchMismatch::TooManyRows
        ))
    ));
    let wrong_second = SelectedTabularInput {
        value: "different-patient".into(),
        ..selected.clone()
    };
    assert!(matches!(
        prepare_cloud_google_arrow_batch(
            vec![
                first_preparation(),
                CloudGoogleArrowPreparation {
                    selected: &wrong_second,
                    ..second_preparation()
                }
            ],
            &verified,
            2,
            100
        ),
        Err(GoogleArrowBatchError::Row(_))
    ));
    assert!(matches!(
        cached_prepare(CurrentGovernance {
            epoch: 8,
            ..current
        }),
        Err(GoogleArrowSelectionError::Authorization(_))
    ));
    let mut alias = selected.clone();
    alias.context_field = "alias_context".into();
    assert!(verified.verify_row(row, &alias).is_ok());
    assert!(matches!(
        (CloudGoogleArrowPreparation {
            selected: &alias,
            ..preparation(current)
        })
        .from_verified_child(&verified),
        Err(GoogleArrowSelectionError::Recipe(
            TableRecipeMismatch::ContextField
        ))
    ));
    let mut surrogate = selected.clone();
    surrogate.surrogate_info_type = Some(SurrogateInfoType("other-token".into()));
    assert!(matches!(
        (CloudGoogleArrowPreparation {
            selected: &surrogate,
            ..preparation(current)
        })
        .from_verified_child(&verified),
        Err(GoogleArrowSelectionError::Recipe(
            TableRecipeMismatch::SurrogateInfoType
        ))
    ));
    assert!(matches!(
        (CloudGoogleArrowPreparation {
            recipe: AesSivTableRecipeBinding {
                admitted_context: Some("study-a"),
                ..recipe
            },
            ..preparation(current)
        })
        .from_verified_child(&verified),
        Err(GoogleArrowSelectionError::Recipe(
            TableRecipeMismatch::AdmittedContext
        ))
    ));
    assert!(matches!(
        (CloudGoogleArrowPreparation {
            recipe: AesSivTableRecipeBinding {
                profile: TokenProfile {
                    scope: "other-study",
                    ..profile
                },
                ..recipe
            },
            ..preparation(current)
        })
        .from_verified_child(&verified),
        Err(GoogleArrowSelectionError::Recipe(
            TableRecipeMismatch::Profile
        ))
    ));
    assert!(matches!(
        prepare(
            &[],
            CurrentGovernance {
                epoch: 8,
                ..current
            }
        ),
        Err(GoogleArrowSelectionError::Authorization(_))
    ));
    let mut tampered = bytes.clone();
    tampered[20] ^= 1;
    assert!(matches!(
        prepare(&tampered, current),
        Err(GoogleArrowSelectionError::ChildCid)
    ));
}
