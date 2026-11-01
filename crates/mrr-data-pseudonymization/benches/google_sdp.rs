//! Stable Cargo bench target for the local Google SDP binding path.
//! No Cedar evaluation, KMS, network, or provider execution is timed here.

use std::hint::black_box;
use std::time::{Duration, Instant};

use cedar_poo_pseudonymization::google_sdp::{
    GoogleSdpResponse, SelectedTabularInput, TabularAesSiv, WrappedKeyBinding,
};
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
use mrr_data_pseudonymization::{
    CloudDataProtectionSelection, CloudPseudonymizationGate, CurrentGovernance, Mode,
    SelectedTokenInput, TokenAction, TokenAuthorizationClaim, TokenAuthorizationRequest,
    TokenInputBinding, TokenLineage, TokenProfile, VerifiedGoogleArrowChild,
    prepare_cloud_google_aes_siv_deidentify, prepare_google_aes_siv_deidentify,
    verify_google_arrow_row,
};
use mrr_data_security::data_protection::{
    DataProtectionDecisions, DataProtectionProfile, ReleaseReceiptClaim,
};
use sha2::{Digest, Sha256};

const ROUNDS: usize = 25;
const CLAIM_ITERATIONS: usize = 100_000;
const GOOGLE_ITERATIONS: usize = 1_000;
const ARROW_ITERATIONS: usize = 40;
const PARENT: &str = "projects/p/locations/us";
const RESPONSE: &[u8] = br#"{"item":{"table":{"headers":[{"name":"patient_id"},{"name":"study_context"}],"rows":[{"values":[{"stringValue":"c3ludGhldGljLWNpcGhlcnRleHQ="},{"stringValue":"study-1"}]}]}},"overview":{"transformationSummaries":[{"field":{"name":"patient_id"},"results":[{"count":"1","code":"SUCCESS"}]}]}}"#;

fn snapshot() -> SnapshotBlock {
    let generation = GenerationId::from_canonical_bytes("generation:google-bench").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("benchmark", "source", "revision").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:google-bench").unwrap();
    let schema = RelationSchema::new(
        relation_id,
        "Benchmark",
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

fn arrow_fixture(rows: usize) -> (SnapshotBlock, RelationCatalog, EntityCatalog, Vec<u8>) {
    let generation = GenerationId::from_canonical_bytes("generation:google-arrow-bench").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("benchmark", "source", "arrow").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:google-arrow-bench").unwrap();
    let relation = RelationSchema::new(
        relation_id,
        "Benchmark",
        vec![
            RelationField::new("patient_id", ValueSchema::String, false).unwrap(),
            RelationField::new("study_context", ValueSchema::String, false).unwrap(),
        ],
        vec![],
    )
    .unwrap();
    let owner = EntityId::from_canonical_bytes("benchmark-owner").unwrap();
    let facts = (0..rows)
        .map(|index| {
            Fact::new(
                FactId::from_canonical_bytes(format!("benchmark-fact-{index}")).unwrap(),
                relation_id,
                vec![
                    Value::String(format!("patient-id-{index}")),
                    Value::String("study-1".into()),
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
    let child = BatchDescriptor::new(raw_cid(&bytes), rows as u64, bytes.len() as u64).unwrap();
    let descriptor = RelationDescriptor::new(relation_id, rows as u64, vec![child]).unwrap();
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

fn bench_arrow_selection() {
    for rows in [2, 128] {
        let (source, relations, entities, bytes) = arrow_fixture(rows);
        let relation = &source.manifest().relations()[0];
        let row = SnapshotRowBinding::new(
            &source,
            relation.relation_id(),
            relation.batches()[0].cid(),
            1,
        )
        .unwrap();
        let limits = IpcImportLimits::new(bytes.len(), rows, 16);
        let selection = selected("patient-id-1");
        let verified =
            VerifiedGoogleArrowChild::admit(row, &relations, &entities, &bytes, limits).unwrap();
        println!("arrow_rows={rows} arrow_bytes={}", bytes.len());
        bench("arrow_full_verify", ARROW_ITERATIONS, || {
            black_box(verify_google_arrow_row(
                black_box(row),
                black_box(&relations),
                black_box(&entities),
                black_box(&bytes),
                limits,
                black_box(&selection),
            ))
            .unwrap();
        });
        bench("arrow_cached_row_verify", CLAIM_ITERATIONS, || {
            black_box(verified.verify_row(black_box(row), black_box(&selection))).unwrap();
        });
    }
}

fn selected(value: &str) -> SelectedTabularInput {
    SelectedTabularInput {
        dataset: "cohort-a".into(),
        value_field: "patient_id".into(),
        context_field: "study_context".into(),
        value: value.into(),
        context: "study-1".into(),
        key_domain: "research-key".into(),
        token_key_version: "key-1".into(),
        transform_version: "normalization-1".into(),
        wrapping_version: "wrapper-1".into(),
        surrogate_info_type: None,
    }
}

fn key() -> WrappedKeyBinding {
    WrappedKeyBinding {
        key_domain: "research-key".into(),
        token_key_version: "key-1".into(),
        wrapping_version: "wrapper-1".into(),
        kms_key_name: "projects/p/locations/us/keyRings/r/cryptoKeys/k".into(),
        wrapped_key_base64: "a2V5".into(),
    }
}

fn timed(iterations: usize, work: &mut impl FnMut()) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        work();
    }
    start.elapsed()
}

fn report(name: &str, iterations: usize, mut samples: Vec<Duration>) {
    samples.sort_unstable();
    let hundredths = |elapsed: Duration| {
        elapsed.as_nanos().saturating_mul(100) / u128::try_from(iterations).unwrap()
    };
    let p50 = hundredths(samples[ROUNDS / 2]);
    let p95 = hundredths(samples[ROUNDS * 95 / 100]);
    println!(
        "{name} iterations={iterations} p50_batch_avg={}.{:02}ns/op p95_batch_avg={}.{:02}ns/op",
        p50 / 100,
        p50 % 100,
        p95 / 100,
        p95 % 100
    );
}

fn bench(name: &str, iterations: usize, mut work: impl FnMut()) {
    let samples = (0..ROUNDS).map(|_| timed(iterations, &mut work)).collect();
    report(name, iterations, samples);
}

fn bench_pair(
    first_name: &str,
    mut first: impl FnMut(),
    second_name: &str,
    mut second: impl FnMut(),
) {
    let mut first_samples = Vec::with_capacity(ROUNDS);
    let mut second_samples = Vec::with_capacity(ROUNDS);
    for round in 0..ROUNDS {
        if round % 2 == 0 {
            first_samples.push(timed(GOOGLE_ITERATIONS, &mut first));
            second_samples.push(timed(GOOGLE_ITERATIONS, &mut second));
        } else {
            second_samples.push(timed(GOOGLE_ITERATIONS, &mut second));
            first_samples.push(timed(GOOGLE_ITERATIONS, &mut first));
        }
    }
    report(first_name, GOOGLE_ITERATIONS, first_samples);
    report(second_name, GOOGLE_ITERATIONS, second_samples);
}

fn bench_claim(
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    policy: &[u8; 32],
) {
    bench("claim_match", CLAIM_ITERATIONS, || {
        black_box(
            black_box(request)
                .check_claim(black_box(claim), black_box(policy), 7, 99)
                .is_ok(),
        );
    });
}

fn bench_cloud(
    source: &SnapshotBlock,
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: CurrentGovernance<'_>,
    input: &TokenInputBinding<'_>,
    selection: &SelectedTabularInput,
) {
    let release = ReleaseReceiptClaim {
        artifact_digest: "sha256:candidate",
        source_commit: "commit-a",
        policy_root: "CustomerDataRelease",
        epoch: 7,
    };
    let protection = DataProtectionProfile::new(source, request.dataset, release);
    let gate = CloudPseudonymizationGate {
        target_profile: *input.profile(),
        admitted_context: input.context(),
        artifact_digest: release.artifact_digest,
        key_authorized: true,
    };
    let cloud = CloudDataProtectionSelection {
        profile: &protection,
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
        gate,
    };
    bench("cloud_gate", CLAIM_ITERATIONS, || {
        black_box(
            gate.check(black_box(input), release.artifact_digest)
                .is_ok(),
        );
    });
    bench("cloud_release", CLAIM_ITERATIONS, || {
        black_box(protection.check(release, 7, cloud.decisions).is_ok());
    });
    bench_pair(
        "cloud_google_prepare",
        || {
            black_box(
                prepare_cloud_google_aes_siv_deidentify(
                    cloud,
                    black_box(request),
                    black_box(claim),
                    current,
                    black_box(selection.clone()),
                    PARENT.to_owned(),
                    key(),
                )
                .unwrap(),
            );
        },
        "google_prepare",
        || {
            black_box(
                prepare_google_aes_siv_deidentify(
                    black_box(request),
                    black_box(claim),
                    current,
                    black_box(selection.clone()),
                    PARENT.to_owned(),
                    key(),
                )
                .unwrap(),
            );
        },
    );
}

fn bound_input<'a>(source: &'a SnapshotBlock, digest: &'a [u8; 32]) -> TokenInputBinding<'a> {
    let relation = &source.manifest().relations()[0];
    let row = SnapshotRowBinding::new(
        source,
        relation.relation_id(),
        relation.batches()[0].cid(),
        0,
    )
    .unwrap();
    SelectedTokenInput {
        field: "patient_id",
        value_digest: digest,
        context: "study-1",
        profile: TokenProfile {
            mode: Mode::AesSiv,
            scope: "study-1",
            lineage: TokenLineage {
                tenant: "tenant-a",
                key_domain: "research-key",
                token_key_version: "key-1",
                transform_version: "normalization-1",
                wrapping_version: "wrapper-1",
            },
        },
    }
    .bind_to_row(row)
}

fn run(value_bytes: usize, source: &SnapshotBlock, response: &GoogleSdpResponse) {
    let value = "x".repeat(value_bytes);
    let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    let policy = [31; 32];
    let input = bound_input(source, &digest);
    let request = TokenAuthorizationRequest {
        subject: "researcher-1",
        purpose: "approved-study",
        dataset: "cohort-a",
        action: TokenAction::Deidentify,
        input: &input,
    };
    let claim = TokenAuthorizationClaim {
        subject: request.subject,
        purpose: request.purpose,
        dataset: request.dataset,
        action: request.action,
        root: source.cid(),
        field: input.field(),
        value_digest: &digest,
        context: input.context(),
        profile: *input.profile(),
        policy_digest: &policy,
        governance_epoch: 7,
        expires_at: 100,
    };
    let current = CurrentGovernance {
        policy_digest: &policy,
        epoch: 7,
        now: 99,
    };
    let selection = selected(&value);
    let direct_plan =
        TabularAesSiv::from_selected(PARENT.to_owned(), selection.clone(), key()).unwrap();
    bench_claim(&request, &claim, &policy);
    bench_cloud(source, &request, &claim, current, &input, &selection);
    bench("sha256_selected_value", GOOGLE_ITERATIONS, || {
        black_box(Sha256::digest(black_box(value.as_bytes())));
    });
    bench("google_bridge_plan", GOOGLE_ITERATIONS, || {
        black_box(
            TabularAesSiv::from_selected(PARENT.to_owned(), black_box(selection.clone()), key())
                .unwrap(),
        );
    });
    bench("google_bridge_response_check", GOOGLE_ITERATIONS, || {
        black_box(
            direct_plan
                .check_deidentify_response(black_box(response))
                .unwrap(),
        );
    });
    bench(
        "google_prepare_and_check_response",
        GOOGLE_ITERATIONS,
        || {
            let plan = prepare_google_aes_siv_deidentify(
                black_box(&request),
                black_box(&claim),
                current,
                black_box(selection.clone()),
                PARENT.to_owned(),
                key(),
            )
            .unwrap();
            black_box(
                plan.check_response(
                    black_box(response),
                    &mrr_data_pseudonymization::GoogleCurrentAuthority::token(current),
                )
                .unwrap(),
            );
        },
    );
}

fn main() {
    let source = snapshot();
    let response = GoogleSdpResponse::from_json_bytes(RESPONSE).unwrap();
    println!("cargo_bench=stable rounds={ROUNDS} provider_io=false profile=release");
    for value_bytes in [32, 4096] {
        println!("selected_value_bytes={value_bytes}");
        run(value_bytes, &source, &response);
    }
    bench_arrow_selection();
}
