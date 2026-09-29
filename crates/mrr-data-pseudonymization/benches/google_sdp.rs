//! Stable Cargo bench target for the local Google SDP binding path.
//! No Cedar evaluation, KMS, network, or provider execution is timed here.

use std::hint::black_box;
use std::time::{Duration, Instant};

use cedar_poo_bridge::google_sdp::{
    GoogleSdpResponse, SelectedTabularInput, TabularAesSiv, WrappedKeyBinding,
};
use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use mrr_data_pseudonymization::{
    CurrentGovernance, Mode, SelectedTokenInput, TokenAction, TokenAuthorizationClaim,
    TokenAuthorizationRequest, TokenLineage, TokenProfile, prepare_google_aes_siv_deidentify,
};
use sha2::{Digest, Sha256};

const ROUNDS: usize = 25;
const CLAIM_ITERATIONS: usize = 100_000;
const GOOGLE_ITERATIONS: usize = 1_000;
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

fn bench(name: &str, iterations: usize, mut work: impl FnMut()) {
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let start = Instant::now();
        for _ in 0..iterations {
            work();
        }
        samples.push(start.elapsed());
    }
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

fn run(value_bytes: usize, source: &SnapshotBlock, response: &GoogleSdpResponse) {
    let value = "x".repeat(value_bytes);
    let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    let policy = [31; 32];
    let input = SelectedTokenInput {
        field: "patient_id",
        value_digest: &digest,
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
    .bind_to(source);
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
    bench("google_prepare", GOOGLE_ITERATIONS, || {
        black_box(
            prepare_google_aes_siv_deidentify(
                black_box(&request),
                black_box(&claim),
                current,
                black_box(selection.clone()),
                PARENT.to_owned(),
                key(),
            )
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
            black_box(plan.check_response(black_box(response)).unwrap());
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
}
