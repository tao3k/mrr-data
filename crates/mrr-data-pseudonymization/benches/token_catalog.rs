//! Measure startup HMAC catalog admission for increasing profile counts.

use std::hint::black_box;
use std::time::{Duration, Instant};

use mrr_data_pseudonymization::{Mode, TokenLineage, TokenProfile, hmac_catalog_separated};

const ROUNDS: usize = 40;

fn run(count: usize) {
    let domains = (0..count)
        .map(|index| format!("key-{index}"))
        .collect::<Vec<_>>();
    let profiles = domains
        .iter()
        .map(|domain| TokenProfile {
            mode: Mode::HmacSha256,
            scope: "research-scope",
            lineage: TokenLineage {
                tenant: "tenant-a",
                key_domain: domain,
                token_key_version: "version-1",
                transform_version: "normalization-1",
                wrapping_version: "wrapping-1",
            },
        })
        .collect::<Vec<_>>();

    assert!(hmac_catalog_separated(&profiles));
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let start = Instant::now();
        black_box(hmac_catalog_separated(black_box(&profiles)));
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    let count = u128::try_from(count).expect("profile count fits in u128");
    let ns_per_profile = |duration: Duration| duration.as_nanos() / count;
    println!(
        "profiles={count} p50_batch_avg={}ns/profile p95_batch_avg={}ns/profile",
        ns_per_profile(samples[ROUNDS / 2]),
        ns_per_profile(samples[ROUNDS * 95 / 100])
    );
}

fn main() {
    println!("rounds={ROUNDS} profile=release startup_catalog_check=true");
    for count in [16, 128, 1024, 8192] {
        run(count);
    }
}
