use super::{
    DEFAULT_CACHE_MAX_BYTES, DEFAULT_S3_TIMEOUT_SECS, DEFAULT_WORKER_THREADS, RuntimeConfig,
};
use std::{collections::BTreeMap, time::Duration};

#[test]
fn runtime_capacity_defaults_remain_bounded() {
    let config = RuntimeConfig::from_lookup(|_| None).unwrap();
    assert_eq!(config.cache_max_bytes, DEFAULT_CACHE_MAX_BYTES);
    assert_eq!(config.worker_threads, DEFAULT_WORKER_THREADS);
    assert_eq!(
        config.s3_timeout,
        Duration::from_secs(DEFAULT_S3_TIMEOUT_SECS)
    );
}

#[test]
fn runtime_owner_can_scale_capacity_threads_and_timeout() {
    let values = BTreeMap::from([
        ("MRR_CACHE_MAX_BYTES", "281474976710656"),
        ("MRR_WORKER_THREADS", "32"),
        ("MRR_S3_REQUEST_TIMEOUT_SECS", "20"),
    ]);
    let config =
        RuntimeConfig::from_lookup(|name| values.get(name).map(ToString::to_string)).unwrap();
    assert_eq!(config.cache_max_bytes, 256 * 1024 * 1024 * 1024 * 1024);
    assert_eq!(config.worker_threads, 32);
    assert_eq!(config.s3_timeout, Duration::from_secs(20));
}

#[test]
fn runtime_owner_cannot_disable_resource_bounds() {
    for (name, value) in [
        ("MRR_CACHE_MAX_BYTES", "0"),
        ("MRR_WORKER_THREADS", "0"),
        ("MRR_WORKER_THREADS", "257"),
        ("MRR_S3_REQUEST_TIMEOUT_SECS", "31"),
        ("MRR_CACHE_MAX_BYTES", "1125899906842625"),
        ("MRR_CACHE_MAX_BYTES", "many"),
    ] {
        let error =
            RuntimeConfig::from_lookup(|candidate| (candidate == name).then(|| value.to_owned()))
                .unwrap_err()
                .to_string();
        assert!(error.contains(name), "unexpected error: {error}");
    }
}
