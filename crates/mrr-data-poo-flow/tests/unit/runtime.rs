use super::{
    DEFAULT_LOCAL_MAX_BYTES, DEFAULT_PROTECTION_SECS, DEFAULT_S3_TIMEOUT_SECS, DEFAULT_SYNC_BATCH,
    DEFAULT_SYNC_INTERVAL_SECS, DEFAULT_WORKER_THREADS, RuntimeConfig,
};
use std::{collections::BTreeMap, time::Duration};

#[test]
fn runtime_capacity_defaults_remain_bounded() {
    let config = RuntimeConfig::from_lookup(|_| None).unwrap();
    assert_eq!(config.worker_threads, DEFAULT_WORKER_THREADS);
    assert_eq!(config.local.max_bytes, DEFAULT_LOCAL_MAX_BYTES);
    assert_eq!(config.local.protection_secs, DEFAULT_PROTECTION_SECS);
    assert_eq!(
        config.sync_batch,
        usize::try_from(DEFAULT_SYNC_BATCH).unwrap()
    );
    assert_eq!(
        config.sync_interval,
        Duration::from_secs(DEFAULT_SYNC_INTERVAL_SECS)
    );
    assert_eq!(
        config.s3_timeout,
        Duration::from_secs(DEFAULT_S3_TIMEOUT_SECS)
    );
}

#[test]
fn runtime_owner_can_scale_capacity_threads_and_timeout() {
    let values = BTreeMap::from([
        ("MRR_WORKER_THREADS", "32"),
        ("MRR_S3_REQUEST_TIMEOUT_SECS", "20"),
        ("MRR_LOCAL_MAX_BYTES", "4294967296"),
        ("MRR_PROTECTION_SECS", "86400"),
        ("MRR_SYNC_INTERVAL_SECS", "30"),
        ("MRR_SYNC_BATCH", "64"),
    ]);
    let config =
        RuntimeConfig::from_lookup(|name| values.get(name).map(ToString::to_string)).unwrap();
    assert_eq!(config.worker_threads, 32);
    assert_eq!(config.s3_timeout, Duration::from_secs(20));
    assert_eq!(config.local.max_bytes, 4 * 1024 * 1024 * 1024);
    assert_eq!(config.local.protection_secs, 86400);
    assert_eq!(config.sync_interval, Duration::from_secs(30));
    assert_eq!(config.sync_batch, 64);
}

#[test]
fn runtime_owner_cannot_disable_resource_bounds() {
    for (name, value) in [
        ("MRR_WORKER_THREADS", "0"),
        ("MRR_WORKER_THREADS", "257"),
        ("MRR_S3_REQUEST_TIMEOUT_SECS", "31"),
        ("MRR_LOCAL_MAX_BYTES", "0"),
        ("MRR_PROTECTION_SECS", "0"),
        ("MRR_SYNC_INTERVAL_SECS", "0"),
        ("MRR_SYNC_BATCH", "129"),
    ] {
        let error =
            RuntimeConfig::from_lookup(|candidate| (candidate == name).then(|| value.to_owned()))
                .unwrap_err()
                .to_string();
        assert!(error.contains(name), "unexpected error: {error}");
    }
}
