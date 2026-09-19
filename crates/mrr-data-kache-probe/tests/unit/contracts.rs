//! Public-API reuse probe, not a production cache implementation.
use std::{fs, io::Write, path::Path};

use anyhow::{Result, ensure};
use cid::Cid;
use kache_store::{ArtifactPolicy, ArtifactStore, config::Config};
use mrr_data_content::{ContentBlock, ContentCodec};
use mrr_data_core::{DAG_CBOR_CODEC, RAW_CODEC};
use tempfile::{NamedTempFile, tempdir};

struct DataPolicy;

impl ArtifactPolicy for DataPolicy {
    fn allow_hardlink(_: &str) -> bool {
        false
    }
    fn allow_empty(_: &str, _: &[String]) -> bool {
        true
    }
    fn emit_kind(_: &str) -> Option<&'static str> {
        None
    }
    fn stable_after_store(_: &str) -> bool {
        true
    }
}

fn config(root: &Path, max_size: u64) -> Config {
    Config {
        cache_dir: root.to_owned(),
        max_size,
        gc_evict_shared: false,
        upload_spool_max_jobs: 16,
        deferred_durability: false,
    }
}

// Kache remote keys require 64 lowercase hex characters. Hash the entire CID,
// including codec, into a cache-local lookup key; MRR still uses the original CID.
fn key(cid: &Cid) -> String {
    blake3::hash(&cid.to_bytes()).to_hex().to_string()
}

fn put(store: &ArtifactStore<DataPolicy>, block: ContentBlock<'_>) -> Result<Cid> {
    let cid = block.cid();
    let mut source = NamedTempFile::new()?;
    source.write_all(block.bytes())?;
    store.put_with_compile_time_independent(
        &key(&cid),
        "mrr-data",
        &[],
        &[],
        "",
        "",
        &[(source.path().to_owned(), "block".into())],
        "",
        "",
        0,
    )?;
    Ok(cid)
}

fn get(store: &ArtifactStore<DataPolicy>, cid: &Cid) -> Result<Option<Vec<u8>>> {
    let Some(meta) = store.get(&key(cid))? else {
        return Ok(None);
    };
    ensure!(meta.cache_key == key(cid), "entry binding mismatch");
    ensure!(
        meta.files.len() == 1 && meta.files[0].name == "block",
        "invalid block entry"
    );
    let bytes = fs::read(store.blob_path(&meta.files[0].hash))?;
    let codec = match cid.codec() {
        RAW_CODEC => ContentCodec::Raw,
        DAG_CBOR_CODEC => ContentCodec::DagCbor,
        _ => anyhow::bail!("unsupported codec"),
    };
    // Kache defaults to metadata-only verification for durable cache hits.
    // The MRR boundary must verify the requested CID independently on every hit.
    ensure!(
        ContentBlock::new(codec, &bytes).cid() == *cid,
        "CID mismatch"
    );
    Ok(Some(bytes))
}

#[test]
fn blocks_survive_reopen_and_codec_distinction_reuses_one_blob() -> Result<()> {
    let root = tempdir()?;
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1_000_000))?;
    // Canonical DAG-CBOR {"v": 1}, also valid as an opaque raw block.
    let bytes = [0xa1, 0x61, b'v', 0x01];
    let raw = put(&store, ContentBlock::new(ContentCodec::Raw, &bytes))?;
    let dag = put(&store, ContentBlock::new(ContentCodec::DagCbor, &bytes))?;
    assert_ne!(raw, dag);
    assert_ne!(key(&raw), key(&dag));
    assert_eq!(store.entry_count()?, 2);
    assert_eq!(store.physical_size()?, 4);
    drop(store);
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1_000_000))?;
    assert_eq!(get(&store, &raw)?, Some(bytes.to_vec()));
    assert_eq!(get(&store, &dag)?, Some(bytes.to_vec()));
    // Removing one entry must not remove the other entry's shared blob.
    store.remove_entry(&key(&raw))?;
    assert_eq!(get(&store, &raw)?, None);
    assert_eq!(get(&store, &dag)?, Some(bytes.to_vec()));
    store.remove_entry(&key(&dag))?;
    assert_eq!(store.physical_size()?, 0);
    Ok(())
}

#[test]
fn empty_block_and_duplicate_insert_use_upstream_policy_and_dedup() -> Result<()> {
    let root = tempdir()?;
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1_000_000))?;
    let block = ContentBlock::new(ContentCodec::Raw, b"");
    let cid = put(&store, block)?;
    assert_eq!(put(&store, block)?, cid);
    assert_eq!(store.entry_count()?, 1);
    assert_eq!(get(&store, &cid)?, Some(vec![]));
    Ok(())
}

#[test]
fn source_mutation_cannot_change_the_cached_block() -> Result<()> {
    let root = tempdir()?;
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1_000_000))?;
    let mut source = NamedTempFile::new()?;
    source.write_all(b"original")?;
    let cid = ContentBlock::new(ContentCodec::Raw, b"original").cid();
    store.put_with_compile_time_independent(
        &key(&cid),
        "mrr-data",
        &[],
        &[],
        "",
        "",
        &[(source.path().to_owned(), "block".into())],
        "",
        "",
        0,
    )?;
    fs::write(source.path(), b"modified")?;
    assert_eq!(get(&store, &cid)?, Some(b"original".to_vec()));
    Ok(())
}

#[test]
fn same_size_corruption_is_never_returned_as_valid_mrr_content() -> Result<()> {
    let root = tempdir()?;
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1_000_000))?;
    let cid = put(&store, ContentBlock::new(ContentCodec::Raw, b"good"))?;
    let meta = store.get(&key(&cid))?.unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    // Replace the read-only inode, rather than weakening its permissions.
    let mut replacement = NamedTempFile::new_in(blob.parent().unwrap())?;
    replacement.write_all(b"evil")?;
    replacement.persist(blob)?;
    match get(&store, &cid) {
        Err(error) => assert!(error.to_string().contains("CID mismatch")),
        Ok(None) => (), // Upstream verification may evict the damaged entry.
        Ok(Some(_)) => panic!("corrupt content escaped verification"),
    }
    Ok(())
}

#[test]
fn upstream_eviction_preserves_recent_entries_and_explicit_removal_reclaims() -> Result<()> {
    let root = tempdir()?;
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1))?;
    let cid = put(&store, ContentBlock::new(ContentCodec::Raw, &[7; 4096]))?;
    assert_eq!(store.physical_size()?, 4096);
    store.evict()?;
    // Upstream protects recent entries for 120 seconds even over max_size.
    // This proves max_size is a soft target, not an Agent disk-budget bound.
    assert_eq!(store.physical_size()?, 4096);
    assert!(get(&store, &cid)?.is_some());
    store.remove_entry(&key(&cid))?;
    assert!(get(&store, &cid)?.is_none());
    assert_eq!(store.physical_size()?, 0);
    Ok(())
}

#[test]
fn upstream_key_lock_excludes_other_process_and_releases_on_drop() -> Result<()> {
    let root = tempdir()?;
    let store = ArtifactStore::<DataPolicy>::open(config(root.path(), 1_000_000))?;
    let cache_key = key(&ContentBlock::new(ContentCodec::Raw, b"lock").cid());
    let guard = store.try_lock(&cache_key)?.expect("first process owns key");
    let run_child = |expected: &str| -> Result<()> {
        let result = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "tests::contracts::lock_child", "--ignored"])
            .env("MRR_KACHE_PROBE_ROOT", root.path())
            .env("MRR_KACHE_PROBE_KEY", &cache_key)
            .env("MRR_KACHE_PROBE_EXPECT", expected)
            .output()?;
        ensure!(
            result.status.success(),
            "lock child failed: {} {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        Ok(())
    };
    run_child("blocked")?;
    drop(guard);
    run_child("acquired")?;
    Ok(())
}

#[test]
#[ignore = "subprocess helper invoked by the cross-process lock contract"]
fn lock_child() -> Result<()> {
    let root = std::env::var("MRR_KACHE_PROBE_ROOT")?;
    let store = ArtifactStore::<DataPolicy>::open(config(Path::new(&root), 1_000_000))?;
    let guard = store.try_lock(&std::env::var("MRR_KACHE_PROBE_KEY")?)?;
    assert_eq!(
        guard.is_some(),
        std::env::var("MRR_KACHE_PROBE_EXPECT")? == "acquired"
    );
    Ok(())
}
