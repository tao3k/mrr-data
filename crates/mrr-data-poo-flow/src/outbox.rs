//! Durable local protection records and cross-process coordination.
use crate::protocol::{PROFILE, text};
use anyhow::{Context as _, Result, ensure};
use cid::Cid;
use mrr_data_cache::BlockingContentStore;
use mrr_data_content::{
    AsyncContentStore, ContentBlock, ContentError, ContentStore, FilesystemContentStore,
    LocalFuture,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read as _, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::NamedTempFile;

const MAX_RECORDS: usize = 4096;
const MAX_RECORD_BYTES: u64 = 4096;
const MAX_BLOCKS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LocalPolicy {
    pub(crate) max_bytes: u64,
    pub(crate) protection_secs: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum State {
    Pending,
    Synced,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub(crate) profile: String,
    pub(crate) root: String,
    pub(crate) source: String,
    pub(crate) revision: String,
    pub(crate) generation: String,
    pub(crate) protected_until: u64,
    pub(crate) state: State,
    pub(crate) blocks: Vec<String>,
}

impl Record {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.profile == PROFILE, "unsupported protection profile");
        ensure!(
            text(&self.source) && text(&self.revision),
            "invalid protection scope"
        );
        ensure!(
            !self.generation.is_empty() && self.generation.len() <= 256,
            "invalid generation"
        );
        canonical_cid(&self.root)?;
        ensure!(
            !self.blocks.is_empty() && self.blocks.len() <= MAX_BLOCKS,
            "invalid closure size"
        );
        let mut seen = BTreeSet::new();
        for block in &self.blocks {
            canonical_cid(block)?;
            ensure!(seen.insert(block), "duplicate protected block");
        }
        ensure!(
            seen.contains(&self.root),
            "protected root absent from closure"
        );
        Ok(())
    }
}

fn canonical_cid(value: &str) -> Result<Cid> {
    let cid: Cid = value.parse()?;
    ensure!(cid.to_string() == value, "canonical CID required");
    Ok(cid)
}

pub(crate) fn now_secs() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn record_path(path: &Path, root: &str) -> Result<PathBuf> {
    canonical_cid(root)?;
    Ok(path.join(format!("snapshot-{root}.json")))
}

fn read_record(path: &Path, root: &str) -> Result<Record> {
    let path = record_path(path, root)?;
    let file =
        File::open(&path).with_context(|| format!("protection record missing for {root}"))?;
    ensure!(
        file.metadata()?.len() <= MAX_RECORD_BYTES,
        "protection record too large"
    );
    let mut bytes = Vec::new();
    file.take(MAX_RECORD_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_RECORD_BYTES,
        "protection record too large"
    );
    let record: Record = serde_json::from_slice(&bytes)?;
    record.validate()?;
    ensure!(record.root == root, "protection record root mismatch");
    Ok(record)
}

fn write_record(path: &Path, record: &Record) -> Result<()> {
    record.validate()?;
    let bytes = serde_json::to_vec(record)?;
    ensure!(
        bytes.len() as u64 <= MAX_RECORD_BYTES,
        "protection record too large"
    );
    let mut temporary = NamedTempFile::new_in(path)?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(record_path(path, &record.root)?)?;
    sync_directory(path)
}

fn records(path: &Path) -> Result<Vec<Record>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().context("non-UTF8 outbox entry")?;
        if let Some(root) = name
            .strip_prefix("snapshot-")
            .and_then(|name| name.strip_suffix(".json"))
        {
            found.push(read_record(path, root)?);
            ensure!(found.len() <= MAX_RECORDS, "too many protection records");
        }
    }
    found.sort_by(|a, b| a.root.cmp(&b.root));
    Ok(found)
}

/// The file lock serializes protection admission, record transitions and GC
/// across independent worker processes. Dropping the file releases the lock.
pub(crate) struct Guard {
    path: PathBuf,
    _lock: File,
}

impl Guard {
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        fs::create_dir_all(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join(".outbox.lock"))?;
        lock.lock()?;
        Ok(Self {
            path: path.to_owned(),
            _lock: lock,
        })
    }

    pub(crate) fn read(&self, root: &str) -> Result<Record> {
        read_record(&self.path, root)
    }

    pub(crate) fn pending(&self, limit: usize) -> Result<Vec<Record>> {
        Ok(records(&self.path)?
            .into_iter()
            .filter(|record| record.state == State::Pending)
            .take(limit)
            .collect())
    }

    /// Removes only expired, remotely acknowledged closures, preserving any
    /// block shared with a pending or unexpired protection record.
    pub(crate) fn maintain(&self, now: u64) -> Result<()> {
        let all = records(&self.path)?;
        let retained: BTreeSet<&str> = all
            .iter()
            .filter(|record| record.state == State::Pending || record.protected_until > now)
            .flat_map(|record| record.blocks.iter().map(String::as_str))
            .collect();
        for record in all
            .iter()
            .filter(|record| record.state == State::Synced && record.protected_until <= now)
        {
            for block in &record.blocks {
                if !retained.contains(block.as_str()) {
                    match fs::remove_file(self.path.join(block)) {
                        Ok(()) => (),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            fs::remove_file(record_path(&self.path, &record.root)?)?;
        }
        sync_directory(&self.path)
    }

    /// Counts physical CID files, including abandoned writes, before admission.
    /// A failed reservation never yields a protection receipt.
    pub(crate) fn preflight(&self, incoming: &[(Cid, usize)], max_bytes: u64) -> Result<()> {
        let mut used = 0_u64;
        let mut files = BTreeSet::new();
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Ok(cid) = canonical_cid(name) else {
                continue;
            };
            ensure!(
                entry.file_type()?.is_file(),
                "CID path is not a regular file"
            );
            used = used
                .checked_add(entry.metadata()?.len())
                .context("local size overflow")?;
            files.insert(cid);
        }
        for (cid, size) in incoming {
            if !files.contains(cid) {
                used = used
                    .checked_add(*size as u64)
                    .context("local size overflow")?;
                files.insert(*cid);
            }
        }
        ensure!(used <= max_bytes, "local protection capacity exceeded");
        Ok(())
    }

    pub(crate) fn commit(&self, mut record: Record) -> Result<()> {
        record.validate()?;
        if record_path(&self.path, &record.root)?.exists() {
            let existing = self.read(&record.root)?;
            ensure!(
                existing.profile == record.profile
                    && existing.source == record.source
                    && existing.revision == record.revision
                    && existing.generation == record.generation
                    && existing.blocks == record.blocks,
                "protection identity drift"
            );
            record.protected_until = record.protected_until.max(existing.protected_until);
        }
        record.state = State::Pending;
        write_record(&self.path, &record)
    }

    pub(crate) fn mark_synced(&self, root: &str, source: &str, revision: &str) -> Result<()> {
        let mut record = self.read(root)?;
        ensure!(
            record.source == source && record.revision == revision,
            "sync scope mismatch"
        );
        record.state = State::Synced;
        write_record(&self.path, &record)
    }
}

pub(crate) fn claim(path: &Path, root: &str) -> Result<Option<File>> {
    canonical_cid(root)?;
    // A fixed shard set avoids leaving one lock file per published snapshot.
    // Collisions serialize unrelated roots, but never allow concurrent replay.
    let shard = &root[root.len() - 2..];
    let claim = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join(format!(".claim-{shard}")))?;
    match claim.try_lock() {
        Ok(()) => Ok(Some(claim)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Query read-through may fetch verified remote bytes even when a disposable
/// local fill cannot fit. Serialize its admission with protection writes so
/// persisted CID blocks remain under the same owner-scoped capacity bound.
pub(crate) struct BoundedQueryCache<'a> {
    pub(crate) local: &'a BlockingContentStore<FilesystemContentStore>,
    pub(crate) path: &'a Path,
    pub(crate) max_bytes: u64,
}

impl AsyncContentStore for BoundedQueryCache<'_> {
    fn load<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> LocalFuture<'a, Vec<u8>> {
        self.local.load(cid, max_bytes)
    }

    fn store<'a>(&'a self, block: ContentBlock<'a>) -> LocalFuture<'a, Cid> {
        Box::pin(async move {
            let path = self.path.to_owned();
            let cid = block.cid();
            let bytes = block.bytes().to_vec();
            let codec = block.codec();
            let max_bytes = self.max_bytes;
            tokio::task::spawn_blocking(move || {
                let guard = Guard::acquire(&path)?;
                guard.preflight(&[(cid, bytes.len())], max_bytes)?;
                let local = FilesystemContentStore::open(&path).map_err(anyhow::Error::from)?;
                local
                    .put(ContentBlock::new(codec, &bytes))
                    .map_err(anyhow::Error::from)
            })
            .await
            .map_err(|_| cache_error())?
            .map_err(|_| cache_error())
        })
    }
}

fn cache_error() -> ContentError {
    ContentError::Io {
        operation: "bounded local query cache admission",
        kind: ErrorKind::Other,
    }
}

#[cfg(test)]
#[path = "../tests/unit/outbox.rs"]
mod tests;
