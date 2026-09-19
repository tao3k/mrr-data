//! Concrete content protocol adapter.
use cid::Cid;
use kache_store::{ArtifactPolicy, ArtifactStore, config::Config};
use mrr_data_content::{ContentBlock, ContentError, ContentStore};
use std::{
    fs,
    io::{ErrorKind, Read, Write},
    path::Path,
};
use tempfile::NamedTempFile;

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

/// Persistent Kache-backed content cache. Operations are synchronous disk I/O;
/// async applications may run them on their blocking executor.
pub struct KacheContentStore {
    store: ArtifactStore<DataPolicy>,
}

fn error(operation: &'static str, error: impl Into<anyhow::Error>) -> ContentError {
    let error = error.into();
    ContentError::Io {
        operation,
        kind: error
            .downcast_ref::<std::io::Error>()
            .map_or(ErrorKind::Other, std::io::Error::kind),
    }
}
fn key(cid: &Cid) -> String {
    blake3::hash(&cid.to_bytes()).to_hex().to_string()
}

impl KacheContentStore {
    /// Opens a dedicated cache directory. `max_bytes` is Kache's soft quota;
    /// explicit maintenance honors upstream grace periods and shared blobs.
    /// # Errors
    /// Returns an I/O error when Kache cannot open its metadata or blob store.
    pub fn open(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, ContentError> {
        let config = Config {
            cache_dir: root.as_ref().to_owned(),
            max_size: max_bytes,
            gc_evict_shared: false,
            upload_spool_max_jobs: 16,
            deferred_durability: false,
        };
        Ok(Self {
            store: ArtifactStore::open(config).map_err(|e| error("open Kache", e))?,
        })
    }

    /// Runs upstream quota maintenance. No separate MRR eviction policy exists.
    /// # Errors
    /// Returns an error if upstream maintenance fails.
    pub fn maintain(&self) -> Result<(), ContentError> {
        let _gc = self
            .store
            .acquire_gc_lock()
            .map_err(|e| error("lock Kache GC", e))?;
        self.store.evict().map_err(|e| error("evict Kache", e))?;
        Ok(())
    }
}

impl ContentStore for KacheContentStore {
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError> {
        let cid = block.cid();
        let _lock = self
            .store
            .try_lock(&key(&cid))
            .map_err(|e| error("lock Kache key", e))?
            .ok_or(ContentError::Io {
                operation: "Kache key busy",
                kind: ErrorKind::WouldBlock,
            })?;
        match self.get_bounded(&cid, block.bytes().len()) {
            Ok(_) => return Ok(cid),
            Err(ContentError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        let mut source = NamedTempFile::new_in(self.store.cache_dir())
            .map_err(|e| error("stage Kache block", e))?;
        source
            .write_all(block.bytes())
            .map_err(|e| error("write Kache staging", e))?;
        self.store
            .put_with_compile_time_independent(
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
            )
            .map_err(|e| error("put Kache block", e))?;
        Ok(cid)
    }

    fn get_bounded(&self, cid: &Cid, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        let codec = mrr_data_content::ContentCodec::from_cid(cid)?;
        let Some(meta) = self
            .store
            .get(&key(cid))
            .map_err(|e| error("get Kache entry", e))?
        else {
            return Err(ContentError::NotFound(Box::new(*cid)));
        };
        if meta.cache_key != key(cid) || meta.files.len() != 1 || meta.files[0].name != "block" {
            return Err(ContentError::Io {
                operation: "invalid Kache entry",
                kind: ErrorKind::InvalidData,
            });
        }
        let file = match fs::File::open(self.store.blob_path(&meta.files[0].hash)) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(ContentError::NotFound(Box::new(*cid)));
            }
            Err(e) => return Err(error("read Kache blob", e)),
        };
        let length = file
            .metadata()
            .map_err(|e| error("inspect Kache blob", e))?
            .len();
        if length > max_bytes as u64 {
            return Err(ContentError::BlockTooLarge {
                limit: max_bytes as u64,
                actual: length,
            });
        }
        let mut bytes = Vec::new();
        file.take((max_bytes as u64).saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|e| error("read Kache blob", e))?;
        if bytes.len() > max_bytes {
            return Err(ContentError::BlockTooLarge {
                limit: max_bytes as u64,
                actual: bytes.len() as u64,
            });
        }
        let actual = ContentBlock::new(codec, &bytes).cid();
        if actual != *cid {
            return Err(ContentError::CidMismatch {
                expected: Box::new(*cid),
                actual: Box::new(actual),
            });
        }
        Ok(bytes)
    }
}

#[cfg(all(test, feature = "blocking"))]
#[path = "../tests/unit/kache_acceptance.rs"]
mod acceptance;
