//! Local content-addressed storage providers.

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::RwLock,
};

use cid::{Cid, Version};
use mrr_data_core::{DAG_CBOR_CODEC, RAW_CODEC, SHA2_256_CODE, dag_cbor_cid, raw_cid};
use tempfile::NamedTempFile;

use crate::ContentError;

/// Explicit codec declaration for one content-addressed block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentCodec {
    Raw,
    DagCbor,
}

/// Borrowed bytes with a caller-declared content codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentBlock<'a> {
    codec: ContentCodec,
    bytes: &'a [u8],
}

impl<'a> ContentBlock<'a> {
    #[must_use]
    pub const fn new(codec: ContentCodec, bytes: &'a [u8]) -> Self {
        Self { codec, bytes }
    }

    #[must_use]
    pub const fn codec(self) -> ContentCodec {
        self.codec
    }

    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }

    #[must_use]
    pub fn cid(self) -> Cid {
        cid_for(self.codec, self.bytes)
    }
}

/// A content store that computes and verifies every address from explicit bytes.
pub trait ContentStore {
    /// Stores a block under its derived CID.
    ///
    /// # Errors
    ///
    /// Returns [`ContentError`] if persistence fails or an existing block is corrupt.
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError>;

    /// Reads a block and verifies the requested CID before returning its bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ContentError`] when the block is absent, corrupt, or unreadable.
    fn get(&self, cid: &Cid) -> Result<Vec<u8>, ContentError>;
}

/// In-process, verified content store.
#[derive(Debug, Default)]
pub struct MemoryContentStore {
    blocks: RwLock<BTreeMap<Cid, Vec<u8>>>,
}

impl ContentStore for MemoryContentStore {
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError> {
        let cid = block.cid();
        let mut blocks = self
            .blocks
            .write()
            .map_err(|_| ContentError::LockPoisoned)?;
        blocks.entry(cid).or_insert_with(|| block.bytes().to_vec());
        Ok(cid)
    }

    fn get(&self, cid: &Cid) -> Result<Vec<u8>, ContentError> {
        validate_profile(cid)?;
        let bytes = self
            .blocks
            .read()
            .map_err(|_| ContentError::LockPoisoned)?
            .get(cid)
            .cloned()
            .ok_or_else(|| ContentError::NotFound(Box::new(*cid)))?;
        verify_bytes(cid, &bytes)?;
        Ok(bytes)
    }
}

/// Filesystem store with one verified block per CID-named file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemContentStore {
    root: PathBuf,
}

impl FilesystemContentStore {
    /// Creates or opens a local content directory.
    ///
    /// # Errors
    ///
    /// Returns [`ContentError`] when the directory cannot be created.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| ContentError::io("create store", &error))?;
        Ok(Self { root })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, cid: &Cid) -> PathBuf {
        self.root.join(cid.to_string())
    }
}

impl ContentStore for FilesystemContentStore {
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError> {
        let cid = block.cid();
        let path = self.path(&cid);
        if path.exists() {
            let existing = self.get(&cid)?;
            if existing != block.bytes() {
                return Err(ContentError::CidMismatch {
                    expected: Box::new(cid),
                    actual: Box::new(cid_for(block.codec(), &existing)),
                });
            }
            return Ok(cid);
        }

        let mut temporary = NamedTempFile::new_in(&self.root)
            .map_err(|error| ContentError::io("create temporary block", &error))?;
        temporary
            .write_all(block.bytes())
            .map_err(|error| ContentError::io("write temporary block", &error))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| ContentError::io("sync temporary block", &error))?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => Ok(cid),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self.get(&cid)?;
                if existing == block.bytes() {
                    Ok(cid)
                } else {
                    Err(ContentError::CidMismatch {
                        expected: Box::new(cid),
                        actual: Box::new(cid_for(block.codec(), &existing)),
                    })
                }
            }
            Err(error) => Err(ContentError::io("persist block", &error.error)),
        }
    }

    fn get(&self, cid: &Cid) -> Result<Vec<u8>, ContentError> {
        validate_profile(cid)?;
        let bytes = match fs::read(self.path(cid)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ContentError::NotFound(Box::new(*cid)));
            }
            Err(error) => return Err(ContentError::io("read block", &error)),
        };
        verify_bytes(cid, &bytes)?;
        Ok(bytes)
    }
}

pub(crate) fn cid_for(codec: ContentCodec, bytes: &[u8]) -> Cid {
    match codec {
        ContentCodec::Raw => raw_cid(bytes),
        ContentCodec::DagCbor => dag_cbor_cid(bytes),
    }
}

pub(crate) fn codec_for(cid: &Cid) -> Result<ContentCodec, ContentError> {
    validate_profile(cid)?;
    match cid.codec() {
        RAW_CODEC => Ok(ContentCodec::Raw),
        DAG_CBOR_CODEC => Ok(ContentCodec::DagCbor),
        _ => Err(ContentError::InvalidCidProfile(Box::new(*cid))),
    }
}

fn validate_profile(cid: &Cid) -> Result<(), ContentError> {
    if cid.version() != Version::V1
        || !matches!(cid.codec(), RAW_CODEC | DAG_CBOR_CODEC)
        || cid.hash().code() != SHA2_256_CODE
        || cid.hash().digest().len() != 32
    {
        return Err(ContentError::InvalidCidProfile(Box::new(*cid)));
    }
    Ok(())
}

fn verify_bytes(expected: &Cid, bytes: &[u8]) -> Result<(), ContentError> {
    let actual = cid_for(codec_for(expected)?, bytes);
    if actual == *expected {
        Ok(())
    } else {
        Err(ContentError::CidMismatch {
            expected: Box::new(*expected),
            actual: Box::new(actual),
        })
    }
}
