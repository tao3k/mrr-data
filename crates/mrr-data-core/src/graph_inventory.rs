//! Bounded content inventory; native metadata traversal and sealing are separate gates.
use crate::{RAW_CODEC, dag_cbor_cid, profile::validate_cid, raw_cid};
use cid::Cid;
use mrr_data_profile::GRAPHAR_FILE_INVENTORY_SCHEMA;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Physical files supported by the initial `GraphAr` inventory profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphFileKind {
    Metadata,
    Parquet,
    Count,
}
/// Immutable descriptor of one relative logical file and its exact bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFile {
    path: String,
    cid: Cid,
    byte_length: u64,
    kind: GraphFileKind,
}
impl GraphFile {
    #[must_use]
    pub fn new(path: String, bytes: &[u8], kind: GraphFileKind) -> Self {
        Self {
            path,
            cid: raw_cid(bytes),
            byte_length: bytes.len() as u64,
            kind,
        }
    }
    /// Hash a bounded reader with a fixed 32 KiB scratch buffer.
    /// # Errors
    /// Refuses invalid paths/limits, I/O failures, empty input or byte overflow.
    pub fn from_reader(
        path: String,
        mut reader: impl std::io::Read,
        kind: GraphFileKind,
        limits: GraphInventoryLimits,
    ) -> Result<Self, GraphInventoryError> {
        validate_limits(limits)?;
        validate_path(&path, limits.max_path_bytes)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 32 * 1024].into_boxed_slice();
        let mut byte_length = 0u64;
        loop {
            let remaining = limits.max_total_bytes - byte_length;
            let capacity = usize::try_from(remaining.saturating_add(1))
                .unwrap_or(usize::MAX)
                .min(buffer.len());
            let read = match reader.read(&mut buffer[..capacity]) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return Err(GraphInventoryError::Io),
                Ok(read) => read,
            };
            if read == 0 {
                break;
            }
            byte_length = byte_length
                .checked_add(read as u64)
                .ok_or(GraphInventoryError::Limit)?;
            if byte_length > limits.max_total_bytes {
                return Err(GraphInventoryError::Limit);
            }
            hasher.update(&buffer[..read]);
        }
        if byte_length == 0 {
            return Err(GraphInventoryError::Limit);
        }
        let hash = cid::multihash::Multihash::<64>::wrap(crate::SHA2_256_CODE, &hasher.finalize())
            .map_err(|_| GraphInventoryError::InvalidCid)?;
        Ok(Self {
            path,
            cid: Cid::new_v1(RAW_CODEC, hash),
            byte_length,
            kind,
        })
    }
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
    #[must_use]
    pub const fn cid(&self) -> &Cid {
        &self.cid
    }
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }
    #[must_use]
    pub const fn kind(&self) -> GraphFileKind {
        self.kind
    }
    /// Check borrowed bytes without copying or interpreting the file payload.
    /// # Errors
    /// Refuses truncated, oversized or changed bytes.
    pub fn verify(&self, bytes: &[u8]) -> Result<(), GraphInventoryError> {
        if bytes.len() as u64 != self.byte_length || raw_cid(bytes) != self.cid {
            return Err(GraphInventoryError::Integrity);
        }
        Ok(())
    }
}
/// Encoded and declared resource bounds; these do not bound native engine RSS.
#[derive(Clone, Copy, Debug)]
pub struct GraphInventoryLimits {
    pub max_files: usize,
    pub max_path_bytes: usize,
    pub max_total_bytes: u64,
    pub max_manifest_bytes: usize,
}
impl Default for GraphInventoryLimits {
    fn default() -> Self {
        Self {
            max_files: 4096,
            max_path_bytes: 1024,
            max_total_bytes: 1 << 30,
            max_manifest_bytes: 1 << 20,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphInventoryError {
    Configuration,
    Io,
    Limit,
    InvalidPath,
    DuplicatePath,
    MissingEntry,
    InvalidCid,
    Integrity,
    Encode,
    Decode,
    NonCanonical,
    UnsupportedVersion,
}
impl std::fmt::Display for GraphInventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "graph inventory: {self:?}")
    }
}
impl std::error::Error for GraphInventoryError {}
/// Canonical standalone inventory. It does not change existing V1 snapshot bindings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDatasetInventory {
    namespace: String,
    version: u64,
    entry: String,
    files: Vec<GraphFile>,
}
const NAMESPACE: &str = GRAPHAR_FILE_INVENTORY_SCHEMA.namespace;
impl GraphDatasetInventory {
    /// Sort a complete caller-declared inventory by portable logical path.
    /// # Errors
    /// Refuses unsafe/duplicate paths, absent metadata entry, invalid CIDs or budgets.
    pub fn admit(
        entry: String,
        mut files: Vec<GraphFile>,
        limits: GraphInventoryLimits,
    ) -> Result<Self, GraphInventoryError> {
        validate_limits(limits)?;
        if files.is_empty() || files.len() > limits.max_files {
            return Err(GraphInventoryError::Limit);
        }
        // Bound comparisons before sorting caller-owned strings.
        validate_path(&entry, limits.max_path_bytes)?;
        for file in &files {
            validate_path(&file.path, limits.max_path_bytes)?;
        }
        files.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        let value = Self {
            namespace: NAMESPACE.into(),
            version: GRAPHAR_FILE_INVENTORY_SCHEMA.version,
            entry,
            files,
        };
        value.validate(limits)?;
        value.canonical_bytes(limits)?;
        Ok(value)
    }
    #[must_use]
    pub fn entry(&self) -> &str {
        &self.entry
    }
    #[must_use]
    pub fn files(&self) -> &[GraphFile] {
        &self.files
    }
    /// Encode this admitted inventory; encoding does not publish or seal files.
    /// # Errors
    /// Refuses invalid limits or encoded-byte overflow.
    pub fn canonical_bytes(
        &self,
        limits: GraphInventoryLimits,
    ) -> Result<Vec<u8>, GraphInventoryError> {
        self.validate(limits)?;
        let bytes = serde_ipld_dagcbor::to_vec(self).map_err(|_| GraphInventoryError::Encode)?;
        if bytes.len() > limits.max_manifest_bytes {
            return Err(GraphInventoryError::Limit);
        }
        Ok(bytes)
    }
    /// Verify the root before decoding, then require exact canonical bytes/order.
    /// # Errors
    /// Refuses oversized/untrusted roots, malformed/unknown fields, unsafe children or drift.
    pub fn decode_checked(
        bytes: &[u8],
        root: &Cid,
        limits: GraphInventoryLimits,
    ) -> Result<Self, GraphInventoryError> {
        validate_limits(limits)?;
        if bytes.len() > limits.max_manifest_bytes {
            return Err(GraphInventoryError::Limit);
        }
        if validate_cid(root, crate::DAG_CBOR_CODEC).is_err() || dag_cbor_cid(bytes) != *root {
            return Err(GraphInventoryError::Integrity);
        }
        let value: Self =
            serde_ipld_dagcbor::from_slice(bytes).map_err(|_| GraphInventoryError::Decode)?;
        value.validate(limits)?;
        if value.canonical_bytes(limits)? != bytes {
            return Err(GraphInventoryError::NonCanonical);
        }
        Ok(value)
    }
    fn validate(&self, limits: GraphInventoryLimits) -> Result<(), GraphInventoryError> {
        validate_limits(limits)?;
        if !GRAPHAR_FILE_INVENTORY_SCHEMA.accepts(&self.namespace, self.version) {
            return Err(GraphInventoryError::UnsupportedVersion);
        }
        if self.files.is_empty() || self.files.len() > limits.max_files {
            return Err(GraphInventoryError::Limit);
        }
        validate_path(&self.entry, limits.max_path_bytes)?;
        let mut total = 0u64;
        let mut previous: Option<&str> = None;
        let mut entry_found = false;
        for file in &self.files {
            validate_path(&file.path, limits.max_path_bytes)?;
            if previous == Some(file.path.as_str()) {
                return Err(GraphInventoryError::DuplicatePath);
            }
            if previous.is_some_and(|p| p > file.path.as_str()) {
                return Err(GraphInventoryError::NonCanonical);
            }
            previous = Some(&file.path);
            if validate_cid(&file.cid, RAW_CODEC).is_err() {
                return Err(GraphInventoryError::InvalidCid);
            }
            total = total
                .checked_add(file.byte_length)
                .ok_or(GraphInventoryError::Limit)?;
            if file.byte_length == 0 || total > limits.max_total_bytes {
                return Err(GraphInventoryError::Limit);
            }
            entry_found |= file.path == self.entry && file.kind == GraphFileKind::Metadata;
        }
        if !entry_found {
            return Err(GraphInventoryError::MissingEntry);
        }
        Ok(())
    }
}
fn validate_limits(limits: GraphInventoryLimits) -> Result<(), GraphInventoryError> {
    if limits.max_files == 0
        || limits.max_path_bytes == 0
        || limits.max_total_bytes == 0
        || limits.max_manifest_bytes == 0
    {
        return Err(GraphInventoryError::Configuration);
    }
    Ok(())
}
fn validate_path(path: &str, max_bytes: usize) -> Result<(), GraphInventoryError> {
    if path.is_empty() || path.len() > max_bytes {
        return Err(GraphInventoryError::InvalidPath);
    }
    for part in path.split('/') {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.len() > 255
            || part.ends_with('.')
            || !part
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
        {
            return Err(GraphInventoryError::InvalidPath);
        }
        let stem = part
            .split('.')
            .next()
            .ok_or(GraphInventoryError::InvalidPath)?;
        if ["con", "prn", "aux", "nul"].contains(&stem)
            || ((stem.starts_with("com") || stem.starts_with("lpt"))
                && stem.len() == 4
                && (b'1'..=b'9').contains(&stem.as_bytes()[3]))
        {
            return Err(GraphInventoryError::InvalidPath);
        }
    }
    Ok(())
}
