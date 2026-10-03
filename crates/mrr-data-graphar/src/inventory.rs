//! Bounded directory receipts for the controlled native writer, without filesystem sealing.
use crate::query_source::GRAPH_INFO_FILE;
use mrr_data_core::{
    GraphDatasetInventory, GraphFile, GraphFileKind, GraphInventoryError, GraphInventoryLimits,
};
use std::{error::Error, fmt, fs, io, path::Path};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphArInventoryError {
    Inventory(GraphInventoryError),
    Io(io::ErrorKind),
    UnsupportedEntry,
    UnsupportedFile,
    Integrity,
}
impl From<GraphInventoryError> for GraphArInventoryError {
    fn from(value: GraphInventoryError) -> Self {
        Self::Inventory(value)
    }
}
impl fmt::Display for GraphArInventoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "graph directory inventory: {self:?}")
    }
}
impl Error for GraphArInventoryError {}
impl From<io::Error> for GraphArInventoryError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}

/// Enumerate every physical file in a caller-owned directory using streaming hashes.
///
/// The initial profile accepts YAML metadata, native count files and Parquet chunks.
/// Traversal visits at most twice `max_files` entries (including directories).
/// Rejects symlink entries and, on Unix, hard-linked files. This is an observation, not a
/// filesystem seal: concurrent replacement remains a separate admission gate.
/// # Errors
/// Refuses unsupported entries, paths/codecs, I/O failures or inventory budgets.
pub fn inventory_graphar_directory(
    root: &Path,
    limits: GraphInventoryLimits,
) -> Result<GraphDatasetInventory, GraphArInventoryError> {
    let max_entries = validate_inventory_limits(limits)?;
    let root_info = fs::symlink_metadata(root).map_err(GraphArInventoryError::from)?;
    if !root_info.is_dir() || root_info.file_type().is_symlink() {
        return Err(GraphArInventoryError::UnsupportedEntry);
    }
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    let mut entries = 0usize;
    let mut total = 0u64;
    while let Some(directory) = pending.pop() {
        for item in fs::read_dir(directory).map_err(GraphArInventoryError::from)? {
            entries += 1;
            if entries > max_entries {
                return Err(GraphInventoryError::Limit.into());
            }
            let item = item.map_err(GraphArInventoryError::from)?;
            let path = item.path();
            let logical = path
                .strip_prefix(root)
                .map_err(|_| GraphArInventoryError::UnsupportedEntry)?
                .to_str()
                .ok_or(GraphInventoryError::InvalidPath)?
                .replace(std::path::MAIN_SEPARATOR, "/");
            if logical.len() > limits.max_path_bytes {
                return Err(GraphInventoryError::InvalidPath.into());
            }
            let metadata = fs::symlink_metadata(&path).map_err(GraphArInventoryError::from)?;
            if metadata.file_type().is_symlink() {
                return Err(GraphArInventoryError::UnsupportedEntry);
            }
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                return Err(GraphArInventoryError::UnsupportedEntry);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(GraphArInventoryError::UnsupportedEntry);
                }
            }
            if files.len() >= limits.max_files
                || metadata.len() == 0
                || metadata.len() > limits.max_total_bytes - total
            {
                return Err(GraphInventoryError::Limit.into());
            }
            let kind = file_kind(&logical)?;
            let mut file = fs::File::open(path).map_err(GraphArInventoryError::from)?;
            check_framing(&mut file, kind, metadata.len())?;
            let descriptor = GraphFile::from_reader(
                logical,
                file,
                kind,
                GraphInventoryLimits {
                    max_total_bytes: limits.max_total_bytes - total,
                    ..limits
                },
            )?;
            total += descriptor.byte_length();
            files.push(descriptor);
        }
    }
    Ok(GraphDatasetInventory::admit(
        GRAPH_INFO_FILE.into(),
        files,
        limits,
    )?)
}

/// Re-enumerate and compare exact paths, types, lengths and content identities.
/// # Errors
/// Refuses missing, changed or extra files, unsupported entries and budget overflow.
pub fn verify_graphar_directory(
    root: &Path,
    expected: &GraphDatasetInventory,
    limits: GraphInventoryLimits,
) -> Result<(), GraphArInventoryError> {
    expected.canonical_bytes(limits)?;
    if inventory_graphar_directory(root, limits)? != *expected {
        return Err(GraphArInventoryError::Integrity);
    }
    Ok(())
}
fn file_kind(path: &str) -> Result<GraphFileKind, GraphArInventoryError> {
    let name = path
        .rsplit('/')
        .next()
        .ok_or(GraphArInventoryError::UnsupportedFile)?;
    if Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml"))
    {
        return Ok(GraphFileKind::Metadata);
    }
    if name == "vertex_count"
        || name
            .strip_prefix("edge_count")
            .is_some_and(|s| s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Ok(GraphFileKind::Count);
    }
    let chunk = name.strip_suffix(".parquet").unwrap_or(name);
    if chunk
        .strip_prefix("chunk")
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Ok(GraphFileKind::Parquet);
    }
    Err(GraphArInventoryError::UnsupportedFile)
}

fn check_framing(
    file: &mut fs::File,
    kind: GraphFileKind,
    length: u64,
) -> Result<(), GraphArInventoryError> {
    use std::io::{Read, Seek, SeekFrom};
    if kind == GraphFileKind::Count && length != 8 {
        return Err(GraphArInventoryError::UnsupportedFile);
    }
    if kind == GraphFileKind::Parquet {
        if length < 12 {
            return Err(GraphArInventoryError::UnsupportedFile);
        }
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)
            .map_err(GraphArInventoryError::from)?;
        if &magic != b"PAR1" {
            return Err(GraphArInventoryError::UnsupportedFile);
        }
        file.seek(SeekFrom::End(-8))
            .map_err(GraphArInventoryError::from)?;
        let mut footer = [0u8; 8];
        file.read_exact(&mut footer)
            .map_err(GraphArInventoryError::from)?;
        let metadata_length = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]);
        if &footer[4..] != b"PAR1" || u64::from(metadata_length) > length - 12 {
            return Err(GraphArInventoryError::UnsupportedFile);
        }
        file.seek(SeekFrom::Start(0))
            .map_err(GraphArInventoryError::from)?;
    }
    Ok(())
}

pub(crate) fn validate_inventory_limits(
    limits: GraphInventoryLimits,
) -> Result<usize, GraphArInventoryError> {
    if limits.max_files == 0
        || limits.max_path_bytes == 0
        || limits.max_total_bytes == 0
        || limits.max_manifest_bytes == 0
    {
        return Err(GraphInventoryError::Configuration.into());
    }
    limits
        .max_files
        .checked_mul(2)
        .ok_or(GraphInventoryError::Configuration.into())
}
