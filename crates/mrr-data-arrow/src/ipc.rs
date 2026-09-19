//! Resource admission for untrusted Arrow IPC before batch materialization.

use arrow_ipc::{Footer, root_as_footer_with_opts, root_as_message_with_opts};
use flatbuffers::VerifierOptions;

use crate::error::ArrowRelationError;

const ARROW_MAGIC: &[u8; 6] = b"ARROW1";
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const FOOTER_TRAILER_BYTES: usize = 10;

/// Caller-owned limits for decoding untrusted Arrow IPC fact batches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpcImportLimits {
    pub(super) bytes: usize,
    pub(super) decoded_bytes: usize,
    pub(super) rows: usize,
    pub(super) columns: usize,
    pub(super) values: usize,
    pub(super) nesting_depth: usize,
}

impl IpcImportLimits {
    #[must_use]
    pub const fn new(max_bytes: usize, max_rows: usize, max_columns: usize) -> Self {
        Self {
            bytes: max_bytes,
            decoded_bytes: max_bytes.saturating_mul(64),
            rows: max_rows,
            columns: max_columns,
            values: max_rows.saturating_mul(max_columns).saturating_mul(16),
            nesting_depth: 16,
        }
    }

    /// Sets the maximum uncompressed IPC body and final Arrow array bytes.
    #[must_use]
    pub const fn with_decoded_bytes(mut self, max_decoded_bytes: usize) -> Self {
        self.decoded_bytes = max_decoded_bytes;
        self
    }

    /// Sets the maximum sum of recursive Arrow field-node lengths.
    #[must_use]
    pub const fn with_values(mut self, max_values: usize) -> Self {
        self.values = max_values;
        self
    }

    /// Sets the maximum recursive Arrow schema depth.
    #[must_use]
    pub const fn with_nesting_depth(mut self, max_nesting_depth: usize) -> Self {
        self.nesting_depth = max_nesting_depth;
        self
    }
}

pub(super) fn check_limit(
    resource: &'static str,
    limit: usize,
    actual: usize,
) -> Result<(), ArrowRelationError> {
    if actual > limit {
        Err(ArrowRelationError::ImportLimitExceeded {
            resource,
            limit,
            actual,
        })
    } else {
        Ok(())
    }
}

fn malformed<T>() -> Result<T, ArrowRelationError> {
    Err(ArrowRelationError::MalformedIpc)
}

fn checked_usize(value: i64) -> Result<usize, ArrowRelationError> {
    usize::try_from(value).map_err(|_| ArrowRelationError::MalformedIpc)
}

fn verifier_options(limits: IpcImportLimits) -> VerifierOptions {
    VerifierOptions {
        max_tables: limits.columns.saturating_mul(4).saturating_add(32),
        max_depth: limits.nesting_depth.saturating_add(8),
        ..VerifierOptions::default()
    }
}

fn parse_footer<'a>(
    bytes: &'a [u8],
    options: &VerifierOptions,
) -> Result<(Footer<'a>, usize), ArrowRelationError> {
    if bytes.len() < FOOTER_TRAILER_BYTES
        || bytes.get(..ARROW_MAGIC.len()) != Some(ARROW_MAGIC)
        || bytes.get(bytes.len() - ARROW_MAGIC.len()..) != Some(ARROW_MAGIC)
    {
        return malformed();
    }
    let footer_length_offset = bytes.len() - FOOTER_TRAILER_BYTES;
    let footer_length = u32::from_le_bytes(
        bytes[footer_length_offset..footer_length_offset + 4]
            .try_into()
            .map_err(|_| ArrowRelationError::MalformedIpc)?,
    ) as usize;
    let footer_start = footer_length_offset
        .checked_sub(footer_length)
        .ok_or(ArrowRelationError::MalformedIpc)?;
    let footer = root_as_footer_with_opts(options, &bytes[footer_start..footer_length_offset])
        .map_err(|_| ArrowRelationError::MalformedIpc)?;
    Ok((footer, footer_start))
}

fn message_payload(metadata: &[u8]) -> Result<&[u8], ArrowRelationError> {
    let (length_offset, payload_offset) = match metadata.get(..4) {
        Some(marker) if marker == CONTINUATION_MARKER => (4_usize, 8_usize),
        Some(_) => (0_usize, 4_usize),
        None => return malformed(),
    };
    let message_length = u32::from_le_bytes(
        metadata
            .get(length_offset..length_offset + 4)
            .ok_or(ArrowRelationError::MalformedIpc)?
            .try_into()
            .map_err(|_| ArrowRelationError::MalformedIpc)?,
    ) as usize;
    let message_end = payload_offset
        .checked_add(message_length)
        .ok_or(ArrowRelationError::MalformedIpc)?;
    metadata
        .get(payload_offset..message_end)
        .ok_or(ArrowRelationError::MalformedIpc)
}

pub(super) fn preflight_ipc(
    bytes: &[u8],
    limits: IpcImportLimits,
) -> Result<(), ArrowRelationError> {
    check_limit("bytes", limits.bytes, bytes.len())?;
    let options = verifier_options(limits);
    let (footer, footer_start) = parse_footer(bytes, &options)?;
    if footer
        .dictionaries()
        .is_some_and(|blocks| !blocks.is_empty())
    {
        return Err(ArrowRelationError::UnsupportedIpcFeature(
            "dictionary batches",
        ));
    }
    let blocks = footer
        .recordBatches()
        .ok_or(ArrowRelationError::MalformedIpc)?;
    if blocks.len() != 1 {
        return Err(ArrowRelationError::UnexpectedBatchCount(blocks.len()));
    }
    let block = blocks.get(0);
    let offset = checked_usize(block.offset())?;
    let metadata_length =
        usize::try_from(block.metaDataLength()).map_err(|_| ArrowRelationError::MalformedIpc)?;
    let body_length = checked_usize(block.bodyLength())?;
    check_limit("decoded-bytes", limits.decoded_bytes, body_length)?;
    let metadata_end = offset
        .checked_add(metadata_length)
        .ok_or(ArrowRelationError::MalformedIpc)?;
    let block_end = metadata_end
        .checked_add(body_length)
        .ok_or(ArrowRelationError::MalformedIpc)?;
    if block_end > footer_start {
        return malformed();
    }
    let metadata = bytes
        .get(offset..metadata_end)
        .ok_or(ArrowRelationError::MalformedIpc)?;
    let message = root_as_message_with_opts(&options, message_payload(metadata)?)
        .map_err(|_| ArrowRelationError::MalformedIpc)?;
    if checked_usize(message.bodyLength())? != body_length {
        return malformed();
    }
    let batch = message
        .header_as_record_batch()
        .ok_or(ArrowRelationError::MalformedIpc)?;
    if batch.compression().is_some() {
        return Err(ArrowRelationError::UnsupportedIpcFeature(
            "compressed record batches",
        ));
    }
    check_limit("rows", limits.rows, checked_usize(batch.length())?)?;
    let nodes = batch.nodes().ok_or(ArrowRelationError::MalformedIpc)?;
    let values = nodes.iter().try_fold(0_usize, |count, node| {
        count
            .checked_add(checked_usize(node.length())?)
            .ok_or(ArrowRelationError::MalformedIpc)
    })?;
    check_limit("values", limits.values, values)?;
    for buffer in batch.buffers().ok_or(ArrowRelationError::MalformedIpc)? {
        let end = checked_usize(buffer.offset())?
            .checked_add(checked_usize(buffer.length())?)
            .ok_or(ArrowRelationError::MalformedIpc)?;
        if end > body_length {
            return malformed();
        }
    }
    Ok(())
}
