//! Bounded Arrow input slices for native appending and conversion qualification.
use crate::BackendError;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use std::sync::OnceLock;

/// Native virtual-table input with checked row, buffer and scalar-type bounds.
/// This carrier grants no semantic admission, query authority or Backend lease.
#[derive(Clone, Debug)]
pub struct DuckDbArrowInput {
    batch: RecordBatch,
}
impl DuckDbArrowInput {
    /// Read the actual native vector capacity through the SDK's safe API.
    #[must_use]
    pub fn row_capacity() -> usize {
        static CAPACITY: OnceLock<usize> = OnceLock::new();
        *CAPACITY.get_or_init(|| {
            let chunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
            chunk.flat_vector(0).capacity()
        })
    }
    /// Validate before passing an input across the SDK's FFI callback boundary.
    /// Buffer accounting conservatively includes buffers retained by slices.
    ///
    /// # Errors
    /// Refuses zero/over-budget limits, oversized input, empty/duplicate field
    /// names and types outside the qualified scalar subset. Splitting batches
    /// and bounding aggregate retained buffers remain the caller's responsibility.
    pub fn admit(
        batch: RecordBatch,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, BackendError> {
        if batch.num_rows() > batch_rows(max_rows)? {
            return Err(BackendError::Limit);
        }
        validate_batch(&batch, max_bytes)?;
        Ok(Self { batch })
    }
    /// Validate one retained source, then lazily yield slices sharing its buffers.
    /// Checks schema and full retained source bytes once, before any slice escapes.
    /// This avoids a materialized vector of batches and repeated schema checks.
    ///
    /// # Errors
    /// Uses the same configuration, buffer and scalar checks as [`Self::admit`].
    /// `max_rows` bounds each yielded batch; `max_bytes` bounds the retained source.
    pub fn batches(
        batch: RecordBatch,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<impl ExactSizeIterator<Item = Self>, BackendError> {
        let rows = batch_rows(max_rows)?;
        validate_batch(&batch, max_bytes)?;
        Ok((0..batch.num_rows()).step_by(rows).map(move |offset| Self {
            batch: batch.slice(offset, rows.min(batch.num_rows() - offset)),
        }))
    }
    /// Consume the validated batch for one native registration.
    #[must_use]
    pub fn into_batch(self) -> RecordBatch {
        self.batch
    }
    /// Copy this admitted slice through a Host-owned native appender.
    /// This avoids the SDK `VTab` helper's process-lifetime source registry.
    /// The Host owns transaction/flush and engine-memory budgets and keeps its
    /// appender/connection on the Backend worker until native work completes.
    /// # Errors
    /// Refuses native conversion/appending errors without exposing SDK details.
    pub fn append_to(self, appender: &mut duckdb::Appender<'_>) -> Result<(), BackendError> {
        appender
            .append_record_batch(self.batch)
            .map_err(|_| BackendError::Unavailable)
    }
}

fn batch_rows(max_rows: usize) -> Result<usize, BackendError> {
    if max_rows == 0 {
        return Err(BackendError::InvalidConfiguration);
    }
    let rows = max_rows.min(DuckDbArrowInput::row_capacity());
    if rows == 0 {
        return Err(BackendError::Unavailable);
    }
    Ok(rows)
}
fn validate_batch(batch: &RecordBatch, max_bytes: usize) -> Result<(), BackendError> {
    if max_bytes == 0 {
        return Err(BackendError::InvalidConfiguration);
    }
    if batch.num_columns() == 0 || batch.get_array_memory_size() > max_bytes {
        return Err(BackendError::Limit);
    }
    let schema = batch.schema();
    let mut names = std::collections::BTreeSet::new();
    for field in schema.fields() {
        if field.name().is_empty() || !names.insert(field.name()) {
            return Err(BackendError::Corrupt);
        }
        if !matches!(
            field.data_type(),
            DataType::Int64 | DataType::Utf8 | DataType::Binary
        ) {
            return Err(BackendError::UnsupportedCapabilities);
        }
    }
    Ok(())
}
