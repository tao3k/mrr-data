//! Measured Parquet ranges in private verified `GraphAr` files.
use super::GraphArSelectiveError as Error;
use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet::{
    arrow::arrow_reader::{
        ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
    },
    errors::{ParquetError, Result as ParquetResult},
    file::{
        metadata::PageIndexPolicy,
        reader::{ChunkReader, Length},
    },
};
use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
pub(super) struct ReadMeter {
    pub bytes: Arc<AtomicUsize>,
    pub paths: BTreeSet<String>,
    pub rows: usize,
}
struct CountedFile {
    file: Arc<File>,
    length: u64,
    bytes: Arc<AtomicUsize>,
}
struct CountedRead {
    file: Arc<File>,
    position: u64,
    bytes: Arc<AtomicUsize>,
}
impl Read for CountedRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(self.file.as_ref(), buffer, self.position)?;
        #[cfg(windows)]
        let read =
            std::os::windows::fs::FileExt::seek_read(self.file.as_ref(), buffer, self.position)?;
        #[cfg(not(any(unix, windows)))]
        let read = return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "positioned file reads",
        ));
        self.position += u64::try_from(read).unwrap_or(u64::MAX);
        self.bytes.fetch_add(read, Ordering::Relaxed);
        Ok(read)
    }
}
impl Length for CountedFile {
    fn len(&self) -> u64 {
        self.length
    }
}
impl ChunkReader for CountedFile {
    type T = CountedRead;
    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(CountedRead {
            file: self.file.clone(),
            position: start,
            bytes: self.bytes.clone(),
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(
                u64::try_from(length)
                    .map_err(|_| ParquetError::General("range overflow".into()))?,
            )
            .ok_or_else(|| ParquetError::General("range overflow".into()))?;
        if end > self.length {
            return Err(ParquetError::General("range exceeds file".into()));
        }
        let mut data = vec![0; length];
        self.get_read(start)?.read_exact(&mut data)?;
        Ok(data.into())
    }
}
/// Select rows in Parquet before materialization, not by filtering a full batch.
pub(super) fn read_range(
    file: Arc<File>,
    name: &str,
    fields: &[&str],
    expected_rows: usize,
    rows: Range<usize>,
    meter: &mut ReadMeter,
) -> Result<RecordBatch, Error> {
    read_ranges(file, name, fields, expected_rows, &[rows], meter)
}

pub(super) fn read_ranges(
    file: Arc<File>,
    name: &str,
    fields: &[&str],
    expected_rows: usize,
    ranges: &[Range<usize>],
    meter: &mut ReadMeter,
) -> Result<RecordBatch, Error> {
    let mut position = 0;
    let mut wanted = 0;
    let mut selectors = Vec::new();
    for range in ranges {
        if range.start < position || range.start >= range.end || range.end > expected_rows {
            return Err(Error::Layout);
        }
        selectors.push(RowSelector::skip(range.start - position));
        selectors.push(RowSelector::select(range.len()));
        wanted += range.len();
        position = range.end;
    }
    if wanted == 0 {
        return Err(Error::Layout);
    }
    selectors.push(RowSelector::skip(expected_rows - position));
    let reader = CountedFile {
        length: file.metadata()?.len(),
        file,
        bytes: meter.bytes.clone(),
    };
    meter.paths.insert(name.into());
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    )?;
    let schema = builder.schema().clone();
    if usize::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(expected_rows)
        || schema.fields().len() != fields.len()
        || schema
            .fields()
            .iter()
            .zip(fields)
            .any(|(field, name)| field.name() != name)
    {
        return Err(Error::Layout);
    }
    let selection: RowSelection = selectors.into();
    let batches = builder
        .with_row_selection(selection)
        .with_batch_size(wanted)
        .build()?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Error::Arrow(error.to_string()))?;
    let actual = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    if actual != wanted {
        return Err(Error::Layout);
    }
    meter.rows += actual;
    if batches.len() == 1 {
        return batches.into_iter().next().ok_or(Error::Layout);
    }
    arrow_select::concat::concat_batches(&schema, &batches).map_err(|e| Error::Arrow(e.to_string()))
}
