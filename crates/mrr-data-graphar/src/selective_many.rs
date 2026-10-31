//! Request-local coalescing; no retained payload cache or widened source scope.
use super::{
    BinaryEntityProjection, EntityId, GraphArSelection, GraphArSelectionMetrics,
    GraphArSelectiveError as Error, Instant, Ordering, PREFIX, PROPERTIES, Range, RecordBatch,
    SelectivePhysicalSnapshot, TOPOLOGY,
    io::{ReadMeter, read_ranges},
    join_batches, validate_topology,
};
use std::collections::{BTreeMap, BTreeSet};

struct Request {
    source: usize,
    range: Range<usize>,
    guard: Range<usize>,
}
type Chunks = BTreeMap<(usize, usize), Vec<Request>>;

impl SelectivePhysicalSnapshot {
    pub(crate) fn outgoing_many_checked(
        &self,
        projection: &BinaryEntityProjection,
        sources: &[EntityId],
        max_edges: usize,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<GraphArSelection, Error> {
        let started = Instant::now();
        check()?;
        let chunks = self.plan_many(sources, max_edges, &mut check)?;
        let mut meter = ReadMeter::default();
        let mut batches = Vec::new();
        for ((part, chunk), requests) in chunks {
            check()?;
            batches.extend(self.read_many_chunk(part, chunk, &requests, &mut meter, &mut check)?);
        }
        check()?;
        let facts = crate::reader::admit_selected_batches(&batches, &self.entities, projection)?;
        if facts
            .iter()
            .any(|fact| fact.context().generation() != self.generation)
        {
            return Err(Error::Scope);
        }
        check()?;
        let metrics = GraphArSelectionMetrics {
            read_bytes: meter.bytes.load(Ordering::Relaxed),
            files_read: meter.paths.len(),
            materialized_rows: meter.rows,
            selected_edges: facts.len(),
            elapsed: started.elapsed(),
        };
        Ok(GraphArSelection { facts, metrics })
    }

    fn plan_many(
        &self,
        sources: &[EntityId],
        max_edges: usize,
        check: &mut impl FnMut() -> Result<(), Error>,
    ) -> Result<Chunks, Error> {
        let mut seen = BTreeSet::new();
        let mut chunks = Chunks::new();
        let mut edges = 0usize;
        let size = self.layout.edge_chunk_size();
        for source in sources {
            check()?;
            let Some(&physical) = self.physical.get(source) else {
                continue;
            };
            if !seen.insert(physical) {
                continue;
            }
            let part = physical / self.layout.vertex_chunk_size();
            let index = physical % self.layout.vertex_chunk_size();
            let range = self.offsets[part][index]..self.offsets[part][index + 1];
            edges = edges.checked_add(range.len()).ok_or(Error::Limit)?;
            if edges > max_edges {
                return Err(Error::Limit);
            }
            let total = *self.offsets[part].last().ok_or(Error::Layout)?;
            let guard = range.start.saturating_sub(1)..range.end.saturating_add(1).min(total);
            if guard.is_empty() {
                continue;
            }
            for chunk in guard.start / size..=(guard.end - 1) / size {
                let base = chunk * size;
                chunks.entry((part, chunk)).or_default().push(Request {
                    source: physical,
                    range: range.clone(),
                    guard: guard.start.max(base)..guard.end.min(base + size),
                });
            }
        }
        Ok(chunks)
    }

    fn read_many_chunk(
        &self,
        part: usize,
        chunk: usize,
        requests: &[Request],
        meter: &mut ReadMeter,
        check: &mut impl FnMut() -> Result<(), Error>,
    ) -> Result<Vec<RecordBatch>, Error> {
        let size = self.layout.edge_chunk_size();
        let base = chunk * size;
        let rows = (self.offsets[part].last().ok_or(Error::Layout)? - base).min(size);
        let guards = union(
            requests
                .iter()
                .map(|r| (r.guard.start - base)..(r.guard.end - base)),
        );
        let topology_name = format!("{PREFIX}/adj_list/part{part}/chunk{chunk}");
        let topology = read_ranges(
            self.file(&topology_name)?,
            &topology_name,
            &TOPOLOGY,
            rows,
            &guards,
            meter,
        )?;
        let mut selected = Vec::new();
        for request in requests {
            let guard = (request.guard.start - base)..(request.guard.end - base);
            validate_topology(
                &slice(&topology, &guards, guard)?,
                request.guard.start,
                &request.range,
                request.source,
                self.entities.len(),
            )?;
            let begin = request.range.start.max(request.guard.start);
            let end = request.range.end.min(request.guard.end);
            if begin < end {
                selected.push((begin - base)..(end - base));
            }
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let ranges = union(selected.iter().cloned());
        check()?;
        let name = format!("{PREFIX}/properties/part{part}/chunk{chunk}");
        let properties = read_ranges(self.file(&name)?, &name, &PROPERTIES, rows, &ranges, meter)?;
        selected
            .into_iter()
            .map(|range| {
                join_batches(
                    &slice(&topology, &guards, range.clone())?,
                    &slice(&properties, &ranges, range)?,
                )
            })
            .collect()
    }
}

fn union(ranges: impl Iterator<Item = Range<usize>>) -> Vec<Range<usize>> {
    let mut ranges = ranges.collect::<Vec<_>>();
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<usize>> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut().filter(|last| range.start <= last.end) {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}
fn slice(
    batch: &RecordBatch,
    ranges: &[Range<usize>],
    wanted: Range<usize>,
) -> Result<RecordBatch, Error> {
    let mut offset = 0;
    for range in ranges {
        if range.start <= wanted.start && wanted.end <= range.end {
            return Ok(batch.slice(offset + wanted.start - range.start, wanted.len()));
        }
        offset += range.len();
    }
    Err(Error::Layout)
}
