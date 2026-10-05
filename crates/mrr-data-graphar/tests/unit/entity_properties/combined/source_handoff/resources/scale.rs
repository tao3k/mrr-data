//! Caller-declared workload and limits, independent of production defaults.
use crate::tests::entity_properties::{acceptance, combined::fixture, fixture as properties};
use crate::{CombinedGraphArLimits, GraphArChunkLayout, GraphArReadLimits};
use mrr_data_content::GraphTransferLimits;
use mrr_data_datafusion::PropertyQueryLimits;
use serde_json::{Value, json};
use std::num::NonZeroUsize;

const RESULT_ROWS: usize = 100;
const RESULT_CELLS: usize = 300;

pub(super) fn result_limits() -> meta_relational_reasoning::QueryResultLimits {
    meta_relational_reasoning::QueryResultLimits::new(
        NonZeroUsize::new(RESULT_ROWS).unwrap(),
        NonZeroUsize::new(RESULT_CELLS).unwrap(),
    )
}

pub(super) fn rows() -> usize {
    let rows = std::env::var("MRR_DATA_SOURCE_SCALE")
        .unwrap_or_else(|_| "4".into())
        .parse::<usize>()
        .unwrap();
    assert!(
        super::schema()["properties"]["scales"]["const"]
            .as_array()
            .unwrap()
            .contains(&json!(rows))
    );
    rows
}
pub(super) fn layout(rows: usize) -> GraphArChunkLayout {
    let chunk = if rows == 4 { 2 } else { 256 };
    GraphArChunkLayout::new(chunk, chunk).unwrap()
}
pub(super) fn capture(rows: usize) -> CombinedGraphArLimits {
    if rows == 4 {
        return fixture::capture_limits();
    }
    let mut properties = acceptance::limits();
    properties.max_rows = 3 * rows + 32;
    properties.max_value_bytes = 16 << 20;
    CombinedGraphArLimits {
        dataset: mrr_data_core::GraphDatasetLimits {
            inventory: properties.inventory,
            max_relations: 8,
            max_property_rows: properties.max_rows,
        },
        properties,
        topology: GraphArReadLimits::new(4 * rows + 32, 2 * rows + 32),
    }
}
pub(super) fn transfer(rows: usize) -> GraphTransferLimits {
    if rows == 4 {
        return fixture::transfer_limits();
    }
    GraphTransferLimits {
        max_blocks: 8192,
        max_block_bytes: 4 << 20,
        max_total_bytes: 128 << 20,
    }
}
pub(super) fn physical(rows: usize) -> PropertyQueryLimits {
    if rows == 4 {
        return properties::limits();
    }
    let potential_join_rows = (rows + 8).checked_mul(rows + 8).unwrap();
    PropertyQueryLimits {
        max_input_rows: 5 * rows + 32,
        max_input_bytes: 64 << 20,
        max_join_rows: potential_join_rows,
        // DataFusion checks this bound before the Healthcare filter. MRR's
        // admitted result keeps its independent 100-row / 300-cell limit.
        max_output_cells: potential_join_rows.checked_mul(3).unwrap(),
        execution_memory_bytes: 128 << 20,
    }
}
pub(super) fn receipt(rows: usize) -> Value {
    let capture = capture(rows);
    let transfer = transfer(rows);
    let physical = physical(rows);
    json!({
        "property_rows": capture.properties.max_rows,
        "property_value_bytes": capture.properties.max_value_bytes,
        "topology_vertices": capture.topology.max_vertices(),
        "topology_edges": capture.topology.max_edges(),
        "transfer_blocks": transfer.max_blocks,
        "transfer_block_bytes": transfer.max_block_bytes,
        "transfer_total_bytes": transfer.max_total_bytes,
        "input_rows": physical.max_input_rows,
        "input_bytes": physical.max_input_bytes,
        "join_rows_before_filters": physical.max_join_rows,
        "physical_output_cells_bound": physical.max_output_cells,
        "admitted_result_rows": RESULT_ROWS,
        "admitted_result_cells": RESULT_CELLS,
        "execution_memory_bytes": physical.execution_memory_bytes,
        "resource_handle_reserved_bytes": super::RESERVED,
        "chunk_vertices": layout(rows).vertex_chunk_size(),
        "chunk_edges": layout(rows).edge_chunk_size(),
    })
}
