//! Fallible native Arrow results over the safe pinned SDK.
use crate::{ArrowQueryEmitter, ArrowQueryError};
use arrow_array::RecordBatch;
use duckdb::{Params, Statement};
/// Execute a Host-prepared physical statement on a Backend query worker.
/// The SDK materializes the native result. Its authoritative row count makes
/// early EOF a refusal instead of silently accepting truncated output.
/// This is not the SDK's streaming result path and does not bound native RSS.
/// Native query memory/spill and input ownership belong to the Host reservation.
/// # Errors
/// Refuses execution/schema/size failures, early fetch termination, empty progress,
/// excess rows and output cancellation. No partial result becomes complete.
pub fn emit_duckdb_arrow(
    statement: &mut Statement<'_>,
    params: impl Params,
    output: &mut ArrowQueryEmitter,
) -> Result<(), ArrowQueryError> {
    let result = emit_materialized(statement, params, output);
    if let Err(error) = result {
        output.fail(error);
    }
    result
}
fn emit_materialized(
    statement: &mut Statement<'_>,
    params: impl Params,
    output: &mut ArrowQueryEmitter,
) -> Result<(), ArrowQueryError> {
    let schema = statement
        .query_arrow(params)
        .map_err(|_| ArrowQueryError::Driver)?
        .get_schema();
    if &schema != output.schema() {
        return Err(ArrowQueryError::Schema);
    }
    let expected = statement.row_count();
    output.check_rows(expected)?;
    let mut rows = 0;
    while rows < expected {
        output.emit(|| {
            let batch = RecordBatch::from(
                &statement
                    .step()
                    .map_err(|_| ArrowQueryError::Driver)?
                    .ok_or(ArrowQueryError::Incomplete)?,
            );
            if batch.num_rows() == 0 {
                return Err(ArrowQueryError::Incomplete);
            }
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or(ArrowQueryError::Limit)?;
            if rows > expected {
                return Err(ArrowQueryError::Incomplete);
            }
            Ok(batch)
        })?;
    }
    Ok(())
}

/// Execute a Host-prepared statement using native streaming result execution.
/// Uses the executed result schema and fallible SDK `step`, never its panicking
/// Arrow iterator. Every fetch, including EOF, runs under the output byte budget.
/// Some native plans still materialize internally; the Host owns memory/spill
/// budgets and must register a native interrupt before calling this driver.
/// # Errors
/// Execution, schema, fetch/conversion errors and output refusals remain sticky.
/// Partial output never obtains a successful terminal report after a refusal.
pub fn emit_duckdb_arrow_stream(
    statement: &mut Statement<'_>,
    params: impl Params,
    output: &mut ArrowQueryEmitter,
) -> Result<(), ArrowQueryError> {
    let result = emit_stream(statement, params, output);
    if let Err(error) = result {
        output.fail(error);
    }
    result
}
fn emit_stream(
    statement: &mut Statement<'_>,
    params: impl Params,
    output: &mut ArrowQueryEmitter,
) -> Result<(), ArrowQueryError> {
    let schema = statement
        .stream_arrow(params)
        .map_err(|_| ArrowQueryError::Driver)?
        .get_schema();
    if &schema != output.schema() {
        return Err(ArrowQueryError::Schema);
    }
    while output.emit_next(|| {
        statement
            .step()
            .map(|array| array.map(|array| RecordBatch::from(&array)))
            .map_err(|_| ArrowQueryError::Driver)
    })? {}
    Ok(())
}
