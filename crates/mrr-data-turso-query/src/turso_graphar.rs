//! First Turso execution slice over one captured `GraphAr` binary-Entity source.
use crate::{SqlQueryError, TursoSingleHopSql};
use meta_relational_reasoning::{FactId, QueryResultValue};
use mrr_data_core::{BoundDataQuery, PhysicalQueryOutput};
use mrr_data_graphar::{BinaryEntityProjection, CapturedGraphArSnapshot};
#[cfg(feature = "backend-worker")]
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Timings of the safe SDK boundary; native calls may buffer before cursor fetch.
#[derive(Clone, Copy, Debug, Default)]
pub struct TursoExecutionTimings {
    pub program_compile: Duration,
    pub source_projection: Duration,
    pub connection_setup: Duration,
    pub derived_image_load: Duration,
    pub native_prepare: Duration,
    pub bind_and_execute: Duration,
    pub first_cursor_row_from_prepare: Option<Duration>,
    pub fetch_and_decode: Duration,
    pub connection_cleanup: Duration,
    pub total: Duration,
}

/// Preflight and result bounds. These do not constrain Turso native RSS.
#[derive(Clone, Copy, Debug)]
pub struct SqlQueryLimits {
    pub max_input_rows: usize,
    pub max_input_bytes: usize,
    pub max_output_rows: usize,
    pub max_output_cells: usize,
}
impl SqlQueryLimits {
    fn check(self) -> Result<(), SqlQueryError> {
        if self.max_input_rows == 0
            || self.max_input_bytes == 0
            || self.max_output_rows == 0
            || self.max_output_cells == 0
        {
            return Err(SqlQueryError::Limit("zero query budget"));
        }
        Ok(())
    }
}
pub(super) struct EdgeRow {
    pub(super) fact_id: String,
    pub(super) relation_id: String,
    pub(super) generation_id: String,
    pub(super) source_entity: String,
    pub(super) target_entity: String,
}
fn prepare_rows(
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: SqlQueryLimits,
    checkpoint: &impl Fn() -> Result<(), SqlQueryError>,
) -> Result<Vec<EdgeRow>, SqlQueryError> {
    limits.check()?;
    if source.binding().relation() != projection.relation_id() {
        return Err(SqlQueryError::SourceMismatch);
    }
    let facts = source
        .facts(query)
        .map_err(|_| SqlQueryError::SourceMismatch)?;
    if facts.len() > limits.max_input_rows {
        return Err(SqlQueryError::Limit("input rows"));
    }
    let mut bytes = 0usize;
    let mut rows = Vec::with_capacity(facts.len());
    let mut previous: Option<FactId> = None;
    for fact in facts {
        checkpoint()?;
        let edge = projection
            .project(fact)
            .map_err(|_| SqlQueryError::SourceMismatch)?;
        if edge.generation_id() != query.query().generation()
            || previous.is_some_and(|last| last >= edge.fact_id())
        {
            return Err(SqlQueryError::SourceMismatch);
        }
        previous = Some(edge.fact_id());
        let row = EdgeRow {
            fact_id: edge.fact_id().to_string(),
            relation_id: edge.relation_id().to_string(),
            generation_id: edge.generation_id().to_string(),
            source_entity: edge.source().to_string(),
            target_entity: edge.destination().to_string(),
        };
        bytes = [
            &row.fact_id,
            &row.relation_id,
            &row.generation_id,
            &row.source_entity,
            &row.target_entity,
        ]
        .into_iter()
        .try_fold(bytes, |total, value| total.checked_add(value.len()))
        .ok_or(SqlQueryError::Limit("input bytes"))?;
        if bytes > limits.max_input_bytes {
            return Err(SqlQueryError::Limit("input bytes"));
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Execute a bounded single-hop query with a new connection from a Host-owned
/// Turso database. The temporary image is rolled back on completion; dropping
/// a cancelled invocation drops its dedicated connection. A Backend query
/// worker, deadline and native memory reservation remain Host responsibilities.
/// This returns physical output only; callers still project and admit with MRR.
/// # Errors
/// Rejects unsupported queries, source drift, limits, driver or output failures.
pub async fn execute_turso_graphar_single_hop(
    database: &turso::Database,
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: SqlQueryLimits,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    execute_checked(database, query, source, projection, limits, &|| Ok(())).await
}
async fn execute_checked(
    database: &turso::Database,
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: SqlQueryLimits,
    checkpoint: &impl Fn() -> Result<(), SqlQueryError>,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    execute_observed(database, query, source, projection, limits, checkpoint)
        .await
        .map(|(output, _)| output)
}

/// Execute the same checked transaction and return measured safe-SDK phases.
/// # Errors
/// Uses the same query, source, result and cleanup refusals as normal execution.
pub async fn execute_turso_graphar_single_hop_observed(
    database: &turso::Database,
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: SqlQueryLimits,
) -> Result<(PhysicalQueryOutput, TursoExecutionTimings), SqlQueryError> {
    execute_observed(database, query, source, projection, limits, &|| Ok(())).await
}
async fn execute_observed(
    database: &turso::Database,
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: SqlQueryLimits,
    checkpoint: &impl Fn() -> Result<(), SqlQueryError>,
) -> Result<(PhysicalQueryOutput, TursoExecutionTimings), SqlQueryError> {
    let total = Instant::now();
    let mut timings = TursoExecutionTimings::default();
    checkpoint()?;
    let started = Instant::now();
    let plan = TursoSingleHopSql::compile(query, projection)?;
    timings.program_compile = started.elapsed();
    let started = Instant::now();
    let rows = prepare_rows(query, source, projection, limits, checkpoint)?;
    timings.source_projection = started.elapsed();
    checkpoint()?;
    let started = Instant::now();
    let connection = database.connect().map_err(|_| SqlQueryError::Native)?;
    connection
        .busy_timeout(Duration::from_millis(250))
        .map_err(|_| SqlQueryError::Native)?;
    connection
        .execute("BEGIN", ())
        .await
        .map_err(|_| SqlQueryError::Native)?;
    timings.connection_setup = started.elapsed();
    let result =
        execute_transaction_observed(&connection, &plan, rows, limits, checkpoint, &mut timings)
            .await;
    let started = Instant::now();
    let output = finish_transaction(&connection, result).await?;
    drop(connection);
    timings.connection_cleanup = started.elapsed();
    checkpoint()?;
    timings.total = total.elapsed();
    Ok((output, timings))
}
pub(super) async fn finish_transaction<T>(
    connection: &turso::Connection,
    result: Result<T, SqlQueryError>,
) -> Result<T, SqlQueryError> {
    connection
        .execute("ROLLBACK", ())
        .await
        .map_err(|_| SqlQueryError::Cleanup)?;
    result
}

/// Inputs owned by a Backend worker while a bounded Turso query runs.
/// The Host supplies a database and captured, authenticated source. The byte
/// reservation is a Host estimate of native work, not a measured RSS ceiling.
#[cfg(feature = "backend-worker")]
pub struct TursoBackendQuery {
    pub database: Arc<turso::Database>,
    pub query: BoundDataQuery,
    pub source: Arc<CapturedGraphArSnapshot>,
    pub projection: BinaryEntityProjection,
    pub limits: SqlQueryLimits,
    pub reserved_bytes: usize,
}

/// Run the first Turso slice on Backend's bounded resource worker lane.
/// The Host runtime must be the same runtime enrolled with Backend. Dropping
/// this waiter skips queued work; an active native call keeps its reservation
/// until it finishes. The returned rows have already passed `SqlQueryLimits`,
/// but MRR still owns result admission.
/// # Errors
/// Refuses Backend admission, physical query scope, limits or native failures.
#[cfg(feature = "backend-worker")]
pub async fn execute_turso_graphar_on_backend(
    backend: &mrr_data_backend::Backend,
    runtime: tokio::runtime::Handle,
    request: TursoBackendQuery,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    backend
        .run_resource(request.reserved_bytes, move || {
            run_backend_request(&runtime, &request)
        })
        .await
        .map_err(SqlQueryError::Backend)?
}

/// Execute with retained output accounting on the shared Backend.
/// The Host reservation includes input/native state, output and conversion
/// scratch space. Keep the returned handle through MRR projection/admission;
/// output clones must not escape that reservation without separate Host accounting.
/// Waiter cancellation is observed cooperatively at driver checkpoints.
/// Native interrupt and hard in-flight-call deadlines remain unimplemented.
/// # Errors
/// Refuses admission, unsupported query/source, driver failure or lost worker.
#[cfg(feature = "backend-worker")]
pub async fn execute_turso_graphar_retained_on_backend(
    backend: &mrr_data_backend::Backend,
    runtime: tokio::runtime::Handle,
    request: TursoBackendQuery,
) -> Result<mrr_data_backend::ResourceHandle<PhysicalQueryOutput>, SqlQueryError> {
    execute_turso_graphar_controlled_on_backend(
        backend,
        runtime,
        request,
        mrr_data_backend::ResourceControl::default(),
    )
    .await
}

/// Retain results with explicit cooperative cancellation and monotonic deadline.
/// Stops are observed before/after native calls and while converting/loading rows.
/// Cleanup completes before the worker releases admission. This does not impose
/// a hard native-call timeout or provide Turso native interrupt.
/// # Errors
/// Returns sticky stops, driver/cleanup refusals or Backend admission failure.
#[cfg(feature = "backend-worker")]
pub async fn execute_turso_graphar_controlled_on_backend(
    backend: &mrr_data_backend::Backend,
    runtime: tokio::runtime::Handle,
    request: TursoBackendQuery,
    control: mrr_data_backend::ResourceControl,
) -> Result<mrr_data_backend::ResourceHandle<PhysicalQueryOutput>, SqlQueryError> {
    use mrr_data_backend::ResourcePreparationError;
    backend
        .prepare_resource_controlled(request.reserved_bytes, control, move |control| {
            runtime.block_on(execute_checked(
                &request.database,
                &request.query,
                &request.source,
                &request.projection,
                request.limits,
                &|| control.check().map_err(SqlQueryError::from),
            ))
        })
        .await
        .map_err(|error| match error {
            ResourcePreparationError::Backend(error) => SqlQueryError::Backend(error),
            ResourcePreparationError::Preparation(error) => error,
        })
}
#[cfg(feature = "backend-worker")]
fn run_backend_request(
    runtime: &tokio::runtime::Handle,
    request: &TursoBackendQuery,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    runtime.block_on(execute_turso_graphar_single_hop(
        &request.database,
        &request.query,
        &request.source,
        &request.projection,
        request.limits,
    ))
}
#[cfg(test)]
pub(super) async fn execute_in_transaction(
    connection: &turso::Connection,
    plan: &TursoSingleHopSql,
    rows: Vec<EdgeRow>,
    limits: SqlQueryLimits,
    checkpoint: &impl Fn() -> Result<(), SqlQueryError>,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    execute_transaction_observed(
        connection,
        plan,
        rows,
        limits,
        checkpoint,
        &mut TursoExecutionTimings::default(),
    )
    .await
}
async fn execute_transaction_observed(
    connection: &turso::Connection,
    plan: &TursoSingleHopSql,
    rows: Vec<EdgeRow>,
    limits: SqlQueryLimits,
    checkpoint: &impl Fn() -> Result<(), SqlQueryError>,
    timings: &mut TursoExecutionTimings,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    let started = Instant::now();
    checkpoint()?;
    connection
        .execute(
            "CREATE TEMP TABLE mrr_query_edges (fact_id TEXT PRIMARY KEY NOT NULL, relation_id TEXT NOT NULL, generation_id TEXT NOT NULL, source_entity TEXT NOT NULL, target_entity TEXT NOT NULL)",
            (),
        )
        .await
        .map_err(|_| SqlQueryError::Native)?;
    for row in rows {
        checkpoint()?;
        connection
            .execute(
                "INSERT INTO mrr_query_edges (fact_id, relation_id, generation_id, source_entity, target_entity) VALUES (?1, ?2, ?3, ?4, ?5)",
                [
                    turso::Value::Text(row.fact_id),
                    turso::Value::Text(row.relation_id),
                    turso::Value::Text(row.generation_id),
                    turso::Value::Text(row.source_entity),
                    turso::Value::Text(row.target_entity),
                ],
            )
            .await
            .map_err(|_| SqlQueryError::Native)?;
    }
    timings.derived_image_load = started.elapsed();
    checkpoint()?;
    let [relation, generation] = plan.bindings();
    let prepare = Instant::now();
    let mut statement = connection
        .prepare(plan.statement())
        .await
        .map_err(|_| SqlQueryError::Native)?;
    timings.native_prepare = prepare.elapsed();
    checkpoint()?;
    let started = Instant::now();
    let mut cursor = statement
        .query([
            turso::Value::Text(relation.to_owned()),
            turso::Value::Text(generation.to_owned()),
        ])
        .await
        .map_err(|_| SqlQueryError::Native)?;
    timings.bind_and_execute = started.elapsed();
    let started = Instant::now();
    let mut output: Vec<Vec<QueryResultValue>> = Vec::new();
    loop {
        checkpoint()?;
        let Some(row) = cursor.next().await.map_err(|_| SqlQueryError::Native)? else {
            break;
        };
        if timings.first_cursor_row_from_prepare.is_none() {
            timings.first_cursor_row_from_prepare = Some(prepare.elapsed());
        }
        checkpoint()?;
        if output.len() >= limits.max_output_rows
            || output
                .len()
                .checked_add(1)
                .and_then(|count| count.checked_mul(plan.outputs().len()))
                .is_none_or(|cells| cells > limits.max_output_cells)
        {
            return Err(SqlQueryError::Limit("output rows or cells"));
        }
        let values = (0..plan.outputs().len())
            .map(|index| match row.get_value(index) {
                Ok(turso::Value::Text(value)) => Ok(value),
                _ => Err(SqlQueryError::CorruptOutput),
            })
            .collect::<Result<Vec<_>, _>>()?;
        output.push(plan.decode(&values)?);
    }
    timings.fetch_and_decode = started.elapsed();
    Ok(plan.output(output))
}
