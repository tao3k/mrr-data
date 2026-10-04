//! First Turso execution slice over one captured `GraphAr` binary-Entity source.
use crate::{SqlQueryError, TursoSingleHopSql};
use meta_relational_reasoning::{FactId, QueryResultValue};
use mrr_data_core::{BoundDataQuery, PhysicalQueryOutput};
use mrr_data_graphar::{BinaryEntityProjection, CapturedGraphArSnapshot};
#[cfg(feature = "backend-worker")]
use std::sync::Arc;

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
struct EdgeRow {
    fact_id: String,
    relation_id: String,
    generation_id: String,
    source_entity: String,
    target_entity: String,
}
fn prepare_rows(
    query: &BoundDataQuery,
    source: &CapturedGraphArSnapshot,
    projection: &BinaryEntityProjection,
    limits: SqlQueryLimits,
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
    let plan = TursoSingleHopSql::compile(query, projection)?;
    let rows = prepare_rows(query, source, projection, limits)?;
    let connection = database.connect().map_err(|_| SqlQueryError::Native)?;
    connection
        .execute("BEGIN", ())
        .await
        .map_err(|_| SqlQueryError::Native)?;
    let result = execute_in_transaction(&connection, &plan, rows, limits).await;
    let rollback = connection.execute("ROLLBACK", ()).await;
    if rollback.is_err() {
        return Err(SqlQueryError::Native);
    }
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
    let TursoBackendQuery {
        database,
        query,
        source,
        projection,
        limits,
        reserved_bytes,
    } = request;
    backend
        .run_resource(reserved_bytes, move || {
            runtime.block_on(execute_turso_graphar_single_hop(
                &database,
                &query,
                &source,
                &projection,
                limits,
            ))
        })
        .await
        .map_err(SqlQueryError::Backend)?
}
async fn execute_in_transaction(
    connection: &turso::Connection,
    plan: &TursoSingleHopSql,
    rows: Vec<EdgeRow>,
    limits: SqlQueryLimits,
) -> Result<PhysicalQueryOutput, SqlQueryError> {
    connection
        .execute(
            "CREATE TEMP TABLE mrr_query_edges (fact_id TEXT PRIMARY KEY NOT NULL, relation_id TEXT NOT NULL, generation_id TEXT NOT NULL, source_entity TEXT NOT NULL, target_entity TEXT NOT NULL)",
            (),
        )
        .await
        .map_err(|_| SqlQueryError::Native)?;
    for row in rows {
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
    let [relation, generation] = plan.bindings();
    let mut cursor = connection
        .query(
            plan.statement(),
            [
                turso::Value::Text(relation.to_owned()),
                turso::Value::Text(generation.to_owned()),
            ],
        )
        .await
        .map_err(|_| SqlQueryError::Native)?;
    let mut output: Vec<Vec<QueryResultValue>> = Vec::new();
    while let Some(row) = cursor.next().await.map_err(|_| SqlQueryError::Native)? {
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
    Ok(plan.output(output))
}
