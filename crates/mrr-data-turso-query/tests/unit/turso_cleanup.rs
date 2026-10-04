//! Native rollback and cleanup-failure precedence for the transaction owner.
use crate::SqlQueryError;
use crate::turso_graphar::finish_transaction;
use crate::turso_graphar::{EdgeRow, SqlQueryLimits, execute_in_transaction};

// This fixture qualifies transaction/driver behavior. The integration fixture
// separately constructs this shape through catalog and snapshot admission.
fn native_plan() -> super::TursoSingleHopSql {
    super::TursoSingleHopSql {
        statement: "SELECT source_entity, target_entity FROM mrr_query_edges WHERE relation_id=?1 AND generation_id=?2 ORDER BY fact_id".into(),
        outputs: vec![super::Binding::new("from").unwrap(), super::Binding::new("to").unwrap()],
        columns: vec![super::EndpointColumn::Source(super::EntityId::from_canonical_bytes("node").unwrap()), super::EndpointColumn::Target(super::EntityId::from_canonical_bytes("node").unwrap())],
        relation: super::RelationId::from_canonical_bytes("knows").unwrap(),
        relation_binding: "relation".into(),
        generation_binding: "generation".into(),
    }
}
fn rows() -> Vec<EdgeRow> {
    ["edge-a", "edge-b"]
        .into_iter()
        .map(|id| EdgeRow {
            fact_id: id.into(),
            relation_id: "relation".into(),
            generation_id: "generation".into(),
            source_entity: super::EntityId::from_canonical_bytes("alice")
                .unwrap()
                .to_string(),
            target_entity: super::EntityId::from_canonical_bytes("bob")
                .unwrap()
                .to_string(),
        })
        .collect()
}
fn limits() -> SqlQueryLimits {
    SqlQueryLimits {
        max_input_rows: 2,
        max_input_bytes: 1024,
        max_output_rows: 2,
        max_output_cells: 4,
    }
}
async fn assert_clean(connection: &turso::Connection) {
    assert!(connection.is_autocommit().unwrap());
    let mut tables = connection
        .query(
            "SELECT name FROM sqlite_temp_master WHERE name='mrr_query_edges'",
            (),
        )
        .await
        .unwrap();
    assert!(tables.next().await.unwrap().is_none());
}

#[tokio::test]
async fn native_load_and_fetch_stops_roll_back_without_partial_output() {
    use std::cell::Cell;
    let dir = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(dir.path().join("stops.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    // Checkpoint 2 is after the first INSERT; checkpoint 6 is after the first
    // result row has been decoded. Both exercise a live native transaction.
    for stop_at in [2, 6] {
        for reason in [SqlQueryError::Cancelled, SqlQueryError::Deadline] {
            connection.execute("BEGIN", ()).await.unwrap();
            let calls = Cell::new(0);
            let result =
                execute_in_transaction(&connection, &native_plan(), rows(), limits(), &|| {
                    let call = calls.get();
                    calls.set(call + 1);
                    if call == stop_at { Err(reason) } else { Ok(()) }
                })
                .await;
            assert_eq!(calls.get(), stop_at + 1);
            assert!(
                matches!(finish_transaction(&connection, result).await, Err(error) if error == reason)
            );
            assert_clean(&connection).await;
        }
    }
    // A later request on the same connection sees neither abandoned input nor
    // an active transaction from any stopped request.
    connection.execute("BEGIN", ()).await.unwrap();
    let result =
        execute_in_transaction(&connection, &native_plan(), rows(), limits(), &|| Ok(())).await;
    assert_eq!(
        finish_transaction(&connection, result)
            .await
            .unwrap()
            .rows()
            .len(),
        2
    );
    assert_clean(&connection).await;
}

#[tokio::test]
async fn native_limits_corrupt_values_and_driver_failure_leave_no_result() {
    let dir = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(dir.path().join("refusals.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    for (input, budget, expected) in [
        (
            rows(),
            SqlQueryLimits {
                max_output_rows: 1,
                ..limits()
            },
            SqlQueryError::Limit("output rows or cells"),
        ),
        (
            rows(),
            SqlQueryLimits {
                max_output_cells: 3,
                ..limits()
            },
            SqlQueryError::Limit("output rows or cells"),
        ),
        (
            {
                let mut values = rows();
                values[1].source_entity = "invalid-id".into();
                values
            },
            limits(),
            SqlQueryError::CorruptOutput,
        ),
        (
            {
                let mut values = rows();
                values[1].fact_id = values[0].fact_id.clone();
                values
            },
            limits(),
            SqlQueryError::Native,
        ),
    ] {
        connection.execute("BEGIN", ()).await.unwrap();
        let result =
            execute_in_transaction(&connection, &native_plan(), input, budget, &|| Ok(())).await;
        assert!(
            matches!(finish_transaction(&connection, result).await, Err(error) if error == expected)
        );
        assert_clean(&connection).await;
    }
}
#[tokio::test]
async fn refusal_rolls_back_temporary_input_before_return() {
    let dir = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(dir.path().join("cleanup.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    connection.execute("BEGIN", ()).await.unwrap();
    connection
        .execute("CREATE TEMP TABLE cleanup_input (value INTEGER)", ())
        .await
        .unwrap();
    connection
        .execute("INSERT INTO cleanup_input VALUES (1)", ())
        .await
        .unwrap();
    assert_eq!(
        finish_transaction::<()>(&connection, Err(SqlQueryError::Cancelled)).await,
        Err(SqlQueryError::Cancelled)
    );
    assert!(connection.is_autocommit().unwrap());
    let mut tables = connection
        .query(
            "SELECT name FROM sqlite_temp_master WHERE name = 'cleanup_input'",
            (),
        )
        .await
        .unwrap();
    assert!(tables.next().await.unwrap().is_none());
    // A missing transaction makes the actual native ROLLBACK fail. Neither
    // successful output nor an earlier stop may conceal that cleanup failure.
    assert_eq!(
        finish_transaction(&connection, Ok(())).await,
        Err(SqlQueryError::Cleanup)
    );
    assert_eq!(
        finish_transaction::<()>(&connection, Err(SqlQueryError::Deadline)).await,
        Err(SqlQueryError::Cleanup)
    );
}
