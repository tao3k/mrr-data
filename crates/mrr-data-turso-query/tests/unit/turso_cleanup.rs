//! Native rollback and cleanup-failure precedence for the transaction owner.
use super::finish_transaction;
use crate::SqlQueryError;
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
