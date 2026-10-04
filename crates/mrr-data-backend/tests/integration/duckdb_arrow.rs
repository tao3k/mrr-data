//! Qualify native Arrow import/export against the workspace Arrow types.
#![cfg(feature = "duckdb")]
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use duckdb::{
    Connection,
    vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params},
};
use mrr_data_backend::{BackendError, providers::DuckDbArrowInput};
use std::sync::Arc;

fn batch(rows: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values((0..rows).map(|id| i64::try_from(id).unwrap()));
    let names: StringArray = (0..rows)
        .map(|id| match id % 3 {
            0 => Some("实体/é"),
            1 => Some(""),
            _ => None,
        })
        .collect();
    let values: BinaryArray = (0..rows)
        .map(|id| match id % 3 {
            0 => Some(&b"\0\xffpayload"[..]),
            1 => Some(&b""[..]),
            _ => None,
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("entity", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ])),
        vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
    )
    .unwrap()
}
fn equal_rows(actual: &[RecordBatch], expected: &RecordBatch) {
    let mut offset = 0;
    for batch in actual {
        assert_eq!(batch.num_columns(), expected.num_columns());
        for (column, want) in batch.columns().iter().zip(expected.columns()) {
            assert_eq!(
                column.to_data(),
                want.slice(offset, batch.num_rows()).to_data()
            );
        }
        offset += batch.num_rows();
    }
    assert_eq!(offset, expected.num_rows());
}
#[test]
fn arrow_append_commit_reopen_preserves_null_empty_unicode_and_binary_over_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("arrow.db");
    let expected = batch(4097);
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE batches(id BIGINT, entity VARCHAR, payload BLOB);
        BEGIN TRANSACTION",
    )
    .unwrap();
    {
        let mut app = conn.appender("batches").unwrap();
        app.append_record_batch(expected.clone()).unwrap();
        app.flush().unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
    conn.close().unwrap();
    let reopened = Connection::open(&path).unwrap();
    let batches: Vec<RecordBatch> = reopened
        .prepare("SELECT * FROM batches ORDER BY id")
        .unwrap()
        .query_arrow([])
        .unwrap()
        .collect();
    reopened.close().unwrap();
    // Exported Rust Arrow buffers remain valid after statement and connection close.
    equal_rows(&batches, &expected);
}
#[test]
fn arrow_append_rollback_leaves_no_durable_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollback.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE batches(id BIGINT, entity VARCHAR, payload BLOB);
        BEGIN TRANSACTION",
    )
    .unwrap();
    {
        let mut app = conn.appender("batches").unwrap();
        app.append_record_batch(batch(4097)).unwrap();
        app.flush().unwrap();
    }
    conn.execute_batch("ROLLBACK").unwrap();
    conn.close().unwrap();
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM batches", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}
#[test]
fn workspace_arrow_batch_is_queryable_as_a_native_virtual_table() {
    let conn = Connection::open_in_memory().unwrap();
    conn.register_table_function::<ArrowVTab>("mrr_arrow")
        .unwrap();
    let expected = batch(4097);
    let capacity = DuckDbArrowInput::row_capacity();
    assert!(matches!(
        DuckDbArrowInput::admit(expected.clone(), usize::MAX, usize::MAX),
        Err(BackendError::Limit)
    ));
    let mut actual = Vec::<RecordBatch>::new();
    for input in DuckDbArrowInput::batches(expected.clone(), capacity, 4 * 1024 * 1024).unwrap() {
        let params = arrow_recordbatch_to_query_params(input.into_batch());
        actual.extend(
            conn.prepare("SELECT * FROM mrr_arrow(?1, ?2) ORDER BY id")
                .unwrap()
                .query_arrow(params)
                .unwrap(),
        );
    }
    equal_rows(&actual, &expected);
}

#[test]
fn lazy_slices_share_buffers_and_budget_the_retained_source() {
    let expected = batch(4097);
    let original = expected
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut slices = DuckDbArrowInput::batches(expected.clone(), 1024, 4 * 1024 * 1024).unwrap();
    assert_eq!(slices.len(), 5);
    let mut offset = 0;
    for input in &mut slices {
        let slice = input.into_batch();
        assert!(Arc::ptr_eq(&slice.schema(), &expected.schema()));
        let ids = slice
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            ids.values().as_ptr(),
            original.values().as_ptr().wrapping_add(offset)
        );
        offset += slice.num_rows();
    }
    assert_eq!(offset, expected.num_rows());
    assert_eq!(slices.len(), 0);
    // A tiny logical slice still retains the source's large backing allocation.
    assert!(matches!(
        DuckDbArrowInput::admit(expected.slice(0, 1), 1, 1024),
        Err(BackendError::Limit)
    ));
    assert!(matches!(
        DuckDbArrowInput::batches(expected, 1024, 1024),
        Err(BackendError::Limit)
    ));
}

#[test]
fn arrow_input_refuses_buffer_limit_configuration_and_unqualified_types() {
    assert!(matches!(
        DuckDbArrowInput::admit(batch(1), 0, 1024),
        Err(BackendError::InvalidConfiguration)
    ));
    assert!(matches!(
        DuckDbArrowInput::admit(batch(1), 1, 1),
        Err(BackendError::Limit)
    ));
    let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "unsupported",
        DataType::Date32,
        false,
    )])));
    assert!(matches!(
        DuckDbArrowInput::admit(empty, 1, 1024),
        Err(BackendError::UnsupportedCapabilities)
    ));
    for fields in [
        vec![Field::new("", DataType::Int64, false)],
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ],
    ] {
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(fields)));
        assert!(matches!(
            DuckDbArrowInput::admit(empty, 1, 4096),
            Err(BackendError::Corrupt)
        ));
    }
}

#[test]
fn admitted_input_appends_without_process_lifetime_source_registration() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE bounded_input(id BIGINT, entity VARCHAR, payload BLOB)")
        .unwrap();
    let source = batch(17);
    let source_array = Arc::downgrade(source.column(0));
    let input = DuckDbArrowInput::admit(source, 17, 65_536).unwrap();
    {
        let mut appender = conn.appender("bounded_input").unwrap();
        input.append_to(&mut appender).unwrap();
        // Array wrappers are released by the synchronous input conversion path.
        assert!(source_array.upgrade().is_none());
        appender.flush().unwrap();
    }
    let actual: Vec<RecordBatch> = conn
        .prepare("SELECT * FROM bounded_input ORDER BY id")
        .unwrap()
        .query_arrow([])
        .unwrap()
        .collect();
    equal_rows(&actual, &batch(17));
}
