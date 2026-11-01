use std::time::Duration;

fn receipt<const N: usize>(
    phases: [(&str, Duration); N],
    first_name: &str,
    first: Option<Duration>,
    first_minimum: Duration,
    total: Duration,
) -> serde_json::Value {
    assert!(
        phases
            .iter()
            .map(|(_, duration)| *duration)
            .sum::<Duration>()
            <= total
    );
    let first = first.unwrap();
    assert!(first >= first_minimum && first <= total);
    let mut receipt = serde_json::Map::new();
    for (name, duration) in phases {
        receipt.insert(name.to_owned(), (duration.as_secs_f64() * 1000.0).into());
    }
    receipt.insert(first_name.into(), (first.as_secs_f64() * 1000.0).into());
    receipt.insert("total".into(), (total.as_secs_f64() * 1000.0).into());
    receipt.into()
}
pub(super) fn duckgql(t: mrr_data_duckgql_query::DuckGqlExecutionTimings) -> serde_json::Value {
    receipt(
        [
            ("program_compile", t.program_compile),
            ("source_projection", t.source_projection),
            ("connection_setup", t.connection_setup),
            ("extension_load", t.extension_load),
            ("derived_image_load", t.derived_image_load),
            ("graph_registration", t.graph_registration),
            ("native_prepare", t.native_prepare),
            ("bind_and_execute", t.bind_and_execute),
            ("fetch_and_decode", t.fetch_and_decode),
            ("connection_cleanup", t.connection_cleanup),
        ],
        "first_cursor_row_from_prepare",
        t.first_cursor_row_from_prepare,
        t.native_prepare + t.bind_and_execute,
        t.total,
    )
}
pub(super) fn turso(t: mrr_data_turso_query::TursoExecutionTimings) -> serde_json::Value {
    receipt(
        [
            ("program_compile", t.program_compile),
            ("source_projection", t.source_projection),
            ("connection_setup", t.connection_setup),
            ("derived_image_load", t.derived_image_load),
            ("native_prepare", t.native_prepare),
            ("bind_and_execute", t.bind_and_execute),
            ("fetch_and_decode", t.fetch_and_decode),
            ("connection_cleanup", t.connection_cleanup),
        ],
        "first_cursor_row_from_prepare",
        t.first_cursor_row_from_prepare,
        t.native_prepare + t.bind_and_execute,
        t.total,
    )
}
pub(super) fn datafusion(t: mrr_data_datafusion::DataFusionExecutionTimings) -> serde_json::Value {
    receipt(
        [
            ("plan_admission", t.plan_admission),
            ("source_registration", t.source_registration),
            ("logical_plan", t.logical_plan),
            ("physical_plan_and_start", t.physical_plan_and_start),
            ("fetch_batches", t.fetch_batches),
            ("decode_output", t.decode_output),
            ("cleanup", t.cleanup),
        ],
        "first_batch_from_start",
        t.first_batch_from_start,
        t.physical_plan_and_start,
        t.total,
    )
}
