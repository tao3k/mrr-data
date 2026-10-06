//! Real `DataFusion` answers checked against the separate IR interpreter.
use super::{batch, fixture, limits};
use crate::{
    execute_property_path_query, reference_property_path_query, verify_property_path_output,
};
use arrow_array::{Array, StringArray};
use meta_relational_reasoning::{QueryResultValue, Value, ValueSchema};
use mrr_data_core::PhysicalQueryOutput;

#[tokio::test]
async fn reference_accepts_actual_join_and_reordered_rows_with_nulls() {
    let fixture = fixture();
    let output = execute_property_path_query(
        &fixture.query,
        &fixture.entities,
        &fixture.relations,
        limits(),
    )
    .await
    .unwrap();
    let mut rows = output.rows().to_vec();
    rows.reverse();
    let reordered = PhysicalQueryOutput::new(output.columns().to_vec(), rows);
    verify_property_path_output(
        &fixture.query,
        &fixture.entities,
        &fixture.relations,
        limits(),
        &reordered,
    )
    .unwrap();
    assert!(
        reordered
            .rows()
            .iter()
            .flatten()
            .any(|cell| cell == &QueryResultValue::Null)
    );
}

#[tokio::test]
async fn reference_preserves_duplicate_edges_and_rejects_missing_multiplicity() {
    let mut fixture = fixture();
    let relation = &fixture.relations[1].batch;
    let columns = relation
        .columns()
        .iter()
        .map(|column| {
            let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..strings.len())
                .chain(0..strings.len())
                .map(|row| Some(strings.value(row).to_owned()))
                .collect()
        })
        .collect();
    fixture.relations[1].batch = batch(&["source", "target"], columns);
    let output = execute_property_path_query(
        &fixture.query,
        &fixture.entities,
        &fixture.relations,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(output.rows().len(), 6);
    verify_property_path_output(
        &fixture.query,
        &fixture.entities,
        &fixture.relations,
        limits(),
        &output,
    )
    .unwrap();
    let missing = PhysicalQueryOutput::new(output.columns().to_vec(), output.rows()[1..].to_vec());
    assert!(
        verify_property_path_output(
            &fixture.query,
            &fixture.entities,
            &fixture.relations,
            limits(),
            &missing
        )
        .is_err()
    );
    let mut wrong = output.rows().to_vec();
    wrong[0][0] = QueryResultValue::Scalar {
        schema: ValueSchema::String,
        value: Value::String("unrelated answer".into()),
    };
    let wrong = PhysicalQueryOutput::new(output.columns().to_vec(), wrong);
    assert!(
        verify_property_path_output(
            &fixture.query,
            &fixture.entities,
            &fixture.relations,
            limits(),
            &wrong
        )
        .is_err()
    );
}

#[test]
fn reference_rejects_join_budget_before_returning_rows() {
    let fixture = fixture();
    let mut limits = limits();
    limits.max_join_rows = 1;
    assert!(
        reference_property_path_query(
            &fixture.query,
            &fixture.entities,
            &fixture.relations,
            limits
        )
        .is_err()
    );
}
