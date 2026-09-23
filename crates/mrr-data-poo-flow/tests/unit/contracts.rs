use crate::{
    protocol::{MAX_EDGES, PROFILE},
    semantic::Context,
};

#[test]
fn source_revision_and_node_namespaces_are_bound_by_mrr() {
    let a = Context::new("plan-a", "rev-1").unwrap();
    let b = Context::new("plan-a", "rev-2").unwrap();
    let c = Context::new("plan-b", "rev-1").unwrap();
    assert_ne!(a.semantic.generation(), b.semantic.generation());
    assert_ne!(a.semantic.generation(), c.semantic.generation());
    let edges = [["compile".into(), "test".into()]];
    let facts = a.facts("plan-a", &edges).unwrap();
    assert_ne!(
        facts[0].values(),
        c.facts("plan-b", &edges).unwrap()[0].values()
    );
    assert_eq!(facts[0].context().generation(), a.query.generation());
    assert!(Context::new("", "rev-1").is_err());
    assert!(Context::new("plan", "\n").is_err());
}

#[test]
fn duplicate_and_excessive_edges_are_rejected_before_publication() {
    let context = Context::new("plan", "rev").unwrap();
    let edge = ["compile".into(), "test".into()];
    assert!(
        context
            .facts("plan", &[edge.clone(), edge.clone()])
            .is_err()
    );
    assert!(context.facts("plan", &vec![edge; MAX_EDGES + 1]).is_err());
    assert!(
        context
            .facts("plan", &[[String::new(), "test".into()]])
            .is_err()
    );
}

#[test]
fn invalid_requests_fail_before_runtime_configuration_or_io() {
    for request in [
        vec![b' '; 1024 * 1024 + 1],
        br#"{"profile":"healthcare.gql","source":"plan","revision":"rev","operation":{"kind":"query","root":"invalid"}}"#.to_vec(),
        format!(r#"{{"profile":"{PROFILE}","source":"plan","revision":"rev","operation":{{"kind":"query","root":"invalid","query":"MATCH anything"}}}}"#).into_bytes(),
    ] {
        let error = crate::execute(&request).unwrap_err().to_string();
        assert!(!error.contains("MRR_CACHE_DIR"), "request reached runtime configuration: {error}");
    }
}

#[test]
fn restored_facts_cannot_change_authority_or_repeat_identities() {
    let context = Context::new("plan", "rev-1").unwrap();
    let edges = [["compile".into(), "test".into()]];
    let facts = context.facts("plan", &edges).unwrap();
    context.validate_facts("plan", &facts).unwrap();
    assert!(context.validate_facts("another-owner", &facts).is_err());
    assert!(
        Context::new("plan", "rev-2")
            .unwrap()
            .validate_facts("plan", &facts)
            .is_err()
    );
    assert!(
        context
            .validate_facts("plan", &[facts[0].clone(), facts[0].clone()])
            .is_err()
    );
}

#[tokio::test]
async fn fixed_query_projects_exact_source_endpoints_before_admission() {
    use meta_relational_reasoning::{EntityId, QueryResultValue};
    let context = Context::new("plan", "rev").unwrap();
    let facts = context
        .facts("plan", &[["compile".into(), "test".into()]])
        .unwrap();
    let batch = mrr_data_arrow::facts_to_record_batch(&context.relation, &facts).unwrap();
    let output =
        mrr_data_datafusion::execute_binary_entity_query(&context.query, &context.relation, batch)
            .await
            .unwrap();
    let row = &output.rows()[0];
    assert_eq!(output.rows().len(), 1);
    let [
        QueryResultValue::Node { id: a, .. },
        QueryResultValue::Node { id: b, .. },
    ] = row.as_slice()
    else {
        panic!("node endpoints required")
    };
    assert_eq!(
        *a,
        EntityId::from_canonical_bytes(serde_json::to_vec(&("plan", "compile")).unwrap()).unwrap()
    );
    assert_eq!(
        *b,
        EntityId::from_canonical_bytes(serde_json::to_vec(&("plan", "test")).unwrap()).unwrap()
    );
}
