use super::{DataFusionQueryError, decode_entity};
use arrow_array::StringArray;
use meta_relational_reasoning::{EntityId, FactId, QueryResultValue};

#[test]
fn encoded_entity_is_parsed_without_deriving_a_second_identity() {
    let id = EntityId::from_canonical_bytes("source node").unwrap();
    let entity_type = EntityId::from_canonical_bytes("node type").unwrap();
    let column = StringArray::from(vec![id.to_string()]);
    assert_eq!(
        decode_entity(&column, 0, entity_type).unwrap(),
        QueryResultValue::node(id, entity_type)
    );
}

#[test]
fn noncanonical_and_wrong_domain_endpoints_are_rejected() {
    let entity_type = EntityId::from_canonical_bytes("node type").unwrap();
    for value in [
        String::new(),
        "not an encoded identity".into(),
        FactId::from_canonical_bytes("fact").unwrap().to_string(),
    ] {
        let column = StringArray::from(vec![value]);
        assert!(matches!(
            decode_entity(&column, 0, entity_type),
            Err(DataFusionQueryError::InvalidEntityIdentity(_))
        ));
    }
}
