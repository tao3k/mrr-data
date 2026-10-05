use super::{Completion, Datum, LIMIT, Reader, decode, encode, key};
use crate::{AuthorityExpectation, AuthorityState, AuthorityStatus, StoredRevision, StoredWrite};
use cid::Cid;

#[test]
fn full_unsigned_domain_and_inert_unicode_roundtrip() {
    for number in [0, 1, u64::MAX] {
        assert_eq!(decode::<u64>(&encode(&number).unwrap()).unwrap(), number);
    }
    let text = "时态 \"quoted\" \\ control\n\0".to_owned();
    assert_eq!(decode::<String>(&encode(&text).unwrap()).unwrap(), text);
    assert_eq!(encode(&true).unwrap(), b"#t");
    assert_eq!(encode(&Option::<u64>::None).unwrap(), b"#f");
}

#[test]
fn records_have_exact_tags_arity_and_nested_types() {
    let root: Cid = "bafkreigh2akiscaildcw4535x3wkd4jkfvxvygqrj3brp6a4p7ch5yqxtu"
        .parse()
        .unwrap();
    let state = AuthorityState {
        generation: u64::MAX,
        commitment: root,
        status: AuthorityStatus::Retired,
    };
    let bytes = encode(&state).unwrap();
    assert_eq!(decode::<AuthorityState>(&bytes).unwrap(), state);
    let write = StoredWrite {
        profile: "p".into(),
        namespace: "n".into(),
        scope: "s".into(),
        operation_id: "o".into(),
        expected: None,
        replacement: root,
        authorities: vec![AuthorityExpectation {
            authority_id: "a".into(),
            state,
        }],
    };
    let completion = Completion {
        write,
        committed: StoredRevision {
            revision: u64::MAX,
            root,
        },
    };
    let bytes = encode(&completion).unwrap();
    let read = decode::<Completion>(&bytes).unwrap();
    assert_eq!(read.write, completion.write);
    assert_eq!(read.committed, completion.committed);
    for invalid in [
        b"(\"mrr.backend.revision.v2\" 1)".as_slice(),
        b"(\"mrr.backend.revision.v1\" 1 \"bad\")",
        b"(\"mrr.backend.revision.v2\" #t \"bad\")",
    ] {
        assert!(decode::<StoredRevision>(invalid).is_err());
    }
}

#[test]
fn oversized_and_malformed_data_are_rejected() {
    for bytes in [
        b"{}".as_slice(),
        b"18446744073709551616",
        b"01",
        b"-1",
        b"#t garbage",
        b"\"\\x110000;\"",
        b"\"\\n\"",
        b"(eval 1)",
        b"#f #f",
        &[255],
    ] {
        assert!(decode::<String>(bytes).is_err());
        assert!(decode::<u64>(bytes).is_err());
    }
    assert!(decode::<bool>(b"#t garbage").is_err());
    assert!(decode::<Vec<bool>>(b"(#t#f)").is_err());
    assert!(decode::<Vec<u64>>(b"(1\"x\")").is_err());
    assert!(decode::<String>(&vec![b' '; LIMIT + 1]).is_err());
    let exact = "x".repeat(LIMIT - 2);
    assert_eq!(encode(&exact).unwrap().len(), LIMIT);
    assert_eq!(decode::<String>(&encode(&exact).unwrap()).unwrap(), exact);
    assert!(encode(&"x".repeat(LIMIT)).is_err());
    assert!(encode(&"\n".repeat(LIMIT / 2)).is_err());
    assert!(encode(&vec![0_u64; 17]).is_err());
    assert!(decode::<Vec<u64>>(b"(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0)").is_err());
    assert!(key("head", &[&"x".repeat(8192)]).is_err());
    let nested = format!("{}0{}", "(".repeat(34), ")".repeat(34));
    assert!(decode::<u64>(nested.as_bytes()).is_err());
}

#[test]
fn key_parts_are_unambiguous_scheme_strings() {
    assert_ne!(
        key("head", &["a", "b"]).unwrap(),
        key("head", &["a b"]).unwrap()
    );
    let encoded = key("head", &["\" ) (evil", "中文"]).unwrap();
    let mut reader = Reader {
        input: &encoded,
        offset: 0,
    };
    let Datum::List(parts) = reader.datum(0).unwrap() else {
        panic!("key must be a list")
    };
    assert_eq!(parts.len(), 4);
    let Datum::List(parts) = &parts[3] else {
        panic!("key parts must be a list")
    };
    assert_eq!(parts.len(), 2);
}

#[test]
fn record_schema_fields_and_provider_markers_are_independent() {
    use mrr_data_profile::{BACKEND_DUCKDB_SCHEMA, BACKEND_REVISION_SCHEMA};
    let root: Cid = "bafkreigh2akiscaildcw4535x3wkd4jkfvxvygqrj3brp6a4p7ch5yqxtu"
        .parse()
        .unwrap();
    let encoded = encode(&StoredRevision { revision: 7, root }).unwrap();
    let expected = format!(
        "(\"{}\" {} 7 \"{}\")",
        BACKEND_REVISION_SCHEMA.namespace, BACKEND_REVISION_SCHEMA.version, root
    );
    assert_eq!(encoded, expected.as_bytes());
    for head in [
        "\"mrr.backend.revision\" 3",
        "\"mrr.backend.revision\" #t",
        "\"mrr.backend.revision\" \"2\"",
        "\"mrr.backend.revision.v2\" 2",
        "\"mrr.backend.revision\"",
    ] {
        let bytes = format!("({head} 7 \"{root}\")");
        assert!(matches!(
            decode::<StoredRevision>(bytes.as_bytes()),
            Err(crate::BackendError::Corrupt)
        ));
    }
    assert_eq!(
        super::schema_marker(BACKEND_DUCKDB_SCHEMA),
        b"(\"mrr-data-backend.duckdb\" 2)"
    );
}
