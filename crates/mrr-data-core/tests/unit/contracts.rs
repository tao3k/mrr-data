use std::collections::BTreeMap;

use cid::Cid;
use ipld_core::ipld::Ipld;
use meta_relational_reasoning::{
    Binding, CatalogBoundQuery, Direction, EntityCatalog, EntityId, EntitySchema, Expression,
    ExternalRevisionIdentity, GenerationId, GraphPattern, NodePattern, PathPattern, PathSegment,
    Projection, QueryId, QueryOperatorId, QueryResult, QueryTemplate, ReasoningBundle,
    ReasoningBundleDeclaration, RelationCatalog, RelationField, RelationId, RelationPattern,
    RelationSchema, RevisionBinding, SemanticSnapshot, SetQuantifier, ValueSchema,
    bind_query_to_catalog,
};
use multihash_codetable::{Code, MultihashDigest};

use crate::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, DAG_CBOR_CODEC, DataEngineProfile,
    DataError, DataQueryBindingError, DataQueryFeature, GraphProjectionDescriptor, RAW_CODEC,
    RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, bind_data_query,
    raw_cid,
};

fn relation_id(name: &str) -> RelationId {
    RelationId::from_canonical_bytes(name).expect("relation identity")
}

fn relation_schema(name: &str) -> RelationSchema {
    RelationSchema::new(
        relation_id(name),
        name,
        vec![RelationField::new("value", ValueSchema::String, false).expect("field")],
        vec![],
    )
    .expect("relation schema")
}

fn semantic_snapshot(reverse: bool) -> SemanticSnapshot {
    let generation = GenerationId::from_canonical_bytes("generation:fixture").expect("generation");
    let first = RevisionBinding::admit(
        ExternalRevisionIdentity::new("git", "repository:a", "commit:a").expect("revision"),
        generation,
    )
    .expect("binding");
    let second = RevisionBinding::admit(
        ExternalRevisionIdentity::new("database", "catalog:b", "lsn:42").expect("revision"),
        generation,
    )
    .expect("binding");
    let revisions = if reverse {
        vec![second, first]
    } else {
        vec![first, second]
    };
    SemanticSnapshot::admit(generation, revisions).expect("snapshot")
}

fn catalogs() -> (RelationCatalog, EntityCatalog) {
    let relations = RelationCatalog::admit(vec![relation_schema("alpha"), relation_schema("beta")])
        .expect("relation catalog");
    let entity = EntitySchema::new(
        EntityId::from_canonical_bytes("entity:fixture").expect("entity identity"),
        "Fixture",
        vec![],
    )
    .expect("entity schema");
    let entities = EntityCatalog::admit(vec![entity]).expect("entity catalog");
    (relations, entities)
}

fn descriptor(name: &str, payload: &[u8], rows: u64) -> RelationDescriptor {
    RelationDescriptor::new(
        relation_id(name),
        rows,
        vec![
            BatchDescriptor::new(raw_cid(payload), rows, payload.len() as u64)
                .expect("batch descriptor"),
        ],
    )
    .expect("relation descriptor")
}

fn manifest(reverse: bool) -> SnapshotManifest {
    manifest_with_graph(reverse, false)
}

fn manifest_with_graph(reverse: bool, with_graph: bool) -> SnapshotManifest {
    manifest_for_semantic(semantic_snapshot(reverse), with_graph, reverse)
}

fn manifest_for_semantic(
    semantic_snapshot: SemanticSnapshot,
    with_graph: bool,
    reverse: bool,
) -> SnapshotManifest {
    let (relations, entities) = catalogs();
    let alpha = descriptor("alpha", b"alpha-arrow-ipc", 2);
    let beta = descriptor("beta", b"beta-arrow-ipc", 3);
    let relation_descriptors = if reverse {
        vec![beta, alpha]
    } else {
        vec![alpha, beta]
    };
    let lineage_a = raw_cid(b"lineage-a");
    let lineage_b = raw_cid(b"lineage-b");
    let lineage = if reverse {
        vec![lineage_b, lineage_a]
    } else {
        vec![lineage_a, lineage_b]
    };
    let mut request = SnapshotManifestRequest::new(
        semantic_snapshot,
        &relations,
        &entities,
        relation_descriptors,
        CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).expect("coverage"),
    )
    .with_lineage_batch_cids(lineage);
    if with_graph {
        request = request.with_graph_projection(
            GraphProjectionDescriptor::new("0.12.0", raw_cid(b"graphar-manifest"))
                .expect("graph projection"),
        );
    }
    SnapshotManifest::admit(request).expect("manifest")
}

fn alternate_semantic_snapshot(generation: GenerationId, revision: &str) -> SemanticSnapshot {
    SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "repository:alternate", revision).unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn bound_query(max_hops: Option<u32>) -> CatalogBoundQuery {
    let entity = EntityId::from_canonical_bytes("entity:fixture").unwrap();
    let query_id = QueryId::from_canonical_bytes("query:fixture").unwrap();
    let operator = |name: &str| QueryOperatorId::from_canonical_bytes(name).unwrap();
    let binding = |name: &str| Binding::new(name).unwrap();
    let query = meta_relational_reasoning::MetaQueryIr::new(
        query_id,
        GraphPattern::new(
            operator("graph:fixture"),
            vec![PathPattern::new(
                NodePattern::new(binding("source"), vec![entity]),
                vec![PathSegment::new(
                    RelationPattern::new(
                        Some(binding("edge")),
                        vec![relation_id("alpha")],
                        Direction::Outgoing,
                        1,
                        max_hops,
                    )
                    .unwrap(),
                    NodePattern::new(binding("target"), vec![entity]),
                )],
            )],
        )
        .unwrap(),
        vec![],
        QueryResult::returning(SetQuantifier::All).with_projections(vec![Projection::new(
            operator("projection:fixture"),
            Expression::Binding(binding("source")),
            binding("source_entity"),
        )]),
    )
    .unwrap();
    let bundle = ReasoningBundle::admit(ReasoningBundleDeclaration {
        relations: vec![relation_schema("alpha"), relation_schema("beta")],
        entities: vec![EntitySchema::new(entity, "Fixture", vec![]).unwrap()],
        query_templates: vec![QueryTemplate::new(query, vec![])],
        ..ReasoningBundleDeclaration::default()
    })
    .unwrap();
    bind_query_to_catalog(&bundle, query_id, &semantic_snapshot(false)).unwrap()
}

#[test]
fn admitted_query_binds_to_exact_physical_snapshot_and_graph_projection() {
    let snapshot = SnapshotBlock::encode(manifest_with_graph(false, true)).unwrap();
    let engine = DataEngineProfile::new("graphar-native", true, []).unwrap();
    let bound = bind_data_query(&bound_query(Some(1)), &snapshot, &engine).unwrap();

    assert_eq!(bound.snapshot_root(), snapshot.cid());
    assert_eq!(
        bound.graph_projection_manifest(),
        snapshot
            .manifest()
            .graph_projection()
            .map(GraphProjectionDescriptor::manifest_cid)
    );
    assert_eq!(bound.engine().name(), "graphar-native");
}

#[test]
fn graph_projection_requirement_is_fail_closed() {
    let snapshot = SnapshotBlock::encode(manifest(false)).unwrap();
    let engine = DataEngineProfile::new("graphar-native", true, []).unwrap();

    assert_eq!(
        bind_data_query(&bound_query(Some(1)), &snapshot, &engine),
        Err(DataQueryBindingError::GraphProjectionRequired)
    );
}

#[test]
fn engine_must_admit_every_required_path_feature() {
    let snapshot = SnapshotBlock::encode(manifest_with_graph(false, true)).unwrap();
    let engine = DataEngineProfile::new("bounded-path-engine", true, []).unwrap();

    assert_eq!(
        bind_data_query(&bound_query(None), &snapshot, &engine),
        Err(DataQueryBindingError::UnsupportedFeature(
            DataQueryFeature::UnboundedPath
        ))
    );
}

#[test]
fn physical_binding_rejects_generation_and_snapshot_drift() {
    let query = bound_query(Some(1));
    let engine = DataEngineProfile::new("arrow-native", false, []).unwrap();
    let other_generation = GenerationId::from_canonical_bytes("generation:other").unwrap();
    let stale = SnapshotBlock::encode(manifest_for_semantic(
        alternate_semantic_snapshot(other_generation, "commit:other"),
        false,
        false,
    ))
    .unwrap();
    assert_eq!(
        bind_data_query(&query, &stale, &engine),
        Err(DataQueryBindingError::GenerationMismatch {
            query: query.generation(),
            snapshot: other_generation,
        })
    );

    let drifted = SnapshotBlock::encode(manifest_for_semantic(
        alternate_semantic_snapshot(query.generation(), "commit:drifted"),
        false,
        false,
    ))
    .unwrap();
    assert_eq!(
        bind_data_query(&query, &drifted, &engine),
        Err(DataQueryBindingError::SemanticSnapshotMismatch)
    );
}

#[test]
fn canonical_manifest_round_trips_with_a_checked_root() {
    let block = SnapshotBlock::encode(manifest(false)).expect("encode manifest");
    assert_eq!(block.cid().version(), cid::Version::V1);
    assert_eq!(block.cid().codec(), DAG_CBOR_CODEC);

    let decoded = SnapshotManifest::decode_checked(block.bytes(), block.cid()).expect("decode");
    assert_eq!(&decoded, block.manifest());
    assert_eq!(decoded.canonical_bytes().unwrap(), block.bytes());
    let (relations, entities) = catalogs();
    decoded.verify_catalogs(&relations, &entities).unwrap();
}

#[test]
fn graph_projection_round_trips_under_its_own_schema_identity() {
    let block = SnapshotBlock::encode(manifest_with_graph(false, true)).unwrap();
    let decoded = SnapshotManifest::decode_checked(block.bytes(), block.cid()).unwrap();
    let graph = decoded.graph_projection().expect("graph projection");
    assert_eq!(graph.graphar_version(), "0.12.0");
    assert_eq!(graph.manifest_cid(), &raw_cid(b"graphar-manifest"));
}

#[test]
fn unordered_semantic_inputs_have_one_root_cid() {
    let left = SnapshotBlock::encode(manifest(false)).expect("left");
    let right = SnapshotBlock::encode(manifest(true)).expect("right");
    assert_eq!(left.bytes(), right.bytes());
    assert_eq!(left.cid(), right.cid());
}

#[test]
fn snapshot_v1_root_has_a_golden_cid() {
    let block = SnapshotBlock::encode(manifest(false)).unwrap();
    assert_eq!(
        block.cid().to_string(),
        "bafyreibsgh7hsmqsgp42ls3jdd26u4coplmgk5vl4l5fbff3vjziaoidai"
    );
}

#[test]
fn changed_physical_bytes_change_the_root() {
    let left = SnapshotBlock::encode(manifest(false)).expect("left");
    let (relations, entities) = catalogs();
    let changed = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            semantic_snapshot(false),
            &relations,
            &entities,
            vec![
                descriptor("alpha", b"changed-alpha-arrow-ipc", 2),
                descriptor("beta", b"beta-arrow-ipc", 3),
            ],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).unwrap(),
        )
        .with_lineage_batch_cids(vec![raw_cid(b"lineage-a"), raw_cid(b"lineage-b")]),
    )
    .unwrap();
    let right = SnapshotBlock::encode(changed).expect("right");
    assert_ne!(left.cid(), right.cid());
}

#[test]
fn native_admission_rejects_a_relation_set_outside_the_catalog() {
    let (relations, entities) = catalogs();
    let request = SnapshotManifestRequest::new(
        semantic_snapshot(false),
        &relations,
        &entities,
        vec![descriptor("alpha", b"alpha-arrow-ipc", 2)],
        CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).unwrap(),
    );
    assert_eq!(
        SnapshotManifest::admit(request),
        Err(DataError::RelationSetMismatch)
    );
}

#[test]
fn native_admission_rejects_one_arrow_child_claimed_by_two_relations() {
    let (relations, entities) = catalogs();
    let cid = raw_cid(b"shared-arrow-ipc");
    let request = SnapshotManifestRequest::new(
        semantic_snapshot(false),
        &relations,
        &entities,
        vec![
            RelationDescriptor::new(
                relation_id("alpha"),
                1,
                vec![BatchDescriptor::new(cid, 1, 16).unwrap()],
            )
            .unwrap(),
            RelationDescriptor::new(
                relation_id("beta"),
                1,
                vec![BatchDescriptor::new(cid, 1, 16).unwrap()],
            )
            .unwrap(),
        ],
        CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).unwrap(),
    );
    assert_eq!(
        SnapshotManifest::admit(request),
        Err(DataError::DuplicateChild(Box::new(cid)))
    );
}

#[test]
fn decoded_manifest_rejects_one_arrow_child_claimed_by_two_relations() {
    let bytes = manifest(false).canonical_bytes().unwrap();
    let mut value: Ipld = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
    replace(
        &mut value,
        &["relations", "1", "batches", "0", "cid"],
        Ipld::Link(raw_cid(b"alpha-arrow-ipc")),
    );
    let aliased = serde_ipld_dagcbor::to_vec(&value).unwrap();
    assert!(matches!(
        SnapshotManifest::decode_canonical(&aliased),
        Err(DataError::DuplicateChild(_))
    ));
}

#[test]
fn decoded_manifest_rejects_the_wrong_resolved_catalog() {
    let decoded = SnapshotManifest::decode_canonical(&manifest(false).canonical_bytes().unwrap())
        .expect("decode");
    let wrong_relations = RelationCatalog::admit(vec![relation_schema("gamma")]).unwrap();
    let (_, entities) = catalogs();
    assert_eq!(
        decoded.verify_catalogs(&wrong_relations, &entities),
        Err(DataError::RelationCatalogMismatch)
    );
}

#[test]
fn relation_descriptor_rejects_inconsistent_rows_and_duplicate_children() {
    let batch = BatchDescriptor::new(raw_cid(b"batch"), 2, 5).unwrap();
    assert_eq!(
        RelationDescriptor::new(relation_id("alpha"), 3, vec![batch.clone()]),
        Err(DataError::BatchRowsMismatch {
            relation: relation_id("alpha"),
            declared: 3,
            actual: 2,
        })
    );
    assert_eq!(
        RelationDescriptor::new(relation_id("alpha"), 4, vec![batch.clone(), batch]),
        Err(DataError::DuplicateChild(Box::new(raw_cid(b"batch"))))
    );
}

#[test]
fn child_descriptors_reject_empty_or_non_raw_payloads() {
    let raw = raw_cid(b"payload");
    assert_eq!(
        BatchDescriptor::new(raw, 1, 0),
        Err(DataError::EmptyPayload(Box::new(raw)))
    );

    let root = SnapshotBlock::encode(manifest(false)).unwrap();
    assert!(matches!(
        BatchDescriptor::new(*root.cid(), 1, root.bytes().len() as u64),
        Err(DataError::InvalidCidProfile { .. })
    ));

    let wrong_hash = Cid::new_v1(RAW_CODEC, Code::Sha2_512.digest(b"payload"));
    assert!(matches!(
        BatchDescriptor::new(wrong_hash, 1, 7),
        Err(DataError::InvalidCidProfile { .. })
    ));
}

#[test]
fn checked_decode_rejects_the_wrong_root() {
    let block = SnapshotBlock::encode(manifest(false)).unwrap();
    let wrong = raw_cid(b"not-a-root");
    assert!(matches!(
        SnapshotManifest::decode_checked(block.bytes(), &wrong),
        Err(DataError::InvalidCidProfile { .. })
    ));
}

#[test]
fn decode_rejects_unknown_root_contract_fields() {
    assert_mutation_error(
        &["schema", "namespace"],
        Ipld::String("other.data.snapshot".into()),
        |error| matches!(error, DataError::UnknownSchemaNamespace(_)),
    );
    assert_mutation_error(&["schema", "version"], Ipld::Integer(2), |error| {
        matches!(error, DataError::UnknownSchemaVersion(2))
    });
    assert_mutation_error(&["integrity", "cid_version"], Ipld::Integer(2), |error| {
        matches!(error, DataError::UnknownCidVersion(2))
    });
    assert_mutation_error(
        &["integrity", "manifest_codec"],
        Ipld::String("json".into()),
        |error| matches!(error, DataError::UnknownManifestCodec(_)),
    );
    assert_mutation_error(
        &["integrity", "multihash"],
        Ipld::String("identity".into()),
        |error| matches!(error, DataError::UnknownMultihash(_)),
    );
}

#[test]
fn decode_rejects_unknown_arrow_schema_and_child_profiles() {
    assert_mutation_error(
        &["relations", "0", "arrow_schema", "namespace"],
        Ipld::String("other.data.arrow.fact-batch".into()),
        |error| matches!(error, DataError::UnknownArrowSchemaNamespace(_)),
    );
    assert_mutation_error(
        &["relations", "0", "arrow_schema", "version"],
        Ipld::Integer(2),
        |error| matches!(error, DataError::UnknownArrowSchemaVersion(2)),
    );
    assert_mutation_error(
        &["relations", "0", "batches", "0", "cid_codec"],
        Ipld::String("dag-cbor".into()),
        |error| matches!(error, DataError::UnknownChildCodec(_)),
    );
    assert_mutation_error(
        &["relations", "0", "batches", "0", "format"],
        Ipld::String("parquet".into()),
        |error| matches!(error, DataError::UnknownPayloadFormat(_)),
    );
    assert_mutation_error(
        &["relations", "0", "batches", "0", "row_count"],
        Ipld::Integer(99),
        |error| matches!(error, DataError::BatchRowsMismatch { .. }),
    );
}

#[test]
fn decode_rejects_unknown_graph_projection_schema() {
    let bytes = manifest_with_graph(false, true).canonical_bytes().unwrap();
    let mut value: Ipld = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
    replace(
        &mut value,
        &["graph_projection", "schema", "version"],
        Ipld::Integer(2),
    );
    let mutated = serde_ipld_dagcbor::to_vec(&value).unwrap();
    assert!(matches!(
        SnapshotManifest::decode_canonical(&mutated),
        Err(DataError::UnknownGraphProjectionVersion(2))
    ));
}

#[test]
fn decode_rejects_tampered_source_identity_and_digest() {
    assert_mutation_error(
        &["source", "revision_bindings", "0", "content_revision"],
        Ipld::String("commit:tampered".into()),
        |error| matches!(error, DataError::RevisionIdentityMismatch { .. }),
    );
    assert_mutation_error(
        &["semantic", "semantic_snapshot_digest"],
        Ipld::Bytes(vec![0; 32]),
        |error| matches!(error, DataError::SemanticSnapshotDigestMismatch),
    );
}

fn assert_mutation_error(
    path: &[&str],
    replacement: Ipld,
    predicate: impl FnOnce(DataError) -> bool,
) {
    let bytes = manifest(false).canonical_bytes().unwrap();
    let mut value: Ipld = serde_ipld_dagcbor::from_slice(&bytes).expect("IPLD decode");
    replace(&mut value, path, replacement);
    let mutated = serde_ipld_dagcbor::to_vec(&value).expect("canonical mutation");
    let error = SnapshotManifest::decode_canonical(&mutated).expect_err("must reject mutation");
    assert!(predicate(error.clone()), "unexpected error: {error:?}");
}

fn replace(value: &mut Ipld, path: &[&str], replacement: Ipld) {
    let Some((head, tail)) = path.split_first() else {
        *value = replacement;
        return;
    };
    match value {
        Ipld::Map(map) => replace(map.get_mut(*head).expect("map field"), tail, replacement),
        Ipld::List(values) => {
            let index = head.parse::<usize>().expect("list index");
            replace(&mut values[index], tail, replacement);
        }
        _ => panic!("path traverses a scalar"),
    }
}

#[allow(dead_code)]
fn _assert_ipld_map_shape(_: BTreeMap<String, Ipld>, _: Cid) {}
