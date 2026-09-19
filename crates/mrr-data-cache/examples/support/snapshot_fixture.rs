//! Real Arrow IPC and admitted catalogs used by the example and S3 contract tests.
use meta_relational_reasoning::{
    EntityCatalog, EntityId, EntitySchema, EvidenceCompleteness, ExternalRevisionIdentity, Fact,
    FactId, FactProvenance, FactValidity, GenerationId, RelationAuthority, RelationCatalog,
    RelationContext, RelationField, RelationId, RelationSchema, RevisionBinding, SemanticSnapshot,
    Value, ValueSchema,
};
use mrr_data_content::{ContentBlock, ContentCodec, ContentError, ContentStore};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};

pub struct Fixture {
    pub snapshot: SnapshotBlock,
    pub relations: RelationCatalog,
    pub entities: EntityCatalog,
    pub relation: RelationSchema,
    pub facts: Vec<Fact>,
    pub ipc: Vec<u8>,
}
impl Fixture {
    pub fn seed(&self, local: &impl ContentStore) -> Result<(), ContentError> {
        for bytes in [self.ipc.as_slice(), b"complete coverage", b"lineage"] {
            local.put(ContentBlock::new(ContentCodec::Raw, bytes))?;
        }
        Ok(())
    }
}

pub fn fixture() -> Fixture {
    let relation_id = RelationId::from_canonical_bytes("snapshot:relation").unwrap();
    let entity = EntityId::from_canonical_bytes("snapshot:source").unwrap();
    let generation = GenerationId::from_canonical_bytes("snapshot:generation").unwrap();
    let relation = RelationSchema::new(
        relation_id,
        "observation",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    let facts = vec![Fact::new(
        FactId::from_canonical_bytes("snapshot:fact").unwrap(),
        relation_id,
        vec![Value::String("verified Arrow fact".into())],
        RelationContext::new(
            generation,
            RelationAuthority::Entity(entity),
            FactProvenance::Source(entity),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    )];
    let ipc = mrr_data_arrow::facts_to_ipc(&relation, &facts).unwrap();
    let relations = RelationCatalog::admit(vec![relation.clone()]).unwrap();
    let entities =
        EntityCatalog::admit(vec![EntitySchema::new(entity, "Source", vec![]).unwrap()]).unwrap();
    let semantic = SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "snapshot-example", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            semantic,
            &relations,
            &entities,
            vec![
                RelationDescriptor::new(
                    relation_id,
                    1,
                    vec![BatchDescriptor::new(raw_cid(&ipc), 1, ipc.len() as u64).unwrap()],
                )
                .unwrap(),
            ],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"complete coverage")).unwrap(),
        )
        .with_lineage_batch_cids(vec![raw_cid(b"lineage")]),
    )
    .unwrap();
    Fixture {
        snapshot: SnapshotBlock::encode(manifest).unwrap(),
        relations,
        entities,
        relation,
        facts,
        ipc,
    }
}
