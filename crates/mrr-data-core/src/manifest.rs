//! Canonical content snapshot descriptors, validation, and DAG-CBOR encoding.

use std::collections::BTreeSet;

use cid::Cid;
use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationId,
    RevisionBinding, RevisionId, SemanticSnapshot,
};
use serde::{Deserialize, Serialize};

use crate::profile::{cid_for, validate_cid};
use crate::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, ARROW_IPC_FILE_FORMAT, CID_VERSION_V1,
    DAG_CBOR_CODEC, DAG_CBOR_CODEC_NAME, DataError, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, RAW_CODEC, RAW_CODEC_NAME, SHA2_256_NAME,
    SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};

/// One immutable Arrow IPC child block and its declared extent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchDescriptor {
    cid: Cid,
    row_count: u64,
    byte_length: u64,
}

impl BatchDescriptor {
    /// Admits one non-empty raw/SHA-256 child descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the CID profile is not V1 raw/SHA-256 or the
    /// declared payload length is zero.
    pub fn new(cid: Cid, row_count: u64, byte_length: u64) -> Result<Self, DataError> {
        validate_cid(&cid, RAW_CODEC)?;
        if byte_length == 0 {
            return Err(DataError::EmptyPayload(Box::new(cid)));
        }
        Ok(Self {
            cid,
            row_count,
            byte_length,
        })
    }

    #[must_use]
    pub const fn cid(&self) -> &Cid {
        &self.cid
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }
}

/// All physical Arrow child blocks for one MRR relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationDescriptor {
    relation_id: RelationId,
    row_count: u64,
    batches: Vec<BatchDescriptor>,
}

impl RelationDescriptor {
    /// Admits ordered child batches whose rows exactly cover the relation total.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] for duplicate children, arithmetic overflow, or a
    /// declared total different from the sum of child row counts.
    pub fn new(
        relation_id: RelationId,
        row_count: u64,
        batches: Vec<BatchDescriptor>,
    ) -> Result<Self, DataError> {
        let mut child_cids = BTreeSet::new();
        let actual = batches.iter().try_fold(0_u64, |total, batch| {
            if !child_cids.insert(batch.cid) {
                return Err(DataError::DuplicateChild(Box::new(batch.cid)));
            }
            total
                .checked_add(batch.row_count)
                .ok_or(DataError::RowCountOverflow(relation_id))
        })?;
        if actual != row_count {
            return Err(DataError::BatchRowsMismatch {
                relation: relation_id,
                declared: row_count,
                actual,
            });
        }
        Ok(Self {
            relation_id,
            row_count,
            batches,
        })
    }

    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    #[must_use]
    pub fn batches(&self) -> &[BatchDescriptor] {
        &self.batches
    }
}

/// Declared evidence coverage of one physical snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageKind {
    Complete,
    Partial,
    Unknown,
}

/// Coverage declaration bound to its immutable evidence block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageDescriptor {
    kind: CoverageKind,
    declaration_cid: Cid,
}

impl CoverageDescriptor {
    /// Admits a coverage kind bound to one raw/SHA-256 declaration block.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the declaration CID does not use the child
    /// content identity profile.
    pub fn new(kind: CoverageKind, declaration_cid: Cid) -> Result<Self, DataError> {
        validate_cid(&declaration_cid, RAW_CODEC)?;
        Ok(Self {
            kind,
            declaration_cid,
        })
    }

    #[must_use]
    pub const fn kind(&self) -> CoverageKind {
        self.kind
    }

    #[must_use]
    pub const fn declaration_cid(&self) -> &Cid {
        &self.declaration_cid
    }
}

/// Optional `GraphAr` projection attached to the same semantic snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphProjectionDescriptor {
    graphar_version: String,
    manifest_cid: Cid,
}

impl GraphProjectionDescriptor {
    /// Admits an explicitly versioned `GraphAr` projection manifest.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] for an empty or padded version and for a manifest
    /// CID outside the raw/SHA-256 child profile.
    pub fn new(graphar_version: impl Into<String>, manifest_cid: Cid) -> Result<Self, DataError> {
        let graphar_version = graphar_version.into();
        if graphar_version.is_empty() || graphar_version.trim() != graphar_version {
            return Err(DataError::InvalidGraphArVersion);
        }
        validate_cid(&manifest_cid, RAW_CODEC)?;
        Ok(Self {
            graphar_version,
            manifest_cid,
        })
    }

    #[must_use]
    pub fn graphar_version(&self) -> &str {
        &self.graphar_version
    }

    #[must_use]
    pub const fn manifest_cid(&self) -> &Cid {
        &self.manifest_cid
    }
}

/// Named inputs admitted into a canonical content snapshot manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifestRequest {
    semantic_snapshot: SemanticSnapshot,
    relation_catalog_digest: [u8; 32],
    entity_catalog_digest: [u8; 32],
    catalog_relation_ids: Vec<RelationId>,
    relations: Vec<RelationDescriptor>,
    graph_projection: Option<GraphProjectionDescriptor>,
    lineage_batch_cids: Vec<Cid>,
    coverage: CoverageDescriptor,
}

impl SnapshotManifestRequest {
    /// Creates the required V1 inputs; optional projections and lineage blocks start absent.
    #[must_use]
    pub fn new(
        semantic_snapshot: SemanticSnapshot,
        relation_catalog: &RelationCatalog,
        entity_catalog: &EntityCatalog,
        relations: Vec<RelationDescriptor>,
        coverage: CoverageDescriptor,
    ) -> Self {
        Self {
            semantic_snapshot,
            relation_catalog_digest: *relation_catalog.digest().as_bytes(),
            entity_catalog_digest: *entity_catalog.digest().as_bytes(),
            catalog_relation_ids: relation_catalog
                .relations()
                .iter()
                .map(meta_relational_reasoning::RelationSchema::id)
                .collect(),
            relations,
            graph_projection: None,
            lineage_batch_cids: Vec::new(),
            coverage,
        }
    }

    /// Attaches the optional, explicitly profiled `GraphAr` projection.
    #[must_use]
    pub fn with_graph_projection(mut self, graph_projection: GraphProjectionDescriptor) -> Self {
        self.graph_projection = Some(graph_projection);
        self
    }

    /// Attaches unordered lineage blocks; admission canonicalizes their order.
    #[must_use]
    pub fn with_lineage_batch_cids(mut self, lineage_batch_cids: Vec<Cid>) -> Self {
        self.lineage_batch_cids = lineage_batch_cids;
        self
    }
}

/// Validated semantic and physical bindings encoded by `mrr.data.snapshot` V1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    semantic_snapshot: SemanticSnapshot,
    relation_catalog_digest: [u8; 32],
    entity_catalog_digest: [u8; 32],
    relations: Vec<RelationDescriptor>,
    graph_projection: Option<GraphProjectionDescriptor>,
    lineage_batch_cids: Vec<Cid>,
    coverage: CoverageDescriptor,
}

impl SnapshotManifest {
    /// Validates and canonicalizes one named snapshot request.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when relations or lineage children are empty,
    /// duplicated, inconsistent, or outside the frozen V1 profiles.
    pub fn admit(mut request: SnapshotManifestRequest) -> Result<Self, DataError> {
        validate_relations(&mut request.relations)?;
        if request
            .relations
            .iter()
            .map(RelationDescriptor::relation_id)
            .ne(request.catalog_relation_ids)
        {
            return Err(DataError::RelationSetMismatch);
        }
        validate_and_sort_lineage(&mut request.lineage_batch_cids)?;
        Ok(Self {
            semantic_snapshot: request.semantic_snapshot,
            relation_catalog_digest: request.relation_catalog_digest,
            entity_catalog_digest: request.entity_catalog_digest,
            relations: request.relations,
            graph_projection: request.graph_projection,
            lineage_batch_cids: request.lineage_batch_cids,
            coverage: request.coverage,
        })
    }

    #[must_use]
    pub const fn semantic_snapshot(&self) -> &SemanticSnapshot {
        &self.semantic_snapshot
    }

    #[must_use]
    pub const fn relation_catalog_digest(&self) -> &[u8; 32] {
        &self.relation_catalog_digest
    }

    #[must_use]
    pub const fn entity_catalog_digest(&self) -> &[u8; 32] {
        &self.entity_catalog_digest
    }

    #[must_use]
    pub fn relations(&self) -> &[RelationDescriptor] {
        &self.relations
    }

    #[must_use]
    pub const fn graph_projection(&self) -> Option<&GraphProjectionDescriptor> {
        self.graph_projection.as_ref()
    }

    #[must_use]
    pub fn lineage_batch_cids(&self) -> &[Cid] {
        &self.lineage_batch_cids
    }

    #[must_use]
    pub const fn coverage(&self) -> &CoverageDescriptor {
        &self.coverage
    }

    /// Returns the unique physical child CIDs reachable from this root.
    ///
    /// The result is sorted by CID bytes so callers can compare closure sets
    /// without depending on CAR block order.
    #[must_use]
    pub fn referenced_cids(&self) -> Vec<Cid> {
        let mut cids = BTreeSet::new();
        cids.extend(
            self.relations
                .iter()
                .flat_map(RelationDescriptor::batches)
                .map(|batch| *batch.cid()),
        );
        cids.extend(self.lineage_batch_cids.iter().copied());
        cids.insert(*self.coverage.declaration_cid());
        if let Some(graph) = &self.graph_projection {
            cids.insert(*graph.manifest_cid());
        }
        cids.into_iter().collect()
    }

    /// Verifies that externally resolved MRR catalogs exactly match this manifest.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when a catalog digest or its complete relation set
    /// differs from the immutable manifest binding.
    pub fn verify_catalogs(
        &self,
        relation_catalog: &RelationCatalog,
        entity_catalog: &EntityCatalog,
    ) -> Result<(), DataError> {
        if relation_catalog.digest().as_bytes() != &self.relation_catalog_digest {
            return Err(DataError::RelationCatalogMismatch);
        }
        if entity_catalog.digest().as_bytes() != &self.entity_catalog_digest {
            return Err(DataError::EntityCatalogMismatch);
        }
        if self
            .relations
            .iter()
            .map(RelationDescriptor::relation_id)
            .ne(relation_catalog
                .relations()
                .iter()
                .map(meta_relational_reasoning::RelationSchema::id))
        {
            return Err(DataError::RelationSetMismatch);
        }
        Ok(())
    }

    /// Encodes the admitted manifest using strict canonical DAG-CBOR.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] if the upstream canonical encoder rejects a field.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DataError> {
        serde_ipld_dagcbor::to_vec(&ManifestWire::from_manifest(self))
            .map_err(|error| DataError::Encode(error.to_string()))
    }

    /// Decodes a canonical V1 DAG-CBOR manifest and revalidates every binding.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] for malformed or non-canonical bytes, unknown
    /// schema/profile values, tampered MRR bindings, or inconsistent children.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, DataError> {
        let wire: ManifestWire = serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|error| DataError::Decode(error.to_string()))?;
        let manifest = wire.into_manifest()?;
        if manifest.canonical_bytes()? != bytes {
            return Err(DataError::NonCanonicalEncoding);
        }
        Ok(manifest)
    }

    /// Decodes canonical bytes only after verifying their expected root CID.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the expected CID has the wrong profile, the
    /// bytes hash to another root, or manifest admission fails.
    pub fn decode_checked(bytes: &[u8], expected: &Cid) -> Result<Self, DataError> {
        validate_cid(expected, DAG_CBOR_CODEC)?;
        let actual = cid_for(DAG_CBOR_CODEC, bytes);
        if actual != *expected {
            return Err(DataError::CidMismatch {
                expected: Box::new(*expected),
                actual: Box::new(actual),
            });
        }
        Self::decode_canonical(bytes)
    }
}

/// Canonical DAG-CBOR bytes and their verified root CID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBlock {
    manifest: SnapshotManifest,
    bytes: Vec<u8>,
    cid: Cid,
}

impl SnapshotBlock {
    /// Encodes an admitted manifest and computes its V1 DAG-CBOR root CID.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] if canonical DAG-CBOR encoding fails.
    pub fn encode(manifest: SnapshotManifest) -> Result<Self, DataError> {
        let bytes = manifest.canonical_bytes()?;
        let cid = cid_for(DAG_CBOR_CODEC, &bytes);
        Ok(Self {
            manifest,
            bytes,
            cid,
        })
    }

    #[must_use]
    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub const fn cid(&self) -> &Cid {
        &self.cid
    }
}

#[must_use]
/// Computes a `CIDv1` raw/SHA-256 address for an opaque child payload.
pub fn raw_cid(bytes: &[u8]) -> Cid {
    cid_for(RAW_CODEC, bytes)
}

fn validate_relations(relations: &mut [RelationDescriptor]) -> Result<(), DataError> {
    if relations.is_empty() {
        return Err(DataError::EmptyRelations);
    }
    relations.sort_by_key(RelationDescriptor::relation_id);
    for pair in relations.windows(2) {
        if pair[0].relation_id == pair[1].relation_id {
            return Err(DataError::DuplicateRelation(pair[0].relation_id));
        }
    }
    let mut child_cids = BTreeSet::new();
    for batch in relations.iter().flat_map(RelationDescriptor::batches) {
        if !child_cids.insert(batch.cid) {
            return Err(DataError::DuplicateChild(Box::new(batch.cid)));
        }
    }
    Ok(())
}

fn validate_and_sort_lineage(cids: &mut [Cid]) -> Result<(), DataError> {
    for cid in cids.iter() {
        validate_cid(cid, RAW_CODEC)?;
    }
    cids.sort_unstable();
    for pair in cids.windows(2) {
        if pair[0] == pair[1] {
            return Err(DataError::DuplicateChild(Box::new(pair[0])));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    schema: SchemaWire,
    semantic: SemanticWire,
    source: SourceWire,
    relations: Vec<RelationWire>,
    graph_projection: Option<GraphProjectionWire>,
    lineage: LineageWire,
    coverage: CoverageWire,
    integrity: IntegrityWire,
}

impl ManifestWire {
    fn from_manifest(manifest: &SnapshotManifest) -> Self {
        Self {
            schema: SchemaWire::snapshot_v1(),
            semantic: SemanticWire {
                generation_id: manifest.semantic_snapshot.generation(),
                semantic_snapshot_digest: manifest.semantic_snapshot.digest().to_vec(),
                relation_catalog_digest: manifest.relation_catalog_digest.to_vec(),
                entity_catalog_digest: manifest.entity_catalog_digest.to_vec(),
            },
            source: SourceWire {
                revision_bindings: manifest
                    .semantic_snapshot
                    .revisions()
                    .iter()
                    .map(SourceRevisionWire::from_binding)
                    .collect(),
            },
            relations: manifest.relations.iter().map(RelationWire::from).collect(),
            graph_projection: manifest
                .graph_projection
                .as_ref()
                .map(GraphProjectionWire::from),
            lineage: LineageWire {
                batch_cids: manifest.lineage_batch_cids.clone(),
            },
            coverage: CoverageWire::from(&manifest.coverage),
            integrity: IntegrityWire::v1(),
        }
    }

    fn into_manifest(self) -> Result<SnapshotManifest, DataError> {
        self.schema.validate_snapshot()?;
        self.integrity.validate()?;
        let semantic_snapshot = self.source.into_snapshot(self.semantic.generation_id)?;
        if semantic_snapshot.digest()
            != digest(
                &self.semantic.semantic_snapshot_digest,
                "semantic_snapshot_digest",
            )?
        {
            return Err(DataError::SemanticSnapshotDigestMismatch);
        }
        let relation_catalog_digest = *digest(
            &self.semantic.relation_catalog_digest,
            "relation_catalog_digest",
        )?;
        let entity_catalog_digest = *digest(
            &self.semantic.entity_catalog_digest,
            "entity_catalog_digest",
        )?;
        let relations = self
            .relations
            .into_iter()
            .map(RelationWire::into_descriptor)
            .collect::<Result<Vec<_>, _>>()?;
        if relations
            .windows(2)
            .any(|pair| pair[0].relation_id >= pair[1].relation_id)
        {
            return Err(DataError::NonCanonicalRelationOrder);
        }
        if relations.is_empty() {
            return Err(DataError::EmptyRelations);
        }
        let mut lineage_batch_cids = self.lineage.batch_cids;
        if lineage_batch_cids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DataError::NonCanonicalLineageOrder);
        }
        for cid in &lineage_batch_cids {
            validate_cid(cid, RAW_CODEC)?;
        }
        Ok(SnapshotManifest {
            semantic_snapshot,
            relation_catalog_digest,
            entity_catalog_digest,
            relations,
            graph_projection: self
                .graph_projection
                .map(GraphProjectionWire::into_descriptor)
                .transpose()?,
            lineage_batch_cids: std::mem::take(&mut lineage_batch_cids),
            coverage: self.coverage.into_descriptor()?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SchemaWire {
    namespace: String,
    version: u64,
}

impl SchemaWire {
    fn snapshot_v1() -> Self {
        Self {
            namespace: SNAPSHOT_SCHEMA_NAMESPACE.to_owned(),
            version: SNAPSHOT_SCHEMA_VERSION,
        }
    }

    fn validate_snapshot(self) -> Result<(), DataError> {
        if self.namespace != SNAPSHOT_SCHEMA_NAMESPACE {
            return Err(DataError::UnknownSchemaNamespace(self.namespace));
        }
        if self.version != SNAPSHOT_SCHEMA_VERSION {
            return Err(DataError::UnknownSchemaVersion(self.version));
        }
        Ok(())
    }

    fn arrow_fact_v1() -> Self {
        Self {
            namespace: ARROW_FACT_SCHEMA_NAMESPACE.to_owned(),
            version: ARROW_FACT_SCHEMA_VERSION,
        }
    }

    fn validate_arrow_fact(self) -> Result<(), DataError> {
        if self.namespace != ARROW_FACT_SCHEMA_NAMESPACE {
            return Err(DataError::UnknownArrowSchemaNamespace(self.namespace));
        }
        if self.version != ARROW_FACT_SCHEMA_VERSION {
            return Err(DataError::UnknownArrowSchemaVersion(self.version));
        }
        Ok(())
    }

    fn graphar_binary_entity_v1() -> Self {
        Self {
            namespace: GRAPHAR_BINARY_ENTITY_NAMESPACE.to_owned(),
            version: GRAPHAR_BINARY_ENTITY_VERSION,
        }
    }

    fn validate_graphar_binary_entity(self) -> Result<(), DataError> {
        if self.namespace != GRAPHAR_BINARY_ENTITY_NAMESPACE {
            return Err(DataError::UnknownGraphProjectionNamespace(self.namespace));
        }
        if self.version != GRAPHAR_BINARY_ENTITY_VERSION {
            return Err(DataError::UnknownGraphProjectionVersion(self.version));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SemanticWire {
    generation_id: GenerationId,
    #[serde(with = "serde_bytes")]
    semantic_snapshot_digest: Vec<u8>,
    #[serde(with = "serde_bytes")]
    relation_catalog_digest: Vec<u8>,
    #[serde(with = "serde_bytes")]
    entity_catalog_digest: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceWire {
    revision_bindings: Vec<SourceRevisionWire>,
}

impl SourceWire {
    fn into_snapshot(self, generation: GenerationId) -> Result<SemanticSnapshot, DataError> {
        let revisions = self
            .revision_bindings
            .into_iter()
            .map(|wire| wire.into_binding(generation))
            .collect::<Result<Vec<_>, _>>()?;
        SemanticSnapshot::admit(generation, revisions)
            .map_err(|error| DataError::InvalidSource(format!("{error:?}")))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceRevisionWire {
    revision_id: RevisionId,
    provider: String,
    logical_change: String,
    content_revision: String,
}

impl SourceRevisionWire {
    fn from_binding(binding: &RevisionBinding) -> Self {
        let external = binding.external();
        Self {
            revision_id: binding.revision(),
            provider: external.provider().to_owned(),
            logical_change: external.logical_change().to_owned(),
            content_revision: external.content_revision().to_owned(),
        }
    }

    fn into_binding(self, generation: GenerationId) -> Result<RevisionBinding, DataError> {
        let external = ExternalRevisionIdentity::new(
            self.provider,
            self.logical_change,
            self.content_revision,
        )
        .map_err(|error| DataError::InvalidSource(format!("{error:?}")))?;
        let binding = RevisionBinding::admit(external, generation)
            .map_err(|error| DataError::InvalidSource(format!("{error:?}")))?;
        if binding.revision() != self.revision_id {
            return Err(DataError::RevisionIdentityMismatch {
                declared: self.revision_id,
                derived: binding.revision(),
            });
        }
        Ok(binding)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RelationWire {
    relation_id: RelationId,
    arrow_schema: SchemaWire,
    row_count: u64,
    batches: Vec<BatchWire>,
}

impl From<&RelationDescriptor> for RelationWire {
    fn from(descriptor: &RelationDescriptor) -> Self {
        Self {
            relation_id: descriptor.relation_id,
            arrow_schema: SchemaWire::arrow_fact_v1(),
            row_count: descriptor.row_count,
            batches: descriptor.batches.iter().map(BatchWire::from).collect(),
        }
    }
}

impl RelationWire {
    fn into_descriptor(self) -> Result<RelationDescriptor, DataError> {
        self.arrow_schema.validate_arrow_fact()?;
        RelationDescriptor::new(
            self.relation_id,
            self.row_count,
            self.batches
                .into_iter()
                .map(BatchWire::into_descriptor)
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BatchWire {
    cid: Cid,
    cid_codec: String,
    format: String,
    row_count: u64,
    byte_length: u64,
}

impl From<&BatchDescriptor> for BatchWire {
    fn from(descriptor: &BatchDescriptor) -> Self {
        Self {
            cid: descriptor.cid,
            cid_codec: RAW_CODEC_NAME.to_owned(),
            format: ARROW_IPC_FILE_FORMAT.to_owned(),
            row_count: descriptor.row_count,
            byte_length: descriptor.byte_length,
        }
    }
}

impl BatchWire {
    fn into_descriptor(self) -> Result<BatchDescriptor, DataError> {
        if self.cid_codec != RAW_CODEC_NAME {
            return Err(DataError::UnknownChildCodec(self.cid_codec));
        }
        if self.format != ARROW_IPC_FILE_FORMAT {
            return Err(DataError::UnknownPayloadFormat(self.format));
        }
        BatchDescriptor::new(self.cid, self.row_count, self.byte_length)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct GraphProjectionWire {
    schema: SchemaWire,
    graphar_version: String,
    manifest_cid: Cid,
}

impl From<&GraphProjectionDescriptor> for GraphProjectionWire {
    fn from(descriptor: &GraphProjectionDescriptor) -> Self {
        Self {
            schema: SchemaWire::graphar_binary_entity_v1(),
            graphar_version: descriptor.graphar_version.clone(),
            manifest_cid: descriptor.manifest_cid,
        }
    }
}

impl GraphProjectionWire {
    fn into_descriptor(self) -> Result<GraphProjectionDescriptor, DataError> {
        self.schema.validate_graphar_binary_entity()?;
        GraphProjectionDescriptor::new(self.graphar_version, self.manifest_cid)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LineageWire {
    batch_cids: Vec<Cid>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CoverageWire {
    kind: CoverageKindWire,
    declaration_cid: Cid,
}

impl From<&CoverageDescriptor> for CoverageWire {
    fn from(descriptor: &CoverageDescriptor) -> Self {
        Self {
            kind: match descriptor.kind {
                CoverageKind::Complete => CoverageKindWire::Complete,
                CoverageKind::Partial => CoverageKindWire::Partial,
                CoverageKind::Unknown => CoverageKindWire::Unknown,
            },
            declaration_cid: descriptor.declaration_cid,
        }
    }
}

impl CoverageWire {
    fn into_descriptor(self) -> Result<CoverageDescriptor, DataError> {
        let kind = match self.kind {
            CoverageKindWire::Complete => CoverageKind::Complete,
            CoverageKindWire::Partial => CoverageKind::Partial,
            CoverageKindWire::Unknown => CoverageKind::Unknown,
        };
        CoverageDescriptor::new(kind, self.declaration_cid)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CoverageKindWire {
    Complete,
    Partial,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IntegrityWire {
    cid_version: u64,
    manifest_codec: String,
    multihash: String,
}

impl IntegrityWire {
    fn v1() -> Self {
        Self {
            cid_version: CID_VERSION_V1,
            manifest_codec: DAG_CBOR_CODEC_NAME.to_owned(),
            multihash: SHA2_256_NAME.to_owned(),
        }
    }

    fn validate(self) -> Result<(), DataError> {
        if self.cid_version != CID_VERSION_V1 {
            return Err(DataError::UnknownCidVersion(self.cid_version));
        }
        if self.manifest_codec != DAG_CBOR_CODEC_NAME {
            return Err(DataError::UnknownManifestCodec(self.manifest_codec));
        }
        if self.multihash != SHA2_256_NAME {
            return Err(DataError::UnknownMultihash(self.multihash));
        }
        Ok(())
    }
}

fn digest<'a>(bytes: &'a [u8], field: &'static str) -> Result<&'a [u8; 32], DataError> {
    bytes
        .try_into()
        .map_err(|_| DataError::InvalidDigestLength {
            field,
            actual: bytes.len(),
        })
}
