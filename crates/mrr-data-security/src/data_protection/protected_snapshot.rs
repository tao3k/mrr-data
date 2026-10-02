//! Bounded protected snapshot staging, physical publication and restore.
//!
//! The caller supplies a Host-controlled ciphertext outbox distinct from the
//! plaintext source. The outbox and remote receive only randomized envelopes.
//! The private child mapping is returned to the Host for durable receipt
//! storage; it must never be written to a shared content cache in the clear.

use std::collections::BTreeMap;

use cid::Cid;
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_content::{
    AsyncContentStore, ContentBlock, ContentCodec, ContentError, ContentProtocolError,
    ContentStore, MemoryContentStore, RemoteContentStore, RemoteError, RestoredSnapshot,
    SnapshotTransferError, SnapshotTransferLimits, TransferSession, read_through,
    restore_snapshot_local,
};

use super::envelope::root_binding_digest;
use super::{
    CurrentStorageState, ProtectedBlockBinding, ProtectedBlockRole, ProtectedEnvelopeError,
    ProtectedEnvelopeKey, ProtectedPhysicalAck, ProtectedPublication, ProtectedReadClaim,
    ProtectedReadIntent, ProtectedReadMismatch, ProtectedStorageMismatch, ProtectionClaim,
    ProtectionIntent, RawStorageTier, open_block, seal_block,
};

#[derive(Debug)]
pub enum ProtectedSnapshotError {
    Admission(ProtectedStorageMismatch),
    Read(ProtectedReadMismatch),
    Envelope(ProtectedEnvelopeError),
    Inner(SnapshotTransferError),
    Transfer(SnapshotTransferError),
    Local(ContentError),
    Content(ContentProtocolError),
    Remote(RemoteError),
    WrongTier,
    WrongRoot,
    WrongBinding,
    InvalidManifest,
    MissingBlock(Cid),
    TooLarge,
}

impl std::fmt::Display for ProtectedSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "protected snapshot: {self:?}")
    }
}

impl std::error::Error for ProtectedSnapshotError {}

impl From<ProtectedEnvelopeError> for ProtectedSnapshotError {
    fn from(value: ProtectedEnvelopeError) -> Self {
        Self::Envelope(value)
    }
}

impl From<ContentError> for ProtectedSnapshotError {
    fn from(value: ContentError) -> Self {
        Self::Local(value)
    }
}

impl From<ContentProtocolError> for ProtectedSnapshotError {
    fn from(value: ContentProtocolError) -> Self {
        Self::Content(value)
    }
}

impl From<SnapshotTransferError> for ProtectedSnapshotError {
    fn from(value: SnapshotTransferError) -> Self {
        Self::Transfer(value)
    }
}

/// Private Host receipt of the staged protected closure. Persist the mapping
/// and intent before remote publication; only outer CIDs name provider bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedProtectedSnapshot {
    inner_root: Cid,
    outer_root: Cid,
    root_block_outer: Cid,
    manifest_plain_cid: Cid,
    child_roots: BTreeMap<Cid, Cid>,
    key_version: String,
    root_binding_digest: [u8; 32],
    total_outer_bytes: usize,
}

/// Owned receipt fields for Host-controlled durable storage. This record is
/// sensitive metadata: the caller must authenticate it and keep it outside
/// shared content caches. Deserializing it does not prove that ciphertext was
/// staged or that the current operation is authorized.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedSnapshotRecord {
    pub inner_root: Cid,
    pub outer_root: Cid,
    pub root_block_outer: Cid,
    pub manifest_plain_cid: Cid,
    pub child_roots: BTreeMap<Cid, Cid>,
    pub key_version: String,
    pub total_outer_bytes: usize,
}

impl PreparedProtectedSnapshot {
    /// Form the pure publication projection after verifying this prepared
    /// closure belongs to the supplied intent.
    /// # Errors
    /// Returns a root or intent-binding mismatch.
    pub fn publication<'a>(
        &'a self,
        intent: ProtectionIntent<'a>,
    ) -> Result<ProtectedPublication<'a>, ProtectedSnapshotError> {
        check_prepared(intent, self)?;
        Ok(ProtectedPublication {
            intent,
            outer_root: &self.outer_root,
            envelope_version: 1,
            key_version: &self.key_version,
        })
    }

    /// Export the Host-owned fields needed to resume publish or restore after
    /// process restart. The Host chooses its authenticated storage format.
    #[must_use]
    pub fn host_record(&self) -> ProtectedSnapshotRecord {
        ProtectedSnapshotRecord {
            inner_root: self.inner_root,
            outer_root: self.outer_root,
            root_block_outer: self.root_block_outer,
            manifest_plain_cid: self.manifest_plain_cid,
            child_roots: self.child_roots.clone(),
            key_version: self.key_version.clone(),
            total_outer_bytes: self.total_outer_bytes,
        }
    }

    /// Rebuild an in-memory handle from a Host-authenticated durable record.
    /// The caller must authenticate the record; publish checks current policy
    /// and staged outer CIDs, while restore authenticates every envelope.
    /// # Errors
    /// Returns an identity, version or malformed-record error.
    pub fn from_authenticated_record(
        intent: ProtectionIntent<'_>,
        record: ProtectedSnapshotRecord,
    ) -> Result<Self, ProtectedSnapshotError> {
        if record.inner_root != *intent.storage.snapshot_root {
            return Err(ProtectedSnapshotError::WrongRoot);
        }
        if record.key_version != intent.key_version {
            return Err(ProtectedSnapshotError::WrongBinding);
        }
        if record.outer_root == record.inner_root
            || record.outer_root == record.root_block_outer
            || record.root_block_outer == record.inner_root
            || record.child_roots.len() > MAX_MANIFEST_CHILDREN
            || record.total_outer_bytes == 0
            || record.child_roots.iter().any(|(inner, outer)| {
                *inner == record.inner_root
                    || *outer == record.outer_root
                    || *outer == record.root_block_outer
            })
        {
            return Err(ProtectedSnapshotError::InvalidManifest);
        }
        let root_binding_digest = root_binding_digest(intent, &record.key_version)?;
        Ok(Self {
            inner_root: record.inner_root,
            outer_root: record.outer_root,
            root_block_outer: record.root_block_outer,
            manifest_plain_cid: record.manifest_plain_cid,
            child_roots: record.child_roots,
            key_version: record.key_version,
            root_binding_digest,
            total_outer_bytes: record.total_outer_bytes,
        })
    }

    #[must_use]
    pub const fn inner_root(&self) -> &Cid {
        &self.inner_root
    }
    #[must_use]
    pub const fn outer_root(&self) -> &Cid {
        &self.outer_root
    }
    #[must_use]
    pub const fn root_block_outer(&self) -> &Cid {
        &self.root_block_outer
    }
    #[must_use]
    pub const fn manifest_plain_cid(&self) -> &Cid {
        &self.manifest_plain_cid
    }
    #[must_use]
    pub const fn child_roots(&self) -> &BTreeMap<Cid, Cid> {
        &self.child_roots
    }
    #[must_use]
    pub fn key_version(&self) -> &str {
        &self.key_version
    }
    #[must_use]
    pub const fn total_outer_bytes(&self) -> usize {
        self.total_outer_bytes
    }
}

pub struct ProtectedStage<'a> {
    pub intent: ProtectionIntent<'a>,
    pub claim: &'a ProtectionClaim<'a>,
    pub current: CurrentStorageState<'a>,
    pub source: &'a dyn AsyncContentStore,
    pub outbox: &'a dyn AsyncContentStore,
    pub relations: &'a RelationCatalog,
    pub entities: &'a EntityCatalog,
    pub inner_limits: SnapshotTransferLimits,
    pub max_outer_block_bytes: usize,
    pub max_outer_total_bytes: usize,
    pub key: &'a ProtectedEnvelopeKey,
}

fn add_outer(total: &mut usize, bytes: usize, limit: usize) -> Result<(), ProtectedSnapshotError> {
    *total = total
        .checked_add(bytes)
        .filter(|sum| *sum <= limit)
        .ok_or(ProtectedSnapshotError::TooLarge)?;
    Ok(())
}

fn add_restored_outer(
    total: &mut usize,
    bytes: usize,
    caller_limit: usize,
    committed_total: usize,
) -> Result<(), ProtectedSnapshotError> {
    add_outer(total, bytes, caller_limit)?;
    if *total > committed_total {
        return Err(ProtectedSnapshotError::WrongBinding);
    }
    Ok(())
}

const MANIFEST_MAGIC: &[u8; 6] = b"MRRPM1";
const MAX_MANIFEST_CHILDREN: usize = 4096;

fn write_cid(output: &mut Vec<u8>, cid: &Cid) -> Result<(), ProtectedSnapshotError> {
    let text = cid.to_string();
    let len = u16::try_from(text.len()).map_err(|_| ProtectedSnapshotError::InvalidManifest)?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(text.as_bytes());
    Ok(())
}

fn encode_manifest(
    inner_root: &Cid,
    root_block_outer: &Cid,
    children: &BTreeMap<Cid, Cid>,
) -> Result<Vec<u8>, ProtectedSnapshotError> {
    if children.len() > MAX_MANIFEST_CHILDREN {
        return Err(ProtectedSnapshotError::TooLarge);
    }
    let mut bytes = MANIFEST_MAGIC.to_vec();
    write_cid(&mut bytes, inner_root)?;
    write_cid(&mut bytes, root_block_outer)?;
    bytes.extend_from_slice(
        &u32::try_from(children.len())
            .map_err(|_| ProtectedSnapshotError::TooLarge)?
            .to_be_bytes(),
    );
    for (inner, outer) in children {
        write_cid(&mut bytes, inner)?;
        write_cid(&mut bytes, outer)?;
    }
    Ok(bytes)
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], ProtectedSnapshotError> {
    let end = offset
        .checked_add(length)
        .ok_or(ProtectedSnapshotError::InvalidManifest)?;
    let value = bytes
        .get(*offset..end)
        .ok_or(ProtectedSnapshotError::InvalidManifest)?;
    *offset = end;
    Ok(value)
}

fn read_cid(bytes: &[u8], offset: &mut usize) -> Result<Cid, ProtectedSnapshotError> {
    let size: [u8; 2] = take(bytes, offset, 2)?
        .try_into()
        .map_err(|_| ProtectedSnapshotError::InvalidManifest)?;
    let text = std::str::from_utf8(take(bytes, offset, usize::from(u16::from_be_bytes(size)))?)
        .map_err(|_| ProtectedSnapshotError::InvalidManifest)?;
    let cid = Cid::try_from(text).map_err(|_| ProtectedSnapshotError::InvalidManifest)?;
    if cid.to_string() != text {
        return Err(ProtectedSnapshotError::InvalidManifest);
    }
    Ok(cid)
}

fn decode_manifest(bytes: &[u8]) -> Result<(Cid, Cid, BTreeMap<Cid, Cid>), ProtectedSnapshotError> {
    let mut offset = 0;
    if take(bytes, &mut offset, MANIFEST_MAGIC.len())? != MANIFEST_MAGIC {
        return Err(ProtectedSnapshotError::InvalidManifest);
    }
    let inner_root = read_cid(bytes, &mut offset)?;
    let root_block_outer = read_cid(bytes, &mut offset)?;
    let size: [u8; 4] = take(bytes, &mut offset, 4)?
        .try_into()
        .map_err(|_| ProtectedSnapshotError::InvalidManifest)?;
    let count = usize::try_from(u32::from_be_bytes(size))
        .map_err(|_| ProtectedSnapshotError::InvalidManifest)?;
    if count > MAX_MANIFEST_CHILDREN {
        return Err(ProtectedSnapshotError::TooLarge);
    }
    let mut children = BTreeMap::new();
    for _ in 0..count {
        let inner = read_cid(bytes, &mut offset)?;
        let outer = read_cid(bytes, &mut offset)?;
        if inner == inner_root || children.insert(inner, outer).is_some() {
            return Err(ProtectedSnapshotError::InvalidManifest);
        }
    }
    if offset != bytes.len() {
        return Err(ProtectedSnapshotError::InvalidManifest);
    }
    Ok((inner_root, root_block_outer, children))
}

async fn store_block(
    outbox: &dyn AsyncContentStore,
    block: &super::ProtectedBlock,
) -> Result<(), ProtectedSnapshotError> {
    let actual = outbox.store(block.content()).await?;
    if actual != *block.outer_cid() {
        return Err(ProtectedSnapshotError::WrongRoot);
    }
    Ok(())
}

/// Verify a complete inner closure, encrypt each child with a fresh nonce and
/// stage only ciphertext in the Host-selected outbox. The protected root is
/// staged last. A failed stage may leave orphan ciphertext for Host cleanup.
/// # Errors
/// Returns admission, inner-integrity, encryption or local persistence errors.
pub async fn stage_protected_snapshot(
    stage: ProtectedStage<'_>,
) -> Result<PreparedProtectedSnapshot, ProtectedSnapshotError> {
    stage
        .intent
        .check_intent(stage.claim, stage.current)
        .map_err(ProtectedSnapshotError::Admission)?;
    let inner_root = *stage.intent.storage.snapshot_root;
    let restored = restore_snapshot_local(
        stage.source,
        &inner_root,
        stage.relations,
        stage.entities,
        stage.inner_limits,
    )
    .await
    .map_err(ProtectedSnapshotError::Inner)?;
    if restored.snapshot().cid() != &inner_root {
        return Err(ProtectedSnapshotError::WrongRoot);
    }
    let mut total = 0;
    let mut children = BTreeMap::new();
    for (inner, bytes) in restored.children() {
        let protected = seal_block(
            ProtectedBlockBinding {
                intent: stage.intent,
                inner_cid: inner,
                role: ProtectedBlockRole::Child,
                key_version: stage.intent.key_version,
            },
            bytes,
            stage.key,
            stage.max_outer_block_bytes,
        )?;
        if protected.bytes().len() > stage.max_outer_block_bytes {
            return Err(ProtectedSnapshotError::TooLarge);
        }
        add_outer(
            &mut total,
            protected.bytes().len(),
            stage.max_outer_total_bytes,
        )?;
        store_block(stage.outbox, &protected).await?;
        children.insert(*inner, *protected.outer_cid());
    }
    let protected_root = seal_block(
        ProtectedBlockBinding {
            intent: stage.intent,
            inner_cid: &inner_root,
            role: ProtectedBlockRole::Root,
            key_version: stage.intent.key_version,
        },
        restored.snapshot().bytes(),
        stage.key,
        stage.max_outer_block_bytes,
    )?;
    if protected_root.bytes().len() > stage.max_outer_block_bytes {
        return Err(ProtectedSnapshotError::TooLarge);
    }
    add_outer(
        &mut total,
        protected_root.bytes().len(),
        stage.max_outer_total_bytes,
    )?;
    store_block(stage.outbox, &protected_root).await?;
    let root_block_outer = *protected_root.outer_cid();
    let manifest_bytes = encode_manifest(&inner_root, &root_block_outer, &children)?;
    let manifest_plain_cid = ContentBlock::new(ContentCodec::Raw, &manifest_bytes).cid();
    let protected_manifest = seal_block(
        ProtectedBlockBinding {
            intent: stage.intent,
            inner_cid: &manifest_plain_cid,
            role: ProtectedBlockRole::Manifest,
            key_version: stage.intent.key_version,
        },
        &manifest_bytes,
        stage.key,
        stage.max_outer_block_bytes,
    )?;
    if protected_manifest.bytes().len() > stage.max_outer_block_bytes {
        return Err(ProtectedSnapshotError::TooLarge);
    }
    add_outer(
        &mut total,
        protected_manifest.bytes().len(),
        stage.max_outer_total_bytes,
    )?;
    store_block(stage.outbox, &protected_manifest).await?;
    Ok(PreparedProtectedSnapshot {
        inner_root,
        outer_root: *protected_manifest.outer_cid(),
        root_block_outer,
        manifest_plain_cid,
        child_roots: children,
        key_version: stage.intent.key_version.to_owned(),
        root_binding_digest: root_binding_digest(stage.intent, stage.intent.key_version)?,
        total_outer_bytes: total,
    })
}

fn check_prepared(
    intent: ProtectionIntent<'_>,
    prepared: &PreparedProtectedSnapshot,
) -> Result<(), ProtectedSnapshotError> {
    if prepared.inner_root != *intent.storage.snapshot_root {
        return Err(ProtectedSnapshotError::WrongRoot);
    }
    if prepared.root_binding_digest != root_binding_digest(intent, &prepared.key_version)? {
        return Err(ProtectedSnapshotError::WrongBinding);
    }
    Ok(())
}

pub struct ProtectedPublish<'a> {
    pub intent: ProtectionIntent<'a>,
    pub claim: &'a ProtectionClaim<'a>,
    pub current: CurrentStorageState<'a>,
    pub prepared: &'a PreparedProtectedSnapshot,
    pub key: &'a ProtectedEnvelopeKey,
    pub outbox: &'a dyn AsyncContentStore,
    pub remote: &'a dyn RemoteContentStore,
    pub session: &'a TransferSession,
    pub max_outer_block_bytes: usize,
}

/// Physical acknowledgement only. The Host must atomically redeem the
/// operation and persist its discoverability pointer and audit afterward.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectedPhysicalPublication {
    pub inner_root: Cid,
    pub outer_root: Cid,
    pub child_count: usize,
    pub total_outer_bytes: usize,
}

impl ProtectedPhysicalPublication {
    #[must_use]
    pub const fn as_ack(&self) -> ProtectedPhysicalAck<'_> {
        ProtectedPhysicalAck {
            inner_root: &self.inner_root,
            outer_root: &self.outer_root,
            child_count: self.child_count,
            total_outer_bytes: self.total_outer_bytes,
        }
    }
}

async fn load_outer(
    outbox: &dyn AsyncContentStore,
    cid: &Cid,
    limit: usize,
) -> Result<Vec<u8>, ProtectedSnapshotError> {
    let bytes = outbox.load(cid, limit).await?;
    if ContentBlock::new(ContentCodec::Raw, &bytes).cid() != *cid {
        return Err(ProtectedSnapshotError::WrongRoot);
    }
    Ok(bytes)
}

/// Upload staged ciphertext children, recheck current authority immediately
/// before the protected root, then upload that root. The `refresh` callback is
/// Host-owned and must read current governance rather than reuse the first
/// projection. The returned physical receipt is not a durable Host commit.
/// # Errors
/// Returns an admission, transport or stale-state error. Uploaded children
/// may remain as undiscoverable ciphertext after an error.
pub async fn publish_prepared_snapshot<'a, F, Fut>(
    publish: ProtectedPublish<'a>,
    refresh: F,
) -> Result<ProtectedPhysicalPublication, ProtectedSnapshotError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<CurrentStorageState<'a>, ProtectedSnapshotError>>,
{
    if publish.intent.storage.destination.tier != RawStorageTier::Remote {
        return Err(ProtectedSnapshotError::WrongTier);
    }
    publish
        .intent
        .check_intent(publish.claim, publish.current)
        .map_err(ProtectedSnapshotError::Admission)?;
    check_prepared(publish.intent, publish.prepared)?;
    let budgeted = publish.session.remote(publish.remote);
    let mut actual_total = 0_usize;
    for outer in publish.prepared.child_roots.values() {
        let bytes = load_outer(publish.outbox, outer, publish.max_outer_block_bytes).await?;
        add_outer(
            &mut actual_total,
            bytes.len(),
            publish.prepared.total_outer_bytes,
        )
        .map_err(|_| ProtectedSnapshotError::WrongBinding)?;
        budgeted
            .put(ContentBlock::new(ContentCodec::Raw, &bytes))
            .await
            .map_err(ProtectedSnapshotError::Remote)?;
    }
    let root_block_bytes = load_outer(
        publish.outbox,
        &publish.prepared.root_block_outer,
        publish.max_outer_block_bytes,
    )
    .await?;
    add_outer(
        &mut actual_total,
        root_block_bytes.len(),
        publish.prepared.total_outer_bytes,
    )
    .map_err(|_| ProtectedSnapshotError::WrongBinding)?;
    budgeted
        .put(ContentBlock::new(ContentCodec::Raw, &root_block_bytes))
        .await
        .map_err(ProtectedSnapshotError::Remote)?;
    let root_bytes = load_outer(
        publish.outbox,
        &publish.prepared.outer_root,
        publish.max_outer_block_bytes,
    )
    .await?;
    add_outer(
        &mut actual_total,
        root_bytes.len(),
        publish.prepared.total_outer_bytes,
    )
    .map_err(|_| ProtectedSnapshotError::WrongBinding)?;
    if actual_total != publish.prepared.total_outer_bytes {
        return Err(ProtectedSnapshotError::WrongBinding);
    }
    let manifest_plaintext = open_block(
        ProtectedBlockBinding {
            intent: publish.intent,
            inner_cid: &publish.prepared.manifest_plain_cid,
            role: ProtectedBlockRole::Manifest,
            key_version: &publish.prepared.key_version,
        },
        &publish.prepared.outer_root,
        &root_bytes,
        publish.key,
        publish.max_outer_block_bytes,
    )?;
    let (inner_root, root_block_outer, child_roots) = decode_manifest(&manifest_plaintext)?;
    if inner_root != publish.prepared.inner_root
        || root_block_outer != publish.prepared.root_block_outer
        || child_roots != publish.prepared.child_roots
    {
        return Err(ProtectedSnapshotError::InvalidManifest);
    }
    let publication = publish.prepared.publication(publish.intent)?;
    let refreshed = publish.session.run(async { refresh().await }).await?;
    publication
        .check_pre_root(publish.claim, refreshed)
        .map_err(ProtectedSnapshotError::Admission)?;
    publish
        .session
        .remote_once(publish.remote)
        .put(ContentBlock::new(ContentCodec::Raw, &root_bytes))
        .await
        .map_err(ProtectedSnapshotError::Remote)?;
    Ok(ProtectedPhysicalPublication {
        inner_root: publish.prepared.inner_root,
        outer_root: publish.prepared.outer_root,
        child_count: publish.prepared.child_roots.len(),
        total_outer_bytes: publish.prepared.total_outer_bytes,
    })
}

pub struct ProtectedRestore<'a> {
    pub read: ProtectedReadIntent<'a>,
    pub claim: &'a ProtectedReadClaim<'a>,
    pub current: CurrentStorageState<'a>,
    pub committed: Option<&'a super::ProtectedCommitReceipt<'a>>,
    pub prepared: &'a PreparedProtectedSnapshot,
    pub protected_cache: &'a dyn AsyncContentStore,
    pub remote: &'a dyn RemoteContentStore,
    pub session: &'a TransferSession,
    pub key: &'a ProtectedEnvelopeKey,
    pub relations: &'a RelationCatalog,
    pub entities: &'a EntityCatalog,
    pub inner_limits: SnapshotTransferLimits,
    pub max_outer_block_bytes: usize,
    pub max_outer_total_bytes: usize,
}

fn check_restore_binding<'a>(
    restore: &ProtectedRestore<'a>,
) -> Result<super::ProtectedCommitReceipt<'a>, ProtectedSnapshotError> {
    restore
        .read
        .check_read(restore.claim, restore.current, restore.committed)
        .map_err(ProtectedSnapshotError::Read)?;
    let receipt = restore.read.receipt;
    check_prepared(receipt.publication.intent, restore.prepared)?;
    if restore.prepared.outer_root != *receipt.publication.outer_root
        || restore.prepared.child_roots.len() != receipt.child_count
        || restore.prepared.total_outer_bytes != receipt.total_outer_bytes
    {
        return Err(ProtectedSnapshotError::WrongBinding);
    }
    Ok(receipt)
}

/// Restore ciphertext through a cache of outer bytes only, authenticate and
/// decrypt into a private in-memory store, then verify the complete original
/// snapshot closure. The caller supplies an authenticated committed row and
/// current read claim; their projected check runs before cache or provider GET.
/// `refresh` must independently obtain current authority and the committed
/// row after verification. Plaintext is returned only if both checks pass.
/// # Errors
/// Returns missing, tampered, oversized, stale or invalid closure errors.
pub async fn restore_protected_snapshot<'a, F, Fut>(
    restore: ProtectedRestore<'a>,
    refresh: F,
) -> Result<RestoredSnapshot, ProtectedSnapshotError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<
            Output = Result<
                (
                    CurrentStorageState<'a>,
                    Option<&'a super::ProtectedCommitReceipt<'a>>,
                ),
                ProtectedSnapshotError,
            >,
        >,
{
    let receipt = check_restore_binding(&restore)?;
    let intent = receipt.publication.intent;
    let memory = MemoryContentStore::default();
    let budgeted = restore.session.remote(restore.remote);
    let mut total = 0;
    let manifest_read = read_through(
        restore.protected_cache,
        &budgeted,
        &restore.prepared.outer_root,
        restore.max_outer_block_bytes,
    )
    .await?
    .ok_or(ProtectedSnapshotError::MissingBlock(
        restore.prepared.outer_root,
    ))?;
    add_restored_outer(
        &mut total,
        manifest_read.bytes.len(),
        restore.max_outer_total_bytes,
        receipt.total_outer_bytes,
    )?;
    let manifest_plaintext = open_block(
        ProtectedBlockBinding {
            intent,
            inner_cid: &restore.prepared.manifest_plain_cid,
            role: ProtectedBlockRole::Manifest,
            key_version: &restore.prepared.key_version,
        },
        &restore.prepared.outer_root,
        &manifest_read.bytes,
        restore.key,
        restore.max_outer_block_bytes,
    )?;
    let (inner_root, root_block_outer, child_roots) = decode_manifest(&manifest_plaintext)?;
    if inner_root != restore.prepared.inner_root
        || root_block_outer != restore.prepared.root_block_outer
        || child_roots != restore.prepared.child_roots
    {
        return Err(ProtectedSnapshotError::InvalidManifest);
    }
    let pairs = std::iter::once((
        restore.prepared.inner_root,
        restore.prepared.root_block_outer,
        ProtectedBlockRole::Root,
    ))
    .chain(
        restore
            .prepared
            .child_roots
            .iter()
            .map(|(inner, outer)| (*inner, *outer, ProtectedBlockRole::Child)),
    );
    for (inner, outer, role) in pairs {
        let read = read_through(
            restore.protected_cache,
            &budgeted,
            &outer,
            restore.max_outer_block_bytes,
        )
        .await?
        .ok_or(ProtectedSnapshotError::MissingBlock(outer))?;
        add_restored_outer(
            &mut total,
            read.bytes.len(),
            restore.max_outer_total_bytes,
            receipt.total_outer_bytes,
        )?;
        let plaintext = open_block(
            ProtectedBlockBinding {
                intent,
                inner_cid: &inner,
                role,
                key_version: &restore.prepared.key_version,
            },
            &outer,
            &read.bytes,
            restore.key,
            restore.max_outer_block_bytes,
        )?;
        let codec = ContentCodec::from_cid(&inner)?;
        let actual = memory.put(ContentBlock::new(codec, &plaintext))?;
        if actual != inner {
            return Err(ProtectedSnapshotError::WrongRoot);
        }
    }
    if total != restore.prepared.total_outer_bytes {
        return Err(ProtectedSnapshotError::WrongBinding);
    }
    verify_and_release(&memory, &restore, refresh).await
}

async fn verify_and_release<'a, F, Fut>(
    memory: &MemoryContentStore,
    restore: &ProtectedRestore<'a>,
    refresh: F,
) -> Result<RestoredSnapshot, ProtectedSnapshotError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<
            Output = Result<
                (
                    CurrentStorageState<'a>,
                    Option<&'a super::ProtectedCommitReceipt<'a>>,
                ),
                ProtectedSnapshotError,
            >,
        >,
{
    let restored = restore_snapshot_local(
        memory,
        &restore.prepared.inner_root,
        restore.relations,
        restore.entities,
        restore.inner_limits,
    )
    .await
    .map_err(ProtectedSnapshotError::Inner)?;
    let (current, committed) = restore.session.run(async { refresh().await }).await?;
    restore
        .read
        .check_release(
            restore.claim,
            (restore.current, restore.committed),
            (current, committed),
        )
        .map_err(ProtectedSnapshotError::Read)?;
    Ok(restored)
}
