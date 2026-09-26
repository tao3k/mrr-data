//! Snapshot publication, verified restore and MRR result admission.
use crate::{
    outbox::{self, Guard, LocalPolicy, Record, State},
    protocol::{self, MAX_EDGES, MAX_INPUT, Operation, Outcome, PROFILE, Receipt, Request},
    semantic::Context,
};
use anyhow::{Context as _, Result, ensure};
use mrr_data_cache::{BlockingContentStore, S3ContentStore};
use mrr_data_content::{
    AsyncContentStore, ContentBlock, ContentCodec, FilesystemContentStore, RemoteTransferLimits,
    SnapshotTransferLimits, TransferSession, restore_snapshot_local,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use serde::Serialize;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Execute one bounded request in an externally managed worker process.
/// # Errors
/// Rejects malformed inputs, identity drift, transport failures and failed MRR admission.
pub(crate) async fn execute(
    input: &[u8],
    local: BlockingContentStore<FilesystemContentStore>,
    remote: Option<S3ContentStore>,
    local_path: String,
    policy: LocalPolicy,
) -> Result<String> {
    ensure!(input.len() <= MAX_INPUT, "request exceeds 1 MiB");
    let request: Request = serde_json::from_slice(input)?;
    ensure!(request.profile == PROFILE, "unsupported consumer profile");
    let context = Context::new(&request.source, &request.revision)?;
    let start = Instant::now();
    let session = transfer_session()?;
    let limits = transfer_limits();
    let (root, outcome) = match &request.operation {
        Operation::Protect { edges } => {
            protect(
                &request,
                &context,
                edges,
                &local,
                &local_path,
                limits,
                policy,
            )
            .await?
        }
        Operation::Publish { edges } => {
            let (root, _) = protect(
                &request,
                &context,
                edges,
                &local,
                &local_path,
                limits,
                policy,
            )
            .await?;
            sync_protected(SyncInput {
                request: &request,
                context: &context,
                root: &root,
                local: &local,
                remote: remote.as_ref().context("S3 required")?,
                session: &session,
                limits,
                local_path: Path::new(&local_path),
            })
            .await?
        }
        Operation::Sync { root } => {
            sync_protected(SyncInput {
                request: &request,
                context: &context,
                root,
                local: &local,
                remote: remote.as_ref().context("S3 required")?,
                session: &session,
                limits,
                local_path: Path::new(&local_path),
            })
            .await?
        }
        Operation::Query { root } => {
            query(
                &request,
                &context,
                root,
                &local,
                remote.as_ref(),
                &session,
                limits,
            )
            .await?
        }
    };
    let stats = session.stats();
    Ok(serde_json::to_string(&Receipt {
        profile: PROFILE,
        producer: "mrr-data-poo-flow",
        request_sha256: protocol::digest(input),
        source: request.source,
        revision: request.revision,
        root,
        generation: context.semantic.generation().to_string(),
        result: outcome,
        remote_operations: stats.operations,
        charged_bytes: stats.charged_bytes,
        elapsed_micros: start.elapsed().as_micros(),
    })?)
}

fn transfer_session() -> Result<TransferSession> {
    Ok(TransferSession::new(
        Duration::from_secs(30),
        RemoteTransferLimits {
            operations: 32,
            bytes: 32 * 1024 * 1024,
            attempts_per_operation: 2,
            retry_delay: Duration::from_millis(50),
        },
    )?)
}

fn transfer_limits() -> SnapshotTransferLimits {
    SnapshotTransferLimits::new(64 * 1024, 8, 8 * 1024 * 1024, 16 * 1024 * 1024)
}

#[derive(Debug, Serialize)]
pub(crate) struct SyncSummary {
    profile: &'static str,
    attempted: usize,
    published: usize,
    pub(crate) failed_roots: Vec<String>,
}

/// Replay a bounded batch of durable outbox entries. Each snapshot gets its
/// own transfer budget; failures remain pending for a later Tokio cycle.
pub(crate) async fn sync_pending(
    local: &BlockingContentStore<FilesystemContentStore>,
    remote: &S3ContentStore,
    local_path: &Path,
    batch_limit: usize,
) -> Result<SyncSummary> {
    let path = local_path.to_owned();
    let pending = tokio::task::spawn_blocking(move || {
        let guard = Guard::acquire(&path)?;
        guard.maintain(outbox::now_secs()?)?;
        guard.pending(batch_limit)
    })
    .await??;
    let mut summary = SyncSummary {
        profile: PROFILE,
        attempted: 0,
        published: 0,
        failed_roots: Vec::new(),
    };
    for record in pending {
        summary.attempted += 1;
        let request = Request {
            profile: PROFILE.to_owned(),
            source: record.source.clone(),
            revision: record.revision.clone(),
            operation: Operation::Sync {
                root: record.root.clone(),
            },
        };
        let context = Context::new(&request.source, &request.revision)?;
        let session = transfer_session()?;
        let result = sync_protected(SyncInput {
            request: &request,
            context: &context,
            root: &record.root,
            local,
            remote,
            session: &session,
            limits: transfer_limits(),
            local_path,
        })
        .await;
        match result {
            Ok(_) => summary.published += 1,
            Err(_) => summary.failed_roots.push(record.root),
        }
    }
    Ok(summary)
}

async fn protect(
    request: &Request,
    context: &Context,
    edges: &[[String; 2]],
    local: &BlockingContentStore<FilesystemContentStore>,
    local_path: &str,
    limits: SnapshotTransferLimits,
    policy: LocalPolicy,
) -> Result<(String, Outcome)> {
    let facts = context.facts(&request.source, edges)?;
    let ipc = mrr_data_arrow::facts_to_ipc(&context.relation, &facts)
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let coverage = serde_json::to_vec(&(PROFILE, &request.source, &request.revision))?;
    let manifest = SnapshotManifest::admit(SnapshotManifestRequest::new(
        context.semantic.clone(),
        &context.relations,
        &context.entities,
        vec![RelationDescriptor::new(
            context.relation.id(),
            facts.len() as u64,
            vec![BatchDescriptor::new(
                raw_cid(&ipc),
                facts.len() as u64,
                ipc.len() as u64,
            )?],
        )?],
        CoverageDescriptor::new(CoverageKind::Complete, raw_cid(&coverage))?,
    ))?;
    let snapshot = SnapshotBlock::encode(manifest)?;
    let root = snapshot.cid().to_string();
    let mut blocks = snapshot
        .manifest()
        .referenced_cids()
        .into_iter()
        .map(|cid| cid.to_string())
        .collect::<Vec<_>>();
    blocks.push(root.clone());
    blocks.sort();
    let now = outbox::now_secs()?;
    let record = Record {
        profile: PROFILE.to_owned(),
        root: root.clone(),
        source: request.source.clone(),
        revision: request.revision.clone(),
        generation: context.semantic.generation().to_string(),
        protected_until: now
            .checked_add(policy.protection_secs)
            .context("protection expiry overflow")?,
        state: State::Pending,
        blocks,
    };
    let path = PathBuf::from(local_path);
    let guard = tokio::task::spawn_blocking(move || Guard::acquire(&path)).await??;
    let incoming = vec![
        (raw_cid(&ipc), ipc.len()),
        (raw_cid(&coverage), coverage.len()),
        (*snapshot.cid(), snapshot.bytes().len()),
    ];
    let guard = tokio::task::spawn_blocking(move || {
        guard.maintain(now)?;
        guard.preflight(&incoming, policy.max_bytes)?;
        Ok::<_, anyhow::Error>(guard)
    })
    .await??;
    for bytes in [&ipc, &coverage] {
        local
            .store(ContentBlock::new(ContentCodec::Raw, bytes))
            .await?;
    }
    local
        .store(ContentBlock::new(ContentCodec::DagCbor, snapshot.bytes()))
        .await?;
    restore_snapshot_local(
        local,
        snapshot.cid(),
        &context.relations,
        &context.entities,
        limits,
    )
    .await?;
    tokio::task::spawn_blocking(move || guard.commit(record)).await??;
    Ok((root, Outcome::Protected))
}

struct SyncInput<'a> {
    request: &'a Request,
    context: &'a Context,
    root: &'a str,
    local: &'a BlockingContentStore<FilesystemContentStore>,
    remote: &'a S3ContentStore,
    session: &'a TransferSession,
    limits: SnapshotTransferLimits,
    local_path: &'a Path,
}

async fn sync_protected(input: SyncInput<'_>) -> Result<(String, Outcome)> {
    let SyncInput {
        request,
        context,
        root,
        local,
        remote,
        session,
        limits,
        local_path,
    } = input;
    let path = local_path.to_owned();
    let root_owned = root.to_owned();
    let claim = tokio::task::spawn_blocking(move || outbox::claim(&path, &root_owned))
        .await??
        .context("snapshot is already synchronizing")?;
    let path = local_path.to_owned();
    let root_owned = root.to_owned();
    let record = tokio::task::spawn_blocking(move || {
        let guard = Guard::acquire(&path)?;
        guard.read(&root_owned)
    })
    .await??;
    ensure!(
        record.state == State::Pending || record.state == State::Synced,
        "invalid protection state"
    );
    ensure!(
        record.source == request.source
            && record.revision == request.revision
            && record.generation == context.semantic.generation().to_string(),
        "sync scope mismatch"
    );
    if record.state == State::Synced {
        return Ok((root.to_owned(), Outcome::Published));
    }
    let cid = root.parse()?;
    let restored =
        restore_snapshot_local(local, &cid, &context.relations, &context.entities, limits).await?;
    ensure!(
        restored.snapshot().manifest().semantic_snapshot() == &context.semantic,
        "snapshot semantic context mismatch"
    );
    let coverage = serde_json::to_vec(&(PROFILE, &request.source, &request.revision))?;
    ensure!(
        restored.snapshot().manifest().coverage().declaration_cid() == &raw_cid(&coverage),
        "snapshot source mismatch"
    );
    session
        .publish_snapshot(
            local,
            remote,
            restored.snapshot(),
            &context.relations,
            &context.entities,
            limits,
        )
        .await?;
    let path = local_path.to_owned();
    let root_owned = root.to_owned();
    let source = request.source.clone();
    let revision = request.revision.clone();
    tokio::task::spawn_blocking(move || {
        let guard = Guard::acquire(&path)?;
        guard.mark_synced(&root_owned, &source, &revision)
    })
    .await??;
    drop(claim);
    Ok((root.to_owned(), Outcome::Published))
}

async fn query(
    request: &Request,
    context: &Context,
    root: &str,
    local: &BlockingContentStore<FilesystemContentStore>,
    remote: Option<&S3ContentStore>,
    session: &TransferSession,
    limits: SnapshotTransferLimits,
) -> Result<(String, Outcome)> {
    let cid = root.parse()?;
    let restored =
        match restore_snapshot_local(local, &cid, &context.relations, &context.entities, limits)
            .await
        {
            Ok(restored) => restored,
            Err(
                mrr_data_content::SnapshotTransferError::Content(
                    mrr_data_content::ContentError::NotFound(_),
                )
                | mrr_data_content::SnapshotTransferError::MissingBlock(_),
            ) => {
                let remote = remote.context("snapshot incomplete locally and S3 unavailable")?;
                session
                    .restore_snapshot(
                        local,
                        remote,
                        &cid,
                        &context.relations,
                        &context.entities,
                        limits,
                    )
                    .await?
            }
            Err(error) => return Err(error.into()),
        };
    let profile = mrr_data_datafusion::datafusion_engine_profile()?;
    let bound = mrr_data_core::bind_data_query(&context.query, restored.snapshot(), &profile)?;
    let manifest = restored.snapshot().manifest();
    ensure!(
        manifest.coverage().kind() == CoverageKind::Complete,
        "incomplete static edges"
    );
    let coverage = serde_json::to_vec(&(PROFILE, &request.source, &request.revision))?;
    ensure!(
        manifest.coverage().declaration_cid() == &raw_cid(&coverage),
        "coverage source mismatch"
    );
    let [relation] = manifest.relations() else {
        anyhow::bail!("exactly one relation required")
    };
    ensure!(
        relation.relation_id() == context.relation.id() && relation.row_count() <= MAX_EDGES as u64,
        "relation mismatch or edge budget exceeded"
    );
    let [batch] = relation.batches() else {
        anyhow::bail!("exactly one batch required")
    };
    let ipc = restored
        .children()
        .get(batch.cid())
        .context("missing Arrow child")?;
    let facts = mrr_data_arrow::ipc_to_facts(
        &context.relation,
        ipc,
        mrr_data_arrow::IpcImportLimits::new(8 * 1024 * 1024, MAX_EDGES, 100),
    )
    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    ensure!(
        facts.len() as u64 == batch.row_count(),
        "declared row count mismatch"
    );
    context.validate_facts(&request.source, &facts)?;
    let arrow = mrr_data_arrow::facts_to_record_batch(&context.relation, &facts)
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let output =
        mrr_data_datafusion::execute_binary_entity_query(&context.query, &context.relation, arrow)
            .await?;
    let candidate = mrr_data_core::project_data_query_output(&bound, &profile, output)?;
    let admission = meta_relational_reasoning::admit_query_result_candidate(
        &context.query,
        &candidate,
        meta_relational_reasoning::QueryResultLimits::new(ROW_LIMIT, CELL_LIMIT),
    )?;
    let rows = candidate
        .rows()
        .iter()
        .map(|row| {
            let [
                meta_relational_reasoning::QueryResultValue::Node { id: a, .. },
                meta_relational_reasoning::QueryResultValue::Node { id: b, .. },
            ] = row.as_slice()
            else {
                anyhow::bail!("unexpected result shape")
            };
            Ok([a.to_string(), b.to_string()])
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((
        root.to_owned(),
        Outcome::Admitted {
            rows,
            admission_digest: protocol::hex(admission.digest()),
        },
    ))
}

const ROW_LIMIT: NonZeroUsize = NonZeroUsize::new(MAX_EDGES).unwrap();
const CELL_LIMIT: NonZeroUsize = NonZeroUsize::new(MAX_EDGES * 2).unwrap();
