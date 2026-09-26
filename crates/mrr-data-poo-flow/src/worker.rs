//! Snapshot publication, verified restore and MRR result admission.
use crate::{
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
use std::io::Write as _;
use std::{
    fs,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};
use tempfile::NamedTempFile;

/// Execute one bounded request in an externally managed worker process.
/// # Errors
/// Rejects malformed inputs, identity drift, transport failures and failed MRR admission.
pub(crate) async fn execute(
    input: &[u8],
    local: BlockingContentStore<FilesystemContentStore>,
    remote: Option<S3ContentStore>,
    local_path: String,
) -> Result<String> {
    ensure!(input.len() <= MAX_INPUT, "request exceeds 1 MiB");
    let request: Request = serde_json::from_slice(input)?;
    ensure!(request.profile == PROFILE, "unsupported consumer profile");
    let context = Context::new(&request.source, &request.revision)?;
    let start = Instant::now();
    let session = TransferSession::new(
        Duration::from_secs(30),
        RemoteTransferLimits {
            operations: 32,
            bytes: 32 * 1024 * 1024,
            attempts_per_operation: 2,
            retry_delay: Duration::from_millis(50),
        },
    )?;
    let limits = SnapshotTransferLimits::new(64 * 1024, 8, 8 * 1024 * 1024, 16 * 1024 * 1024);
    let (root, outcome) = match &request.operation {
        Operation::Protect { edges } => {
            protect(&request, &context, edges, &local, &local_path, limits).await?
        }
        Operation::Publish { edges } => {
            let (root, _) = protect(&request, &context, edges, &local, &local_path, limits).await?;
            let receipt = sync(
                &request,
                &context,
                &root,
                &local,
                remote.as_ref().context("S3 required")?,
                &session,
                limits,
            )
            .await?;
            clear_pending(Path::new(&local_path), &root)?;
            receipt
        }
        Operation::Sync { root } => {
            let receipt = sync(
                &request,
                &context,
                root,
                &local,
                remote.as_ref().context("S3 required")?,
                &session,
                limits,
            )
            .await?;
            clear_pending(Path::new(&local_path), root)?;
            receipt
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

async fn protect(
    request: &Request,
    context: &Context,
    edges: &[[String; 2]],
    local: &BlockingContentStore<FilesystemContentStore>,
    local_path: &str,
    limits: SnapshotTransferLimits,
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
    mark_pending(Path::new(local_path), &snapshot.cid().to_string())?;
    Ok((snapshot.cid().to_string(), Outcome::Protected))
}

async fn sync(
    request: &Request,
    context: &Context,
    root: &str,
    local: &BlockingContentStore<FilesystemContentStore>,
    remote: &S3ContentStore,
    session: &TransferSession,
    limits: SnapshotTransferLimits,
) -> Result<(String, Outcome)> {
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
    Ok((root.to_owned(), Outcome::Published))
}

fn marker_path(path: &Path, root: &str) -> Result<std::path::PathBuf> {
    let cid: cid::Cid = root.parse()?;
    ensure!(cid.to_string() == root, "canonical root required");
    Ok(path.join(format!("pending-{root}")))
}

fn sync_dir(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn mark_pending(path: &Path, root: &str) -> Result<()> {
    let marker = marker_path(path, root)?;
    if marker.exists() {
        ensure!(
            fs::symlink_metadata(&marker)?.file_type().is_file(),
            "pending marker is not a file"
        );
        ensure!(
            fs::read(&marker)? == root.as_bytes(),
            "pending marker mismatch"
        );
        sync_dir(path)?;
        return Ok(());
    }
    let mut temporary = NamedTempFile::new_in(path)?;
    temporary.write_all(root.as_bytes())?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(&marker) {
        Ok(_) => (),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                fs::symlink_metadata(&marker)?.file_type().is_file(),
                "pending marker is not a file"
            );
            ensure!(
                fs::read(&marker)? == root.as_bytes(),
                "pending marker mismatch"
            );
        }
        Err(error) => return Err(error.error.into()),
    }
    sync_dir(path)
}

fn clear_pending(path: &Path, root: &str) -> Result<()> {
    let marker = marker_path(path, root)?;
    match fs::remove_file(marker) {
        Ok(()) => sync_dir(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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
            Err(mrr_data_content::SnapshotTransferError::Content(
                mrr_data_content::ContentError::NotFound(_),
            ))
            | Err(mrr_data_content::SnapshotTransferError::MissingBlock(_)) => {
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
