//! Snapshot publication, verified restore and MRR result admission.
use crate::{
    protocol::{self, MAX_EDGES, MAX_INPUT, Operation, Outcome, PROFILE, Receipt, Request},
    semantic::Context,
};
use anyhow::{Context as _, Result, ensure};
use mrr_data_cache::{BlockingContentStore, KacheContentStore, S3ContentStore};
use mrr_data_content::{
    AsyncContentStore, ContentBlock, ContentCodec, RemoteTransferLimits, SnapshotTransferLimits,
    TransferSession,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

/// Execute one bounded request in an externally managed worker process.
/// # Errors
/// Rejects malformed inputs, identity drift, transport failures and failed MRR admission.
pub(crate) async fn execute(
    input: &[u8],
    local: BlockingContentStore<KacheContentStore>,
    remote: S3ContentStore,
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
        Operation::Publish { edges } => {
            publish(&request, &context, edges, &local, &remote, &session, limits).await?
        }
        Operation::Query { root } => {
            query(&request, &context, root, &local, &remote, &session, limits).await?
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

async fn publish(
    request: &Request,
    context: &Context,
    edges: &[[String; 2]],
    local: &BlockingContentStore<KacheContentStore>,
    remote: &S3ContentStore,
    session: &TransferSession,
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
    let publication = session
        .publish_snapshot(
            local,
            remote,
            &snapshot,
            &context.relations,
            &context.entities,
            limits,
        )
        .await?;
    Ok((publication.root().to_string(), Outcome::Published))
}

async fn query(
    request: &Request,
    context: &Context,
    root: &str,
    local: &BlockingContentStore<KacheContentStore>,
    remote: &S3ContentStore,
    session: &TransferSession,
    limits: SnapshotTransferLimits,
) -> Result<(String, Outcome)> {
    let restored = session
        .restore_snapshot(
            local,
            remote,
            &root.parse()?,
            &context.relations,
            &context.entities,
            limits,
        )
        .await?;
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
