//! Source-bound property query execution over one verified physical snapshot.
//! MRR owns parsing, semantic binding, and result admission; this crate only
//! connects those authorities to the MRR Data physical executor.

use anyhow::{Context, Result, ensure};
use cid::Cid;
use meta_relational_reasoning::{
    CandidateQueryResult, EntityCatalog, QueryResultAdmissionReceipt, QueryResultLimits,
    QueryTemplate, ReasoningBundle, ReasoningBundleDeclaration, RelationCatalog,
    admit_query_result_candidate, bind_query_to_catalog,
};
use mrr_data_content::{
    AsyncContentStore, RemoteContentStore, RestoredSnapshot, SnapshotTransferLimits,
    TransferSession,
};
use mrr_data_core::{bind_data_query, project_data_query_output};
use mrr_data_datafusion::{
    PropertyQueryLimits, RestoredPropertyQuery, datafusion_engine_profile,
    execute_restored_property_path_query,
};
use mrr_frontends::{
    ParserLanguage, ParserOwnedCompilation, ParserOwnedCompilationReceipt, QueryFrontend,
};

/// All source and catalog authority is caller-owned; the snapshot must already
/// have passed complete closure verification before this entry point is called.
pub struct RestoredPropertySourceQuery<'a> {
    pub source_name: &'a str,
    pub source_text: &'a str,
    pub expected_source_digest: &'a str,
    pub restored: &'a RestoredSnapshot,
    pub relation_catalog: &'a RelationCatalog,
    pub entity_catalog: &'a EntityCatalog,
    pub physical_limits: PropertyQueryLimits,
    pub result_limits: QueryResultLimits,
}

/// Immutable root and caller-owned authorities for the external property worker.
/// The worker receives no domain-specific parser, schema, or result rule.
pub struct PropertySourceWorkerQuery<'a> {
    pub root: &'a Cid,
    pub source_name: &'a str,
    pub source_text: &'a str,
    pub expected_source_digest: &'a str,
    pub relation_catalog: &'a RelationCatalog,
    pub entity_catalog: &'a EntityCatalog,
    pub transfer_limits: SnapshotTransferLimits,
    pub physical_limits: PropertyQueryLimits,
    pub result_limits: QueryResultLimits,
}

/// The exact source compilation and MRR-admitted result for one immutable root.
pub struct AdmittedPropertySourceResult {
    pub compilation: ParserOwnedCompilationReceipt,
    pub root: String,
    pub candidate: CandidateQueryResult,
    pub admission: QueryResultAdmissionReceipt,
}

/// Restore one complete content closure and admit the original GQL result.
/// The caller owns the process lifetime and cancellation across both restore
/// and execution; the session bounds transfer while physical limits bound data.
///
/// # Errors
/// Rejects transfer, content, source, catalog, execution, or result-admission
/// failures without returning partial evidence.
pub async fn execute_property_source_worker_query(
    input: PropertySourceWorkerQuery<'_>,
    local: &(impl AsyncContentStore + ?Sized),
    remote: &(impl RemoteContentStore + ?Sized),
    session: &TransferSession,
) -> Result<AdmittedPropertySourceResult> {
    let restored = session
        .restore_snapshot(
            local,
            remote,
            input.root,
            input.relation_catalog,
            input.entity_catalog,
            input.transfer_limits,
        )
        .await
        .context("verified snapshot restoration")?;
    execute_restored_property_source_query(RestoredPropertySourceQuery {
        source_name: input.source_name,
        source_text: input.source_text,
        expected_source_digest: input.expected_source_digest,
        restored: &restored,
        relation_catalog: input.relation_catalog,
        entity_catalog: input.entity_catalog,
        physical_limits: input.physical_limits,
        result_limits: input.result_limits,
    })
    .await
}

/// Compile one original GQL source and execute it against a verified snapshot.
///
/// # Errors
/// Fails closed on source drift, parser diagnostics, catalog/snapshot mismatch,
/// unsupported physical shapes, execution limits, or MRR result rejection.
pub async fn execute_restored_property_source_query(
    input: RestoredPropertySourceQuery<'_>,
) -> Result<AdmittedPropertySourceResult> {
    let compilation = compile_original_source(
        input.source_name,
        input.source_text,
        input.expected_source_digest,
    )?;
    let query_id = compilation.query.id();
    let bundle = ReasoningBundle::admit(ReasoningBundleDeclaration {
        entities: input.entity_catalog.entities().to_vec(),
        relations: input.relation_catalog.relations().to_vec(),
        query_templates: vec![QueryTemplate::new(compilation.query, vec![])],
        ..ReasoningBundleDeclaration::default()
    })
    .context("MRR query bundle admission")?;
    let query = bind_query_to_catalog(
        &bundle,
        query_id,
        input.restored.snapshot().manifest().semantic_snapshot(),
    )
    .context("MRR catalog binding")?;
    let profile = datafusion_engine_profile().context("physical engine profile")?;
    let physical = bind_data_query(&query, input.restored.snapshot(), &profile)
        .context("physical snapshot binding")?;
    let output = execute_restored_property_path_query(RestoredPropertyQuery {
        query: &query,
        restored: input.restored,
        relation_catalog: input.relation_catalog,
        entity_catalog: input.entity_catalog,
        limits: input.physical_limits,
    })
    .await
    .context("verified property execution")?;
    let candidate = project_data_query_output(&physical, &profile, output)
        .context("physical output projection")?;
    let admission = admit_query_result_candidate(&query, &candidate, input.result_limits)
        .context("MRR result admission")?;
    Ok(AdmittedPropertySourceResult {
        compilation: compilation.receipt,
        root: input.restored.snapshot().cid().to_string(),
        candidate,
        admission,
    })
}

fn compile_original_source(
    source_name: &str,
    source_text: &str,
    expected_source_digest: &str,
) -> Result<ParserOwnedCompilation> {
    let compilation = QueryFrontend::new(ParserLanguage::Gql)
        .compile_with_receipt(source_name, source_text)
        .map_err(|error| anyhow::anyhow!("parser-owned GQL compilation: {error:?}"))?;
    ensure!(
        compilation.receipt.source_digest == expected_source_digest,
        "original GQL source digest mismatch"
    );
    Ok(compilation)
}

#[cfg(test)]
#[path = "../tests/unit/property_source.rs"]
mod tests;
