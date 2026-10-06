//! Actual restored property-query solver with identity witness transport.
//! The caller supplies an already admitted identity edge and current verifier.
//! This runtime does not manufacture a certificate or authenticate that owner.
use super::{RestoredPropertyBackend, RestoredPropertyQuery, verify_restored_property_path_output};
use meta_relational_reasoning::{
    AdmittedPropertyExecution, AsyncTransformationRuntime, AuthenticatedTransformationGrant,
    CatalogBoundQuery, QueryResultLimits, TransformationAdmission, TransformationBinding,
    TransformationEndpoint, TransformationError, TransformationGrantOperation as GrantOp,
    TransformationLimits, TransformationStep, Value, ValueSchema, export_query_result_transport,
    transformation_grant_binding_digest, transport_transformation_bytes,
    verify_query_result_transport,
};
use mrr_data_core::{BoundDataQuery, PhysicalQueryOutput};
use sha2::{Digest, Sha256};
use std::{
    num::NonZeroUsize,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

/// Exact production implementation identity for each operation. Certification
/// must bind these source artifacts; hashes alone do not establish refinement.
#[must_use]
pub fn property_transformation_artifact(operation: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"mrr-data.property-transformation-artifact.v1");
    hash.update((operation.len() as u64).to_le_bytes());
    hash.update(operation.as_bytes());
    for source in [
        include_bytes!("transformation.rs").as_slice(),
        include_bytes!("reference.rs"),
        include_bytes!("restored.rs"),
        include_bytes!("execution.rs"),
        include_bytes!("validation.rs"),
        include_bytes!("backend.rs"),
        include_bytes!("../../Cargo.toml"),
        include_bytes!("../../../../Cargo.toml"),
        include_bytes!("../../../../Cargo.lock"),
    ] {
        hash.update((source.len() as u64).to_le_bytes());
        hash.update(source);
    }
    hash.update([u8::from(cfg!(feature = "source-handoff"))]);
    hash.finalize().into()
}

/// Stable query meaning and catalogs, independent of the selected generation.
#[must_use]
pub fn property_transformation_endpoint(query: &CatalogBoundQuery) -> TransformationEndpoint {
    let mut hash = Sha256::new();
    hash.update(b"mrr-data.property-transformation-endpoint.v1");
    hash.update(query.query_digest());
    hash.update(query.catalog_digest().as_bytes());
    hash.update(query.entity_catalog_digest().as_bytes());
    TransformationEndpoint {
        semantics: hash.finalize().into(),
        input: ValueSchema::ByteString,
        result: ValueSchema::ByteString,
    }
}

/// Physical root parameter required in the current execution step.
#[must_use]
pub fn property_transformation_root(restored: &mrr_data_content::RestoredSnapshot) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"mrr-data.property-transformation-root.v1");
    hash.update(restored.snapshot().cid().to_bytes());
    hash.finalize().into()
}

/// Signed grant cut for this physical query, root and implementation closure.
#[must_use]
pub fn property_transformation_grant_catalog(
    query: &CatalogBoundQuery,
    restored: &mrr_data_content::RestoredSnapshot,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"mrr-data.property-transformation-grant-cut.v1");
    hash.update(property_transformation_artifact("dependencies"));
    hash.update(query.digest());
    hash.update(property_transformation_root(restored));
    hash.finalize().into()
}

/// One immutable query/root, one previously admitted identity edge, one solver.
/// Input binds the original catalog-bound query digest and physical root. Answer bytes retain MRR's
/// full typed result transport; extraction never invents or projects rows.
/// The independent checker re-evaluates original IR over the verified closure.
pub struct PropertyTransformationRuntime<'a> {
    query: &'a CatalogBoundQuery,
    backend: RestoredPropertyBackend<'a>,
    edge: TransformationAdmission,
    authority: AuthenticatedTransformationGrant,
    operations: AtomicU64,
    parameters: [u8; 32],
    solver: [u8; 32],
    identity: [u8; 32],
    result_limits: QueryResultLimits,
    max_bytes: NonZeroUsize,
    revoked: AtomicBool,
    physical: Mutex<Option<AdmittedPropertyExecution<BoundDataQuery>>>,
}
impl<'a> PropertyTransformationRuntime<'a> {
    /// Requires the actual admission owner to have certified identity transport
    /// for this endpoint and bound these exact artifacts. Current authorization
    /// is also rechecked by the caller's verifier at every async return.
    /// # Errors
    /// Rejects mismatched endpoint, artifacts, physical root or source binding.
    #[allow(
        clippy::too_many_arguments,
        reason = "Constructor binds query, backend, certified edge and separate authenticated authority with their explicit bounds."
    )]
    pub fn new(
        query: &'a CatalogBoundQuery,
        backend: RestoredPropertyBackend<'a>,
        edge: TransformationAdmission,
        authority: AuthenticatedTransformationGrant,
        authority_limits: TransformationLimits,
        parameters: [u8; 32],
        result_limits: QueryResultLimits,
        max_bytes: NonZeroUsize,
    ) -> Result<Self, TransformationError> {
        if authority.binding_digest()
            != &transformation_grant_binding_digest(edge.binding(), authority_limits)?
            || authority.catalog_digest()
                != &property_transformation_grant_catalog(query, backend.restored)
        {
            return Err(TransformationError::BindingMismatch);
        }
        authority.check_current()?;
        let definition = edge.definition();
        let endpoint = property_transformation_endpoint(query);
        if definition.source != endpoint
            || definition.target != endpoint
            || definition.forward_artifact != property_transformation_artifact("forward")
            || definition.extract_artifact != property_transformation_artifact("extract")
            || !definition
                .dependencies
                .contains(&property_transformation_artifact("dependencies"))
            || edge.binding().snapshot_digest() != query.snapshot_digest()
            || parameters != property_transformation_root(backend.restored)
            || definition.parameter_contract != property_transformation_artifact("root-parameter")
        {
            return Err(TransformationError::BindingMismatch);
        }
        let mut hash = Sha256::new();
        hash.update(property_transformation_artifact("runtime"));
        hash.update(query.digest());
        hash.update(property_transformation_root(backend.restored));
        hash.update(parameters);
        for bound in [
            backend.limits.max_input_rows,
            backend.limits.max_input_bytes,
            backend.limits.max_join_rows,
            backend.limits.max_output_cells,
            backend.limits.execution_memory_bytes,
            max_bytes.get(),
        ] {
            hash.update((bound as u64).to_le_bytes());
        }
        // This internal lease identity uses Debug only under the source closure
        // pin above (including the exact MRR revision in Cargo.lock), never as
        // a portable semantic definition or interchange encoding.
        hash.update(format!("{result_limits:?}").as_bytes());
        hash.update(edge.digest());
        hash.update(authority.identity());
        let identity: [u8; 32] = hash.finalize().into();
        let mut hash = Sha256::new();
        hash.update(property_transformation_artifact("solve"));
        hash.update(identity);
        let solver = hash.finalize().into();
        Ok(Self {
            query,
            backend,
            edge,
            authority,
            operations: AtomicU64::new(0),
            parameters,
            solver,
            identity,
            result_limits,
            max_bytes,
            revoked: AtomicBool::new(false),
            physical: Mutex::new(None),
        })
    }
    #[must_use]
    pub const fn solver(&self) -> &[u8; 32] {
        &self.solver
    }
    #[must_use]
    pub fn input(&self) -> Value {
        let mut hash = Sha256::new();
        hash.update(b"mrr-data.property-transformation-instance.v1");
        hash.update(self.query.digest());
        hash.update(self.parameters);
        Value::ByteString(hash.finalize().to_vec())
    }
    /// Monotone live-owner revocation. Historical physical evidence is retained
    /// but cannot be executed or released as a new completed transformation.
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }
    /// Retain the original backend-owned binding and MRR admission, rather than
    /// reconstructing either from transformed answer bytes. Historical only.
    /// # Errors
    /// Returns Unknown if a previous panic poisoned the evidence slot.
    pub fn take_physical_execution(
        &self,
    ) -> Result<Option<AdmittedPropertyExecution<BoundDataQuery>>, TransformationError> {
        self.physical
            .lock()
            .map_err(|_| TransformationError::Unknown)
            .map(|mut stored| stored.take())
    }
    /// Persist nonce revocation, including for a newly opened runtime.
    /// # Errors
    /// Returns the ledger persistence error; the live runtime stays revoked.
    pub fn revoke_source_grant(&self) -> Result<(), TransformationError> {
        self.revoked.store(true, Ordering::Release);
        self.authority.revoke()
    }
    /// Commit owner-provided publication under a current authenticated lease.
    /// The action must be synchronous; uncertain persistence remains its error.
    /// # Errors
    /// Rejects revoked grants, denied permission and exhausted budgets.
    /// Expiry or local revocation after the action is `PublicationUncertain`.
    pub fn publish<T>(
        &self,
        value_bytes: u64,
        action: impl FnOnce() -> Result<T, TransformationError>,
    ) -> Result<T, TransformationError> {
        self.current(self.edge.binding())?;
        let n = self.next_operation()?;
        let mut started = false;
        let result = self
            .authority
            .authorize(GrantOp::Publish, n, value_bytes, || {
                if self.revoked.load(Ordering::Acquire) {
                    return Err(TransformationError::Revoked);
                }
                started = true;
                let value = action()?;
                if self.revoked.load(Ordering::Acquire) {
                    return Err(TransformationError::PublicationUncertain);
                }
                Ok(value)
            });
        match result {
            Err(TransformationError::Revoked) if started => {
                Err(TransformationError::PublicationUncertain)
            }
            other => other,
        }
    }
    fn next_operation(&self) -> Result<u64, TransformationError> {
        self.operations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map(|n| n + 1)
            .map_err(|_| TransformationError::Budget)
    }
    fn authorize(&self, op: GrantOp, value: &Value) -> Result<(), TransformationError> {
        let Value::ByteString(bytes) = value else {
            return Err(TransformationError::InvalidSchema);
        };
        let n = self.next_operation()?;
        self.authority
            .authorize(op, n, bytes.len() as u64, || Ok(()))
    }
    fn current(&self, binding: &TransformationBinding) -> Result<(), TransformationError> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(TransformationError::Revoked);
        }
        self.authority.check_current()?;
        if binding != self.edge.binding() {
            return Err(TransformationError::BindingMismatch);
        }
        Ok(())
    }
    fn instance(&self, input: &Value) -> Result<(), TransformationError> {
        match input {
            Value::ByteString(_) if input == &self.input() => Ok(()),
            _ => Err(TransformationError::InstanceMismatch),
        }
    }
    fn step(&self, step: &TransformationStep) -> Result<(), TransformationError> {
        if step.admission != self.edge || step.parameters != self.parameters {
            return Err(TransformationError::BindingMismatch);
        }
        Ok(())
    }
}
impl AsyncTransformationRuntime for PropertyTransformationRuntime<'_> {
    fn identity(&self) -> [u8; 32] {
        if self.current(self.edge.binding()).is_err() {
            [0; 32]
        } else {
            self.identity
        }
    }
    async fn forward(
        &self,
        step: &TransformationStep,
        input: &Value,
        binding: &TransformationBinding,
    ) -> Result<Value, TransformationError> {
        self.current(binding)?;
        self.step(step)?;
        self.instance(input)?;
        self.authorize(GrantOp::Forward, input)?;
        let Value::ByteString(bytes) = input else {
            return Err(TransformationError::InvalidSchema);
        };
        Ok(Value::ByteString(transport_transformation_bytes(bytes)))
    }
    async fn solve(
        &self,
        solver: &[u8; 32],
        target: &TransformationEndpoint,
        input: &Value,
        binding: &TransformationBinding,
    ) -> Result<Value, TransformationError> {
        self.current(binding)?;
        self.instance(input)?;
        if solver != &self.solver || target != &self.edge.definition().target {
            return Err(TransformationError::BindingMismatch);
        }
        self.authorize(GrantOp::Solve, input)?;
        let physical = self
            .query
            .execute_with(&self.backend, self.result_limits)
            .await
            .map_err(|_| TransformationError::Unknown)?;
        self.current(binding)?;
        let bytes = export_query_result_transport(
            self.query,
            physical.candidate(),
            self.result_limits,
            self.max_bytes,
        )
        .map_err(|_| TransformationError::Budget)?;
        self.authority.authorize(
            GrantOp::Solve,
            self.operations.load(Ordering::Acquire),
            bytes.len() as u64,
            || Ok(()),
        )?;
        self.current(binding)?;
        *self
            .physical
            .lock()
            .map_err(|_| TransformationError::Unknown)? = Some(physical);
        Ok(Value::ByteString(bytes))
    }
    async fn extract(
        &self,
        step: &TransformationStep,
        source: &Value,
        answer: &Value,
        binding: &TransformationBinding,
    ) -> Result<Value, TransformationError> {
        self.current(binding)?;
        self.step(step)?;
        self.instance(source)?;
        match answer {
            Value::ByteString(bytes) if bytes.len() <= self.max_bytes.get() => (),
            Value::ByteString(_) => return Err(TransformationError::Budget),
            _ => return Err(TransformationError::InvalidSchema),
        }
        self.authorize(GrantOp::Extract, answer)?;
        let Value::ByteString(bytes) = answer else {
            return Err(TransformationError::InvalidSchema);
        };
        Ok(Value::ByteString(transport_transformation_bytes(bytes)))
    }
    async fn check_answer(
        &self,
        endpoint: &TransformationEndpoint,
        input: &Value,
        answer: &Value,
        binding: &TransformationBinding,
    ) -> Result<[u8; 32], TransformationError> {
        self.current(binding)?;
        self.instance(input)?;
        if endpoint != &self.edge.definition().target {
            return Err(TransformationError::EndpointMismatch);
        }
        self.authorize(GrantOp::Solve, answer)?;
        let Value::ByteString(bytes) = answer else {
            return Err(TransformationError::InvalidSchema);
        };
        let verified =
            verify_query_result_transport(self.query, bytes, self.result_limits, self.max_bytes)
                .map_err(|_| TransformationError::Rejected)?;
        let candidate = verified.candidate();
        let output =
            PhysicalQueryOutput::new(candidate.columns().to_vec(), candidate.rows().to_vec());
        verify_restored_property_path_output(
            &RestoredPropertyQuery {
                query: self.query,
                restored: self.backend.restored,
                relation_catalog: self.backend.relation_catalog,
                entity_catalog: self.backend.entity_catalog,
                limits: self.backend.limits,
            },
            &output,
        )
        .map_err(|_| TransformationError::Rejected)?;
        self.current(binding)?;
        Ok(*verified.receipt().digest())
    }
}
