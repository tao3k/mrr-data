//! Production owner for one byte identity edge over the original query endpoint.
//! The byte primitive is source-proven. Guard composition and endpoint answer
//! semantics are explicit trusted owner boundaries, not `DataFusion` refinement.
use super::transformation::{
    PropertyTransformationRuntime, property_transformation_artifact,
    property_transformation_endpoint, property_transformation_grant_catalog,
    property_transformation_root,
};
use meta_relational_reasoning::{
    AsyncTransformationPublisher, AuthenticatedTransformationGrant, CatalogBoundQuery,
    QueryResultLimits, TransformationAdmission, TransformationBinding, TransformationDefinition,
    TransformationError, TransformationEvidence, TransformationLimits, TransformationPlanCandidate,
    TransformationProfile, TransformationStep, TransformationVerifier, ValueSchema,
    admit_transformation, transformation_byte_identity_proof, transformation_grant_binding_digest,
    transformation_identity, transformation_value_digest,
};
use std::{num::NonZeroUsize, sync::Arc};

/// Selected production identity edge, verifier and actual physical runtime.
/// Construction accepts no arbitrary definition, proof, checker or solver.
pub struct PropertyIdentityTransformation<'a> {
    verifier: PropertyIdentityVerifier,
    runtime: PropertyTransformationRuntime<'a>,
    edge: TransformationAdmission,
}
impl<'a> PropertyIdentityTransformation<'a> {
    /// Bind the library's byte identity law to the original query and signed cut.
    /// The caller's grant issuer is an independently configured trust root.
    /// # Errors
    /// Rejects mismatched physical/source cuts and invalid admission bounds.
    pub fn new(
        query: &'a CatalogBoundQuery,
        backend: super::RestoredPropertyBackend<'a>,
        binding: TransformationBinding,
        grant: AuthenticatedTransformationGrant,
        limits: TransformationLimits,
        result_limits: QueryResultLimits,
        max_bytes: NonZeroUsize,
    ) -> Result<Self, TransformationError> {
        if binding.snapshot_digest() != query.snapshot_digest()
            || grant.binding_digest() != &transformation_grant_binding_digest(&binding, limits)?
            || grant.catalog_digest()
                != &property_transformation_grant_catalog(query, backend.restored)
        {
            return Err(TransformationError::BindingMismatch);
        }
        grant.check_current()?;
        let proof = transformation_byte_identity_proof();
        let endpoint = property_transformation_endpoint(query);
        let definition = TransformationDefinition {
            version: 1,
            profile: TransformationProfile::TotalWitnessTransport,
            source: endpoint.clone(),
            target: endpoint,
            forward_artifact: property_transformation_artifact("forward"),
            extract_artifact: property_transformation_artifact("extract"),
            codec: property_transformation_artifact("typed-result-transport"),
            parameter_contract: property_transformation_artifact("root-parameter"),
            statement: *proof.identity(),
            dependencies: vec![property_transformation_artifact("dependencies")],
            requirements: vec![property_transformation_artifact(
                "audited-identity-guard-composition",
            )],
        };
        let evidence = TransformationEvidence {
            transformation: transformation_identity(&definition, limits)?,
            statement: definition.statement,
            forward_artifact: definition.forward_artifact,
            extract_artifact: definition.extract_artifact,
            codec: definition.codec,
            checker: property_transformation_artifact("identity-admission-policy"),
            toolchain: *proof.identity(),
            environment: *grant.catalog_digest(),
            assumptions: *proof.assumptions(),
            source_revisions: binding.revisions().to_vec(),
        };
        let parameters = property_transformation_root(backend.restored);
        let mut verifier = PropertyIdentityVerifier {
            definition,
            evidence,
            binding,
            grant: grant.clone(),
            parameters,
            input: [0; 32],
            solver: [0; 32],
            lease: None,
        };
        let edge = admit_transformation(
            &verifier.definition,
            &verifier.evidence,
            &verifier.binding,
            limits,
            &verifier,
        )?;
        let runtime = PropertyTransformationRuntime::new(
            query,
            backend,
            edge.clone(),
            grant,
            limits,
            parameters,
            result_limits,
            max_bytes,
        )?;
        verifier.input =
            transformation_value_digest(&ValueSchema::ByteString, &runtime.input(), limits)?;
        verifier.solver = *runtime.solver();
        verifier.lease = Some(runtime.publication_lease());
        Ok(Self {
            verifier,
            runtime,
            edge,
        })
    }
    #[must_use]
    pub const fn verifier(&self) -> &PropertyIdentityVerifier {
        &self.verifier
    }
    #[must_use]
    pub const fn runtime(&self) -> &PropertyTransformationRuntime<'a> {
        &self.runtime
    }
    #[must_use]
    pub fn plan(&self) -> TransformationPlanCandidate {
        TransformationPlanCandidate {
            binding: self.verifier.binding.clone(),
            source: self.verifier.definition.source.clone(),
            target: self.verifier.definition.target.clone(),
            input: self.verifier.input,
            solver: self.verifier.solver,
            steps: vec![TransformationStep {
                admission: self.edge.clone(),
                input: self.verifier.input,
                target_input: self.verifier.input,
                parameters: self.verifier.parameters,
            }],
        }
    }
}

/// Immutable owner for the selected identity plan. No caller-supplied proof or
/// hash can expand its accepted definition, physical root, instance or solver.
pub struct PropertyIdentityVerifier {
    definition: TransformationDefinition,
    evidence: TransformationEvidence,
    binding: TransformationBinding,
    grant: AuthenticatedTransformationGrant,
    parameters: [u8; 32],
    input: [u8; 32],
    solver: [u8; 32],
    lease: Option<Arc<dyn meta_relational_reasoning::TransformationPublicationLease>>,
}
impl PropertyIdentityVerifier {
    fn current(&self, binding: &TransformationBinding) -> Result<(), TransformationError> {
        if binding != &self.binding {
            return Err(TransformationError::BindingMismatch);
        }
        self.grant.check_current()?;
        let capabilities = self.grant.capabilities();
        if !capabilities.forward || !capabilities.extract || !capabilities.solve {
            return Err(TransformationError::Rejected);
        }
        if self.lease.as_ref().is_some_and(|lease| !lease.is_current()) {
            return Err(TransformationError::Revoked);
        }
        Ok(())
    }
}
impl TransformationVerifier for PropertyIdentityVerifier {
    fn policy(&self) -> [u8; 32] {
        property_transformation_artifact("identity-admission-policy")
    }
    fn check_definition(
        &self,
        definition: &TransformationDefinition,
        evidence: &TransformationEvidence,
        binding: &TransformationBinding,
    ) -> Result<[u8; 32], TransformationError> {
        self.current(binding)?;
        if definition != &self.definition || evidence != &self.evidence {
            return Err(TransformationError::EvidenceMismatch);
        }
        Ok(*transformation_byte_identity_proof().identity())
    }
    fn check_step(
        &self,
        step: &TransformationStep,
        binding: &TransformationBinding,
        index: usize,
        _: &[u8; 32],
    ) -> Result<[u8; 32], TransformationError> {
        self.check_definition(
            step.admission.definition(),
            step.admission.evidence(),
            binding,
        )?;
        if index != 0
            || step.parameters != self.parameters
            || step.input != self.input
            || step.target_input != self.input
        {
            return Err(TransformationError::InstanceMismatch);
        }
        Ok(self.policy())
    }
    fn check_solver(
        &self,
        target: &meta_relational_reasoning::TransformationEndpoint,
        solver: &[u8; 32],
        input: &[u8; 32],
        binding: &TransformationBinding,
        _: &[u8; 32],
    ) -> Result<[u8; 32], TransformationError> {
        self.current(binding)?;
        if self.solver == [0; 32]
            || solver != &self.solver
            || input != &self.input
            || target != &self.definition.target
        {
            return Err(TransformationError::BindingMismatch);
        }
        Ok(self.solver)
    }
}
