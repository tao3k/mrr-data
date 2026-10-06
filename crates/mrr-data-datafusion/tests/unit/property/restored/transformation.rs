//! Actual `DataFusion` dispatch under a test admission owner, not a certificate.
use super::{EntityChildMode, fixture, limits, mrr, restored};
use crate::{
    PropertyTransformationRuntime, RestoredPropertyBackend, property_transformation_artifact,
    property_transformation_endpoint, property_transformation_grant_catalog,
    property_transformation_root,
};
use std::{cell::Cell, num::NonZeroUsize};

struct TestOwner {
    definition: mrr::TransformationDefinition,
    binding: mrr::TransformationBinding,
    solver: Cell<[u8; 32]>,
    parameters: [u8; 32],
}
impl mrr::TransformationVerifier for TestOwner {
    fn policy(&self) -> [u8; 32] {
        [71; 32]
    }
    fn check_definition(
        &self,
        definition: &mrr::TransformationDefinition,
        _: &mrr::TransformationEvidence,
        binding: &mrr::TransformationBinding,
    ) -> Result<[u8; 32], mrr::TransformationError> {
        if definition != &self.definition || binding != &self.binding {
            return Err(mrr::TransformationError::BindingMismatch);
        }
        Ok([72; 32])
    }
    fn check_step(
        &self,
        step: &mrr::TransformationStep,
        binding: &mrr::TransformationBinding,
        _: usize,
        _: &[u8; 32],
    ) -> Result<[u8; 32], mrr::TransformationError> {
        if step.admission.definition() != &self.definition
            || binding != &self.binding
            || step.parameters != self.parameters
            || step.input != step.target_input
        {
            return Err(mrr::TransformationError::BindingMismatch);
        }
        Ok([74; 32])
    }
    fn check_solver(
        &self,
        target: &mrr::TransformationEndpoint,
        solver: &[u8; 32],
        _: &[u8; 32],
        binding: &mrr::TransformationBinding,
        _: &[u8; 32],
    ) -> Result<[u8; 32], mrr::TransformationError> {
        if target != &self.definition.target
            || solver != &self.solver.get()
            || binding != &self.binding
        {
            return Err(mrr::TransformationError::BindingMismatch);
        }
        Ok([75; 32])
    }
}
fn transform_limits() -> mrr::TransformationLimits {
    mrr::TransformationLimits {
        max_bytes: NonZeroUsize::new(1_048_576).unwrap(),
        max_schema_nodes: NonZeroUsize::new(32).unwrap(),
        max_schema_depth: NonZeroUsize::new(8).unwrap(),
        max_dependencies: NonZeroUsize::new(16).unwrap(),
        max_steps: NonZeroUsize::new(4).unwrap(),
    }
}
fn owner(
    query: &mrr::CatalogBoundQuery,
    semantic: &mrr::SemanticSnapshot,
    cold: &mrr_data_content::RestoredSnapshot,
) -> TestOwner {
    let endpoint = property_transformation_endpoint(query);
    TestOwner {
        definition: mrr::TransformationDefinition {
            version: 1,
            profile: mrr::TransformationProfile::TotalWitnessTransport,
            source: endpoint.clone(),
            target: endpoint,
            forward_artifact: property_transformation_artifact("forward"),
            extract_artifact: property_transformation_artifact("extract"),
            codec: [76; 32],
            parameter_contract: property_transformation_artifact("root-parameter"),
            statement: [78; 32],
            dependencies: vec![property_transformation_artifact("dependencies")],
            requirements: vec![[79; 32]],
        },
        binding: mrr::TransformationBinding::new(semantic, [80; 32], [81; 32], [82; 32], 100, 200)
            .unwrap(),
        solver: Cell::new([0; 32]),
        parameters: property_transformation_root(cold),
    }
}
fn self_edge(owner: &TestOwner) -> mrr::TransformationAdmission {
    let definition = &owner.definition;
    let evidence = mrr::TransformationEvidence {
        transformation: mrr::transformation_identity(definition, transform_limits()).unwrap(),
        statement: definition.statement,
        forward_artifact: definition.forward_artifact,
        extract_artifact: definition.extract_artifact,
        codec: definition.codec,
        checker: [83; 32],
        toolchain: [84; 32],
        environment: [85; 32],
        assumptions: [86; 32],
        source_revisions: owner.binding.revisions().to_vec(),
    };
    mrr::admit_transformation(
        definition,
        &evidence,
        &owner.binding,
        transform_limits(),
        owner,
    )
    .unwrap()
}
fn grant(
    query: &mrr::CatalogBoundQuery,
    cold: &mrr_data_content::RestoredSnapshot,
    edge: &mrr::TransformationAdmission,
    publish: bool,
) -> (mrr::AuthenticatedTransformationGrant, tempfile::TempDir) {
    use ed25519_dalek::{Signer, SigningKey};
    let key = SigningKey::from_bytes(&[173; 32]);
    let directory = tempfile::tempdir().unwrap();
    let ledger = mrr::TransformationGrantLedger::open(
        directory.path().join("grant.cbor"),
        key.verifying_key().to_bytes(),
        transform_limits(),
    )
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let binding =
        mrr::transformation_grant_binding_digest(edge.binding(), transform_limits()).unwrap();
    let catalog = property_transformation_grant_catalog(query, cold);
    let grant = mrr::TransformationSourceGrant {
        version: 1,
        issuer: key.verifying_key().to_bytes(),
        nonce: [174; 32],
        binding,
        catalog,
        not_before: now,
        expires: now + 3600,
        capabilities: mrr::TransformationCapabilities {
            forward: true,
            solve: true,
            extract: true,
            publish,
        },
        resources: mrr::TransformationExecutionBudget {
            max_operations: 100,
            max_value_bytes: 1_048_576,
        },
    };
    let signed = mrr::SignedTransformationSourceGrant {
        signature: key
            .sign(&grant.signing_bytes(transform_limits()).unwrap())
            .to_bytes()
            .to_vec(),
        grant,
    };
    (
        mrr::AuthenticatedTransformationGrant::authenticate(
            signed,
            ledger,
            binding,
            catalog,
            transform_limits(),
        )
        .unwrap(),
        directory,
    )
}
fn result_limits() -> mrr::QueryResultLimits {
    mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    )
}
#[tokio::test]
async fn property_transform_executes_actual_root_and_retains_original_physical_evidence() {
    let fixture = fixture();
    let (cold, _, relations, entities) = restored(&fixture, EntityChildMode::Valid).await;
    let owner = owner(&fixture.query, &fixture.semantic, &cold);
    let edge = self_edge(&owner);
    let (grant, _directory) = grant(&fixture.query, &cold, &edge, false);
    let runtime = PropertyTransformationRuntime::new(
        &fixture.query,
        RestoredPropertyBackend {
            restored: &cold,
            relation_catalog: &relations,
            entity_catalog: &entities,
            limits: limits(),
        },
        edge.clone(),
        grant,
        transform_limits(),
        owner.parameters,
        result_limits(),
        transform_limits().max_bytes,
    )
    .unwrap();
    owner.solver.set(*runtime.solver());
    let input = runtime.input();
    let digest =
        mrr::transformation_value_digest(&mrr::ValueSchema::ByteString, &input, transform_limits())
            .unwrap();
    let plan = mrr::TransformationPlanCandidate {
        binding: owner.binding.clone(),
        source: owner.definition.source.clone(),
        target: owner.definition.target.clone(),
        input: digest,
        solver: *runtime.solver(),
        steps: vec![mrr::TransformationStep {
            admission: edge,
            input: digest,
            target_input: digest,
            parameters: owner.parameters,
        }],
    };
    let admitted = mrr::admit_transformation_plan(&plan, transform_limits(), &owner).unwrap();
    #[cfg(not(feature = "source-handoff"))]
    let execution = mrr::execute_transformation_plan_async(
        &plan,
        &admitted,
        input,
        transform_limits(),
        &owner,
        &runtime,
    )
    .await
    .unwrap();
    #[cfg(feature = "source-handoff")]
    let execution = execute_native(&plan, &admitted, input, &owner, &runtime).await;
    let mrr::Value::ByteString(answer) = execution.answer() else {
        panic!("typed transport required")
    };
    let verified = mrr::verify_query_result_transport(
        &fixture.query,
        answer,
        result_limits(),
        transform_limits().max_bytes,
    )
    .unwrap();
    assert_eq!(verified.receipt().row_count(), 3);
    let physical = runtime.take_physical_execution().unwrap().unwrap();
    assert_eq!(physical.candidate(), verified.candidate());
    assert_eq!(
        physical.physical_evidence().snapshot_root(),
        cold.snapshot().cid()
    );
    assert_eq!(execution.answer_checks().len(), 2);
    println!("PROPERTY-TRANSFORMATION-OK: original root, 3 rows, 2 independent answer checks");
}
#[tokio::test]
async fn property_checker_rejects_typed_empty_answer_and_live_revocation() {
    use mrr::AsyncTransformationRuntime;
    let fixture = fixture();
    let (cold, _, relations, entities) = restored(&fixture, EntityChildMode::Valid).await;
    let owner = owner(&fixture.query, &fixture.semantic, &cold);
    let cap = NonZeroUsize::new(1_048_576).unwrap();
    let grant_edge = self_edge(&owner);
    let (grant, _directory) = grant(&fixture.query, &cold, &grant_edge, false);
    let runtime = PropertyTransformationRuntime::new(
        &fixture.query,
        RestoredPropertyBackend {
            restored: &cold,
            relation_catalog: &relations,
            entity_catalog: &entities,
            limits: limits(),
        },
        grant_edge,
        grant,
        transform_limits(),
        owner.parameters,
        result_limits(),
        cap,
    )
    .unwrap();
    let empty = mrr::CandidateQueryResult::new(
        mrr::QueryResultBinding::for_query(&fixture.query),
        fixture
            .query
            .query()
            .projections()
            .iter()
            .map(|p| p.alias().clone())
            .collect(),
        vec![],
    );
    let answer = mrr::Value::ByteString(
        mrr::export_query_result_transport(&fixture.query, &empty, result_limits(), cap).unwrap(),
    );
    assert_eq!(
        runtime
            .check_answer(
                &owner.definition.target,
                &runtime.input(),
                &answer,
                &owner.binding
            )
            .await
            .unwrap_err(),
        mrr::TransformationError::Rejected
    );
    let called = Cell::new(false);
    assert!(
        runtime
            .publish(1, || {
                called.set(true);
                Ok(())
            })
            .is_err()
    );
    assert!(!called.get());
    runtime.revoke_source_grant().unwrap();
    assert_eq!(
        runtime
            .solve(
                runtime.solver(),
                &owner.definition.target,
                &runtime.input(),
                &owner.binding
            )
            .await
            .unwrap_err(),
        mrr::TransformationError::Revoked
    );
    assert!(runtime.take_physical_execution().unwrap().is_none());
}

#[tokio::test]
async fn granted_publication_never_acknowledges_a_locally_revoked_action() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    let owner = owner(&f.query, &f.semantic, &cold);
    let edge = self_edge(&owner);
    let (grant, _directory) = grant(&f.query, &cold, &edge, true);
    let runtime = PropertyTransformationRuntime::new(
        &f.query,
        RestoredPropertyBackend {
            restored: &cold,
            relation_catalog: &relations,
            entity_catalog: &entities,
            limits: limits(),
        },
        edge,
        grant,
        transform_limits(),
        owner.parameters,
        result_limits(),
        NonZeroUsize::new(1_048_576).unwrap(),
    )
    .unwrap();
    assert_eq!(runtime.publish(1, || Ok(7)).unwrap(), 7);
    assert_eq!(
        runtime.publish(1, || {
            runtime.revoke();
            Ok(8)
        }),
        Err(mrr::TransformationError::PublicationUncertain)
    );
    let called = Cell::new(false);
    assert_eq!(
        runtime.publish(1, || {
            called.set(true);
            Ok(9)
        }),
        Err(mrr::TransformationError::Revoked)
    );
    assert!(!called.get());
}

#[cfg(feature = "source-handoff")]
async fn execute_native(
    plan: &mrr::TransformationPlanCandidate,
    admitted: &mrr::TransformationPlanAdmission,
    input: mrr::Value,
    owner: &TestOwner,
    runtime: &PropertyTransformationRuntime<'_>,
) -> mrr::TransformationExecutionReceipt {
    let edges: Vec<_> = plan
        .steps
        .iter()
        .map(|step| step.admission.clone())
        .collect();
    let native = mrr::execute_native_transformation_plan_async(
        mrr::NativeTransformationExecutionRequest {
            edges: &edges,
            candidate: plan,
            input,
            limits: transform_limits(),
            closure_limits: mrr::DeductionLimits::new(
                NonZeroUsize::new(16).unwrap(),
                NonZeroUsize::new(16).unwrap(),
                NonZeroUsize::new(16).unwrap(),
            )
            .closure_limits(),
        },
        owner,
        runtime,
    )
    .await
    .unwrap();
    assert!(
        native
            .search()
            .routes()
            .iter()
            .any(|route| route.edges == vec![*plan.steps[0].admission.digest()])
    );
    assert_eq!(native.execution().plan_digest(), admitted.digest());
    native.execution().clone()
}
