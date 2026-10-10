use crate::{
    DataSearchCandidateBranch as Branch, DataSearchCandidateComposition as Mode,
    DataSearchCandidateError as Error, DataSearchSourceBinding, compose_data_search_candidates,
};
use meta_relational_reasoning::{GenerationId, SearchFactor, SearchFactorRole};
use std::{collections::BTreeSet, num::NonZeroUsize};
fn source(revision: &str) -> DataSearchSourceBinding {
    DataSearchSourceBinding::new(
        "project/workspace".into(),
        revision.into(),
        "resident-view".into(),
        "poo.search.composition.v1".into(),
        GenerationId::from_canonical_bytes("generation").unwrap(),
    )
    .unwrap()
}
fn factor(name: &str, role: SearchFactorRole) -> SearchFactor {
    SearchFactor::from_canonical_input(name, role).unwrap()
}
fn owners(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|s| (*s).to_owned()).collect()
}
fn branches() -> Vec<Branch> {
    [
        ("grep", vec!["src/a.rs", "src/b.rs"]),
        ("index", vec!["src/b.rs", "src/c.rs"]),
    ]
    .into_iter()
    .map(|(name, candidates)| {
        Branch::new(
            source("revision"),
            factor(name, SearchFactorRole::Acquisition),
            owners(&candidates),
            true,
            false,
        )
    })
    .collect()
}
fn merge() -> SearchFactor {
    factor("merge", SearchFactorRole::Refinement)
}
fn limit() -> NonZeroUsize {
    NonZeroUsize::new(100).unwrap()
}

#[cfg(feature = "native-search")]
#[test]
fn actual_poo_plan_is_preserved_and_wrong_graphs_are_refused() {
    use crate::{DataPooSearchCandidateBranch, compose_poo_data_search_candidates};
    use meta_relational_reasoning::{
        PooSearchPlan as Plan, PooSearchRole as Role, compile_poo_search_plan,
    };
    let stage = |name: &str| Plan::Stage {
        name: name.into(),
        role: Role::Acquisition,
        input_domain: "workspace".into(),
        output_domain: "candidates".into(),
    };
    let parallel = Plan::Parallel {
        name: "branches".into(),
        children: vec![stage("grep"), stage("index")],
    };
    let plan = Plan::Merge {
        name: "joined".into(),
        parallel: Box::new(parallel.clone()),
        stage_name: "merge".into(),
        role: Role::Refinement,
        output_domain: "candidates".into(),
    };
    let binding = source("revision");
    let projection = compile_poo_search_plan("data-poo", binding.generation(), &plan).unwrap();
    let branches = vec![
        DataPooSearchCandidateBranch {
            stage_name: "grep".into(),
            binding: binding.clone(),
            candidates: owners(&["a", "b"]),
            complete: true,
            truncated: false,
        },
        DataPooSearchCandidateBranch {
            stage_name: "index".into(),
            binding: binding.clone(),
            candidates: owners(&["b", "c"]),
            complete: true,
            truncated: false,
        },
    ];
    let data = compose_poo_data_search_candidates(
        &binding,
        Mode::Intersect,
        &projection,
        "merge",
        &branches,
        limit(),
    )
    .unwrap();
    assert_eq!(data.merged_candidates(), &owners(&["b"]));
    assert_eq!(data.factors(), projection.factors());
    assert_eq!(data.edges(), projection.edges());
    assert_eq!(
        compose_poo_data_search_candidates(
            &binding,
            Mode::Intersect,
            &projection,
            "merge",
            &branches[..1],
            limit()
        )
        .unwrap_err(),
        Error::PooPlanMismatch
    );
    let foreign_projection = compile_poo_search_plan(
        "data-poo",
        GenerationId::from_canonical_bytes("stale").unwrap(),
        &plan,
    )
    .unwrap();
    assert_eq!(
        compose_poo_data_search_candidates(
            &binding,
            Mode::Intersect,
            &foreign_projection,
            "merge",
            &branches,
            limit()
        )
        .unwrap_err(),
        Error::BindingMismatch
    );
    assert_execution_admission(&binding, &projection, &foreign_projection, &branches, &data);
    let missing_merge =
        compile_poo_search_plan("data-poo", binding.generation(), &parallel).unwrap();
    assert_eq!(
        compose_poo_data_search_candidates(
            &binding,
            Mode::Intersect,
            &missing_merge,
            "merge",
            &branches,
            limit()
        )
        .unwrap_err(),
        Error::PooPlanMismatch
    );
}

#[cfg(feature = "native-search")]
fn assert_execution_admission(
    binding: &DataSearchSourceBinding,
    projection: &meta_relational_reasoning::PooSearchProjection,
    foreign_projection: &meta_relational_reasoning::PooSearchProjection,
    branches: &[crate::DataPooSearchCandidateBranch],
    data: &crate::DataSearchCandidateReceipt,
) {
    use crate::DataPooSearchCandidateBranch;
    let executed = crate::execute_poo_data_search_candidates(
        binding,
        Mode::Intersect,
        projection,
        "merge",
        branches,
        limit(),
        meta_relational_reasoning::evaluate_poo_search_factors,
    )
    .unwrap();
    assert_eq!(
        executed.candidates.merged_candidates(),
        data.merged_candidates()
    );
    assert_eq!(executed.reasoning.generation(), binding.generation());
    assert!(matches!(
        crate::execute_poo_data_search_candidates(
            &source("revoked"),
            Mode::Intersect,
            projection,
            "merge",
            branches,
            limit(),
            |_, _, _| panic!("revoked physical sources must fail before inference"),
        ),
        Err(crate::DataSearchExecutionError::Candidates(
            Error::BindingMismatch
        ))
    ));
    let foreign_binding = DataSearchSourceBinding::new(
        binding.scope().to_owned(),
        binding.source_digest().to_owned(),
        binding.resident_view_digest().to_owned(),
        binding.composition_abi().to_owned(),
        foreign_projection.generation(),
    )
    .unwrap();
    let foreign_branches = branches
        .iter()
        .map(|branch| DataPooSearchCandidateBranch {
            stage_name: branch.stage_name.clone(),
            binding: foreign_binding.clone(),
            candidates: branch.candidates.clone(),
            complete: branch.complete,
            truncated: branch.truncated,
        })
        .collect::<Vec<_>>();
    let foreign_execution = crate::execute_poo_data_search_candidates(
        &foreign_binding,
        Mode::Intersect,
        foreign_projection,
        "merge",
        &foreign_branches,
        limit(),
        meta_relational_reasoning::evaluate_poo_search_factors,
    )
    .unwrap();
    assert!(matches!(
        crate::execute_poo_data_search_candidates(
            binding,
            Mode::Intersect,
            projection,
            "merge",
            branches,
            limit(),
            |_, _, _| Ok(foreign_execution.reasoning),
        ),
        Err(crate::DataSearchExecutionError::IncompleteInference)
    ));
}

#[test]
fn actual_intersection_and_rank_join_preserve_distinct_truth_sets_and_causal_rows() {
    let intersection = compose_data_search_candidates(
        &source("revision"),
        Mode::Intersect,
        merge(),
        &branches(),
        limit(),
    )
    .unwrap();
    let ranked = compose_data_search_candidates(
        &source("revision"),
        Mode::RankJoin,
        merge(),
        &branches(),
        limit(),
    )
    .unwrap();
    assert_eq!(intersection.merged_candidates(), &owners(&["src/b.rs"]));
    assert_ne!(intersection.composition_id(), ranked.composition_id());
    assert_eq!(
        ranked.merged_candidates(),
        &owners(&["src/a.rs", "src/b.rs"])
    );
    assert_eq!(intersection.observations().len(), 5);
    let row = intersection.observations().last().unwrap();
    assert_eq!(row.causal_parents().len(), 2);
    assert!(
        intersection
            .observations()
            .iter()
            .all(|row| row.generation() == source("revision").generation())
    );
    assert_eq!(intersection.factors().len(), 3);
    assert_eq!(intersection.edges().len(), 2);
}

#[test]
fn source_generation_view_and_abi_substitution_are_rejected() {
    let expected = source("revision");
    let retained =
        compose_data_search_candidates(&expected, Mode::Intersect, merge(), &branches(), limit())
            .unwrap();
    let mut foreign = vec![source("foreign")];
    for (view, abi, generation) in [
        ("foreign-view", "poo.search.composition.v1", "generation"),
        ("resident-view", "foreign-abi", "generation"),
        (
            "resident-view",
            "poo.search.composition.v1",
            "stale-generation",
        ),
    ] {
        foreign.push(
            DataSearchSourceBinding::new(
                "project/workspace".into(),
                "revision".into(),
                view.into(),
                abi.into(),
                GenerationId::from_canonical_bytes(generation).unwrap(),
            )
            .unwrap(),
        );
    }
    for binding in foreign {
        assert_eq!(
            retained.verify_binding(&binding),
            Err(Error::BindingMismatch)
        );
        let leaves = [Branch::new(
            binding,
            factor("grep", SearchFactorRole::Acquisition),
            owners(&["a"]),
            true,
            false,
        )];
        assert_eq!(
            compose_data_search_candidates(&expected, Mode::Single, merge(), &leaves, limit())
                .unwrap_err(),
            Error::BindingMismatch
        );
    }
}

#[test]
fn withdrawn_physical_owner_does_not_survive_fresh_intersection() {
    let before = compose_data_search_candidates(
        &source("revision"),
        Mode::Intersect,
        merge(),
        &branches(),
        limit(),
    )
    .unwrap();
    let mut after_branches = branches();
    after_branches[1] = Branch::new(
        source("revision"),
        factor("index", SearchFactorRole::Acquisition),
        owners(&["src/c.rs"]),
        true,
        false,
    );
    let after = compose_data_search_candidates(
        &source("revision"),
        Mode::Intersect,
        merge(),
        &after_branches,
        limit(),
    )
    .unwrap();
    assert_eq!(before.merged_candidates(), &owners(&["src/b.rs"]));
    assert!(after.merged_candidates().is_empty());
    assert_ne!(before.composition_id(), after.composition_id());
    assert!(
        after
            .observations()
            .iter()
            .all(|row| row.factor() != merge().id())
    );
}

#[test]
fn truncated_rank_evidence_cannot_change_primary_truth_or_satisfy_intersection() {
    let leaves = [
        Branch::new(
            source("revision"),
            factor("grep", SearchFactorRole::Acquisition),
            owners(&["a", "b"]),
            true,
            false,
        ),
        Branch::new(
            source("revision"),
            factor("rank", SearchFactorRole::Acquisition),
            owners(&["b"]),
            false,
            true,
        ),
    ];
    let receipt = compose_data_search_candidates(
        &source("revision"),
        Mode::RankJoin,
        merge(),
        &leaves,
        limit(),
    )
    .unwrap();
    assert_eq!(receipt.merged_candidates(), &owners(&["a", "b"]));
    assert_eq!(
        receipt.branch_completeness(),
        &[(true, false), (false, true)]
    );
    assert_eq!(
        compose_data_search_candidates(
            &source("revision"),
            Mode::Intersect,
            merge(),
            &leaves,
            limit()
        )
        .unwrap_err(),
        Error::IncompleteBranch
    );
    assert_eq!(
        compose_data_search_candidates(
            &source("revision"),
            Mode::Single,
            merge(),
            &leaves[1..],
            limit()
        )
        .unwrap()
        .branch_completeness(),
        &[(false, true)]
    );
}

#[test]
fn empty_intersection_is_valid_but_overflow_and_duplicate_factors_are_rejected() {
    let mut leaves = branches();
    leaves[1] = Branch::new(
        source("revision"),
        factor("index", SearchFactorRole::Acquisition),
        owners(&["unrelated"]),
        true,
        false,
    );
    assert!(
        compose_data_search_candidates(
            &source("revision"),
            Mode::Intersect,
            merge(),
            &leaves,
            limit()
        )
        .unwrap()
        .merged_candidates()
        .is_empty()
    );
    assert_eq!(
        compose_data_search_candidates(
            &source("revision"),
            Mode::RankJoin,
            merge(),
            &leaves,
            NonZeroUsize::new(3).unwrap()
        )
        .unwrap_err(),
        Error::CandidateLimit
    );
    leaves[1] = leaves[0].clone();
    assert_eq!(
        compose_data_search_candidates(
            &source("revision"),
            Mode::Intersect,
            merge(),
            &leaves,
            limit()
        )
        .unwrap_err(),
        Error::DuplicateFactor
    );
}

#[cfg(feature = "native-search")]
#[test]
fn actual_complete_owner_rows_flow_through_native_mrr_factor_inference() {
    use meta_relational_reasoning::{SearchFrameworkLimits, SearchFrameworkStatus};
    let receipt = compose_data_search_candidates(
        &source("revision"),
        Mode::Intersect,
        merge(),
        &branches(),
        limit(),
    )
    .unwrap();
    println!(
        "Data complete owner intersection admitted: {} owners",
        receipt.merged_candidates().len()
    );
    let native = crate::evaluate_data_search_candidates(
        &receipt,
        SearchFrameworkLimits::new(limit(), limit(), limit(), limit(), limit()),
    )
    .unwrap();
    assert_eq!(native.generation(), source("revision").generation());
    assert_eq!(native.status(), SearchFrameworkStatus::Complete);
    assert!(!native.influences().is_empty());
    assert!(native.influences().iter().all(|influence| {
        receipt
            .observations()
            .iter()
            .any(|row| row.candidate() == influence.candidate())
    }));
    println!(
        "DATA-NATIVE-SEARCH-OK observations={} influences={} digest={}",
        receipt.observations().len(),
        native.influences().len(),
        native.digest()
    );
}

#[test]
fn candidate_identity_binds_exact_utf8_owner_and_every_source_component() {
    use meta_relational_reasoning::FactId;
    let expected = source("revision");
    let generation = expected.generation().to_string();
    // Independent canonical byte encoding, including a non-ASCII owner.
    let encode = |parts: &[&str]| {
        use std::fmt::Write;
        let mut bytes = String::new();
        for part in parts {
            write!(bytes, "{}:{part}", part.len()).unwrap();
        }
        bytes
    };
    let source_bytes = encode(&[
        "project/workspace",
        "revision",
        "resident-view",
        "poo.search.composition.v1",
        &generation,
    ]);
    let owner = "src/搜索.rs";
    let candidate = expected.candidate_identity(owner).unwrap();
    assert_eq!(
        candidate,
        FactId::from_canonical_bytes(encode(&["mrr.data.search.owner.v1", &source_bytes, owner]))
            .unwrap()
    );
    assert_ne!(
        candidate,
        expected.candidate_identity("src/./搜索.rs").unwrap()
    );
    for field in 0..5 {
        let mut fields = [
            "project/workspace",
            "revision",
            "resident-view",
            "poo.search.composition.v1",
        ];
        if field < 4 {
            fields[field] = "different";
        }
        let changed = DataSearchSourceBinding::new(
            fields[0].into(),
            fields[1].into(),
            fields[2].into(),
            fields[3].into(),
            if field == 4 {
                GenerationId::from_canonical_bytes("next-generation").unwrap()
            } else {
                expected.generation()
            },
        )
        .unwrap();
        assert_ne!(candidate, changed.candidate_identity(owner).unwrap());
    }
    assert_eq!(
        expected.candidate_identity(" src/a.rs"),
        Err(Error::InvalidCandidate)
    );
    println!("DATA-CANDIDATE-IDENTITY-OK utf8=exact binding-components=5");
}

#[test]
fn receipt_candidate_admission_rejects_forgery_unknown_owner_and_revocation() {
    use meta_relational_reasoning::FactId;
    let expected = source("revision");
    let receipt =
        compose_data_search_candidates(&expected, Mode::Intersect, merge(), &branches(), limit())
            .unwrap();
    assert_eq!(
        receipt
            .candidate_owners()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        owners(&["src/a.rs", "src/b.rs", "src/c.rs"])
    );
    for (owner, candidate) in receipt.candidate_owners() {
        receipt
            .verify_candidate(&expected, owner, *candidate)
            .unwrap();
        assert!(
            receipt
                .observations()
                .iter()
                .any(|row| row.candidate() == *candidate)
        );
    }
    let candidate = expected.candidate_identity("src/b.rs").unwrap();
    assert_eq!(
        receipt.verify_candidate(&expected, "src/a.rs", candidate),
        Err(Error::CandidateIdentityMismatch)
    );
    assert_eq!(
        receipt.verify_candidate(
            &expected,
            "unobserved.rs",
            expected.candidate_identity("unobserved.rs").unwrap()
        ),
        Err(Error::CandidateIdentityMismatch)
    );
    assert_eq!(
        receipt.verify_candidate(
            &expected,
            "src/b.rs",
            FactId::from_canonical_bytes("forged").unwrap()
        ),
        Err(Error::CandidateIdentityMismatch)
    );
    assert_eq!(
        receipt.verify_candidate(&source("revoked"), "src/b.rs", candidate),
        Err(Error::BindingMismatch)
    );
    let foreign_projection = DataSearchSourceBinding::new(
        expected.scope().into(),
        expected.source_digest().into(),
        expected.resident_view_digest().into(),
        expected.composition_abi().into(),
        GenerationId::from_canonical_bytes("stale").unwrap(),
    )
    .unwrap();
    assert_eq!(
        receipt.verify_candidate(&foreign_projection, "src/b.rs", candidate),
        Err(Error::BindingMismatch)
    );
    println!("DATA-CANDIDATE-ADMISSION-OK union=3 merged=1 negative-controls=5");
}
