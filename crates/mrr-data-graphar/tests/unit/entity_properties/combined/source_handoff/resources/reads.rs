use crate::tests::entity_properties::{combined::fixture::Fixture, fixture as properties};
use crate::{CapturedCombinedGraphArSelective, CapturedGraphArRelation};
use std::collections::BTreeSet;

pub(super) fn read(
    f: &Fixture,
    source: &CapturedCombinedGraphArSelective,
    selective: bool,
) -> (Vec<CapturedGraphArRelation>, usize, usize, usize) {
    let row_limit = super::scale::physical(super::scale::rows()).max_input_rows;
    let first = f.original.relations[0].schema.id();
    let second = f.original.relations[1].schema.id();
    let mut rows = 0;
    let mut bytes = 0;
    let mut relations = Vec::new();
    if selective {
        let selection = source
            .outgoing(&f.query, first, properties::entity("s1"), row_limit)
            .unwrap();
        let targets = selection
            .facts()
            .iter()
            .map(|fact| {
                let meta_relational_reasoning::Value::Entity(target) = fact.values()[1] else {
                    panic!("binary Entity")
                };
                target
            })
            .collect::<BTreeSet<_>>();
        rows += selection.metrics().materialized_rows;
        bytes += selection.metrics().read_bytes;
        relations.push(CapturedGraphArRelation {
            relation: first,
            facts: selection.into_facts().into(),
        });
        let targets = targets.into_iter().collect::<Vec<_>>();
        let selection = source
            .outgoing_many(&f.query, second, &targets, row_limit)
            .unwrap();
        rows += selection.metrics().materialized_rows;
        bytes += selection.metrics().read_bytes;
        let facts = selection.into_facts();
        relations.push(CapturedGraphArRelation {
            relation: second,
            facts: facts.into(),
        });
    } else {
        for relation in [first, second] {
            let selection = source.scan_all(&f.query, relation, row_limit).unwrap();
            rows += selection.metrics().materialized_rows;
            bytes += selection.metrics().read_bytes;
            relations.push(CapturedGraphArRelation {
                relation,
                facts: selection.into_facts().into(),
            });
        }
    }
    let selected_edges = relations.iter().map(|r| r.facts.len()).sum::<usize>();
    assert!(selected_edges <= rows);
    (relations, rows, selected_edges, bytes)
}
