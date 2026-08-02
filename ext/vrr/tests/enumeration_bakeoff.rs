mod support;

use support::{assert_complete_cases, Relation, Sender};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct Case {
    from: Sender,
    to: Sender,
    relation: Relation,
}

const SENDERS: [Sender; 5] = [
    Sender::Leader,
    Sender::BackupOne,
    Sender::BackupTwo,
    Sender::SelfNode,
    Sender::NonMember,
];
const CASE_COUNT: usize = SENDERS.len() * SENDERS.len() * Relation::ALL.len();

fn relation_cases() -> impl Iterator<Item = Case> {
    SENDERS.into_iter().flat_map(|from| {
        SENDERS.into_iter().flat_map(move |to| {
            Relation::ALL
                .into_iter()
                .map(move |relation| Case { from, to, relation })
        })
    })
}

#[test]
fn pico_iterator_enumerates_the_complete_symbolic_relation_product() {
    let cases: Vec<_> = relation_cases().collect();
    assert_complete_cases(
        "sender x sender x relation",
        CASE_COUNT,
        cases.iter().copied(),
    );

    let repeated: Vec<_> = relation_cases().collect();
    assert_eq!(cases, repeated, "unstable case sequence: {cases:?}");
}
