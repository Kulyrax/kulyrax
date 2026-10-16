use std::collections::HashMap;

use kulyrax_core::runtime::dependency_graph::DependencyOrderError;
use kulyrax_core::{AdapterExecutionMeta, execution_order_for_routed};

fn meta_map(entries: &[(&str, u32, &[&str])]) -> HashMap<String, AdapterExecutionMeta> {
    entries
        .iter()
        .map(|(id, priority, deps)| {
            (
                (*id).to_string(),
                AdapterExecutionMeta {
                    priority: *priority,
                    depends_on: deps.iter().map(|s| (*s).to_string()).collect(),
                },
            )
        })
        .collect()
}

#[test]
fn two_node_cycle_is_rejected() {
    // A depends on B, B depends on A: no valid topological order exists.
    let meta = meta_map(&[("A", 10, &["B"]), ("B", 10, &["A"])]);

    let err = execution_order_for_routed(&["A".into(), "B".into()], &meta).unwrap_err();

    match err {
        DependencyOrderError::Cycle { adapters } => {
            assert_eq!(adapters, vec!["A".to_string(), "B".to_string()]);
        }
        other => panic!("expected DependencyOrderError::Cycle, got {:?}", other),
    }
}

#[test]
fn three_node_cycle_is_rejected() {
    // A -> B -> C -> A
    let meta = meta_map(&[("A", 10, &["C"]), ("B", 10, &["A"]), ("C", 10, &["B"])]);

    let err = execution_order_for_routed(&["A".into(), "B".into(), "C".into()], &meta).unwrap_err();

    match err {
        DependencyOrderError::Cycle { adapters } => {
            assert_eq!(
                adapters,
                vec!["A".to_string(), "B".to_string(), "C".to_string()]
            );
        }
        other => panic!("expected DependencyOrderError::Cycle, got {:?}", other),
    }
}

#[test]
fn self_dependency_is_rejected_as_cycle() {
    let meta = meta_map(&[("A", 10, &["A"])]);

    let err = execution_order_for_routed(&["A".into()], &meta).unwrap_err();

    assert_eq!(
        err,
        DependencyOrderError::Cycle {
            adapters: vec!["A".to_string()]
        }
    );
}

#[test]
fn cycle_isolated_from_acyclic_part_still_detected() {
    // D is a standalone, valid node; A/B form a cycle. The whole routed set must fail.
    let meta = meta_map(&[("A", 10, &["B"]), ("B", 10, &["A"]), ("D", 10, &[])]);

    let err = execution_order_for_routed(&["A".into(), "B".into(), "D".into()], &meta).unwrap_err();

    match err {
        DependencyOrderError::Cycle { adapters } => {
            assert_eq!(adapters, vec!["A".to_string(), "B".to_string()]);
        }
        other => panic!("expected DependencyOrderError::Cycle, got {:?}", other),
    }
}

#[test]
fn acyclic_graph_still_succeeds() {
    let meta = meta_map(&[("A", 10, &["B"]), ("B", 10, &[])]);

    let order = execution_order_for_routed(&["A".into(), "B".into()], &meta).unwrap();

    assert_eq!(order, vec!["B".to_string(), "A".to_string()]);
}
