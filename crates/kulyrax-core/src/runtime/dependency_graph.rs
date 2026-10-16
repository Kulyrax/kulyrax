//! Phase 9: adapter dependency metadata and deterministic execution order.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::routing::AdapterId;
use crate::runtime::config::KulyraxConfig;

/// Precomputed priority and dependencies for topological ordering.
#[derive(Debug, Clone)]
pub struct AdapterExecutionMeta {
    pub priority: u32,
    pub depends_on: Vec<AdapterId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyOrderError {
    /// Adapters involved in a cycle (always sorted by id).
    Cycle { adapters: Vec<AdapterId> },
    /// Routed adapter missing from metadata map.
    MissingAdapterMeta { adapter_id: AdapterId },
}

/// Single pass over config adapters.
pub fn adapter_metadata_map(config: &KulyraxConfig) -> HashMap<AdapterId, AdapterExecutionMeta> {
    config
        .adapters
        .iter()
        .map(|a| {
            (
                a.id().to_string(),
                AdapterExecutionMeta {
                    priority: a.priority(),
                    depends_on: a.depends_on().to_vec(),
                },
            )
        })
        .collect()
}

#[derive(Eq, PartialEq)]
struct ReadyKey {
    priority: u32,
    id: AdapterId,
}

impl Ord for ReadyKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.id.cmp(&other.id),
            o => o,
        }
    }
}

impl PartialOrd for ReadyKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Deterministic topological order over unique routed adapters.
/// Tie-break when multiple ready: lower `priority`, then lexicographic `id`.
pub fn execution_order_for_routed(
    routed: &[AdapterId],
    meta: &HashMap<AdapterId, AdapterExecutionMeta>,
) -> Result<Vec<AdapterId>, DependencyOrderError> {
    let nodes: HashSet<AdapterId> = routed.iter().cloned().collect();

    for id in &nodes {
        if !meta.contains_key(id) {
            return Err(DependencyOrderError::MissingAdapterMeta {
                adapter_id: id.clone(),
            });
        }
    }

    if nodes.is_empty() {
        return Ok(Vec::new());
    }

    let mut in_degree: HashMap<AdapterId, usize> = nodes.iter().map(|id| (id.clone(), 0)).collect();
    let mut successors: HashMap<AdapterId, Vec<AdapterId>> = HashMap::new();

    // Every `n` is present in `meta` and `in_degree` by construction (both were
    // built from `nodes`, and membership in `meta` was checked above); the
    // `if let`/`entry` forms below fall through harmlessly rather than panicking
    // if that invariant is ever violated.
    for n in &nodes {
        let Some(m) = meta.get(n) else { continue };
        for d in &m.depends_on {
            if nodes.contains(d) {
                *in_degree.entry(n.clone()).or_insert(0) += 1;
                successors.entry(d.clone()).or_default().push(n.clone());
            }
        }
    }

    let mut heap: BinaryHeap<std::cmp::Reverse<ReadyKey>> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| {
            let p = meta.get(id).map(|m| m.priority).unwrap_or(u32::MAX);
            std::cmp::Reverse(ReadyKey {
                priority: p,
                id: id.clone(),
            })
        })
        .collect();

    let mut order = Vec::with_capacity(nodes.len());
    while let Some(std::cmp::Reverse(k)) = heap.pop() {
        order.push(k.id.clone());
        if let Some(succs) = successors.get(&k.id) {
            for s in succs {
                let Some(deg) = in_degree.get_mut(s) else {
                    continue;
                };
                *deg -= 1;
                if *deg == 0 {
                    let p = meta.get(s).map(|m| m.priority).unwrap_or(u32::MAX);
                    heap.push(std::cmp::Reverse(ReadyKey {
                        priority: p,
                        id: s.clone(),
                    }));
                }
            }
        }
    }

    if order.len() == nodes.len() {
        return Ok(order);
    }

    let executed: HashSet<AdapterId> = order.iter().cloned().collect();
    let mut remaining: Vec<AdapterId> = nodes
        .into_iter()
        .filter(|id| !executed.contains(id))
        .collect();
    remaining.sort();
    Err(DependencyOrderError::Cycle {
        adapters: remaining,
    })
}

// ---------------------------------------------------------------------------
// Phase 9.5: dependency layers (explain / ops only; no execution change)
// ---------------------------------------------------------------------------

/// Warn when any adapter sits beyond the third stage (layer index > 2).
pub const RECOMMENDED_MAX_DEPENDENCY_LAYER: u32 = 2;

/// Fixed CLI/JSON warning when depth exceeds recommendation (stable for tests).
pub const PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING: &str =
    "projection dependency depth exceeds recommended limit (3 stages)";

/// Per-position dependency layer aligned with `execution_order` (Phase 9 topo).
/// `layer(id) = 0` if no routed deps; else `1 + max(layer(dep))` over routed deps.
/// Missing `meta` entry ⇒ empty deps. Uses `HashSet` for O(1) routed membership.
pub fn dependency_layers_parallel(
    execution_order: &[AdapterId],
    meta: &HashMap<AdapterId, AdapterExecutionMeta>,
    routed: &HashSet<AdapterId>,
) -> Vec<u32> {
    let mut id_to_layer: HashMap<&str, u32> = HashMap::new();
    let mut layers = Vec::with_capacity(execution_order.len());
    for id in execution_order {
        let deps = meta.get(id).map(|m| m.depends_on.as_slice()).unwrap_or(&[]);
        let mut layer = 0u32;
        for d in deps {
            if routed.contains(d) {
                let dl = *id_to_layer.get(d.as_str()).unwrap_or(&0);
                layer = layer.max(1 + dl);
            }
        }
        id_to_layer.insert(id.as_str(), layer);
        layers.push(layer);
    }
    layers
}

/// Merge consecutive adapters sharing the same layer; order within each group matches `execution_order`.
pub fn dependency_layers_grouped(
    execution_order: &[AdapterId],
    layers: &[u32],
) -> Vec<(u32, Vec<AdapterId>)> {
    debug_assert_eq!(execution_order.len(), layers.len());
    let mut out: Vec<(u32, Vec<AdapterId>)> = Vec::new();
    for (id, &layer) in execution_order.iter().zip(layers.iter()) {
        if let Some((last_l, v)) = out.last_mut()
            && *last_l == layer
        {
            v.push(id.clone());
            continue;
        }
        out.push((layer, vec![id.clone()]));
    }
    out
}

#[inline]
pub fn max_dependency_layer(layers: &[u32]) -> u32 {
    layers.iter().copied().max().unwrap_or(0)
}

#[inline]
pub fn dependency_depth_exceeds_recommended(layers: &[u32]) -> bool {
    max_dependency_layer(layers) > RECOMMENDED_MAX_DEPENDENCY_LAYER
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_map(entries: &[(&str, u32, &[&str])]) -> HashMap<AdapterId, AdapterExecutionMeta> {
        entries
            .iter()
            .map(|(id, pr, deps)| {
                (
                    (*id).to_string(),
                    AdapterExecutionMeta {
                        priority: *pr,
                        depends_on: deps.iter().map(|s| (*s).to_string()).collect(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn b_before_a_when_a_depends_on_b() {
        let meta = meta_map(&[("A", 10, &["B"]), ("B", 10, &[])]);
        let o = execution_order_for_routed(&["A".into(), "B".into()], &meta).unwrap();
        assert_eq!(o, vec!["B", "A"]);
    }

    #[test]
    fn independent_order_by_priority_then_id() {
        let meta = meta_map(&[("z", 20, &[]), ("a", 10, &[])]);
        let o = execution_order_for_routed(&["z".into(), "a".into()], &meta).unwrap();
        assert_eq!(o, vec!["a", "z"]);
    }

    #[test]
    fn same_priority_sorted_by_id() {
        let meta = meta_map(&[("m", 5, &[]), ("a", 5, &[])]);
        let o = execution_order_for_routed(&["m".into(), "a".into()], &meta).unwrap();
        assert_eq!(o, vec!["a", "m"]);
    }

    #[test]
    fn cycle_adapters_sorted() {
        let meta = meta_map(&[("A", 10, &["B"]), ("B", 10, &["A"])]);
        let e = execution_order_for_routed(&["A".into(), "B".into()], &meta).unwrap_err();
        match e {
            DependencyOrderError::Cycle { adapters } => assert_eq!(adapters, vec!["A", "B"]),
            _ => panic!("expected cycle"),
        }
    }

    #[test]
    fn dedupes_duplicate_routed_ids() {
        let meta = meta_map(&[("A", 1, &[])]);
        let o = execution_order_for_routed(&["A".into(), "A".into()], &meta).unwrap();
        assert_eq!(o, vec!["A"]);
    }

    fn routed_set(ids: &[&str]) -> HashSet<AdapterId> {
        ids.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn layers_single_layer_all_zero() {
        let meta = meta_map(&[("a", 10, &[]), ("b", 20, &[])]);
        let order = vec!["a".into(), "b".into()];
        let r = routed_set(&["a", "b"]);
        let layers = dependency_layers_parallel(&order, &meta, &r);
        assert_eq!(layers, vec![0, 0]);
    }

    #[test]
    fn layers_two_step_a_then_b() {
        let meta = meta_map(&[("A", 10, &[]), ("B", 10, &["A"])]);
        let order = execution_order_for_routed(&["A".into(), "B".into()], &meta).unwrap();
        let r = routed_set(&["A", "B"]);
        let layers = dependency_layers_parallel(&order, &meta, &r);
        assert_eq!(layers, vec![0, 1]);
    }

    #[test]
    fn layers_fan_out_preserves_execution_order_in_layer_one() {
        let meta = meta_map(&[
            ("sql", 10, &[]),
            ("search_index", 20, &["sql"]),
            ("analytics", 30, &["sql"]),
        ]);
        let order = execution_order_for_routed(
            &["sql".into(), "search_index".into(), "analytics".into()],
            &meta,
        )
        .unwrap();
        let r = routed_set(&["sql", "search_index", "analytics"]);
        let layers = dependency_layers_parallel(&order, &meta, &r);
        assert_eq!(layers, vec![0, 1, 1]);
        let grouped = dependency_layers_grouped(&order, &layers);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[1].1, vec!["search_index", "analytics"]);
    }

    #[test]
    fn layers_diamond() {
        let meta = meta_map(&[("A", 10, &[]), ("B", 10, &[]), ("C", 10, &["A", "B"])]);
        let order =
            execution_order_for_routed(&["A".into(), "B".into(), "C".into()], &meta).unwrap();
        let r = routed_set(&["A", "B", "C"]);
        let layers = dependency_layers_parallel(&order, &meta, &r);
        assert_eq!(layers, vec![0, 0, 1]);
    }

    #[test]
    fn layers_deep_chain_a_through_d() {
        let meta = meta_map(&[
            ("A", 10, &[]),
            ("B", 10, &["A"]),
            ("C", 10, &["B"]),
            ("D", 10, &["C"]),
        ]);
        let order =
            execution_order_for_routed(&["A".into(), "B".into(), "C".into(), "D".into()], &meta)
                .unwrap();
        let r = routed_set(&["A", "B", "C", "D"]);
        let layers = dependency_layers_parallel(&order, &meta, &r);
        assert_eq!(layers, vec![0, 1, 2, 3]);
        assert!(dependency_depth_exceeds_recommended(&layers));
        assert_eq!(max_dependency_layer(&layers), 3);
    }

    #[test]
    fn layers_missing_meta_treats_as_no_deps() {
        let meta = meta_map(&[("A", 10, &[])]);
        let order = vec!["A".into(), "orphan".into()];
        let r = routed_set(&["A", "orphan"]);
        let layers = dependency_layers_parallel(&order, &meta, &r);
        assert_eq!(layers, vec![0, 0]);
    }
}
