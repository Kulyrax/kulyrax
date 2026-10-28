//! Phase 2: one transaction per adapter per batch.
//!
//! [`dispatch_batch`] dispatches a batch of events exactly as [`dispatch`]
//! does one at a time — events in order, each through its adapters in
//! dependency order — but inside one transaction per adapter
//! ([`StorageAdapter::begin_batch`]), committed together at the end. It is
//! optimistic: the first failure rolls every adapter back and the caller
//! dispatches the batch again one event at a time, the path that isolates the
//! failing event for retry and dead-lettering. No savepoints.
//!
//! Decision record: `docs/ledger/decisions/2026-10-28-batch-transactions.md`.

use std::collections::HashMap;

use crate::adapter::StorageAdapter;
use crate::dispatch::dispatch;
use crate::event::Event;
use crate::execution::stages::{Stage, timed};
use crate::execution::{ExecutionReport, ExecutionStatus};
use crate::routing::{AdapterId, route_with_rules};
use crate::runtime::config::FailurePolicy;
use crate::runtime::dependency_graph::{AdapterExecutionMeta, execution_order_for_routed};

/// Why a batch was rolled back instead of committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackReason {
    /// The adapters' dependency order could not be resolved, or a routed
    /// adapter does not exist; the per-event path reports it.
    Routing,
    /// An adapter could not open its batch transaction.
    Begin { adapter_id: String, message: String },
    /// The event at this index of the batch failed.
    Event { index: usize },
    /// An adapter failed to commit. Adapters earlier in dependency order may
    /// already have committed; their guards skip those events when the batch
    /// is re-dispatched.
    Commit { adapter_id: String, message: String },
}

/// The result of [`dispatch_batch`].
#[derive(Debug)]
pub enum BatchResult {
    /// Every adapter committed. One report per event, in batch order.
    Committed(Vec<ExecutionReport>),
    /// Every open batch transaction was rolled back; dispatch the batch one
    /// event at a time.
    RolledBack(RollbackReason),
}

/// Dispatch `events` in one transaction per adapter. See the module docs.
pub fn dispatch_batch(
    events: &[&Event],
    adapters: &mut [Box<dyn StorageAdapter>],
    failure_policy: FailurePolicy,
    routing_rules: &HashMap<String, Vec<AdapterId>>,
    adapter_meta: &HashMap<AdapterId, AdapterExecutionMeta>,
) -> BatchResult {
    // Each routed adapter and its events, in dispatch order.
    let mut routed: Vec<AdapterId> = Vec::new();
    let mut events_for: HashMap<AdapterId, Vec<&Event>> = HashMap::new();
    for &event in events {
        for id in route_with_rules(event, routing_rules) {
            let list = events_for.entry(id.clone()).or_insert_with(|| {
                routed.push(id.clone());
                Vec::new()
            });
            list.push(event);
        }
    }

    // Commit order: the same dependency order dispatch uses per event.
    let Ok(order) = execution_order_for_routed(&routed, adapter_meta) else {
        return BatchResult::RolledBack(RollbackReason::Routing);
    };
    let index_of: HashMap<&str, usize> = adapters
        .iter()
        .enumerate()
        .map(|(i, a)| (a.id(), i))
        .collect();
    let mut indices = Vec::with_capacity(order.len());
    for id in &order {
        match index_of.get(id.as_str()) {
            Some(&i) => indices.push(i),
            None => return BatchResult::RolledBack(RollbackReason::Routing),
        }
    }

    for (n, &i) in indices.iter().enumerate() {
        let batch_events = events_for
            .get(adapters[i].id())
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Err(e) = timed(Stage::Batch, || adapters[i].begin_batch(batch_events)) {
            let reason = RollbackReason::Begin {
                adapter_id: adapters[i].id().to_string(),
                message: e.to_string(),
            };
            rollback(adapters, &indices[..n]);
            return BatchResult::RolledBack(reason);
        }
    }

    let mut reports = Vec::with_capacity(events.len());
    for (index, &event) in events.iter().enumerate() {
        let report = dispatch(event, adapters, failure_policy, routing_rules, adapter_meta);
        if report.status == ExecutionStatus::Failed {
            rollback(adapters, &indices);
            return BatchResult::RolledBack(RollbackReason::Event { index });
        }
        reports.push(report);
    }

    for (n, &i) in indices.iter().enumerate() {
        if let Err(e) = timed(Stage::Batch, || adapters[i].commit_batch()) {
            let reason = RollbackReason::Commit {
                adapter_id: adapters[i].id().to_string(),
                message: e.to_string(),
            };
            rollback(adapters, &indices[n..]);
            return BatchResult::RolledBack(reason);
        }
    }
    BatchResult::Committed(reports)
}

/// Roll back these adapters' batches, last in dependency order first.
fn rollback(adapters: &[Box<dyn StorageAdapter>], indices: &[usize]) {
    for &i in indices.iter().rev() {
        timed(Stage::Batch, || adapters[i].rollback_batch());
    }
}
