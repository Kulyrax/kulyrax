use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use crate::adapter::{AdapterOutcome, StorageAdapter};
use crate::event::Event;
use crate::execution::{AdapterExecutionReport, AdapterReportError, ExecutionReport};
use crate::routing::{AdapterId, StorageKind, route_with_rules};
use crate::runtime::config::FailurePolicy;
use crate::runtime::dependency_graph::{
    AdapterExecutionMeta, DependencyOrderError, execution_order_for_routed,
};

fn dependency_order_failure_report(
    event: &Event,
    started_at: SystemTime,
    message: String,
) -> ExecutionReport {
    let trace_id = format!("trace-{}", event.id());
    let mut report = ExecutionReport::new(
        event.id().to_string(),
        event.event_type().to_string(),
        trace_id,
        started_at,
    )
    .with_source_version(event.version());
    let adapter_report =
        AdapterExecutionReport::new("_dispatch".to_string(), StorageKind::Sql, started_at)
            .finish_failed(started_at, AdapterReportError::WriteFailed { message });
    report.push_adapter_report(adapter_report);
    report.finish(started_at)
}

/// Dispatch a single event: resolve routing, order adapters by dependency, and
/// run each in order, honoring `failure_policy` on the first failure.
pub fn dispatch(
    event: &Event,
    adapters: &mut [Box<dyn StorageAdapter>],
    failure_policy: FailurePolicy,
    routing_rules: &HashMap<String, Vec<AdapterId>>,
    adapter_meta: &HashMap<AdapterId, AdapterExecutionMeta>,
) -> ExecutionReport {
    let routed = route_with_rules(event, routing_rules);
    let targets: HashSet<_> = routed.iter().cloned().collect();

    let trace_id = format!("trace-{}", event.id());
    let started_at = SystemTime::now();

    let mut report = ExecutionReport::new(
        event.id().to_string(),
        event.event_type().to_string(),
        trace_id,
        started_at,
    )
    .with_source_version(event.version());

    if targets.is_empty() {
        return report.finish(SystemTime::now());
    }

    let order = match execution_order_for_routed(&routed, adapter_meta) {
        Ok(o) => o,
        Err(DependencyOrderError::Cycle { adapters }) => {
            return dependency_order_failure_report(
                event,
                started_at,
                format!("cyclic adapter dependencies among: {}", adapters.join(", ")),
            );
        }
        Err(DependencyOrderError::MissingAdapterMeta { adapter_id }) => {
            return dependency_order_failure_report(
                event,
                started_at,
                format!("no adapter metadata for routed id {:?}", adapter_id),
            );
        }
    };

    let id_to_index: HashMap<String, usize> = adapters
        .iter()
        .enumerate()
        .map(|(i, a)| (a.id().to_string(), i))
        .collect();

    for adapter_id in order {
        let Some(&idx) = id_to_index.get(adapter_id.as_str()) else {
            return dependency_order_failure_report(
                event,
                started_at,
                format!("no adapter instance for id {:?}", adapter_id),
            );
        };

        let adapter = &mut adapters[idx];
        let adapter_started_at = SystemTime::now();
        let result = adapter.handle(event);
        let adapter_finished_at = SystemTime::now();

        let adapter_report = AdapterExecutionReport::new(
            adapter.id().to_string(),
            adapter.kind(),
            adapter_started_at,
        )
        .with_versions(result.source_version, result.projected_version);

        let failed = matches!(result.outcome, AdapterOutcome::Failed(_));

        let adapter_report = match result.outcome {
            AdapterOutcome::Created => adapter_report.finish_created(adapter_finished_at),
            AdapterOutcome::Updated => adapter_report.finish_updated(adapter_finished_at),
            AdapterOutcome::Deleted => adapter_report.finish_deleted(adapter_finished_at),
            AdapterOutcome::Skipped(reason) => {
                adapter_report.finish_skipped(adapter_finished_at, reason)
            }
            AdapterOutcome::Failed(err) => {
                adapter_report.finish_failed(adapter_finished_at, (&err).into())
            }
        };

        report.push_adapter_report(adapter_report);

        if failed && failure_policy == FailurePolicy::FailFast {
            return report.finish(adapter_finished_at);
        }
    }

    report.finish(SystemTime::now())
}
