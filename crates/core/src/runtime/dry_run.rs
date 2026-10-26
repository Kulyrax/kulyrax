//! `dry-run`: show what one event would write, without writing it.
//!
//! The adapters are never called and no store is contacted. For each adapter
//! the event routes to, in execution order, the adapter's storage kind is
//! looked up and that kind's real projection builder resolves the write, with
//! the same upcasting a live run applies. Because no guard state is read, the
//! report shows the write a live run would *attempt*; whether it would then be
//! skipped as already projected or stale is not evaluated.

use std::collections::HashMap;

use serde::Serialize;

use crate::adapter::document::runtime::DocumentRuntimeBuilder;
use crate::adapter::graph::mapping::ResolvedRecord;
use crate::adapter::graph::runtime::GraphRuntimeBuilder;
use crate::adapter::keyvalue::runtime::KvRuntimeBuilder;
use crate::adapter::sql::runtime::SqlRuntimeBuilder;
use crate::adapter::{OnExisting, Operation};
use crate::event::Event;
use crate::routing::{AdapterId, StorageKind};
use crate::runtime::config::KulyraxConfig;
use crate::runtime::registry::MappingSet;
use crate::runtime::validation::{
    ValidationReport, storage_kind, validate_routing_and_dependencies_for_event_type,
};
use crate::upcast::UpcasterRegistry;

/// What one event would write, adapter by adapter, in execution order.
#[derive(Debug, Clone, Serialize)]
pub struct DryRunReport {
    pub event_id: String,
    pub event_type: String,
    pub adapters: Vec<DryRunAdapter>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DryRunAdapter {
    pub adapter_id: AdapterId,
    /// `None` for a third-party adapter type, whose storage kind is only
    /// known once its factory builds it.
    pub kind: Option<StorageKind>,
    pub outcome: DryRunOutcome,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DryRunOutcome {
    /// The write a live run would attempt.
    Planned(PlannedWrite),
    /// The projection could not be resolved (no mapping, a missing payload
    /// field, a failed upcast); a live run would fail the same way.
    Failed { error: String },
    /// A third-party adapter type: its kind, and so its projection, is
    /// unknown without building it, which dry-run never does.
    NotPreviewable { reason: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct PlannedWrite {
    pub operation: Operation,
    /// Only meaningful for `operation: upsert`.
    pub on_existing: OnExisting,
    /// Only meaningful for `operation: delete`.
    pub permanent: bool,
    /// Empty for the default facet (the whole entity).
    pub facet: String,
    /// Table, collection, key-value namespace, `node <label>` or `edge <type>`.
    pub target: String,
    pub entity_key: String,
    /// Column values, the document, the stored value, or the node/edge
    /// properties.
    pub values: serde_json::Value,
    pub source_version: u32,
    pub projected_version: u32,
}

pub(crate) fn dry_run_event(
    event: &Event,
    config: &KulyraxConfig,
    routing: &HashMap<String, Vec<AdapterId>>,
    mappings: &MappingSet,
    upcasters: &UpcasterRegistry,
) -> Result<DryRunReport, ValidationReport> {
    let order =
        validate_routing_and_dependencies_for_event_type(config, routing, &event.event_type)?;

    let adapters = order
        .into_iter()
        .map(|adapter_id| {
            let kind = config
                .adapters
                .iter()
                .find(|a| a.id() == adapter_id)
                .and_then(storage_kind);
            let outcome = match kind {
                Some(kind) => match plan_write(kind, event, mappings, upcasters) {
                    Ok(plan) => DryRunOutcome::Planned(plan),
                    Err(error) => DryRunOutcome::Failed { error },
                },
                None => DryRunOutcome::NotPreviewable {
                    reason:
                        "third-party adapter type; its storage kind is unknown until it is built"
                            .to_string(),
                },
            };
            DryRunAdapter {
                adapter_id,
                kind,
                outcome,
            }
        })
        .collect();

    Ok(DryRunReport {
        event_id: event.event_id.clone(),
        event_type: event.event_type.clone(),
        adapters,
    })
}

/// Resolve the write with the kind's own builder, handing it only the
/// mapping for this event so no mapping table is cloned whole.
fn plan_write(
    kind: StorageKind,
    event: &Event,
    mappings: &MappingSet,
    upcasters: &UpcasterRegistry,
) -> Result<PlannedWrite, String> {
    match kind {
        StorageKind::Sql => {
            let mapping = mappings.sql.get(&event.event_type);
            let builder = SqlRuntimeBuilder::new(single(event, mapping));
            let p = builder.build(event, upcasters).map_err(|e| e.to_string())?;
            let values = p
                .write
                .columns
                .iter()
                .cloned()
                .zip(p.write.values.iter().cloned())
                .collect::<serde_json::Map<_, _>>();
            Ok(PlannedWrite {
                operation: p.operation,
                on_existing: p.on_existing,
                permanent: p.permanent,
                facet: p.facet,
                target: p.table,
                entity_key: p.entity_key,
                values: serde_json::Value::Object(values),
                source_version: p.source_version,
                projected_version: p.projected_version,
            })
        }
        StorageKind::Document => {
            let mapping = mappings.document.get(&event.event_type);
            let builder = DocumentRuntimeBuilder::new(single(event, mapping));
            let p = builder.build(event, upcasters).map_err(|e| e.to_string())?;
            Ok(PlannedWrite {
                operation: p.operation,
                on_existing: p.on_existing,
                permanent: p.permanent,
                facet: p.facet,
                target: p.collection,
                entity_key: p.entity_id,
                values: p.document,
                source_version: p.source_version,
                projected_version: p.projected_version,
            })
        }
        StorageKind::KeyValue => {
            let mapping = mappings.kv.get(&event.event_type);
            let builder = KvRuntimeBuilder::new(single(event, mapping));
            let p = builder.build(event, upcasters).map_err(|e| e.to_string())?;
            Ok(PlannedWrite {
                operation: p.operation,
                on_existing: p.on_existing,
                permanent: p.permanent,
                facet: p.facet,
                target: p.namespace,
                entity_key: p.entity_key,
                values: p.value,
                source_version: p.source_version,
                projected_version: p.projected_version,
            })
        }
        StorageKind::Graph => {
            let mapping = mappings.graph.get(&event.event_type);
            let builder = GraphRuntimeBuilder::new(single(event, mapping));
            let p = builder.build(event, upcasters).map_err(|e| e.to_string())?;
            let (target, entity_key, values) = match p.record {
                ResolvedRecord::Node {
                    label,
                    node_id,
                    properties,
                } => (format!("node {label}"), node_id, properties),
                ResolvedRecord::Edge {
                    edge_type,
                    from,
                    to,
                    discriminator,
                    properties,
                } => {
                    let key = match discriminator {
                        Some(d) => format!("{from} -> {to} ({d})"),
                        None => format!("{from} -> {to}"),
                    };
                    (format!("edge {edge_type}"), key, properties)
                }
            };
            Ok(PlannedWrite {
                operation: p.operation,
                on_existing: p.on_existing,
                permanent: p.permanent,
                facet: p.facet,
                target,
                entity_key,
                values,
                source_version: p.source_version,
                projected_version: p.projected_version,
            })
        }
        StorageKind::Custom => Err("custom adapters have no storage kind to preview".to_string()),
    }
}

fn single<M: Clone>(event: &Event, mapping: Option<&M>) -> HashMap<String, M> {
    mapping
        .map(|m| HashMap::from([(event.event_type.clone(), m.clone())]))
        .unwrap_or_default()
}
