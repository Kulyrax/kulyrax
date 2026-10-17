//! Phase 8: capability-safe projection validation (routing, mappings, adapters).

use core::fmt;
use std::collections::{HashMap, HashSet};

use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::mapping::SqlMapping;
use crate::adapter::{OnExisting, Operation};
use crate::routing::{AdapterId, StorageKind};
use crate::runtime::config::{AdapterCapability, AdapterConfig, KulyraxConfig};
use crate::runtime::dependency_graph::{
    AdapterExecutionMeta, DependencyOrderError, adapter_metadata_map, execution_order_for_routed,
};

// ---------------------------------------------------------------------------
// Issues & report
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ValidationIssue {
    InvalidSqlMapping {
        event: String,
        reason: String,
    },
    InvalidDocumentMapping {
        event: String,
        reason: String,
    },
    InvalidKvMapping {
        event: String,
        reason: String,
    },
    InvalidGraphMapping {
        event: String,
        reason: String,
    },
    UnknownAdapter {
        event_type: String,
        adapter_id: String,
    },
    MissingSqlMapping {
        event_type: String,
        adapter_id: String,
    },
    MissingDocumentMapping {
        event_type: String,
        adapter_id: String,
    },
    MissingKvMapping {
        event_type: String,
        adapter_id: String,
    },
    MissingGraphMapping {
        event_type: String,
        adapter_id: String,
    },
    CapabilityMismatch {
        event_type: String,
        adapter_id: String,
        missing: Vec<AdapterCapability>,
    },
    /// SQL mapping exists but event type is not in routing.
    UnroutedSqlMapping {
        event: String,
    },
    /// Event is routed but no Sqlite adapter on the route (SQL mapping unreachable).
    SqlMappingNoSqlTarget {
        event: String,
    },
    UnroutedDocumentMapping {
        event: String,
    },
    DocumentMappingNoFileTarget {
        event: String,
    },
    /// Key-value mapping exists but event type is not in routing.
    UnroutedKvMapping {
        event: String,
    },
    /// Event is routed but no key-value adapter on the route (mapping unreachable).
    KvMappingNoKeyValueTarget {
        event: String,
    },
    /// Graph mapping exists but event type is not in routing.
    UnroutedGraphMapping {
        event: String,
    },
    /// Event is routed but no graph adapter on the route (mapping unreachable).
    GraphMappingNoGraphTarget {
        event: String,
    },
    /// HashMap key must match `event` field (routing and lookups use the key).
    MappingKeyMismatch {
        map_key: String,
        event_field: String,
        kind: &'static str,
    },
    /// Same adapter id listed more than once for one event type.
    DuplicateRoutingTarget {
        event_type: String,
        adapter_id: String,
    },
    /// Config lists the same adapter id more than once (defense in depth vs `KulyraxConfig::validate`).
    DuplicateAdapterId {
        adapter_id: String,
    },
    /// Routed adapter depends on another adapter not on this event's route.
    DependencyNotOnRoute {
        event_type: String,
        adapter_id: String,
        dependency: String,
    },
    /// Cyclic depends_on among adapters routed for this event (`adapters` sorted by id).
    DependencyCycle {
        event_type: String,
        adapters: Vec<AdapterId>,
    },
    /// Phase 15.1: a faceted entity has a mapping using `on_existing: replace` —
    /// whole-entity replace and facets are mutually exclusive.
    FacetReplaceConflict {
        entity: String,
        kind: &'static str,
    },
    /// Phase 15.1: `operation: delete` on a named-facet mapping. You delete an
    /// entity, not a facet — delete is only valid on the default facet.
    FacetDeleteNotDefault {
        event: String,
        kind: &'static str,
    },
    /// Phase 15.1: a named facet owns no non-key field — nothing for its lane
    /// to write.
    FacetHasNoFields {
        event: String,
        kind: &'static str,
    },
    /// Phase 15.1: two facets of one entity both claim the same field — they
    /// would race their independent sequence gates.
    FacetFieldOverlap {
        entity: String,
        field: String,
        kind: &'static str,
    },
    /// Phase 15.1: a named facet's identity (primary_key / id / key) differs
    /// from the entity's — it must identify the same entity.
    FacetKeyMismatch {
        event: String,
        kind: &'static str,
    },
}

impl fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationIssue::InvalidSqlMapping { event, reason } => {
                write!(f, "invalid SQL mapping for event {:?}: {}", event, reason)
            }
            ValidationIssue::InvalidDocumentMapping { event, reason } => {
                write!(
                    f,
                    "invalid document mapping for event {:?}: {}",
                    event, reason
                )
            }
            ValidationIssue::InvalidKvMapping { event, reason } => {
                write!(
                    f,
                    "invalid key-value mapping for event {:?}: {}",
                    event, reason
                )
            }
            ValidationIssue::InvalidGraphMapping { event, reason } => {
                write!(f, "invalid graph mapping for event {:?}: {}", event, reason)
            }
            ValidationIssue::UnknownAdapter {
                event_type,
                adapter_id,
            } => {
                write!(
                    f,
                    "routing: event {:?} references unknown adapter {:?}",
                    event_type, adapter_id
                )
            }
            ValidationIssue::MissingSqlMapping {
                event_type,
                adapter_id,
            } => {
                write!(
                    f,
                    "routing: event {:?} targets sql adapter {:?} but no SQL mapping exists for that event",
                    event_type, adapter_id
                )
            }
            ValidationIssue::MissingDocumentMapping {
                event_type,
                adapter_id,
            } => {
                write!(
                    f,
                    "routing: event {:?} targets document adapter {:?} but no document mapping exists for that event",
                    event_type, adapter_id
                )
            }
            ValidationIssue::MissingKvMapping {
                event_type,
                adapter_id,
            } => {
                write!(
                    f,
                    "routing: event {:?} targets key-value adapter {:?} but no key-value mapping exists for that event",
                    event_type, adapter_id
                )
            }
            ValidationIssue::MissingGraphMapping {
                event_type,
                adapter_id,
            } => {
                write!(
                    f,
                    "routing: event {:?} targets graph adapter {:?} but no graph mapping exists for that event",
                    event_type, adapter_id
                )
            }
            ValidationIssue::CapabilityMismatch {
                event_type,
                adapter_id,
                missing,
            } => {
                let caps: Vec<_> = missing.iter().map(cap_label).collect();
                write!(
                    f,
                    "routing: event {:?} adapter {:?} lacks required capabilities: {}",
                    event_type,
                    adapter_id,
                    caps.join(", ")
                )
            }
            ValidationIssue::UnroutedSqlMapping { event } => {
                write!(
                    f,
                    "SQL mapping for event {:?} is never routed (add route or remove mapping)",
                    event
                )
            }
            ValidationIssue::SqlMappingNoSqlTarget { event } => {
                write!(
                    f,
                    "SQL mapping for event {:?} has no sqlite adapter on its route",
                    event
                )
            }
            ValidationIssue::UnroutedDocumentMapping { event } => {
                write!(f, "document mapping for event {:?} is never routed", event)
            }
            ValidationIssue::DocumentMappingNoFileTarget { event } => {
                write!(
                    f,
                    "document mapping for event {:?} has no file adapter on its route",
                    event
                )
            }
            ValidationIssue::UnroutedKvMapping { event } => {
                write!(
                    f,
                    "key-value mapping for event {:?} is never routed (add route or remove mapping)",
                    event
                )
            }
            ValidationIssue::KvMappingNoKeyValueTarget { event } => {
                write!(
                    f,
                    "key-value mapping for event {:?} has no key-value adapter on its route",
                    event
                )
            }
            ValidationIssue::UnroutedGraphMapping { event } => {
                write!(
                    f,
                    "graph mapping for event {:?} is never routed (add route or remove mapping)",
                    event
                )
            }
            ValidationIssue::GraphMappingNoGraphTarget { event } => {
                write!(
                    f,
                    "graph mapping for event {:?} has no graph adapter on its route",
                    event
                )
            }
            ValidationIssue::MappingKeyMismatch {
                map_key,
                event_field,
                kind,
            } => {
                write!(
                    f,
                    "{} mapping: map key {:?} must match event field {:?}",
                    kind, map_key, event_field
                )
            }
            ValidationIssue::DuplicateRoutingTarget {
                event_type,
                adapter_id,
            } => {
                write!(
                    f,
                    "routing: event {:?} lists adapter {:?} more than once",
                    event_type, adapter_id
                )
            }
            ValidationIssue::DuplicateAdapterId { adapter_id } => {
                write!(f, "config: duplicate adapter id {:?}", adapter_id)
            }
            ValidationIssue::DependencyNotOnRoute {
                event_type,
                adapter_id,
                dependency,
            } => {
                write!(
                    f,
                    "routing: event {:?} adapter {:?} depends on {:?} which is not on this route",
                    event_type, adapter_id, dependency
                )
            }
            ValidationIssue::DependencyCycle {
                event_type,
                adapters,
            } => {
                write!(
                    f,
                    "routing: event {:?} has cyclic adapter dependencies among {:?}",
                    event_type,
                    adapters.join(", ")
                )
            }
            ValidationIssue::FacetReplaceConflict { entity, kind } => {
                write!(
                    f,
                    "{} facet: entity {:?} is faceted, so no mapping may use 'on_existing: replace'",
                    kind, entity
                )
            }
            ValidationIssue::FacetDeleteNotDefault { event, kind } => {
                write!(
                    f,
                    "{} facet: mapping for event {:?} has 'operation: delete' on a named facet (delete is default-facet only)",
                    kind, event
                )
            }
            ValidationIssue::FacetHasNoFields { event, kind } => {
                write!(
                    f,
                    "{} facet: mapping for event {:?} declares a facet but owns no non-key field",
                    kind, event
                )
            }
            ValidationIssue::FacetFieldOverlap {
                entity,
                field,
                kind,
            } => {
                write!(
                    f,
                    "{} facet: entity {:?} has two facets both claiming field {:?}",
                    kind, entity, field
                )
            }
            ValidationIssue::FacetKeyMismatch { event, kind } => {
                write!(
                    f,
                    "{} facet: mapping for event {:?} has a facet identity that differs from the entity's primary key",
                    kind, event
                )
            }
        }
    }
}

fn cap_label(c: &AdapterCapability) -> &'static str {
    match c {
        AdapterCapability::Write => "write",
        AdapterCapability::Idempotent => "idempotent",
        AdapterCapability::Upsert => "upsert",
        AdapterCapability::Transactions => "transactions",
        AdapterCapability::Delete => "delete",
    }
}

#[derive(Debug, Clone, Default)]
pub struct ValidationReport(pub Vec<ValidationIssue>);

impl ValidationReport {
    fn push(&mut self, issue: ValidationIssue) {
        self.0.push(issue);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_result(self) -> Result<(), Self> {
        if self.is_empty() { Ok(()) } else { Err(self) }
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return write!(f, "(no validation issues)");
        }
        for (i, issue) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", issue)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationReport {}

// ---------------------------------------------------------------------------
// Validation logic
// ---------------------------------------------------------------------------

/// First adapter per id; reports duplicate ids in `report`.
fn build_adapter_by_id<'a>(
    config: &'a KulyraxConfig,
    report: &mut ValidationReport,
) -> HashMap<&'a str, &'a AdapterConfig> {
    use std::collections::hash_map::Entry;
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for a in &config.adapters {
        *counts.entry(a.id()).or_insert(0) += 1;
    }
    for (id, n) in &counts {
        if *n > 1 {
            report.push(ValidationIssue::DuplicateAdapterId {
                adapter_id: (*id).to_string(),
            });
        }
    }
    let mut map: HashMap<&'a str, &'a AdapterConfig> = HashMap::new();
    for a in &config.adapters {
        match map.entry(a.id()) {
            Entry::Vacant(v) => {
                v.insert(a);
            }
            Entry::Occupied(_) => {}
        }
    }
    map
}

/// Storage kind of the first-party backends that ship as their own
/// `kulyrax-adapter-*` crates. Validation runs before the registry resolves
/// any custom type, so without this table their mappings and capabilities
/// would go unchecked.
fn first_party_backend_kind(type_name: &str) -> Option<StorageKind> {
    match type_name {
        "postgres" | "mysql" => Some(StorageKind::Sql),
        "mongodb" | "elasticsearch" => Some(StorageKind::Document),
        "redis" => Some(StorageKind::KeyValue),
        "neo4j" => Some(StorageKind::Graph),
        _ => None,
    }
}

/// `None` for a third-party custom type, whose kind is unknown until its
/// factory runs; its kind-specific checks are deferred to build time.
fn storage_kind(a: &AdapterConfig) -> Option<StorageKind> {
    match a {
        AdapterConfig::Sqlite(_) => Some(StorageKind::Sql),
        AdapterConfig::File(_) => Some(StorageKind::Document),
        AdapterConfig::KeyValue(_) => Some(StorageKind::KeyValue),
        AdapterConfig::Graph(_) => Some(StorageKind::Graph),
        AdapterConfig::Custom(c) => first_party_backend_kind(&c.type_name),
    }
}

fn is_sql(a: &AdapterConfig) -> bool {
    storage_kind(a) == Some(StorageKind::Sql)
}

fn is_document(a: &AdapterConfig) -> bool {
    storage_kind(a) == Some(StorageKind::Document)
}

fn is_keyvalue(a: &AdapterConfig) -> bool {
    storage_kind(a) == Some(StorageKind::KeyValue)
}

fn is_graph(a: &AdapterConfig) -> bool {
    storage_kind(a) == Some(StorageKind::Graph)
}

/// A route whose only target is an unknown custom type may well be the right
/// kind, so the "mapping has a target of its kind" checks give it the benefit
/// of the doubt.
fn kind_unknown(a: &AdapterConfig) -> bool {
    storage_kind(a).is_none()
}

fn is_scalar_path(p: &str) -> bool {
    p.starts_with("payload.") || p.starts_with("metadata.")
}

/// When unset or empty, adapter is treated as declaring only `Write`.
fn effective_capabilities(adapter: &AdapterConfig) -> HashSet<AdapterCapability> {
    match adapter.capabilities() {
        None | Some([]) => [AdapterCapability::Write].into_iter().collect(),
        Some(v) => v.iter().copied().collect(),
    }
}

/// Effective required capabilities for a mapping (Phase 12.5, extended
/// 13.5): `on_existing: replace` implies `Upsert`, and `operation: delete`
/// implies `Delete` — whether or not the mapping's `requires_capabilities`
/// lists them explicitly.
fn required_capabilities(
    declared: &[AdapterCapability],
    on_existing: OnExisting,
    operation: Operation,
) -> Vec<AdapterCapability> {
    let mut required = declared.to_vec();
    if on_existing == OnExisting::Replace && !required.contains(&AdapterCapability::Upsert) {
        required.push(AdapterCapability::Upsert);
    }
    if operation == Operation::Delete && !required.contains(&AdapterCapability::Delete) {
        required.push(AdapterCapability::Delete);
    }
    required
}

fn document_template_empty(doc: &serde_json::Value) -> bool {
    doc.is_null()
        || doc.as_object().is_some_and(|o| o.is_empty())
        || doc.as_array().is_some_and(|a| a.is_empty())
}

/// One mapping, normalized for Phase 15.1 facet validation.
struct FacetView {
    event: String,
    /// table / collection / namespace.
    entity: String,
    /// `""` = default facet.
    facet: String,
    on_existing: OnExisting,
    operation: Operation,
    /// primary_key column(s) / `[id path]` / `[key path]`, in declared order.
    identity: Vec<String>,
    /// non-key column names / top-level object keys this mapping writes.
    fields: Vec<String>,
    kind: &'static str,
}

/// Phase 15.1: an entity partitioned into facets must keep them consistent —
/// no whole-entity replace, disjoint field ownership, one shared identity, and
/// delete only on the default facet.
fn validate_facets(mut views: Vec<FacetView>, report: &mut ValidationReport) {
    // Deterministic order regardless of HashMap iteration.
    views.sort_by(|a, b| (a.entity.as_str(), a.event.as_str()).cmp(&(&b.entity, &b.event)));

    let mut by_entity: std::collections::BTreeMap<(&str, &str), Vec<&FacetView>> =
        std::collections::BTreeMap::new();
    for v in &views {
        by_entity
            .entry((v.kind, v.entity.as_str()))
            .or_default()
            .push(v);
    }

    for ((kind, entity), group) in by_entity {
        let is_faceted = group.iter().any(|v| !v.facet.is_empty());
        if !is_faceted {
            continue;
        }

        // No whole-entity replace anywhere on a faceted entity.
        if group.iter().any(|v| v.on_existing == OnExisting::Replace) {
            report.push(ValidationIssue::FacetReplaceConflict {
                entity: entity.to_string(),
                kind,
            });
        }

        // Every mapping for a faceted entity shares one identity.
        let canonical = group
            .iter()
            .find(|v| v.facet.is_empty())
            .or_else(|| group.first())
            .map(|v| v.identity.clone())
            .unwrap_or_default();
        for v in &group {
            if v.identity != canonical {
                report.push(ValidationIssue::FacetKeyMismatch {
                    event: v.event.clone(),
                    kind,
                });
            }
        }

        // Named-facet rules + disjoint field ownership.
        let mut owner: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
        for v in &group {
            if !v.facet.is_empty() {
                if v.operation == Operation::Delete {
                    report.push(ValidationIssue::FacetDeleteNotDefault {
                        event: v.event.clone(),
                        kind,
                    });
                }
                if v.fields.is_empty() {
                    report.push(ValidationIssue::FacetHasNoFields {
                        event: v.event.clone(),
                        kind,
                    });
                }
            }
            for field in &v.fields {
                match owner.get(field.as_str()) {
                    Some(existing) if *existing != v.facet.as_str() => {
                        report.push(ValidationIssue::FacetFieldOverlap {
                            entity: entity.to_string(),
                            field: field.clone(),
                            kind,
                        });
                    }
                    _ => {
                        owner.insert(field.as_str(), v.facet.as_str());
                    }
                }
            }
        }
    }
}

fn validate_route_dependencies(
    event_type: &str,
    adapter_ids: &[AdapterId],
    adapters_by_id: &HashMap<&str, &AdapterConfig>,
    adapter_meta: &HashMap<AdapterId, AdapterExecutionMeta>,
    report: &mut ValidationReport,
) {
    if !adapter_ids
        .iter()
        .all(|id| adapters_by_id.contains_key(id.as_str()))
    {
        return;
    }
    let routed_set: HashSet<_> = adapter_ids.iter().cloned().collect();
    for aid in &routed_set {
        let ac = adapters_by_id[aid.as_str()];
        for d in ac.depends_on() {
            if !routed_set.contains(d) {
                report.push(ValidationIssue::DependencyNotOnRoute {
                    event_type: event_type.to_string(),
                    adapter_id: aid.clone(),
                    dependency: d.clone(),
                });
            }
        }
    }
    match execution_order_for_routed(adapter_ids, adapter_meta) {
        Err(DependencyOrderError::Cycle { adapters }) => {
            report.push(ValidationIssue::DependencyCycle {
                event_type: event_type.to_string(),
                adapters,
            });
        }
        Err(DependencyOrderError::MissingAdapterMeta { .. }) => {}
        Ok(_) => {}
    }
}

/// Validate routing + dependency closure + acyclicity for one event type; returns execution order.
pub fn validate_routing_and_dependencies_for_event_type(
    config: &KulyraxConfig,
    routing: &HashMap<String, Vec<AdapterId>>,
    event_type: &str,
) -> Result<Vec<AdapterId>, ValidationReport> {
    let mut report = ValidationReport::default();
    let adapters_by_id = build_adapter_by_id(config, &mut report);
    let adapter_meta = adapter_metadata_map(config);

    let Some(adapter_ids) = routing.get(event_type) else {
        return if report.is_empty() {
            Ok(Vec::new())
        } else {
            Err(report)
        };
    };

    let mut seen_route = HashSet::new();
    for adapter_id in adapter_ids {
        if !seen_route.insert(adapter_id.as_str()) {
            report.push(ValidationIssue::DuplicateRoutingTarget {
                event_type: event_type.to_string(),
                adapter_id: adapter_id.clone(),
            });
        }
    }
    for adapter_id in adapter_ids {
        if !adapters_by_id.contains_key(adapter_id.as_str()) {
            report.push(ValidationIssue::UnknownAdapter {
                event_type: event_type.to_string(),
                adapter_id: adapter_id.clone(),
            });
        }
    }

    validate_route_dependencies(
        event_type,
        adapter_ids,
        &adapters_by_id,
        &adapter_meta,
        &mut report,
    );

    if !report.is_empty() {
        return Err(report);
    }

    match execution_order_for_routed(adapter_ids, &adapter_meta) {
        Ok(order) => Ok(order),
        Err(DependencyOrderError::Cycle { adapters }) => {
            report.push(ValidationIssue::DependencyCycle {
                event_type: event_type.to_string(),
                adapters,
            });
            Err(report)
        }
        Err(DependencyOrderError::MissingAdapterMeta { adapter_id }) => {
            report.push(ValidationIssue::UnknownAdapter {
                event_type: event_type.to_string(),
                adapter_id,
            });
            Err(report)
        }
    }
}

/// Full Phase 8 validation: routing ↔ adapters ↔ mappings ↔ capabilities, plus orphan mappings.
#[allow(clippy::too_many_arguments)]
pub fn validate_projection_config(
    config: &KulyraxConfig,
    routing: &HashMap<String, Vec<AdapterId>>,
    sql: &HashMap<String, SqlMapping>,
    doc: &HashMap<String, DocumentMapping>,
    kv: &HashMap<String, KvMapping>,
    graph: &HashMap<String, GraphMapping>,
) -> Result<(), ValidationReport> {
    let mut report = ValidationReport::default();
    let adapters_by_id = build_adapter_by_id(config, &mut report);
    let adapter_meta = adapter_metadata_map(config);

    // Connection settings of registry-resolved backends are checked by each
    // backend's factory when adapters are built, not here.

    for (event_type, adapter_ids) in routing {
        let mut seen_route: HashSet<&str> = HashSet::new();
        for adapter_id in adapter_ids {
            if !seen_route.insert(adapter_id.as_str()) {
                report.push(ValidationIssue::DuplicateRoutingTarget {
                    event_type: event_type.clone(),
                    adapter_id: adapter_id.clone(),
                });
            }
        }
    }

    for (event_type, adapter_ids) in routing {
        validate_route_dependencies(
            event_type,
            adapter_ids,
            &adapters_by_id,
            &adapter_meta,
            &mut report,
        );
    }

    for (key, m) in sql {
        if key != m.event.as_str() {
            report.push(ValidationIssue::MappingKeyMismatch {
                map_key: key.clone(),
                event_field: m.event.clone(),
                kind: "SQL",
            });
        }
        if m.event.trim().is_empty() {
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: "empty event".into(),
            });
        } else if m.table.trim().is_empty() {
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: "empty table".into(),
            });
        } else if m.primary_key.is_empty() || m.primary_key.iter().any(|c| c.trim().is_empty()) {
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: "primary_key must list at least one non-empty column name".into(),
            });
        } else if m.columns.is_empty() {
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: "columns must be non-empty".into(),
            });
        } else if let Some(missing) = m
            .primary_key
            .iter()
            .find(|c| !m.columns.contains_key(c.as_str()))
        {
            // Phase 11.1: `build()` resolves entity identity off these column(s)
            // (composite keys included); catch a stale/misspelled primary_key
            // at startup, not mid-run.
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: format!("primary_key column '{}' not found in columns", missing),
            });
        } else if m.operation == Operation::Delete && m.columns.len() != m.primary_key.len() {
            // Phase 13.1: a delete mapping resolves only the entity key — no
            // hidden projection body. The previous branch already guarantees
            // every primary_key column is present in columns; matching
            // lengths means columns can't contain anything else.
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: "delete mapping's columns may only contain the primary_key column(s)"
                    .into(),
            });
        } else if m.version < 1 {
            report.push(ValidationIssue::InvalidSqlMapping {
                event: key.clone(),
                reason: "version must be >= 1".into(),
            });
        }
    }

    for (key, m) in doc {
        if key != m.event.as_str() {
            report.push(ValidationIssue::MappingKeyMismatch {
                map_key: key.clone(),
                event_field: m.event.clone(),
                kind: "document",
            });
        }
        if m.event.trim().is_empty() {
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: "empty event".into(),
            });
        } else if m.collection.trim().is_empty() {
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: "empty collection".into(),
            });
        } else if m.operation == Operation::Upsert && document_template_empty(&m.document) {
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: "document template must not be empty (null, {}, or [])".into(),
            });
        } else if m.operation == Operation::Delete && !document_template_empty(&m.document) {
            // Phase 13.1: a delete mapping resolves only the entity key — no
            // hidden projection body.
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: "delete mapping's document must be empty (null, {}, or [])".into(),
            });
        } else if m.id.trim().is_empty() {
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: "empty id".into(),
            });
        } else if !(m.id.starts_with("payload.") || m.id.starts_with("metadata.")) {
            // Phase 11.1: `id` is resolved the same way as a `document` leaf;
            // catch a malformed entity-identity path at startup, not mid-run.
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: format!("id {:?} must be a 'payload.' or 'metadata.' path", m.id),
            });
        } else if m.version < 1 {
            report.push(ValidationIssue::InvalidDocumentMapping {
                event: key.clone(),
                reason: "version must be >= 1".into(),
            });
        }
    }

    for (key, m) in kv {
        if key != m.event.as_str() {
            report.push(ValidationIssue::MappingKeyMismatch {
                map_key: key.clone(),
                event_field: m.event.clone(),
                kind: "key-value",
            });
        }
        if m.event.trim().is_empty() {
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: "empty event".into(),
            });
        } else if m.namespace.trim().is_empty() {
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: "empty namespace".into(),
            });
        } else if m.key.trim().is_empty() {
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: "empty key".into(),
            });
        } else if !(m.key.starts_with("payload.") || m.key.starts_with("metadata.")) {
            // Phase 11.1: `key` is resolved the same way as a `value` leaf;
            // catch a malformed entity-identity path at startup, not mid-run.
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: format!("key {:?} must be a 'payload.' or 'metadata.' path", m.key),
            });
        } else if m.operation == Operation::Upsert && document_template_empty(&m.value) {
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: "value template must not be empty (null, {}, or [])".into(),
            });
        } else if m.operation == Operation::Delete && !document_template_empty(&m.value) {
            // Phase 13.1: a delete mapping resolves only the entity key — no
            // hidden projection body.
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: "delete mapping's value must be empty (null, {}, or [])".into(),
            });
        } else if m.version < 1 {
            report.push(ValidationIssue::InvalidKvMapping {
                event: key.clone(),
                reason: "version must be >= 1".into(),
            });
        }
    }

    // Phase 16.5: graph mapping structural checks (folded into the live
    // validator, per the Phase 14 finding that `*/validate.rs` are dead code).
    for (key, m) in graph {
        if key != m.event() {
            report.push(ValidationIssue::MappingKeyMismatch {
                map_key: key.clone(),
                event_field: m.event().to_string(),
                kind: "graph",
            });
        }
        let mut push = |reason: &str| {
            report.push(ValidationIssue::InvalidGraphMapping {
                event: key.clone(),
                reason: reason.to_string(),
            });
        };
        match m {
            GraphMapping::Node(n) => {
                if n.label.trim().is_empty() {
                    push("empty label");
                } else if !is_scalar_path(&n.key) {
                    push("key must be a 'payload.' or 'metadata.' path");
                } else if n.operation == Operation::Upsert && document_template_empty(&n.properties)
                {
                    push("node properties must not be empty for an upsert mapping");
                } else if n.operation == Operation::Delete
                    && !document_template_empty(&n.properties)
                {
                    push("delete mapping's properties must be empty");
                } else if n.operation == Operation::Delete && n.facet.is_some() {
                    push("operation: delete is only valid on the default facet");
                } else if n.version < 1 {
                    push("version must be >= 1");
                }
            }
            GraphMapping::Edge(e) => {
                if e.edge_type.trim().is_empty() {
                    push("empty edge_type");
                } else if !is_scalar_path(&e.from) {
                    push("from must be a 'payload.' or 'metadata.' path");
                } else if !is_scalar_path(&e.to) {
                    push("to must be a 'payload.' or 'metadata.' path");
                } else if e
                    .discriminator
                    .as_deref()
                    .is_some_and(|d| !is_scalar_path(d))
                {
                    push("discriminator must be a 'payload.' or 'metadata.' path");
                } else if e.operation == Operation::Delete
                    && !document_template_empty(&e.properties)
                {
                    // An edge with no properties is a valid bare relationship;
                    // only a delete mapping is required to carry none.
                    push("delete mapping's properties must be empty");
                } else if e.version < 1 {
                    push("version must be >= 1");
                }
            }
        }
    }

    // Phase 15.1: facet declaration & ownership validation, one entity group at
    // a time (a group is a table / collection / namespace / node label).
    {
        fn object_keys(v: &serde_json::Value) -> Vec<String> {
            v.as_object()
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default()
        }
        let mut views: Vec<FacetView> = Vec::new();
        for m in sql.values() {
            let identity: Vec<String> = m.primary_key.iter().cloned().collect();
            let fields: Vec<String> = m
                .columns
                .keys()
                .filter(|c| !identity.contains(c))
                .cloned()
                .collect();
            views.push(FacetView {
                event: m.event.clone(),
                entity: m.table.clone(),
                facet: m.facet_key().to_string(),
                on_existing: m.on_existing,
                operation: m.operation,
                identity,
                fields,
                kind: "SQL",
            });
        }
        for m in doc.values() {
            views.push(FacetView {
                event: m.event.clone(),
                entity: m.collection.clone(),
                facet: m.facet_key().to_string(),
                on_existing: m.on_existing,
                operation: m.operation,
                identity: vec![m.id.clone()],
                fields: object_keys(&m.document),
                kind: "document",
            });
        }
        for m in kv.values() {
            views.push(FacetView {
                event: m.event.clone(),
                entity: m.namespace.clone(),
                facet: m.facet_key().to_string(),
                on_existing: m.on_existing,
                operation: m.operation,
                identity: vec![m.key.clone()],
                fields: object_keys(&m.value),
                kind: "key-value",
            });
        }
        for m in graph.values() {
            // Only nodes are faceted (Phase 16.1); an edge contributes no view.
            if let GraphMapping::Node(n) = m {
                let identity = vec![n.key.clone()];
                views.push(FacetView {
                    event: n.event.clone(),
                    entity: n.label.clone(),
                    facet: n.facet.as_deref().unwrap_or("").to_string(),
                    on_existing: n.on_existing,
                    operation: n.operation,
                    identity,
                    fields: object_keys(&n.properties),
                    kind: "graph node",
                });
            }
        }
        validate_facets(views, &mut report);
    }

    for (event_type, adapter_ids) in routing {
        for adapter_id in adapter_ids {
            let Some(ac) = adapters_by_id.get(adapter_id.as_str()).copied() else {
                report.push(ValidationIssue::UnknownAdapter {
                    event_type: event_type.clone(),
                    adapter_id: adapter_id.clone(),
                });
                continue;
            };

            if is_sql(ac) {
                let Some(sql_map) = sql.get(event_type) else {
                    report.push(ValidationIssue::MissingSqlMapping {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                    });
                    continue;
                };
                let eff = effective_capabilities(ac);
                let required = required_capabilities(
                    &sql_map.requires_capabilities,
                    sql_map.on_existing,
                    sql_map.operation,
                );
                let missing: Vec<_> = required.into_iter().filter(|c| !eff.contains(c)).collect();
                if !missing.is_empty() {
                    report.push(ValidationIssue::CapabilityMismatch {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                        missing,
                    });
                }
            } else if is_document(ac) {
                let Some(doc_map) = doc.get(event_type) else {
                    report.push(ValidationIssue::MissingDocumentMapping {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                    });
                    continue;
                };
                let eff = effective_capabilities(ac);
                let required = required_capabilities(
                    &doc_map.requires_capabilities,
                    doc_map.on_existing,
                    doc_map.operation,
                );
                let missing: Vec<_> = required.into_iter().filter(|c| !eff.contains(c)).collect();
                if !missing.is_empty() {
                    report.push(ValidationIssue::CapabilityMismatch {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                        missing,
                    });
                }
            } else if is_keyvalue(ac) {
                let Some(kv_map) = kv.get(event_type) else {
                    report.push(ValidationIssue::MissingKvMapping {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                    });
                    continue;
                };
                let eff = effective_capabilities(ac);
                let required = required_capabilities(
                    &kv_map.requires_capabilities,
                    kv_map.on_existing,
                    kv_map.operation,
                );
                let missing: Vec<_> = required.into_iter().filter(|c| !eff.contains(c)).collect();
                if !missing.is_empty() {
                    report.push(ValidationIssue::CapabilityMismatch {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                        missing,
                    });
                }
            } else if is_graph(ac) {
                let Some(graph_map) = graph.get(event_type) else {
                    report.push(ValidationIssue::MissingGraphMapping {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                    });
                    continue;
                };
                let eff = effective_capabilities(ac);
                let required = required_capabilities(
                    graph_map.requires_capabilities(),
                    graph_map.on_existing(),
                    graph_map.operation(),
                );
                let missing: Vec<_> = required.into_iter().filter(|c| !eff.contains(c)).collect();
                if !missing.is_empty() {
                    report.push(ValidationIssue::CapabilityMismatch {
                        event_type: event_type.clone(),
                        adapter_id: adapter_id.clone(),
                        missing,
                    });
                }
            }
        }
    }

    for event in sql.keys() {
        let Some(ids) = routing.get(event) else {
            report.push(ValidationIssue::UnroutedSqlMapping {
                event: event.clone(),
            });
            continue;
        };
        let has_sqlite = ids.iter().any(|id| {
            adapters_by_id
                .get(id.as_str())
                .copied()
                .map(|a| is_sql(a) || kind_unknown(a))
                .unwrap_or(false)
        });
        if !has_sqlite {
            report.push(ValidationIssue::SqlMappingNoSqlTarget {
                event: event.clone(),
            });
        }
    }

    for event in doc.keys() {
        let Some(ids) = routing.get(event) else {
            report.push(ValidationIssue::UnroutedDocumentMapping {
                event: event.clone(),
            });
            continue;
        };
        let has_document_target = ids.iter().any(|id| {
            adapters_by_id
                .get(id.as_str())
                .copied()
                .map(|a| is_document(a) || kind_unknown(a))
                .unwrap_or(false)
        });
        if !has_document_target {
            report.push(ValidationIssue::DocumentMappingNoFileTarget {
                event: event.clone(),
            });
        }
    }

    for event in kv.keys() {
        let Some(ids) = routing.get(event) else {
            report.push(ValidationIssue::UnroutedKvMapping {
                event: event.clone(),
            });
            continue;
        };
        let has_keyvalue = ids.iter().any(|id| {
            adapters_by_id
                .get(id.as_str())
                .copied()
                .map(|a| is_keyvalue(a) || kind_unknown(a))
                .unwrap_or(false)
        });
        if !has_keyvalue {
            report.push(ValidationIssue::KvMappingNoKeyValueTarget {
                event: event.clone(),
            });
        }
    }

    for event in graph.keys() {
        let Some(ids) = routing.get(event) else {
            report.push(ValidationIssue::UnroutedGraphMapping {
                event: event.clone(),
            });
            continue;
        };
        let has_graph = ids.iter().any(|id| {
            adapters_by_id
                .get(id.as_str())
                .copied()
                .map(|a| is_graph(a) || kind_unknown(a))
                .unwrap_or(false)
        });
        if !has_graph {
            report.push(ValidationIssue::GraphMappingNoGraphTarget {
                event: event.clone(),
            });
        }
    }

    report.into_result()
}

/// Validate that every adapter id appearing anywhere in routing exists in config.
pub fn validate_routing_adapter_ids(
    config: &KulyraxConfig,
    routing: &HashMap<String, Vec<AdapterId>>,
) -> Result<(), ValidationReport> {
    let mut report = ValidationReport::default();
    let adapters_by_id = build_adapter_by_id(config, &mut report);
    for (event_type, adapter_ids) in routing {
        for adapter_id in adapter_ids {
            if !adapters_by_id.contains_key(adapter_id.as_str()) {
                report.push(ValidationIssue::UnknownAdapter {
                    event_type: event_type.clone(),
                    adapter_id: adapter_id.clone(),
                });
            }
        }
    }
    report.into_result()
}

/// Adapters returned by routing for this event type must exist in config (e.g. `explain` without full mappings).
pub fn validate_routing_for_event_type(
    config: &KulyraxConfig,
    routing: &HashMap<String, Vec<AdapterId>>,
    event_type: &str,
) -> Result<(), ValidationReport> {
    let mut report = ValidationReport::default();
    let adapters_by_id = build_adapter_by_id(config, &mut report);
    let Some(adapter_ids) = routing.get(event_type) else {
        return report.into_result();
    };
    for adapter_id in adapter_ids {
        if !adapters_by_id.contains_key(adapter_id.as_str()) {
            report.push(ValidationIssue::UnknownAdapter {
                event_type: event_type.to_string(),
                adapter_id: adapter_id.clone(),
            });
        }
    }
    report.into_result()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_template_empty_detects_null_object_array() {
        assert!(document_template_empty(&serde_json::Value::Null));
        assert!(document_template_empty(&serde_json::json!({})));
        assert!(document_template_empty(&serde_json::json!([])));
        assert!(!document_template_empty(&serde_json::json!({ "a": 1 })));
    }
}
