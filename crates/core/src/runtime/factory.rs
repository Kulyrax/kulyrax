use std::sync::Arc;

use crate::adapter::StorageAdapter;
use crate::adapter::document::file::FileDocumentAdapter;
use crate::adapter::document::runtime::DocumentRuntimeBuilder;
use crate::adapter::graph::runtime::GraphRuntimeBuilder;
use crate::adapter::graph::store::GraphStore;
use crate::adapter::keyvalue::runtime::KvRuntimeBuilder;
use crate::adapter::keyvalue::store::FileKvStore;
use crate::adapter::sql::runtime::SqlRuntimeBuilder;
use crate::adapter::sql::sqlite::SqliteAdapter;
use crate::adapter::{AdapterError, AdapterResult};
use crate::event::Event;
use crate::routing::StorageKind;
use crate::upcast::UpcasterRegistry;

use crate::runtime::config::{AdapterConfig, ConfigError, KulyraxConfig};
use crate::runtime::registry::{AdapterBuildContext, AdapterRegistry, MappingSet};

/// Stands in for a custom adapter whose type is unregistered or whose factory
/// rejected its entry. Every other adapter still builds, and the error
/// surfaces on this adapter's first `handle()`, so building stays infallible.
struct FailedAdapter {
    id: String,
    priority: u32,
    error: String,
}

impl StorageAdapter for FailedAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Custom
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }
    fn handle(&self, _event: &Event) -> AdapterResult {
        AdapterResult::failure(
            self.id.clone(),
            StorageKind::Custom,
            AdapterError::WriteFailed(self.error.clone()),
        )
    }
}

/// A custom adapter that could not be built: its type is unregistered, or its
/// factory rejected the entry (e.g. a connection URL that does not parse).
#[derive(Debug, Clone)]
pub struct AdapterBuildFailure {
    pub adapter_id: String,
    pub reason: String,
}

/// Build every configured adapter, sorted by priority. Each adapter gets its
/// own clone of the mapping table for its kind, so one event type can route
/// to many targets with the same projection definitions. Pass
/// [`AdapterRegistry::new`] when no custom adapter types are registered.
///
/// A failed custom adapter is replaced by a placeholder that fails every
/// event; use [`build_adapters_reporting_failures`] to detect it up front.
pub fn build_adapters_from_config(
    config: &KulyraxConfig,
    mappings: MappingSet,
    upcasters: Arc<UpcasterRegistry>,
    registry: &AdapterRegistry,
) -> Vec<Box<dyn StorageAdapter>> {
    build_adapters_reporting_failures(config, mappings, upcasters, registry).0
}

/// Same as [`build_adapters_from_config`], also returning every adapter that
/// was replaced by a failing placeholder.
pub fn build_adapters_reporting_failures(
    config: &KulyraxConfig,
    mappings: MappingSet,
    upcasters: Arc<UpcasterRegistry>,
    registry: &AdapterRegistry,
) -> (Vec<Box<dyn StorageAdapter>>, Vec<AdapterBuildFailure>) {
    let ctx = AdapterBuildContext {
        mappings,
        upcasters,
        migration_policy: config.migration_policy,
    };
    let mut adapters: Vec<Box<dyn StorageAdapter>> = Vec::new();
    let mut failures = Vec::new();

    for adapter in &config.adapters {
        match adapter {
            AdapterConfig::Sqlite(cfg) => {
                let builder = SqlRuntimeBuilder::new(ctx.mappings.sql.clone());
                adapters.push(Box::new(SqliteAdapter::new(
                    cfg.id.clone(),
                    cfg.config.path.clone(),
                    cfg.priority,
                    builder,
                    Arc::clone(&ctx.upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::File(cfg) => {
                let builder = DocumentRuntimeBuilder::new(ctx.mappings.document.clone());
                adapters.push(Box::new(FileDocumentAdapter::new(
                    cfg.id.clone(),
                    cfg.config.root.clone().into(),
                    cfg.priority,
                    builder,
                    Arc::clone(&ctx.upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::KeyValue(cfg) => {
                let builder = KvRuntimeBuilder::new(ctx.mappings.kv.clone());
                adapters.push(Box::new(FileKvStore::new(
                    cfg.id.clone(),
                    cfg.config.root.clone().into(),
                    cfg.priority,
                    builder,
                    Arc::clone(&ctx.upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::Graph(cfg) => {
                let builder = GraphRuntimeBuilder::new(ctx.mappings.graph.clone());
                adapters.push(Box::new(GraphStore::new(
                    cfg.id.clone(),
                    cfg.config.root.clone().into(),
                    cfg.priority,
                    builder,
                    Arc::clone(&ctx.upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::Custom(cfg) => {
                let built = registry.build(cfg, &ctx).unwrap_or_else(|| {
                    Err(ConfigError::UnregisteredAdapterType(cfg.type_name.clone()))
                });
                match built {
                    Ok(adapter) => adapters.push(adapter),
                    Err(e) => {
                        failures.push(AdapterBuildFailure {
                            adapter_id: cfg.id.clone(),
                            reason: e.to_string(),
                        });
                        adapters.push(Box::new(FailedAdapter {
                            id: cfg.id.clone(),
                            priority: cfg.priority,
                            error: e.to_string(),
                        }));
                    }
                }
            }
        }
    }

    adapters.sort_by_key(|a| a.priority());
    (adapters, failures)
}
