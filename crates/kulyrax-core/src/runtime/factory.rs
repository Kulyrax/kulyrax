use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::StorageAdapter;
use crate::adapter::document::file::FileDocumentAdapter;
use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::document::mongo::MongoDbAdapter;
use crate::adapter::document::runtime::DocumentRuntimeBuilder;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::graph::neo4j::Neo4jAdapter;
use crate::adapter::graph::runtime::GraphRuntimeBuilder;
use crate::adapter::graph::store::GraphStore;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::keyvalue::redis::RedisAdapter;
use crate::adapter::keyvalue::runtime::KvRuntimeBuilder;
use crate::adapter::keyvalue::store::FileKvStore;
use crate::adapter::sql::mapping::SqlMapping;
use crate::adapter::sql::mysql::MySqlAdapter;
use crate::adapter::sql::runtime::SqlRuntimeBuilder;
use crate::adapter::sql::sqlite::SqliteAdapter;
use crate::adapter::{AdapterError, AdapterResult};
use crate::event::Event;
use crate::routing::StorageKind;
use crate::upcast::UpcasterRegistry;

use crate::runtime::config::{AdapterConfig, ConfigError, KulyraxConfig};
use crate::runtime::registry::AdapterRegistry;

/// Phase 25.2: pushed in place of a real adapter when an `AdapterConfig::Custom`'s
/// `type:` has no factory registered (or the registered factory itself
/// errored) at build time. Mirrors the existing "store the error, surface it
/// on first `handle()`" pattern every connectable built-in backend already
/// uses for a bad URL (`PostgresAdapter`, `RedisAdapter`, `MongoDbAdapter`,
/// `Neo4jAdapter`, `MySqlAdapter`) — kept here instead of making
/// `build_adapters_from_config` fallible, preserving both its own infallible
/// signature and the Zero Panic Rule (CLAUDE.md).
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

/// Build all adapters from config. Each SQL adapter gets a **clone** of `sql_mappings`; each file
/// adapter gets a **clone** of `document_mappings`; each key-value adapter gets a **clone** of
/// `kv_mappings` — so one event type can route to many targets (e.g. `pgsql_master`,
/// `pgsql_slave`) with the same projection definitions.
///
/// `upcasters` is shared (via `Arc`) across every adapter instance; `config.migration_policy`
/// governs how each adapter reacts when no upcaster chain exists to its mapping's target version.
///
/// **Phase 25.2:** `AdapterConfig::Custom` (an unrecognized `type:`) is
/// resolved against `registry` here, at build time — not at parse time, so a
/// factory registered after config parsing but before this call still
/// resolves. An unregistered type, or a factory that itself returns `Err`,
/// becomes a [`FailedAdapter`] placeholder rather than making this function
/// fallible: every other adapter in the config still builds and runs.
///
/// **Phase 27:** `registry` is caller-owned (e.g.
/// [`crate::runtime::engine::KulyraxRuntime`]) rather than a process-wide
/// global — pass [`AdapterRegistry::new`] for a call site with no custom
/// adapter types.
#[allow(clippy::too_many_arguments)]
pub fn build_adapters_from_config(
    config: &KulyraxConfig,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    upcasters: Arc<UpcasterRegistry>,
    registry: &AdapterRegistry,
) -> Vec<Box<dyn StorageAdapter>> {
    let mut adapters: Vec<Box<dyn StorageAdapter>> = Vec::new();

    for adapter in &config.adapters {
        match adapter {
            AdapterConfig::Sqlite(cfg) => {
                let builder = SqlRuntimeBuilder::new(sql_mappings.clone());
                adapters.push(Box::new(SqliteAdapter::new(
                    cfg.id.clone(),
                    cfg.config.path.clone(),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::File(cfg) => {
                let builder = DocumentRuntimeBuilder::new(document_mappings.clone());
                adapters.push(Box::new(FileDocumentAdapter::new(
                    cfg.id.clone(),
                    cfg.config.root.clone().into(),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::KeyValue(cfg) => {
                let builder = KvRuntimeBuilder::new(kv_mappings.clone());
                adapters.push(Box::new(FileKvStore::new(
                    cfg.id.clone(),
                    cfg.config.root.clone().into(),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::Graph(cfg) => {
                let builder = GraphRuntimeBuilder::new(graph_mappings.clone());
                adapters.push(Box::new(GraphStore::new(
                    cfg.id.clone(),
                    cfg.config.root.clone().into(),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::Redis(cfg) => {
                let builder = KvRuntimeBuilder::new(kv_mappings.clone());
                let url = crate::runtime::config::expand_env(&cfg.config.url);
                adapters.push(Box::new(RedisAdapter::new(
                    cfg.id.clone(),
                    &url,
                    cfg.config.pool_size.unwrap_or(4),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::MongoDb(cfg) => {
                let builder = DocumentRuntimeBuilder::new(document_mappings.clone());
                let url = crate::runtime::config::expand_env(&cfg.config.url);
                adapters.push(Box::new(MongoDbAdapter::new(
                    cfg.id.clone(),
                    &url,
                    cfg.config.database.clone(),
                    cfg.config.pool_size.unwrap_or(4),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::Neo4j(cfg) => {
                let builder = GraphRuntimeBuilder::new(graph_mappings.clone());
                let uri = crate::runtime::config::expand_env(&cfg.config.uri);
                let password = crate::runtime::config::expand_env(&cfg.config.password);
                adapters.push(Box::new(Neo4jAdapter::new(
                    cfg.id.clone(),
                    &uri,
                    &cfg.config.user,
                    &password,
                    cfg.config.database.clone(),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::MySql(cfg) => {
                let builder = SqlRuntimeBuilder::new(sql_mappings.clone());
                let url = crate::runtime::config::expand_env(&cfg.config.url);
                adapters.push(Box::new(MySqlAdapter::new(
                    cfg.id.clone(),
                    &url,
                    cfg.config.pool_size.unwrap_or(4),
                    cfg.priority,
                    builder,
                    Arc::clone(&upcasters),
                    config.migration_policy,
                )));
            }

            AdapterConfig::Custom(cfg) => {
                let adapter: Box<dyn StorageAdapter> =
                    match registry.build(&cfg.type_name, cfg.raw.clone()) {
                        Some(Ok(a)) => a,
                        Some(Err(e)) => Box::new(FailedAdapter {
                            id: cfg.id.clone(),
                            priority: cfg.priority,
                            error: e.to_string(),
                        }),
                        None => Box::new(FailedAdapter {
                            id: cfg.id.clone(),
                            priority: cfg.priority,
                            error: ConfigError::UnregisteredAdapterType(cfg.type_name.clone())
                                .to_string(),
                        }),
                    };
                adapters.push(adapter);
            }
        }
    }

    adapters.sort_by_key(|a| a.priority());
    adapters
}
