//! Adapter factory registry: how a `type:` that core does not build in (e.g.
//! `postgres`) becomes a [`StorageAdapter`]. Extension is compile-time only —
//! a backend crate exposes a factory and an embedder registers it on its
//! runtime. Dynamic loading is a deliberate non-goal.
//!
//! The registry is owned per runtime, never global, so two runtimes in one
//! process cannot see each other's registrations.

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::StorageAdapter;
use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::mapping::SqlMapping;
use crate::runtime::config::{ConfigError, CustomAdapterConfig, MigrationPolicy};
use crate::upcast::UpcasterRegistry;

/// Every loaded projection mapping, one table per storage kind.
#[derive(Debug, Clone, Default)]
pub struct MappingSet {
    pub sql: HashMap<String, SqlMapping>,
    pub document: HashMap<String, DocumentMapping>,
    pub kv: HashMap<String, KvMapping>,
    pub graph: HashMap<String, GraphMapping>,
}

/// What a factory needs beyond its own config entry. Handed over at build
/// time, so a factory can be registered before or after mappings load.
pub struct AdapterBuildContext {
    pub mappings: MappingSet,
    pub upcasters: Arc<UpcasterRegistry>,
    pub migration_policy: MigrationPolicy,
}

pub type AdapterFactory = Arc<
    dyn Fn(
            &CustomAdapterConfig,
            &AdapterBuildContext,
        ) -> Result<Box<dyn StorageAdapter>, ConfigError>
        + Send
        + Sync,
>;

#[derive(Default, Clone)]
pub struct AdapterRegistry {
    factories: HashMap<String, AdapterFactory>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registering the same `type_name` twice replaces the earlier factory.
    pub fn register(
        &mut self,
        type_name: &str,
        factory: impl Fn(
            &CustomAdapterConfig,
            &AdapterBuildContext,
        ) -> Result<Box<dyn StorageAdapter>, ConfigError>
        + Send
        + Sync
        + 'static,
    ) {
        self.factories
            .insert(type_name.to_string(), Arc::new(factory));
    }

    /// `None` means nothing is registered for the entry's type; `Some(Err(_))`
    /// means the factory rejected the entry.
    pub(crate) fn build(
        &self,
        entry: &CustomAdapterConfig,
        ctx: &AdapterBuildContext,
    ) -> Option<Result<Box<dyn StorageAdapter>, ConfigError>> {
        let factory = self.factories.get(&entry.type_name)?;
        Some(factory(entry, ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::AdapterResult;
    use crate::event::Event;
    use crate::routing::StorageKind;

    struct Noop;
    impl StorageAdapter for Noop {
        fn kind(&self) -> StorageKind {
            StorageKind::KeyValue
        }
        fn id(&self) -> &str {
            "noop"
        }
        fn priority(&self) -> u32 {
            0
        }
        fn handle(&self, _event: &Event) -> AdapterResult {
            AdapterResult::created(self.id().to_string(), self.kind())
        }
    }

    fn entry(type_name: &str, raw: serde_yaml::Value) -> CustomAdapterConfig {
        CustomAdapterConfig {
            type_name: type_name.to_string(),
            id: "a".to_string(),
            priority: 0,
            raw,
            capabilities: None,
            depends_on: Vec::new(),
        }
    }

    fn ctx() -> AdapterBuildContext {
        AdapterBuildContext {
            mappings: MappingSet::default(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            migration_policy: MigrationPolicy::default(),
        }
    }

    #[test]
    fn unregistered_type_resolves_to_none() {
        let registry = AdapterRegistry::new();
        let e = entry("definitely-not-registered-xyz", serde_yaml::Value::Null);
        assert!(registry.build(&e, &ctx()).is_none());
    }

    #[test]
    fn registered_factory_is_invoked_with_its_entry() {
        let mut registry = AdapterRegistry::new();
        registry.register("test-noop-registry", |entry, _ctx| {
            assert_eq!(entry.raw.as_str(), Some("hello"));
            Ok(Box::new(Noop) as Box<dyn StorageAdapter>)
        });
        let e = entry(
            "test-noop-registry",
            serde_yaml::Value::String("hello".into()),
        );
        assert!(matches!(registry.build(&e, &ctx()), Some(Ok(_))));
    }

    #[test]
    fn two_registries_do_not_see_each_others_registrations() {
        let mut a = AdapterRegistry::new();
        a.register("shared-name", |_, _| {
            Ok(Box::new(Noop) as Box<dyn StorageAdapter>)
        });
        let b = AdapterRegistry::new();
        let e = entry("shared-name", serde_yaml::Value::Null);
        assert!(a.build(&e, &ctx()).is_some());
        assert!(b.build(&e, &ctx()).is_none());
    }

    #[derive(serde::Deserialize)]
    struct UrlConfig {
        url: String,
    }

    #[test]
    fn backend_config_reads_the_config_block() {
        let raw = serde_yaml::from_str("{type: mydb, id: a, priority: 0, config: {url: x}}")
            .unwrap_or(serde_yaml::Value::Null);
        let cfg: UrlConfig = entry("mydb", raw).backend_config().unwrap();
        assert_eq!(cfg.url, "x");
    }

    #[test]
    fn backend_config_errors_name_the_type_and_id() {
        let missing = serde_yaml::from_str("{type: mydb, id: a, priority: 0}")
            .unwrap_or(serde_yaml::Value::Null);
        let err = entry("mydb", missing)
            .backend_config::<UrlConfig>()
            .err()
            .map(|e| e.to_string());
        assert_eq!(err.as_deref(), Some("mydb adapter \"a\" missing `config`"));

        let invalid = serde_yaml::from_str("{type: mydb, id: a, priority: 0, config: {}}")
            .unwrap_or(serde_yaml::Value::Null);
        let err = entry("mydb", invalid)
            .backend_config::<UrlConfig>()
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            err.starts_with("mydb adapter \"a\": invalid `config`"),
            "{err}"
        );
    }
}
