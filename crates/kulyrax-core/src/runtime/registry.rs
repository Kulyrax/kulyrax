//! Phase 25.2 / 27: the adapter factory registry — the mechanism behind
//! `AdapterConfig::Custom`. Register a factory for an unrecognized `type:`
//! string on an owned [`AdapterRegistry`] (typically via
//! [`crate::runtime::engine::KulyraxRuntime::register_adapter_factory`])
//! before adapters are built; from that point on `type: mysql` (or any other
//! name) in the YAML config resolves through here exactly like a built-in
//! adapter type resolves through a fixed `match` arm.
//!
//! **No dynamic loading.** This is Rust's ordinary compile-time extension
//! model — implement [`crate::adapter::StorageAdapter`] (directly, or via one
//! of the four public backend seams), depend on `kulyrax-core`, register a
//! factory on the runtime you build. `dlopen`/FFI plugin loading is a
//! deliberate non-goal (see the phase doc).
//!
//! **Phase 27:** this used to be a process-wide `static Lazy<Mutex<...>>` —
//! two engines in one process shared (and could stomp) each other's
//! registrations, and tests could only avoid collisions by convention
//! (globally-unique type-name strings). It is now a plain owned value with no
//! `Mutex`/`Send + Sync` requirement of its own — those were only ever needed
//! because the old registry was a global.

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::StorageAdapter;
use crate::runtime::config::ConfigError;

/// A registered adapter factory: the adapter's entire raw YAML list entry in
/// (`type`, `id`, `priority`, `config`, `capabilities`, `depends_on` — see
/// [`crate::runtime::config::CustomAdapterConfig`]'s doc comment for why the
/// whole entry rather than just `config:`), a fully built adapter out.
pub type AdapterFactory =
    Arc<dyn Fn(serde_yaml::Value) -> Result<Box<dyn StorageAdapter>, ConfigError> + Send + Sync>;

/// An instance-owned table of adapter-type factories. Each [`KulyraxRuntime`](
/// crate::runtime::engine::KulyraxRuntime) owns its own, so two runtimes in
/// one process (or two tests in one binary) never see each other's
/// registrations.
#[derive(Default, Clone)]
pub struct AdapterRegistry {
    factories: HashMap<String, AdapterFactory>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory for adapter `type: <type_name>`. Call this before
    /// the owning runtime resolves adapters. Registering the same
    /// `type_name` twice replaces the earlier factory.
    pub fn register(
        &mut self,
        type_name: &str,
        factory: impl Fn(serde_yaml::Value) -> Result<Box<dyn StorageAdapter>, ConfigError>
        + Send
        + Sync
        + 'static,
    ) {
        self.factories
            .insert(type_name.to_string(), Arc::new(factory));
    }

    /// Look up and invoke the factory for `type_name`. `None` means nothing is
    /// registered under that name; `Some(Err(_))` means the factory itself
    /// rejected the config.
    pub(crate) fn build(
        &self,
        type_name: &str,
        config: serde_yaml::Value,
    ) -> Option<Result<Box<dyn StorageAdapter>, ConfigError>> {
        let factory = self.factories.get(type_name)?.clone();
        Some(factory(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{AdapterResult, StorageAdapter};
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

    #[test]
    fn unregistered_type_resolves_to_none() {
        let registry = AdapterRegistry::new();
        assert!(
            registry
                .build("definitely-not-registered-xyz", serde_yaml::Value::Null)
                .is_none()
        );
    }

    #[test]
    fn registered_factory_is_invoked_with_its_config_block() {
        let mut registry = AdapterRegistry::new();
        registry.register("test-noop-registry", |cfg| {
            assert_eq!(cfg.as_str(), Some("hello"));
            Ok(Box::new(Noop) as Box<dyn StorageAdapter>)
        });
        let result = registry.build(
            "test-noop-registry",
            serde_yaml::Value::String("hello".into()),
        );
        assert!(matches!(result, Some(Ok(_))));
    }

    #[test]
    fn two_registries_do_not_see_each_others_registrations() {
        let mut a = AdapterRegistry::new();
        a.register("shared-name", |_| {
            Ok(Box::new(Noop) as Box<dyn StorageAdapter>)
        });
        let b = AdapterRegistry::new();
        assert!(a.build("shared-name", serde_yaml::Value::Null).is_some());
        assert!(b.build("shared-name", serde_yaml::Value::Null).is_none());
    }
}
