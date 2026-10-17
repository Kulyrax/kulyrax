//! Redis key-value backend over core's `KvBackend` seam. Registers as
//! `type: redis` via [`factory`].
//!
//! Layout: values at `{namespace}:{key}`, guards at
//! `__kulyrax:guard:{namespace}:{key}`. Redis has no row lock reachable from a
//! client transaction, so writes use optimistic `WATCH`/`MULTI`/`EXEC` with a
//! bounded retry.

use std::sync::Arc;

use r2d2::{Pool, PooledConnection};
use redis::Commands;
use serde::Deserialize;
use serde_json::Value;

use kulyrax_core::adapter::keyvalue::adapter::KvError;
use kulyrax_core::adapter::keyvalue::exec::{self, KvBackend, KvGuard, KvOutcome, KvPlan};
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{
    ConfigError, CustomAdapterConfig, MigrationPolicy, expand_env,
};
use kulyrax_core::upcast::UpcasterRegistry;

type RedisPool = Pool<redis::Client>;

/// Not a config knob: five attempts against one contended entity is already
/// unusual write pressure.
const MAX_ATTEMPTS: u32 = 5;

/// The `config:` block of a `type: redis` adapter entry.
#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// `redis://[:password@]host:port[/db]`; `${ENV_VAR}` references are expanded.
    pub url: String,
    /// Connection pool size (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Registry factory for `type: redis`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let id = entry.id.clone();
    let cfg: RedisConfig = entry.backend_config()?;
    let url = expand_env(&cfg.url);
    if url.trim().is_empty() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "redis adapter {id:?}: `url` is empty"
        )));
    }
    if redis::Client::open(url.as_str()).is_err() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "redis adapter {id:?}: `url` does not parse: {url}"
        )));
    }
    Ok(Box::new(RedisAdapter::new(
        id,
        &url,
        cfg.pool_size.unwrap_or(4),
        entry.priority,
        KvRuntimeBuilder::new(ctx.mappings.kv.clone()),
        Arc::clone(&ctx.upcasters),
        ctx.migration_policy,
    )))
}

fn guard_key(namespace: &str, key: &str) -> String {
    format!("__kulyrax:guard:{namespace}:{key}")
}

fn value_key(namespace: &str, key: &str) -> String {
    format!("{namespace}:{key}")
}

fn redis_err(e: redis::RedisError) -> KvError {
    KvError::WriteFailed(e.to_string())
}

/// Concurrent writers to the same entity race on `WATCH`/`MULTI`/`EXEC`
/// rather than relying on a single-writer assumption.
pub struct RedisAdapter {
    id: String,
    priority: u32,
    /// `Err` when the URL did not parse; every `handle()` then fails cleanly.
    pool: Result<RedisPool, String>,
    builder: KvRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl RedisAdapter {
    /// The pool connects lazily (`build_unchecked`), so an unreachable server
    /// surfaces on the first write rather than here.
    pub fn new(
        id: String,
        url: &str,
        pool_size: u32,
        priority: u32,
        builder: KvRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        let pool = redis::Client::open(url)
            .map_err(|e| format!("invalid redis url: {e}"))
            .map(|client| {
                Pool::builder()
                    .max_size(pool_size.max(1))
                    .build_unchecked(client)
            });
        Self {
            id,
            priority,
            pool,
            builder,
            upcasters,
            migration_policy,
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::KeyValue, error)
    }
}

impl StorageAdapter for RedisAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::KeyValue
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(KvError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                };
            }
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        let source_version = projection.source_version;
        let projected_version = projection.projected_version;
        let plan = KvPlan {
            namespace: projection.namespace,
            entity_key: projection.entity_key,
            value: projection.value,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
        };

        let pool = match &self.pool {
            Ok(p) => p,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.clone())),
        };
        let conn = match pool.get() {
            Ok(c) => c,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };
        let mut backend = RedisBackend { conn };

        // On a conflict, retry the whole read-decide-write cycle with a fresh
        // WATCH and guard read. Once the budget is spent, the failure goes to
        // the run loop, which retries the batch like any transient error.
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            let outcome = exec::project(&mut backend, &plan, event);
            // Defensive, matching `redis::transaction`'s own pattern: EXEC
            // (success or aborted) already clears WATCH, but a hard error
            // before EXEC would leave it dangling on the pooled connection.
            let _: Result<(), redis::RedisError> = redis::cmd("UNWATCH").query(&mut *backend.conn);
            match outcome {
                Err(KvError::WriteConflict) if attempts < MAX_ATTEMPTS => continue,
                other => break other,
            }
        };

        match result {
            Ok(KvOutcome::Created) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(KvOutcome::Updated) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(KvOutcome::Deleted) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(KvOutcome::Skipped(reason)) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                reason,
                source_version,
                projected_version,
            ),
            Err(KvError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "redis write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

struct RedisBackend {
    conn: PooledConnection<redis::Client>,
}

impl KvBackend for RedisBackend {
    /// `WATCH` the guard key, then read it — the transaction any subsequent
    /// `write`/`delete` commits aborts if this key changes before `EXEC`.
    fn read_guard(&mut self, namespace: &str, key: &str) -> Result<Option<KvGuard>, KvError> {
        let gk = guard_key(namespace, key);
        redis::cmd("WATCH")
            .arg(&gk)
            .query::<()>(&mut *self.conn)
            .map_err(redis_err)?;
        let raw: Option<String> = self.conn.get(&gk).map_err(redis_err)?;
        match raw {
            None => Ok(None),
            Some(s) => serde_json::from_str(&s)
                .map(Some)
                .map_err(|e| KvError::WriteFailed(format!("corrupt guard key: {e}"))),
        }
    }

    fn read_value(&mut self, namespace: &str, key: &str) -> Result<Option<Value>, KvError> {
        let vk = value_key(namespace, key);
        let raw: Option<String> = self.conn.get(&vk).map_err(redis_err)?;
        Ok(raw.map(|s| serde_json::from_str(&s).unwrap_or(Value::Object(Default::default()))))
    }

    fn write(
        &mut self,
        namespace: &str,
        key: &str,
        value: Option<&Value>,
        guard: &KvGuard,
    ) -> Result<(), KvError> {
        let gk = guard_key(namespace, key);
        let guard_json = serde_json::to_string(guard)
            .map_err(|e| KvError::WriteFailed(format!("failed to serialize guard: {e}")))?;

        let mut pipe = redis::pipe();
        pipe.atomic();
        if let Some(v) = value {
            let vk = value_key(namespace, key);
            let value_json = serde_json::to_string(v)
                .map_err(|e| KvError::WriteFailed(format!("failed to serialize value: {e}")))?;
            pipe.set(vk, value_json).ignore();
        }
        pipe.set(gk, guard_json).ignore();

        let result: Option<()> = pipe.query(&mut *self.conn).map_err(redis_err)?;
        result.ok_or(KvError::WriteConflict)
    }

    fn delete(&mut self, namespace: &str, key: &str, guard: &KvGuard) -> Result<(), KvError> {
        let gk = guard_key(namespace, key);
        let vk = value_key(namespace, key);
        let guard_json = serde_json::to_string(guard)
            .map_err(|e| KvError::WriteFailed(format!("failed to serialize guard: {e}")))?;

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.del(vk).ignore();
        pipe.set(gk, guard_json).ignore();

        let result: Option<()> = pipe.query(&mut *self.conn).map_err(redis_err)?;
        result.ok_or(KvError::WriteConflict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(url: &str) -> Result<Box<dyn StorageAdapter>, ConfigError> {
        let raw = serde_yaml::to_value(serde_json::json!({
            "type": "redis",
            "id": "r1",
            "priority": 10,
            "config": { "url": url },
        }))
        .map_err(|e| ConfigError::FactoryConfigInvalid(e.to_string()))?;
        let entry = CustomAdapterConfig {
            type_name: "redis".into(),
            id: "r1".into(),
            priority: 10,
            raw,
            capabilities: None,
            depends_on: Vec::new(),
        };
        let ctx = AdapterBuildContext {
            mappings: Default::default(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            migration_policy: MigrationPolicy::default(),
        };
        factory(&entry, &ctx)
    }

    fn error_of(url: &str) -> String {
        build(url).err().map(|e| e.to_string()).unwrap_or_default()
    }

    #[test]
    fn factory_rejects_an_empty_url() {
        assert!(error_of("").contains("`url` is empty"));
    }

    #[test]
    fn factory_rejects_an_unparseable_url() {
        let err = error_of("not a url");
        assert!(err.contains("`url` does not parse"), "{err}");
    }

    #[test]
    fn factory_builds_without_a_server_because_the_pool_connects_lazily() {
        let adapter = build("redis://127.0.0.1:1/0");
        assert!(adapter.is_ok());
        assert_eq!(adapter.map(|a| a.kind()).ok(), Some(StorageKind::KeyValue));
    }
}
