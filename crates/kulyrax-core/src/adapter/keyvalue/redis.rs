//! Phase 22.2–22.3: the Redis key-value backend.
//!
//! Same `KvMapping` / `decide()` / guard model as the file-backed store — this
//! file supplies only what a driver genuinely owns: an r2d2 connection pool,
//! the `{namespace}:{key}` / `__kulyrax:guard:{namespace}:{key}` layout, and
//! an optimistic `WATCH`/`MULTI`/`EXEC` transaction with bounded retry (Redis
//! has no server-side row lock reachable from a client transaction, unlike
//! Postgres's `FOR UPDATE` — Phase 19.4).

use std::sync::Arc;

use r2d2::{Pool, PooledConnection};
use redis::Commands;
use serde_json::Value;

use super::adapter::KvError;
use super::exec::{self, KvBackend, KvGuard, KvOutcome, KvPlan};
use super::runtime::KvRuntimeBuilder;
use crate::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use crate::event::Event;
use crate::routing::StorageKind;
use crate::runtime::config::MigrationPolicy;
use crate::upcast::UpcasterRegistry;

type RedisPool = Pool<redis::Client>;

/// Bounded optimistic-retry budget for a WATCH/MULTI/EXEC conflict (Phase
/// 22.3) — not a config knob; five attempts against one contended entity is
/// already an unusual amount of concurrent write pressure.
const MAX_ATTEMPTS: u32 = 5;

fn guard_key(namespace: &str, key: &str) -> String {
    format!("__kulyrax:guard:{namespace}:{key}")
}

fn value_key(namespace: &str, key: &str) -> String {
    format!("{namespace}:{key}")
}

fn redis_err(e: redis::RedisError) -> KvError {
    KvError::WriteFailed(e.to_string())
}

/// Redis key-value adapter (Phase 22). Like [`crate::adapter::sql::postgres::PostgresAdapter`],
/// the first KV backend **without** a single-writer assumption — concurrent
/// writers to the same entity race on `WATCH`/`MULTI`/`EXEC` instead of a lock.
pub struct RedisAdapter {
    id: String,
    priority: u32,
    /// `Err` (a stored message) when the URL didn't parse — every `handle()`
    /// then fails cleanly rather than the factory being fallible.
    pool: Result<RedisPool, String>,
    builder: KvRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl RedisAdapter {
    /// Build the pool from a connection URL (`redis://[:password@]host:port[/db]`).
    /// Infallible — a bad URL is stored and surfaced per-event, keeping
    /// `build_adapters_from_config` free of a `Result`. The pool is
    /// `build_unchecked` (lazy connect): connectivity problems surface on the
    /// first real write, not at construction.
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

        // Phase 22.3: WATCH/MULTI/EXEC is optimistic — retry the whole
        // read-decide-write cycle (a fresh WATCH + guard read each time) on a
        // conflict, bounded, then surface a batch failure like any other
        // transient error (retried by the run loop — Phase 17.5).
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

// ---------------------------------------------------------------------------
// RedisBackend — the driver
// ---------------------------------------------------------------------------

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
