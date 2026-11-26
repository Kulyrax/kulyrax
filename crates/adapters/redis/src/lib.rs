//! Redis key-value backend over core's `KvBackend` seam. Registers as
//! `type: redis` via [`factory`].
//!
//! Layout: values at `{namespace}:{key}`, guards at
//! `__kulyrax:guard:{namespace}:{key}`. Redis has no row lock reachable from a
//! client transaction, so writes use optimistic `WATCH`/`MULTI`/`EXEC` with a
//! bounded retry.
//!
//! A batch (`begin_batch` … `commit_batch`) holds one connection. It
//! `WATCH`es the guard of every entity the batch touches, loads guards and
//! values with `MGET`, decides every event in memory, and commits the final
//! state of each touched entity in one `MULTI`/`EXEC`: the whole batch lands
//! or none of it. If another writer changed a watched guard, `EXEC` aborts,
//! `commit_batch` fails, and the batch falls back to one event at a time,
//! with its usual retry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use r2d2::{Pool, PooledConnection};
use redis::Commands;
use serde::Deserialize;
use serde_json::Value;

use kulyrax_core::adapter::keyvalue::adapter::KvError;
use kulyrax_core::adapter::keyvalue::exec::{self, KvBackend, KvGuard, KvOutcome, KvPlan};
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages::{Stage, timed};
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

/// Keys per `WATCH` / `MGET`.
const KEY_CHUNK: usize = 1_000;

/// The `config:` block of a `type: redis` adapter entry.
#[derive(Clone, Deserialize)]
pub struct RedisConfig {
    /// `redis://[:password@]host:port[/db]`; `${ENV_VAR}` references are expanded.
    pub url: String,
    /// Connection pool size (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Shows the URL with its password and secret query parameters hidden.
impl std::fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &kulyrax_core::redact::redact_url(&self.url))
            .field("pool_size", &self.pool_size)
            .finish()
    }
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
            "redis adapter {id:?}: `url` does not parse: {}",
            kulyrax_core::redact::redact_url(&url)
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
    /// The open batch, from `begin_batch` to `commit_batch` / `rollback_batch`.
    batch: Mutex<Option<RedisBatch>>,
}

/// An event's plan and its observed / target schema versions.
struct Planned {
    plan: KvPlan,
    source_version: u32,
    projected_version: u32,
}

/// One batch's connection, with every touched guard `WATCH`ed, and every
/// entity it has read or written, by `(namespace, key)`, in first-touch order.
struct RedisBatch {
    conn: PooledConnection<redis::Client>,
    entities: HashMap<(String, String), BatchEntity>,
    order: Vec<(String, String)>,
}

#[derive(Default)]
struct BatchEntity {
    guard: Option<KvGuard>,
    value: Option<Value>,
    guard_dirty: bool,
    /// Set by a write with a value, or a delete: `commit_batch` sets (or,
    /// for `None`, deletes) the value key.
    value_dirty: bool,
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("Redis batch lock poisoned by an earlier panic".to_string())
}

fn parse_guard(raw: &str) -> Result<KvGuard, KvError> {
    serde_json::from_str(raw).map_err(|e| KvError::WriteFailed(format!("corrupt guard key: {e}")))
}

/// A stored value; unparseable JSON reads as an empty object, as it always has.
fn parse_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or(Value::Object(Default::default()))
}

fn to_json<T: serde::Serialize>(what: &str, v: &T) -> Result<String, KvError> {
    serde_json::to_string(v)
        .map_err(|e| KvError::WriteFailed(format!("failed to serialize {what}: {e}")))
}

impl RedisBatch {
    /// `WATCH` the guards of `keys`, then load their guards and values.
    fn load(&mut self, keys: &[(String, String)]) -> Result<(), KvError> {
        for chunk in keys.chunks(KEY_CHUNK) {
            let guards: Vec<String> = chunk.iter().map(|(ns, k)| guard_key(ns, k)).collect();
            let values: Vec<String> = chunk.iter().map(|(ns, k)| value_key(ns, k)).collect();
            redis::cmd("WATCH")
                .arg(&guards)
                .query::<()>(&mut *self.conn)
                .map_err(redis_err)?;
            let (raw_guards, raw_values): (Vec<Option<String>>, Vec<Option<String>>) =
                redis::pipe()
                    .cmd("MGET")
                    .arg(&guards)
                    .cmd("MGET")
                    .arg(&values)
                    .query(&mut *self.conn)
                    .map_err(redis_err)?;
            if raw_guards.len() != chunk.len() || raw_values.len() != chunk.len() {
                return Err(KvError::WriteFailed(format!(
                    "redis MGET returned {} guards and {} values for {} keys",
                    raw_guards.len(),
                    raw_values.len(),
                    chunk.len()
                )));
            }
            for ((key, guard), value) in chunk.iter().zip(raw_guards).zip(raw_values) {
                let entity = BatchEntity {
                    guard: guard.as_deref().map(parse_guard).transpose()?,
                    value: value.as_deref().map(parse_value),
                    ..BatchEntity::default()
                };
                if self.entities.insert(key.clone(), entity).is_none() {
                    self.order.push(key.clone());
                }
            }
        }
        Ok(())
    }

    fn entity(&mut self, namespace: &str, key: &str) -> Result<&mut BatchEntity, KvError> {
        let id = (namespace.to_string(), key.to_string());
        if !self.entities.contains_key(&id) {
            self.load(std::slice::from_ref(&id))?;
        }
        self.entities
            .get_mut(&id)
            .ok_or_else(|| KvError::WriteFailed(format!("{namespace}:{key} was not loaded")))
    }

    /// Every dirty entity's final state in one `MULTI`/`EXEC`. `Ok(false)`
    /// when `EXEC` aborted because a watched guard changed.
    fn commit(&mut self) -> Result<bool, KvError> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        let mut writes = 0usize;
        for id in &self.order {
            let Some(e) = self.entities.get(id) else {
                continue;
            };
            let (namespace, key) = id;
            if e.value_dirty {
                let vk = value_key(namespace, key);
                match &e.value {
                    Some(v) => pipe.set(vk, to_json("value", v)?).ignore(),
                    None => pipe.del(vk).ignore(),
                };
                writes += 1;
            }
            if e.guard_dirty
                && let Some(g) = &e.guard
            {
                pipe.set(guard_key(namespace, key), to_json("guard", g)?)
                    .ignore();
                writes += 1;
            }
        }
        if writes == 0 {
            let _: Result<(), redis::RedisError> = redis::cmd("UNWATCH").query(&mut *self.conn);
            return Ok(true);
        }
        let result: Option<()> = pipe.query(&mut *self.conn).map_err(redis_err)?;
        Ok(result.is_some())
    }
}

/// An event inside an open batch: reads and writes the batch's entities in
/// memory. An entity `begin_batch` did not load is watched and loaded on
/// first read.
struct BatchBackend<'a> {
    batch: &'a mut RedisBatch,
}

impl KvBackend for BatchBackend<'_> {
    fn read_guard(&mut self, namespace: &str, key: &str) -> Result<Option<KvGuard>, KvError> {
        Ok(self.batch.entity(namespace, key)?.guard.clone())
    }

    fn read_value(&mut self, namespace: &str, key: &str) -> Result<Option<Value>, KvError> {
        Ok(self.batch.entity(namespace, key)?.value.clone())
    }

    fn write(
        &mut self,
        namespace: &str,
        key: &str,
        value: Option<&Value>,
        guard: &KvGuard,
    ) -> Result<(), KvError> {
        let e = self.batch.entity(namespace, key)?;
        if let Some(v) = value {
            e.value = Some(v.clone());
            e.value_dirty = true;
        }
        e.guard = Some(guard.clone());
        e.guard_dirty = true;
        Ok(())
    }

    fn delete(&mut self, namespace: &str, key: &str, guard: &KvGuard) -> Result<(), KvError> {
        let e = self.batch.entity(namespace, key)?;
        e.value = None;
        e.value_dirty = true;
        e.guard = Some(guard.clone());
        e.guard_dirty = true;
        Ok(())
    }
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
            batch: Mutex::new(None),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::KeyValue, error)
    }

    fn connection(&self) -> Result<PooledConnection<redis::Client>, AdapterError> {
        let pool = self
            .pool
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))?;
        pool.get()
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    /// Build `event`'s plan. `Err` is the finished result to return instead:
    /// a skip or failure for a version with no upcaster chain (per the
    /// migration policy), or a failure to build.
    fn plan(&self, event: &Event) -> Result<Planned, AdapterResult> {
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(KvError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return Err(match self.migration_policy {
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                    // `Strict`, and any policy this adapter does not know
                    // (`MigrationPolicy` is `#[non_exhaustive]`): refuse.
                    _ => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        reason,
                        from_version,
                        to_version,
                    ),
                });
            }
            Err(e) => return Err(self.failure(AdapterError::WriteFailed(e.to_string()))),
        };
        Ok(Planned {
            source_version: projection.source_version,
            projected_version: projection.projected_version,
            plan: KvPlan::from(projection),
        })
    }

    fn result(&self, planned: &Planned, outcome: Result<KvOutcome, KvError>) -> AdapterResult {
        let (id, kind) = (self.id.clone(), StorageKind::KeyValue);
        let (sv, pv) = (planned.source_version, planned.projected_version);
        match outcome {
            Ok(KvOutcome::Created) => AdapterResult::created_versioned(id, kind, sv, pv),
            Ok(KvOutcome::Updated) => AdapterResult::updated_versioned(id, kind, sv, pv),
            Ok(KvOutcome::Deleted) => AdapterResult::deleted_versioned(id, kind, sv, pv),
            Ok(KvOutcome::Skipped(reason)) => {
                AdapterResult::skipped_versioned(id, kind, reason, sv, pv)
            }
            // `KvOutcome` is `#[non_exhaustive]`: an outcome this adapter does
            // not know is a failure, never a silent success.
            Ok(other) => self.failure(AdapterError::WriteFailed(format!(
                "redis: unknown key-value outcome {other:?}"
            ))),
            Err(KvError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "redis write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
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

    /// Refuse namespaces that could make two keys meet: values live at
    /// `<namespace>:<key>`, so a namespace containing `:` can share keys with
    /// another (`a:b` + `c` and `a` + `b:c`), and one starting with
    /// `__kulyrax` can reach the guards (`__kulyrax:guard:…`).
    fn prepare(&self) -> Result<(), AdapterError> {
        let mut bad: Vec<String> = self
            .builder
            .mappings()
            .values()
            .map(|m| m.namespace.clone())
            .filter(|ns| ns.is_empty() || ns.contains(':') || ns.starts_with("__kulyrax"))
            .collect();
        if bad.is_empty() {
            return Ok(());
        }
        bad.sort();
        bad.dedup();
        Err(AdapterError::WriteFailed(format!(
            "redis adapter {:?}: namespaces must be non-empty, contain no `:` and not start \
             with `__kulyrax`: {}",
            self.id,
            bad.join(", ")
        )))
    }

    /// Check out one connection for the batch, `WATCH` the guard of every
    /// entity its events touch, and load their guards and values.
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        // Events that fail to plan are left to `handle`, which reports them.
        let mut keys: Vec<(String, String)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for event in events {
            if let Ok(p) = self.plan(event) {
                let key = (p.plan.namespace, p.plan.entity_key);
                if seen.insert(key.clone()) {
                    keys.push(key);
                }
            }
        }
        let mut open = RedisBatch {
            conn: self.connection()?,
            entities: HashMap::new(),
            order: Vec::new(),
        };
        if let Err(e) = timed(Stage::Read, || open.load(&keys)) {
            let _: Result<(), redis::RedisError> = redis::cmd("UNWATCH").query(&mut *open.conn);
            return Err(AdapterError::WriteFailed(e.to_string()));
        }
        *batch = Some(open);
        Ok(())
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        let Some(mut open) = batch.take() else {
            return Ok(());
        };
        match timed(Stage::Commit, || open.commit()) {
            Ok(true) => Ok(()),
            Ok(false) => Err(AdapterError::WriteFailed(
                "redis batch aborted: another writer changed a guard this batch read".to_string(),
            )),
            Err(e) => {
                let _: Result<(), redis::RedisError> = redis::cmd("UNWATCH").query(&mut *open.conn);
                Err(AdapterError::WriteFailed(e.to_string()))
            }
        }
    }

    fn rollback_batch(&self) {
        let Ok(mut batch) = self.batch.lock() else {
            return;
        };
        if let Some(mut open) = batch.take() {
            let _: Result<(), redis::RedisError> = redis::cmd("UNWATCH").query(&mut *open.conn);
        }
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        let planned = match self.plan(event) {
            Ok(p) => p,
            Err(result) => return result,
        };
        let plan = &planned.plan;

        let mut batch = match self.batch.lock() {
            Ok(b) => b,
            Err(_) => return self.failure(poisoned()),
        };
        if let Some(open) = batch.as_mut() {
            let outcome = exec::project(&mut BatchBackend { batch: open }, plan, event);
            return self.result(&planned, outcome);
        }
        drop(batch);

        let conn = match self.connection() {
            Ok(c) => c,
            Err(e) => return self.failure(e),
        };
        let mut backend = RedisBackend { conn };

        // On a conflict, retry the whole read-decide-write cycle with a fresh
        // WATCH and guard read. Once the budget is spent, the failure goes to
        // the run loop, which retries the batch like any transient error.
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            let outcome = exec::project(&mut backend, plan, event);
            // Defensive, matching `redis::transaction`'s own pattern: EXEC
            // (success or aborted) already clears WATCH, but a hard error
            // before EXEC would leave it dangling on the pooled connection.
            let _: Result<(), redis::RedisError> = redis::cmd("UNWATCH").query(&mut *backend.conn);
            match outcome {
                Err(KvError::WriteConflict) if attempts < MAX_ATTEMPTS => continue,
                other => break other,
            }
        };
        self.result(&planned, result)
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
        raw.as_deref().map(parse_guard).transpose()
    }

    fn read_value(&mut self, namespace: &str, key: &str) -> Result<Option<Value>, KvError> {
        let vk = value_key(namespace, key);
        let raw: Option<String> = self.conn.get(&vk).map_err(redis_err)?;
        Ok(raw.as_deref().map(parse_value))
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
        let raw = serde_json::to_value(serde_json::json!({
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
        let ctx = AdapterBuildContext::new(
            Default::default(),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
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
