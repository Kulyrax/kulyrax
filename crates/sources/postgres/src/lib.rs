//! A Postgres outbox table as a Kulyrax [`EventSource`]: `type:
//! postgres_outbox`. Registered by `kulyrax-backends` (feature
//! `postgres-outbox`) through [`factory`].
//!
//! Rows are read in the order of a sequence column the user names, which is
//! also each event's `sequence`. Read only: Kulyrax never inserts, updates or
//! deletes outbox rows, and keeps its position in its own checkpoint.
//!
//! **Gaps.** A sequence value is taken when a row is inserted but its row
//! becomes visible only when the inserting transaction commits, so a reader
//! can see value 12 while 11 is still uncommitted, or rolled back for good.
//! Delivery stops at the first missing value. When a gap is first seen, the
//! source records the `xmax` of that read's snapshot; the gap is final once a
//! later read's `xmin` reaches it (every transaction that could still insert
//! the missing rows has ended, and the same read shows they did not commit),
//! and is skipped once it has also been held `gap_grace_ms`: a producer that
//! took its value with `nextval()` before it had a transaction id is
//! invisible to `xmax`, and the grace gives it that long to insert.
//! `gap_timeout_ms` is a backstop for a gap held back by a long-running
//! transaction. Skipped values are watched for
//! `watch_retention_ms` (at most [`WATCH_CAP`] of them): one that appears is
//! delivered late, logged with its outcome, and counted.
//!
//! Decision record: `docs/ledger/decisions/2026-11-24-postgres-outbox-source.md`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use postgres::{Client, IsolationLevel, NoTls};
use serde::{Deserialize, Serialize};

use kulyrax_core::event::Event;
use kulyrax_core::execution::{AdapterOutcome, ExecutionReport};
use kulyrax_core::runtime::SourceBuildContext;
use kulyrax_core::runtime::config::{CustomSourceConfig, expand_env};
use kulyrax_core::source::checkpoint::{read_checkpoint, write_checkpoint};
use kulyrax_core::source::{EventSource, SourceError, SourcePosition, SourcedEvent, lock_source};

/// Most skipped values watched at once; the oldest drop off first.
pub const WATCH_CAP: usize = 10_000;

fn default_gap_grace_ms() -> u64 {
    1_000
}

fn default_gap_timeout_ms() -> u64 {
    10_000
}

fn default_watch_retention_ms() -> u64 {
    3_600_000
}

/// The `config:` block of a `type: postgres_outbox` source.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxConfig {
    /// `postgres://user:pass@host/db`; `${ENV_VAR}` references are expanded.
    pub url: String,
    /// The outbox table, optionally schema-qualified (`public.outbox`).
    pub table: String,
    /// A strictly increasing integer column: delivery order and each
    /// event's `sequence`.
    pub sequence_column: String,
    #[serde(default)]
    pub columns: OutboxColumns,
    /// A final gap is still held at least this long, for a producer that
    /// took its value before it had a transaction id. `0` skips final gaps
    /// on the next poll.
    #[serde(default = "default_gap_grace_ms")]
    pub gap_grace_ms: u64,
    /// Backstop: skip a gap that has not become final after this long.
    #[serde(default = "default_gap_timeout_ms")]
    pub gap_timeout_ms: u64,
    /// How long a skipped value is watched for.
    #[serde(default = "default_watch_retention_ms")]
    pub watch_retention_ms: u64,
    /// Start although `sequence_column` has no sequence behind it (ids the
    /// application assigns), giving up the check that values follow insert
    /// order.
    #[serde(default)]
    pub allow_unverified_sequence: bool,
}

/// Shows the URL with its password and secret query parameters hidden.
impl std::fmt::Debug for OutboxConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxConfig")
            .field("url", &kulyrax_core::redact::redact_url(&self.url))
            .field("table", &self.table)
            .field("sequence_column", &self.sequence_column)
            .field("columns", &self.columns)
            .field("gap_grace_ms", &self.gap_grace_ms)
            .field("gap_timeout_ms", &self.gap_timeout_ms)
            .field("watch_retention_ms", &self.watch_retention_ms)
            .field("allow_unverified_sequence", &self.allow_unverified_sequence)
            .finish()
    }
}

/// Which column feeds each `Event` field. `metadata` and `version` may be
/// `~`: no such column (empty metadata, version 1).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OutboxColumns {
    pub event_id: String,
    pub event_type: String,
    pub payload: String,
    pub metadata: Option<String>,
    pub version: Option<String>,
}

impl Default for OutboxColumns {
    fn default() -> Self {
        Self {
            event_id: "event_id".into(),
            event_type: "event_type".into(),
            payload: "payload".into(),
            metadata: Some("metadata".into()),
            version: Some("version".into()),
        }
    }
}

/// Registry factory for `type: postgres_outbox`. Outside `inspect`, takes
/// the source's lock, connects, and runs the start-up checks.
pub fn factory(
    entry: &CustomSourceConfig,
    ctx: &SourceBuildContext,
) -> Result<Box<dyn EventSource>, SourceError> {
    let config: OutboxConfig = entry
        .source_config()
        .map_err(|e| SourceError::Unavailable(e.to_string()))?;
    let state_dir = entry.state_dir.as_deref().ok_or_else(|| {
        SourceError::Unavailable(format!(
            "postgres_outbox source {:?} needs a `state_dir` for its checkpoint",
            entry.id
        ))
    })?;
    let state_dir = ctx.base_dir.join(state_dir);
    let source = if ctx.inspect {
        PostgresOutboxSource::inspect(entry.id.clone(), state_dir, config)?
    } else {
        PostgresOutboxSource::open(entry.id.clone(), state_dir, config)?
    };
    Ok(Box::new(source))
}

/// What the checkpoint records: every value up to `after` was delivered or
/// skipped; `watch` holds skipped values still read for; the counters are
/// late deliveries handed out (`late_seq`, part of the position), and late
/// rows resolved and skipped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct State {
    after: Option<i64>,
    watch: Vec<Watched>,
    late_seq: u64,
    late_rows: u64,
    late_skipped: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Watched {
    seq: i64,
    /// Wall clock of the Kulyrax host, milliseconds since the Unix epoch:
    /// retention only, never ordering.
    skipped_at_ms: u64,
}

/// A gap seen and not yet released, keyed by its first missing value.
struct OpenGap {
    /// The `xmax` of the snapshot that first saw it.
    xmax: u64,
    seen: Instant,
}

/// The outbox reader. See the crate docs.
pub struct PostgresOutboxSource {
    id: String,
    state_dir: PathBuf,
    config: OutboxConfig,
    /// `None` in `inspect` mode, and after a connection error until the next
    /// poll reconnects.
    client: Option<Client>,
    /// The single-reader lock, held for the source's life (`None` in
    /// `inspect`).
    _lock: Option<std::fs::File>,
    committed: State,
    committed_position: Option<SourcePosition>,
    /// The state each position of the last poll stands for.
    pending: HashMap<String, State>,
    /// The late rows of the last poll, by position.
    late: HashMap<String, i64>,
    gaps: HashMap<i64, OpenGap>,
    query: String,
}

fn unavailable(e: impl std::fmt::Display) -> SourceError {
    SourceError::Unavailable(e.to_string())
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Text order equals delivery order: `after`, then the late-delivery count.
fn position(after: Option<i64>, late_seq: u64) -> SourcePosition {
    SourcePosition(format!(
        "{:020}.{:010}",
        after.unwrap_or(0).max(0) as u64,
        late_seq
    ))
}

impl PostgresOutboxSource {
    /// Lock, connect, check the table, and load the checkpoint.
    pub fn open(id: String, state_dir: PathBuf, config: OutboxConfig) -> Result<Self, SourceError> {
        let lock = lock_source(&state_dir, &id)?;
        let mut client = Client::connect(&expand_env(&config.url), NoTls).map_err(|e| {
            SourceError::Unavailable(format!("postgres_outbox source {id:?}: connect: {e}"))
        })?;
        check_table(&mut client, &config)
            .map_err(|e| SourceError::Unavailable(format!("postgres_outbox source {id:?}: {e}")))?;
        let mut source = Self::inspect(id, state_dir, config)?;
        source.client = Some(client);
        source._lock = Some(lock);
        Ok(source)
    }

    /// Load the checkpoint only: for `kulyrax sources`.
    pub fn inspect(
        id: String,
        state_dir: PathBuf,
        config: OutboxConfig,
    ) -> Result<Self, SourceError> {
        let committed = match read_checkpoint(&state_dir, &id)? {
            Some(cp) => serde_json::from_str(cp.position.as_str()).map_err(|e| {
                SourceError::Checkpoint(format!("postgres_outbox source {id:?}: {e}"))
            })?,
            None => State::default(),
        };
        let committed_position = committed
            .after
            .map(|_| position(committed.after, committed.late_seq));
        let query = select_rows(&config);
        Ok(Self {
            id,
            state_dir,
            config,
            client: None,
            _lock: None,
            committed,
            committed_position,
            pending: HashMap::new(),
            late: HashMap::new(),
            gaps: HashMap::new(),
            query,
        })
    }

    fn client(&mut self) -> Result<&mut Client, SourceError> {
        if self.client.is_none() {
            if self._lock.is_none() {
                return Err(unavailable(format!(
                    "source {:?} was opened for inspection only",
                    self.id
                )));
            }
            let client = Client::connect(&expand_env(&self.config.url), NoTls)
                .map_err(|e| unavailable(format!("source {:?}: reconnect: {e}", self.id)))?;
            self.client = Some(client);
        }
        self.client
            .as_mut()
            .ok_or_else(|| unavailable("no connection"))
    }

    /// One consistent read: the snapshot's `xmin` / `xmax`, and the rows
    /// after `after` or on the watch list, in sequence order.
    fn read(
        &mut self,
        after: Option<i64>,
        watched: &[i64],
        limit: i64,
    ) -> Result<(u64, u64, Vec<postgres::Row>), SourceError> {
        let query = self.query.clone();
        let client = self.client()?;
        let result = (|| -> Result<_, postgres::Error> {
            let mut tx = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()?;
            let snap = tx.query_one(
                "SELECT pg_snapshot_xmin(pg_current_snapshot())::text,
                        pg_snapshot_xmax(pg_current_snapshot())::text",
                &[],
            )?;
            let rows = tx.query(&query, &[&after.unwrap_or(i64::MIN), &watched, &limit])?;
            tx.commit()?;
            Ok((snap.get::<_, String>(0), snap.get::<_, String>(1), rows))
        })();
        let (xmin, xmax, rows) = result.map_err(|e| {
            // Reconnect on the next poll.
            self.client = None;
            unavailable(format!("source {:?}: {e}", self.id))
        })?;
        let parse = |s: String| {
            s.parse::<u64>()
                .map_err(|e| unavailable(format!("snapshot id {s:?}: {e}")))
        };
        Ok((parse(xmin)?, parse(xmax)?, rows))
    }

    /// Drop watched values past their retention, and past [`WATCH_CAP`].
    fn trim_watch(&self, state: &mut State, now_ms: u64) {
        let retention = self.config.watch_retention_ms;
        let (keep, expired): (Vec<Watched>, Vec<Watched>) = state
            .watch
            .drain(..)
            .partition(|w| now_ms.saturating_sub(w.skipped_at_ms) < retention);
        state.watch = keep;
        if !expired.is_empty() {
            eprintln!(
                "kulyrax: source {}: stopped watching {} skipped value(s) after {} ms: {}",
                self.id,
                expired.len(),
                retention,
                summarize(&expired)
            );
        }
        if state.watch.len() > WATCH_CAP {
            let over = state.watch.len() - WATCH_CAP;
            let dropped: Vec<Watched> = state.watch.drain(..over).collect();
            eprintln!(
                "kulyrax: source {}: watching more than {WATCH_CAP} skipped values; stopped \
                 watching the oldest {}: {}",
                self.id,
                dropped.len(),
                summarize(&dropped)
            );
        }
    }

    fn event(&self, row: &postgres::Row) -> Result<(i64, Event), SourceError> {
        let seq: i64 = row.get(0);
        let field = |i: usize, name: &str| -> Result<String, SourceError> {
            row.get::<_, Option<String>>(i).ok_or_else(|| {
                SourceError::Parse(format!(
                    "source {:?}: outbox row {seq}: {name} is null",
                    self.id
                ))
            })
        };
        if seq < 0 {
            return Err(SourceError::Parse(format!(
                "source {:?}: outbox row {seq}: the sequence is negative",
                self.id
            )));
        }
        let metadata = match row.get::<_, Option<String>>(4) {
            None => HashMap::new(),
            Some(text) => {
                let object: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(&text).map_err(|e| {
                        SourceError::Parse(format!(
                            "source {:?}: outbox row {seq}: metadata is not a JSON object: {e}",
                            self.id
                        ))
                    })?;
                object
                    .into_iter()
                    .map(|(k, v)| match v {
                        serde_json::Value::String(s) => (k, s),
                        other => (k, other.to_string()),
                    })
                    .collect()
            }
        };
        let version = match row.get::<_, Option<i64>>(5) {
            None => 1,
            Some(v) => u32::try_from(v).map_err(|_| {
                SourceError::Parse(format!(
                    "source {:?}: outbox row {seq}: version {v} is out of range",
                    self.id
                ))
            })?,
        };
        Ok((
            seq,
            Event::new(
                field(1, "event_id")?,
                field(2, "event_type")?,
                seq as u64,
                field(3, "payload")?,
            )
            .with_metadata(metadata)
            .with_version(version),
        ))
    }
}

fn summarize(values: &[Watched]) -> String {
    let shown: Vec<String> = values.iter().take(20).map(|w| w.seq.to_string()).collect();
    if values.len() > shown.len() {
        format!("{} …", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

/// The rows query: column `i` of the result is, in order, the sequence,
/// event id, event type, payload, metadata, version (as text / bigint).
fn select_rows(config: &OutboxConfig) -> String {
    let c = &config.columns;
    let metadata = c
        .metadata
        .as_deref()
        .map_or("NULL::text".to_string(), |m| format!("{m}::text"));
    let version = c
        .version
        .as_deref()
        .map_or("NULL::bigint".to_string(), |v| format!("{v}::bigint"));
    format!(
        "SELECT {seq}::bigint, {id}::text, {ty}::text, {payload}::text, {metadata}, {version}
         FROM {table}
         WHERE {seq} > $1 OR {seq} = ANY($2)
         ORDER BY {seq}
         LIMIT $3",
        seq = config.sequence_column,
        id = c.event_id,
        ty = c.event_type,
        payload = c.payload,
        table = config.table,
    )
}

/// The start-up checks: the table and every configured column exist with a
/// usable type, and the sequence behind `sequence_column` hands out values
/// one at a time in insert order. `Err` names what is wrong.
fn check_table(client: &mut Client, config: &OutboxConfig) -> Result<(), String> {
    let table = &config.table;
    let exists: bool = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[table])
        .map_err(|e| e.to_string())?
        .get(0);
    if !exists {
        return Err(format!("table {table} does not exist"));
    }
    let types: HashMap<String, String> = client
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)
             FROM pg_attribute a
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped",
            &[table],
        )
        .map_err(|e| e.to_string())?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();

    let integer = |t: &str| matches!(t, "smallint" | "integer" | "bigint");
    let text =
        |t: &str| t == "text" || t.starts_with("character varying") || t.starts_with("character(");
    let json = |t: &str| t == "json" || t == "jsonb";
    let c = &config.columns;
    // (config key, column, the type it needs, as said in the error)
    let mut wanted: Vec<(&str, &str, &str)> = vec![
        (
            "sequence_column",
            config.sequence_column.as_str(),
            "an integer type",
        ),
        ("columns.event_id", c.event_id.as_str(), "text"),
        ("columns.event_type", c.event_type.as_str(), "text"),
        ("columns.payload", c.payload.as_str(), "json, jsonb or text"),
    ];
    if let Some(m) = &c.metadata {
        wanted.push(("columns.metadata", m, "json or jsonb"));
    }
    if let Some(v) = &c.version {
        wanted.push(("columns.version", v, "an integer type"));
    }
    for (key, column, kind) in wanted {
        let Some(t) = types.get(column) else {
            return Err(format!("{key}: table {table} has no column {column:?}"));
        };
        let ok = match key {
            "sequence_column" | "columns.version" => integer(t),
            "columns.event_id" | "columns.event_type" => text(t),
            "columns.payload" => json(t) || text(t),
            _ => json(t),
        };
        if !ok {
            return Err(format!(
                "{key}: column {column:?} of {table} is {t}, not {kind}"
            ));
        }
    }

    let sequence: Option<String> = client
        .query_one(
            "SELECT pg_get_serial_sequence($1, $2)",
            &[table, &config.sequence_column],
        )
        .map_err(|e| e.to_string())?
        .get(0);
    match sequence {
        Some(seq) => {
            let row = client
                .query_one(
                    "SELECT seqincrement, seqcache FROM pg_sequence WHERE seqrelid = $1::text::regclass",
                    &[&seq],
                )
                .map_err(|e| e.to_string())?;
            let (increment, cache): (i64, i64) = (row.get(0), row.get(1));
            if increment != 1 || cache != 1 {
                return Err(format!(
                    "sequence {seq} behind {table}.{} has INCREMENT {increment} and CACHE \
                     {cache}; the outbox source needs INCREMENT 1 and CACHE 1, so values \
                     follow insert order with no built-in gaps (ALTER SEQUENCE {seq} \
                     INCREMENT 1 CACHE 1)",
                    config.sequence_column
                ));
            }
        }
        None if config.allow_unverified_sequence => {}
        None => {
            return Err(format!(
                "column {table}.{} has no sequence behind it (serial or identity), so \
                 Kulyrax cannot check that its values follow insert order. Use a serial or \
                 identity column, or set allow_unverified_sequence: true to accept ids the \
                 application assigns",
                config.sequence_column
            ));
        }
    }
    Ok(())
}

impl EventSource for PostgresOutboxSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        self.client()?;
        let max_batch = max_batch.max(1);
        let mut state = self.committed.clone();
        self.trim_watch(&mut state, wall_ms());
        let watched: Vec<i64> = state.watch.iter().map(|w| w.seq).collect();
        let limit = (max_batch + watched.len()) as i64;
        let (xmin, xmax, rows) = self.read(state.after, &watched, limit)?;

        let now = Instant::now();
        let timeout = Duration::from_millis(self.config.gap_timeout_ms);
        let grace = Duration::from_millis(self.config.gap_grace_ms);
        self.pending.clear();
        self.late.clear();
        let mut out = Vec::new();
        for row in &rows {
            if out.len() >= max_batch {
                break;
            }
            let (seq, event) = self.event(row)?;
            let below = state.after.is_some_and(|after| seq <= after);
            if below {
                // Not watched (any more): delivered already.
                if !state.watch.iter().any(|w| w.seq == seq) {
                    continue;
                }
                state.watch.retain(|w| w.seq != seq);
                state.late_seq += 1;
                let pos = position(state.after, state.late_seq);
                eprintln!(
                    "kulyrax: source {}: late row {seq} ({}) delivered after its gap was skipped",
                    self.id, event.event_id
                );
                self.late.insert(pos.0.clone(), seq);
                self.pending.insert(pos.0.clone(), state.clone());
                out.push(SourcedEvent::new(event, pos));
                continue;
            }
            if let Some(after) = state.after {
                let expected = after + 1;
                if seq > expected {
                    match self.gaps.get(&expected) {
                        None => {
                            self.gaps.insert(expected, OpenGap { xmax, seen: now });
                            break;
                        }
                        Some(gap) => {
                            let is_final = xmin >= gap.xmax;
                            let held = now.duration_since(gap.seen);
                            // The grace covers a producer that took its value
                            // with `nextval()` before it had a transaction id:
                            // no `xmax` can account for it, so a final gap is
                            // still held this long.
                            let skip = (is_final && held >= grace) || held >= timeout;
                            if !skip {
                                break;
                            }
                            let missing = seq - expected;
                            eprintln!(
                                "kulyrax: source {}: skipping {missing} missing value(s) {expected}..={} ({}); watching for them",
                                self.id,
                                seq - 1,
                                if is_final && held >= grace {
                                    "no transaction that could insert them is still running, and gap_grace_ms passed"
                                } else {
                                    "gap_timeout_ms passed"
                                }
                            );
                            let skipped_at_ms = wall_ms();
                            let first = (seq - WATCH_CAP as i64).max(expected);
                            state
                                .watch
                                .extend((first..seq).map(|seq| Watched { seq, skipped_at_ms }));
                            if first > expected {
                                eprintln!(
                                    "kulyrax: source {}: not watching {expected}..={}: more than {WATCH_CAP} values",
                                    self.id,
                                    first - 1
                                );
                            }
                            self.trim_watch(&mut state, skipped_at_ms);
                            self.gaps.remove(&expected);
                        }
                    }
                }
            }
            state.after = Some(seq);
            let pos = position(state.after, state.late_seq);
            self.pending.insert(pos.0.clone(), state.clone());
            out.push(SourcedEvent::new(event, pos));
        }
        // Gaps behind the cursor are no longer open.
        let after = state.after;
        self.gaps
            .retain(|start, _| after.is_none_or(|after| *start > after));
        Ok(out)
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        if self.committed_position.as_ref() == Some(&position) {
            return Ok(());
        }
        let Some(state) = self.pending.get(position.as_str()) else {
            return Err(SourceError::Checkpoint(format!(
                "source {:?}: position {position} was not handed out by the last poll",
                self.id
            )));
        };
        // The counters `on_resolved` just raised live on `committed`.
        let mut state = state.clone();
        state.late_rows = self.committed.late_rows;
        state.late_skipped = self.committed.late_skipped;
        let body =
            serde_json::to_string(&state).map_err(|e| SourceError::Checkpoint(e.to_string()))?;
        write_checkpoint(&self.state_dir, &self.id, &SourcePosition(body))?;
        self.committed = state;
        self.committed_position = Some(position);
        self.pending.clear();
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed_position.as_ref()
    }

    fn on_resolved(&mut self, event: &SourcedEvent, report: &ExecutionReport) {
        let Some(seq) = self.late.get(event.position.as_str()).copied() else {
            return;
        };
        let decisions: Vec<String> = report
            .adapter_reports
            .iter()
            .map(|a| format!("{}: {:?}", a.adapter_id, a.outcome))
            .collect();
        let skipped = !report.adapter_reports.is_empty()
            && report
                .adapter_reports
                .iter()
                .all(|a| matches!(a.outcome, AdapterOutcome::Skipped { .. }));
        // Counted on the committed state: `commit` follows for this batch.
        self.committed.late_rows += 1;
        if skipped {
            self.committed.late_skipped += 1;
        }
        eprintln!(
            "kulyrax: source {}: late row {seq} ({}) resolved {:?}{}: {}",
            self.id,
            event.event.event_id,
            report.status,
            if skipped {
                ", skipped by every adapter"
            } else {
                ""
            },
            decisions.join("; ")
        );
    }

    fn details(&self) -> Vec<(String, String)> {
        vec![
            (
                "watched gaps".into(),
                self.committed.watch.len().to_string(),
            ),
            ("late rows".into(), self.committed.late_rows.to_string()),
            (
                "late rows skipped".into(),
                self.committed.late_skipped.to_string(),
            ),
        ]
    }
}
