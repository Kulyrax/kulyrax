//! A batch's writes, collapsed in memory and written in bulk when the batch
//! commits (Phase 2b).
//!
//! Inside a batch, `decide()` still runs per event, in memory, against the
//! batch's guard lanes ([`kulyrax_core::adapter::sql::exec::project_in`]); the
//! writes it asks for are recorded here instead of sent one statement at a
//! time. Each target row and each guard lane keeps only its net effect for the
//! batch, because one `INSERT … ON CONFLICT DO UPDATE` cannot touch the same
//! row twice:
//!
//! - successive writes to a row merge: the union of their columns, the latest
//!   value of each;
//! - a delete discards what came before it; writes after it build a new row,
//!   inserted after the delete;
//! - a guard lane keeps its final state.
//!
//! [`Pending::flush`] then writes, per table, one `DELETE … USING unnest(…)`
//! and one `INSERT … SELECT FROM unnest(…)` per row shape, then one guard
//! upsert and one guard update, all in the batch transaction. Rows within an
//! entity keep their order (delete, then insert); different entities don't
//! interact. A facet cascade touches guard lanes the batch never loaded, so
//! the adapter flushes before running it directly (see `PostgresTxn`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use postgres::types::ToSql;

use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::adapter::sql::exec::{GuardRow, SqlWrite};

use super::{Bound, ColTypes, PgColType};

/// A row to insert or upsert: column → value.
struct PendingRow {
    values: BTreeMap<String, Bound>,
    set_columns: BTreeSet<String>,
    /// Empty: a plain `INSERT` (the row must not exist).
    conflict_target: Vec<String>,
}

impl PendingRow {
    /// Fold a later write to the same row into this one.
    fn merge(&mut self, later: PendingRow) -> Result<(), SqlError> {
        if !self.conflict_target.is_empty() && later.conflict_target.is_empty() {
            // Per event this would be a plain INSERT on a row written just
            // before it, which fails; let the per-event path report it.
            return Err(SqlError::ExecutionFailed(
                "plain insert of a row already written in this batch".to_string(),
            ));
        }
        self.values.extend(later.values);
        self.set_columns.extend(later.set_columns);
        Ok(())
    }
}

/// One target row's net effect in the batch.
enum RowOp {
    /// Insert or upsert, with no delete before it in the batch.
    Write(PendingRow),
    /// Delete the stored row, then insert `then` if anything was written after.
    Replace {
        key: Vec<(String, Bound)>,
        then: Option<PendingRow>,
    },
}

/// One guard lane's net effect in the batch.
enum GuardOp {
    Full {
        last_sequence: u64,
        last_event_id: String,
        deleted: bool,
        permanent: bool,
    },
    /// `SkipAlreadyDeleted` on a lane not otherwise written in the batch:
    /// only the sequence and event id change.
    Bump {
        last_sequence: u64,
        last_event_id: String,
    },
}

/// The writes of one open batch, not yet sent.
#[derive(Default)]
pub(crate) struct Pending {
    /// Keyed by `(table, entity_key)`.
    rows: HashMap<(String, String), RowOp>,
    /// Column types of every table with a pending row.
    tables: HashMap<String, Arc<ColTypes>>,
    /// Keyed by `(table, entity_key, facet)`.
    guards: HashMap<(String, String, String), GuardOp>,
}

impl Pending {
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.guards.is_empty()
    }

    /// Record `w`, whose values are already bound (`values`, paired with
    /// `w.columns`).
    pub(crate) fn write(
        &mut self,
        entity_key: &str,
        col_types: &Arc<ColTypes>,
        w: &SqlWrite,
        values: Vec<Bound>,
    ) -> Result<(), SqlError> {
        let row = PendingRow {
            values: w.columns.iter().cloned().zip(values).collect(),
            set_columns: w.set_columns.iter().cloned().collect(),
            conflict_target: w.conflict_target.clone(),
        };
        self.tables
            .entry(w.table.clone())
            .or_insert_with(|| Arc::clone(col_types));
        match self
            .rows
            .get_mut(&(w.table.clone(), entity_key.to_string()))
        {
            None => {
                self.rows
                    .insert((w.table.clone(), entity_key.to_string()), RowOp::Write(row));
            }
            Some(RowOp::Write(acc)) => acc.merge(row)?,
            Some(RowOp::Replace {
                then: Some(acc), ..
            }) => acc.merge(row)?,
            Some(RowOp::Replace { then, .. }) => *then = Some(row),
        }
        Ok(())
    }

    /// Record a delete of the row whose primary key is `key`.
    pub(crate) fn delete(
        &mut self,
        table: &str,
        entity_key: &str,
        col_types: &Arc<ColTypes>,
        key: Vec<(String, Bound)>,
    ) {
        self.tables
            .entry(table.to_string())
            .or_insert_with(|| Arc::clone(col_types));
        self.rows.insert(
            (table.to_string(), entity_key.to_string()),
            RowOp::Replace { key, then: None },
        );
    }

    pub(crate) fn guard(&mut self, row: &GuardRow) {
        self.guards.insert(
            (row.table.clone(), row.entity_key.clone(), row.facet.clone()),
            GuardOp::Full {
                last_sequence: row.last_sequence,
                last_event_id: row.last_event_id.clone(),
                deleted: row.deleted,
                permanent: row.permanent,
            },
        );
    }

    pub(crate) fn bump(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        seq: u64,
        event_id: &str,
    ) {
        let lane = (table.to_string(), entity_key.to_string(), facet.to_string());
        match self.guards.get_mut(&lane) {
            Some(GuardOp::Full {
                last_sequence,
                last_event_id,
                ..
            })
            | Some(GuardOp::Bump {
                last_sequence,
                last_event_id,
            }) => {
                *last_sequence = seq;
                *last_event_id = event_id.to_string();
            }
            None => {
                self.guards.insert(
                    lane,
                    GuardOp::Bump {
                        last_sequence: seq,
                        last_event_id: event_id.to_string(),
                    },
                );
            }
        }
    }

    /// Write everything recorded, through `execute`, and clear it. Tables,
    /// rows and lanes go in sorted order, so concurrent writers lock in the
    /// same order.
    pub(crate) fn flush(
        &mut self,
        mut execute: impl FnMut(&str, &[&(dyn ToSql + Sync)]) -> Result<(), SqlError>,
    ) -> Result<(), SqlError> {
        let mut rows: Vec<((String, String), RowOp)> = self.rows.drain().collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        // Per table: deletes first, then inserts and upserts by row shape.
        let mut by_table: BTreeMap<String, TableWrites> = BTreeMap::new();
        for ((table, _), op) in rows {
            let t = by_table.entry(table).or_default();
            match op {
                RowOp::Write(row) => t.add(row, false),
                RowOp::Replace { key, then } => {
                    t.deletes.push(key);
                    if let Some(row) = then {
                        // The row was just deleted: a plain insert.
                        t.add(row, true);
                    }
                }
            }
        }
        for (table, writes) in by_table {
            let types = self.tables.get(&table).cloned().unwrap_or_default();
            if !writes.deletes.is_empty() {
                delete_rows(&table, &types, writes.deletes, &mut execute)?;
            }
            for (shape, rows) in writes.shapes {
                insert_rows(&table, &types, &shape, rows, &mut execute)?;
            }
        }

        let mut guards: Vec<((String, String, String), GuardOp)> = self.guards.drain().collect();
        guards.sort_by(|a, b| a.0.cmp(&b.0));
        write_guards(guards, &mut execute)?;
        self.tables.clear();
        Ok(())
    }
}

/// The columns, `ON CONFLICT` target and `SET` columns that rows share, so
/// they fit one statement.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Shape {
    columns: Vec<String>,
    conflict_target: Vec<String>,
    set_columns: Vec<String>,
}

#[derive(Default)]
struct TableWrites {
    deletes: Vec<Vec<(String, Bound)>>,
    shapes: BTreeMap<Shape, Vec<BTreeMap<String, Bound>>>,
}

impl TableWrites {
    fn add(&mut self, row: PendingRow, plain_insert: bool) {
        let shape = if plain_insert || row.conflict_target.is_empty() {
            Shape {
                columns: row.values.keys().cloned().collect(),
                conflict_target: Vec::new(),
                set_columns: Vec::new(),
            }
        } else {
            Shape {
                columns: row.values.keys().cloned().collect(),
                conflict_target: row.conflict_target,
                set_columns: row.set_columns.into_iter().collect(),
            }
        };
        self.shapes.entry(shape).or_default().push(row.values);
    }
}

fn col_type(types: &ColTypes, column: &str) -> PgColType {
    types.get(column).copied().unwrap_or(PgColType::Text)
}

/// `$1::text[], $2::int8[], …` for `columns`.
fn unnest_args(columns: &[Column]) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("${}::{}", i + 1, c.sql_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn params(columns: &[Column]) -> Vec<&(dyn ToSql + Sync)> {
    columns.iter().map(Column::as_sql).collect()
}

fn delete_rows(
    table: &str,
    types: &ColTypes,
    keys: Vec<Vec<(String, Bound)>>,
    execute: &mut impl FnMut(&str, &[&(dyn ToSql + Sync)]) -> Result<(), SqlError>,
) -> Result<(), SqlError> {
    let Some(first) = keys.first() else {
        return Ok(());
    };
    let names: Vec<String> = first.iter().map(|(c, _)| c.clone()).collect();
    let mut columns: Vec<Column> = names
        .iter()
        .map(|c| Column::new(col_type(types, c)))
        .collect();
    for key in keys {
        for (column, (_, value)) in columns.iter_mut().zip(key) {
            column.push(value)?;
        }
    }
    let aliases: Vec<String> = (0..names.len()).map(|i| format!("k{i}")).collect();
    let matches = names
        .iter()
        .zip(&aliases)
        .map(|(c, a)| format!("t.{c} = d.{a}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "DELETE FROM {table} AS t USING unnest({}) AS d({}) WHERE {matches}",
        unnest_args(&columns),
        aliases.join(", ")
    );
    execute(&sql, &params(&columns))
}

fn insert_rows(
    table: &str,
    types: &ColTypes,
    shape: &Shape,
    rows: Vec<BTreeMap<String, Bound>>,
    execute: &mut impl FnMut(&str, &[&(dyn ToSql + Sync)]) -> Result<(), SqlError>,
) -> Result<(), SqlError> {
    let mut columns: Vec<Column> = shape
        .columns
        .iter()
        .map(|c| Column::new(col_type(types, c)))
        .collect();
    for mut row in rows {
        for (column, name) in columns.iter_mut().zip(&shape.columns) {
            column.push(row.remove(name).unwrap_or(Bound::Null))?;
        }
    }
    let mut sql = format!(
        "INSERT INTO {table} ({}) SELECT * FROM unnest({})",
        shape.columns.join(", "),
        unnest_args(&columns)
    );
    if !shape.conflict_target.is_empty() {
        let set = shape
            .set_columns
            .iter()
            .map(|c| format!("{c} = excluded.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(
            " ON CONFLICT ({}) DO UPDATE SET {set}",
            shape.conflict_target.join(", ")
        ));
    }
    execute(&sql, &params(&columns))
}

fn write_guards(
    guards: Vec<((String, String, String), GuardOp)>,
    execute: &mut impl FnMut(&str, &[&(dyn ToSql + Sync)]) -> Result<(), SqlError>,
) -> Result<(), SqlError> {
    let mut full: [Vec<String>; 4] = Default::default(); // table, key, facet, event id
    let (mut full_seq, mut full_deleted, mut full_permanent) = (Vec::new(), Vec::new(), Vec::new());
    let mut bump: [Vec<String>; 4] = Default::default();
    let mut bump_seq = Vec::new();
    for ((table, key, facet), op) in guards {
        match op {
            GuardOp::Full {
                last_sequence,
                last_event_id,
                deleted,
                permanent,
            } => {
                for (v, s) in full.iter_mut().zip([table, key, facet, last_event_id]) {
                    v.push(s);
                }
                full_seq.push(last_sequence as i64);
                full_deleted.push(deleted);
                full_permanent.push(permanent);
            }
            GuardOp::Bump {
                last_sequence,
                last_event_id,
            } => {
                for (v, s) in bump.iter_mut().zip([table, key, facet, last_event_id]) {
                    v.push(s);
                }
                bump_seq.push(last_sequence as i64);
            }
        }
    }
    if !full_seq.is_empty() {
        execute(
            "INSERT INTO conduit_projection_state
                (target_table, entity_key, facet, last_sequence, last_event_id, deleted, permanent, processed_at)
             SELECT t, k, f, s, e, d, p, now()
             FROM unnest($1::text[], $2::text[], $3::text[], $4::int8[], $5::text[], $6::bool[], $7::bool[])
                  AS g(t, k, f, s, e, d, p)
             ON CONFLICT (target_table, entity_key, facet) DO UPDATE SET
                last_sequence = excluded.last_sequence,
                last_event_id = excluded.last_event_id,
                deleted = excluded.deleted,
                permanent = excluded.permanent,
                processed_at = excluded.processed_at",
            &[
                &full[0],
                &full[1],
                &full[2],
                &full_seq,
                &full[3],
                &full_deleted,
                &full_permanent,
            ],
        )?;
    }
    if !bump_seq.is_empty() {
        execute(
            "UPDATE conduit_projection_state AS g
                SET last_sequence = b.s, last_event_id = b.e, processed_at = now()
             FROM unnest($1::text[], $2::text[], $3::text[], $4::int8[], $5::text[]) AS b(t, k, f, s, e)
             WHERE g.target_table = b.t AND g.entity_key = b.k AND g.facet = b.f",
            &[&bump[0], &bump[1], &bump[2], &bump_seq, &bump[3]],
        )?;
    }
    Ok(())
}

/// One column of rows as a typed array parameter, typed by the column's
/// introspected type (the same type its per-event bind uses).
enum Column {
    Text(Vec<Option<String>>),
    SmallInt(Vec<Option<i16>>),
    Int(Vec<Option<i32>>),
    BigInt(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Tstz(Vec<Option<DateTime<Utc>>>),
    Ts(Vec<Option<NaiveDateTime>>),
    Date(Vec<Option<NaiveDate>>),
}

impl Column {
    fn new(t: PgColType) -> Self {
        match t {
            PgColType::Text | PgColType::Other => Column::Text(Vec::new()),
            PgColType::SmallInt => Column::SmallInt(Vec::new()),
            PgColType::Int => Column::Int(Vec::new()),
            PgColType::BigInt => Column::BigInt(Vec::new()),
            PgColType::Float => Column::Float(Vec::new()),
            PgColType::Bool => Column::Bool(Vec::new()),
            PgColType::Timestamptz => Column::Tstz(Vec::new()),
            PgColType::Timestamp => Column::Ts(Vec::new()),
            PgColType::Date => Column::Date(Vec::new()),
        }
    }

    fn sql_type(&self) -> &'static str {
        match self {
            Column::Text(_) => "text[]",
            Column::SmallInt(_) => "int2[]",
            Column::Int(_) => "int4[]",
            Column::BigInt(_) => "int8[]",
            Column::Float(_) => "float8[]",
            Column::Bool(_) => "bool[]",
            Column::Tstz(_) => "timestamptz[]",
            Column::Ts(_) => "timestamp[]",
            Column::Date(_) => "date[]",
        }
    }

    fn push(&mut self, value: Bound) -> Result<(), SqlError> {
        match (self, value) {
            (Column::Text(v), Bound::Null) => v.push(None),
            (Column::SmallInt(v), Bound::Null) => v.push(None),
            (Column::Int(v), Bound::Null) => v.push(None),
            (Column::BigInt(v), Bound::Null) => v.push(None),
            (Column::Float(v), Bound::Null) => v.push(None),
            (Column::Bool(v), Bound::Null) => v.push(None),
            (Column::Tstz(v), Bound::Null) => v.push(None),
            (Column::Ts(v), Bound::Null) => v.push(None),
            (Column::Date(v), Bound::Null) => v.push(None),
            (Column::Text(v), Bound::Text(x)) => v.push(Some(x)),
            (Column::SmallInt(v), Bound::SmallInt(x)) => v.push(Some(x)),
            (Column::Int(v), Bound::Int(x)) => v.push(Some(x)),
            (Column::BigInt(v), Bound::BigInt(x)) => v.push(Some(x)),
            (Column::Float(v), Bound::Float(x)) => v.push(Some(x)),
            (Column::Bool(v), Bound::Bool(x)) => v.push(Some(x)),
            (Column::Tstz(v), Bound::Tstz(x)) => v.push(Some(x)),
            (Column::Ts(v), Bound::Ts(x)) => v.push(Some(x)),
            (Column::Date(v), Bound::Date(x)) => v.push(Some(x)),
            (c, _) => {
                return Err(SqlError::ExecutionFailed(format!(
                    "a value does not match its column's {} array",
                    c.sql_type()
                )));
            }
        }
        Ok(())
    }

    fn as_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Column::Text(v) => v,
            Column::SmallInt(v) => v,
            Column::Int(v) => v,
            Column::BigInt(v) => v,
            Column::Float(v) => v,
            Column::Bool(v) => v,
            Column::Tstz(v) => v,
            Column::Ts(v) => v,
            Column::Date(v) => v,
        }
    }
}
