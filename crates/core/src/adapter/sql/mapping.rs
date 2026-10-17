use std::collections::HashMap;

use serde::Deserialize;

use super::adapter::SqlError;
use crate::adapter::{OnExisting, Operation, is_identity_scalar};
use crate::event::Event;
use crate::runtime::config::AdapterCapability;
use serde_json::Value;

/// One value, or several in declared order — used for `primary_key` (Phase
/// 11.1), which is a single column name for most tables and a list of column
/// names for tables whose real identity is composite (bridge/junction tables,
/// many-to-many association tables).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        match self {
            OneOrMany::One(v) => std::slice::from_ref(v).iter(),
            OneOrMany::Many(v) => v.iter(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            OneOrMany::One(_) => 1,
            OneOrMany::Many(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<&str> for OneOrMany<String> {
    fn from(s: &str) -> Self {
        OneOrMany::One(s.to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqlMapping {
    pub event: String,
    pub table: String,

    /// Target schema version this mapping projects into. Must be explicit ($\ge 1$).
    pub version: u32,

    /// Column name (single-column identity), or a list of column names for a
    /// composite key (Phase 11.1). Every listed column must also appear in
    /// `columns`.
    pub primary_key: OneOrMany<String>,

    pub columns: HashMap<String, String>,

    /// Write mode for an entity that already has a projected row (Phase
    /// 12.2). Defaults to `ignore` — every mapping written before Phase 12
    /// keeps its Phase 11 behavior unchanged. Read only when `operation:
    /// upsert` (Phase 13.1).
    #[serde(default)]
    pub on_existing: OnExisting,

    /// What this mapping does: create/update the entity, or remove it (Phase
    /// 13.1). Defaults to `upsert` — every mapping written before Phase 13
    /// keeps its Phase 11/12 behavior unchanged. A `delete` mapping's
    /// `columns` must contain exactly the `primary_key` column(s) — no
    /// projection body, it only resolves the entity to remove.
    #[serde(default)]
    pub operation: Operation,

    /// `operation: delete` only (Phase 13.4): this tombstone rejects every
    /// later event for this key, forever — no resurrection. Ignored for
    /// `operation: upsert`.
    #[serde(default)]
    pub permanent: bool,

    /// Phase 15.1: the named facet this mapping owns. `None` → the **default
    /// facet** (the whole entity — every pre-Phase-15 mapping). `Some("contact")`
    /// → this mapping writes only its own non-key columns and gates on its own
    /// per-facet sequence lane, leaving the rest of the row untouched. A named
    /// facet is always a sequence-gated partial replace: the adapter passes
    /// `OnExisting::Replace` to `decide()` for the facet's lane regardless of
    /// this mapping's `on_existing` (which stays `ignore`). `operation: delete`
    /// is only valid on the default facet.
    #[serde(default)]
    pub facet: Option<String>,

    /// Capabilities adapters must provide to run this projection (enum; parse-time validated).
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

impl SqlMapping {
    /// Normalized facet key — `""` for the default facet (Phase 15.1).
    pub fn facet_key(&self) -> &str {
        self.facet.as_deref().unwrap_or("")
    }

    /// Build the dialect-neutral [`SqlWrite`](super::exec::SqlWrite) and the
    /// resolved entity identity (Phase 11.1) — the `primary_key` column(s)'
    /// value(s), in declared `primary_key` order, read off the same resolution
    /// pass that builds `columns` so they can never drift from what is actually
    /// inserted. Each key value must be a JSON scalar (string/number/bool); an
    /// object/array/null fails the build rather than reach the guard key.
    ///
    /// The backend renders the `SqlWrite` — Phase 19.1 moved the SQL-string
    /// generation out of the mapping.
    pub fn build(&self, event: &Event) -> Result<(super::exec::SqlWrite, Vec<Value>), SqlError> {
        // Parse payload JSON
        let payload: Value = serde_json::from_str(&event.payload)
            .map_err(|e| SqlError::BuildFailed(format!("invalid payload JSON: {}", e)))?;

        let payload_obj = payload
            .as_object()
            .ok_or_else(|| SqlError::BuildFailed("payload must be a JSON object".into()))?;

        // Deterministic column order (important!)
        let mut column_pairs: Vec<(&String, &String)> = self.columns.iter().collect();

        column_pairs.sort_by(|a, b| a.0.cmp(b.0));

        let mut columns = Vec::with_capacity(column_pairs.len());
        let mut values = Vec::with_capacity(column_pairs.len());
        let mut resolved_by_column: HashMap<&str, Value> =
            HashMap::with_capacity(column_pairs.len());

        for (column, path_expr) in column_pairs {
            let value = self.resolve_path(path_expr, payload_obj, &event.metadata)?;

            resolved_by_column.insert(column.as_str(), value.clone());
            columns.push(column.clone());
            values.push(value);
        }

        let mut key_values = Vec::with_capacity(self.primary_key.len());
        for key_column in self.primary_key.iter() {
            let value = resolved_by_column
                .get(key_column.as_str())
                .cloned()
                .ok_or_else(|| {
                    SqlError::BuildFailed(format!(
                        "primary key column '{}' not present in mapping columns",
                        key_column
                    ))
                })?;

            if !is_identity_scalar(&value) {
                return Err(SqlError::BuildFailed(format!(
                    "primary key column '{}' resolved to a non-scalar value; \
                     entity identity must be a string, number, or bool",
                    key_column
                )));
            }

            key_values.push(value);
        }

        // Phase 15.3: a named-facet mapping is always a partial upsert — every
        // column is still inserted (the row may not exist on a resurrection
        // edge), but only the facet's own **non-key** columns appear in the
        // `SET` list, so a facet update never clobbers another facet's columns
        // or the create mapping's columns. The entity-existence pre-check in
        // the adapter guarantees the `DO UPDATE` branch is the one that runs.
        //
        // Phase 12.3: for `on_existing: replace` every mapped column is in the
        // SET list (full replace). `on_existing: ignore` emits a plain INSERT —
        // no `ON CONFLICT` clause (empty `conflict_target`). An `ON CONFLICT`
        // target requires the target table to declare a matching PRIMARY KEY /
        // UNIQUE constraint (SQLite and Postgres both) — schema ownership
        // outside Kulyrax.
        let pk_names: Vec<String> = self.primary_key.iter().cloned().collect();
        let pk_set: std::collections::HashSet<&str> =
            self.primary_key.iter().map(|s| s.as_str()).collect();

        let (conflict_target, set_columns) = if self.facet.is_some() {
            (
                pk_names.clone(),
                columns
                    .iter()
                    .filter(|c| !pk_set.contains(c.as_str()))
                    .cloned()
                    .collect(),
            )
        } else {
            match self.on_existing {
                OnExisting::Ignore => (Vec::new(), Vec::new()),
                OnExisting::Replace => (pk_names.clone(), columns.clone()),
            }
        };

        let write = super::exec::SqlWrite {
            table: self.table.clone(),
            columns,
            values,
            conflict_target,
            set_columns,
        };

        Ok((write, key_values))
    }

    fn resolve_path(
        &self,
        path: &str,
        payload: &serde_json::Map<String, Value>,
        metadata: &HashMap<String, String>,
    ) -> Result<Value, SqlError> {
        if let Some(rest) = path.strip_prefix("payload.") {
            payload
                .get(rest)
                .cloned()
                .ok_or_else(|| SqlError::BuildFailed(format!("payload field '{}' not found", rest)))
        } else if let Some(rest) = path.strip_prefix("metadata.") {
            metadata
                .get(rest)
                .map(|v| Value::String(v.clone()))
                .ok_or_else(|| {
                    SqlError::BuildFailed(format!("metadata field '{}' not found", rest))
                })
        } else {
            Err(SqlError::BuildFailed(format!(
                "invalid path '{}' - must start with 'payload.' or 'metadata.'",
                path
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping_with_primary_key(yaml_primary_key: &str) -> SqlMapping {
        serde_yaml::from_str(&format!(
            r#"
event: UserCreated
table: users
version: 1
primary_key: {}
columns:
  id: payload.id
  email: payload.email
"#,
            yaml_primary_key
        ))
        .unwrap()
    }

    #[test]
    fn scalar_primary_key_deserializes_as_one() {
        let m = mapping_with_primary_key("id");
        assert_eq!(m.primary_key.iter().collect::<Vec<_>>(), vec!["id"]);
    }

    #[test]
    fn list_primary_key_deserializes_as_many() {
        let m = mapping_with_primary_key("[id, email]");
        assert_eq!(
            m.primary_key.iter().collect::<Vec<_>>(),
            vec!["id", "email"]
        );
    }

    #[test]
    fn build_resolves_composite_key_in_declared_order() {
        let m = mapping_with_primary_key("[email, id]"); // declared order differs from column sort order
        let event = Event {
            event_id: "e1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{ "id": "u1", "email": "a@b.com" }"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        let (_, key_values) = m.build(&event).unwrap();
        assert_eq!(
            key_values,
            vec![Value::String("a@b.com".into()), Value::String("u1".into())]
        );
    }

    #[test]
    fn build_rejects_non_scalar_primary_key_value() {
        let m = mapping_with_primary_key("id");
        let event = Event {
            event_id: "e1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{ "id": ["not", "scalar"], "email": "a@b.com" }"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        let err = m.build(&event).unwrap_err();
        assert!(err.to_string().contains("non-scalar"));
    }

    #[test]
    fn ignore_mode_emits_plain_insert() {
        let m = mapping_with_primary_key("id");
        let event = Event {
            event_id: "e1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{ "id": "u1", "email": "a@b.com" }"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        let (write, _) = m.build(&event).unwrap();
        assert!(write.conflict_target.is_empty());
        let sql = write.render(super::super::exec::Placeholders::Question);
        assert!(!sql.contains("ON CONFLICT"), "{sql}");
        assert!(sql.starts_with("INSERT INTO users"), "{sql}");
    }

    #[test]
    fn replace_mode_emits_upsert_with_every_column_in_set_and_composite_conflict_target() {
        let mut m = mapping_with_primary_key("[id, email]");
        m.on_existing = OnExisting::Replace;
        let event = Event {
            event_id: "e1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{ "id": "u1", "email": "a@b.com" }"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        let (write, _) = m.build(&event).unwrap();
        assert_eq!(write.conflict_target, vec!["id", "email"]);
        let sql = write.render(super::super::exec::Placeholders::Question);
        assert!(sql.contains("ON CONFLICT (id, email)"), "{sql}");
        assert!(sql.contains("DO UPDATE SET"), "{sql}");
        assert!(sql.contains("email = excluded.email"), "{sql}");
        assert!(sql.contains("id = excluded.id"), "{sql}");
    }
}
