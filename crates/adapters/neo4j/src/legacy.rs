//! Kulyrax 0.12 labelled guard nodes `__ConduitGuard`. The first `prepare`
//! relabels them `__KulyraxGuard` (`kulyrax_core::adapter::legacy`).

use neo4rs::{Graph, Query};

use kulyrax_core::adapter::graph::adapter::GraphError;

use crate::neo_err;

/// The guard label in Kulyrax 0.12 and earlier.
const LEGACY_GUARD_LABEL: &str = "__ConduitGuard";
/// Guard nodes relabelled per transaction.
const RELABEL_BATCH: usize = 10_000;
/// At most this many conflicting lanes are named in the error.
const SHOW_CONFLICTS: usize = 20;

/// Neo4j cannot rename a label, so the nodes are relabelled in batches, each its own transaction:
/// a node moves atomically and keeps its identity, so a migration killed
/// between batches continues where it stopped. The new label's uniqueness
/// constraint exists first. A lane under both labels (a 0.12 process wrote
/// after the upgrade) is refused before anything moves, naming the lanes.
/// Then the old label's constraint is dropped.
///
/// No tombstone is possible here: Neo4j Community has no constraint that
/// stops a 0.12 process from creating `__ConduitGuard` nodes again. The
/// release notes say to stop every 0.12 process first.
///
/// Two processes migrating one database can deadlock on the same nodes; a
/// transient error (`WriteConflict`) restarts the migration from the current
/// state, up to [`ATTEMPTS`] times.
pub(crate) async fn migrate_guards(graph: &Graph) -> Result<(), GraphError> {
    let mut result = attempt(graph).await;
    for _ in 1..ATTEMPTS {
        if !matches!(result, Err(GraphError::WriteConflict)) {
            break;
        }
        result = attempt(graph).await;
    }
    result
}

/// Migrations restart at most this many times.
const ATTEMPTS: usize = 5;

async fn attempt(graph: &Graph) -> Result<(), GraphError> {
    let old = LEGACY_GUARD_LABEL;
    let mut conflicts = Vec::new();
    let mut rows = graph
        .execute(Query::new(format!(
            "MATCH (o:{old}) \
             MATCH (n:__KulyraxGuard {{kind: o.kind, target: o.target, \
                   entity_key: o.entity_key, facet: o.facet}}) \
             WHERE o <> n \
             RETURN o.kind AS kind, o.target AS target, o.entity_key AS entity_key, \
                    o.facet AS facet LIMIT {SHOW_CONFLICTS}"
        )))
        .await
        .map_err(neo_err)?;
    while let Some(row) = rows.next().await.map_err(neo_err)? {
        let field = |name: &str| row.get::<String>(name).unwrap_or_default();
        conflicts.push(format!(
            "({}, {}, {}, facet {:?})",
            field("kind"),
            field("target"),
            field("entity_key"),
            field("facet")
        ));
    }
    if !conflicts.is_empty() {
        return Err(GraphError::WriteFailed(format!(
            "guard lanes exist under both {old} (Kulyrax 0.12) and __KulyraxGuard: {}; a 0.12 \
             process wrote here after the upgrade. Keep the nodes you want, delete the others, \
             and start again",
            conflicts.join(", ")
        )));
    }
    let batch = kulyrax_core::crash::test_knob("RELABEL_BATCH")
        .and_then(|v| v.parse().ok())
        .unwrap_or(RELABEL_BATCH);
    loop {
        let mut rows = graph
            .execute(Query::new(format!(
                "MATCH (g:{old}) WITH g LIMIT {batch} \
                 SET g:__KulyraxGuard REMOVE g:{old} RETURN count(g) AS moved"
            )))
            .await
            .map_err(neo_err)?;
        let moved = match rows.next().await.map_err(neo_err)? {
            Some(row) => row.get::<i64>("moved").unwrap_or(0),
            None => 0,
        };
        if moved == 0 {
            break;
        }
        kulyrax_core::crash::crash_point("guard-relabel-batch");
    }
    let mut rows = graph
        .execute(Query::new(format!(
            "SHOW CONSTRAINTS YIELD name, labelsOrTypes \
             WHERE '{old}' IN labelsOrTypes RETURN name"
        )))
        .await
        .map_err(neo_err)?;
    let mut names = Vec::new();
    while let Some(row) = rows.next().await.map_err(neo_err)? {
        if let Ok(name) = row.get::<String>("name") {
            names.push(name);
        }
    }
    for name in names {
        graph
            .run(Query::new(format!(
                "DROP CONSTRAINT `{}` IF EXISTS",
                name.replace('`', "``")
            )))
            .await
            .map_err(neo_err)?;
    }
    Ok(())
}
