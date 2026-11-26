//! `OutboxConfig::new` + `with_*` and `OutboxColumns::default()` + `with_*`
//! build what the same YAML parses to (decision
//! 2026-11-26-adapter-from-config). No database is needed.

use kulyrax_source_postgres::{OutboxColumns, OutboxConfig};

fn parsed(yaml: &str) -> OutboxConfig {
    kulyrax_core::yaml::from_str(yaml).expect("yaml")
}

fn same(built: &OutboxConfig, parsed: &OutboxConfig) {
    assert_eq!(format!("{built:?}"), format!("{parsed:?}"));
}

#[test]
fn new_has_the_yaml_defaults() {
    same(
        &OutboxConfig::new("postgres://app@db/app", "outbox", "id"),
        &parsed("url: postgres://app@db/app\ntable: outbox\nsequence_column: id\n"),
    );
}

#[test]
fn every_setter_matches_its_key() {
    let built = OutboxConfig::new("postgres://app@db/app", "public.outbox", "seq")
        .with_columns(
            OutboxColumns::default()
                .with_event_id("eid")
                .with_event_type("kind")
                .with_payload("body")
                .with_metadata(None)
                .with_version(Some("v".into())),
        )
        .with_gap_grace_ms(5)
        .with_gap_timeout_ms(6)
        .with_watch_retention_ms(7)
        .with_allow_unverified_sequence(true);
    same(
        &built,
        &parsed(
            "url: postgres://app@db/app\ntable: public.outbox\nsequence_column: seq\n\
             columns: { event_id: eid, event_type: kind, payload: body, metadata: ~, version: v }\n\
             gap_grace_ms: 5\ngap_timeout_ms: 6\nwatch_retention_ms: 7\n\
             allow_unverified_sequence: true\n",
        ),
    );
}
