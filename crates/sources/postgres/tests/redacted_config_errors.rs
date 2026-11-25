//! A Postgres outbox URL holding a password and a secret query parameter never
//! shows either one: not in the error when it cannot connect, not in the
//! config's Debug.

use kulyrax_core::runtime::SourceBuildContext;
use kulyrax_core::runtime::config::CustomSourceConfig;
use kulyrax_source_postgres::{OutboxConfig, factory};

const PASSWORD: &str = "Hunter2Secret";
const TOKEN: &str = "TokenS3cretValue";

fn config() -> serde_json::Value {
    serde_json::json!({
        "url": format!("postgres://app:{PASSWORD}@db.internal:54x32/app?password={TOKEN}"),
        "table": "public.outbox",
        "sequence_column": "id",
    })
}

fn assert_hidden(what: &str, text: &str) {
    for secret in [PASSWORD, TOKEN] {
        assert!(!text.contains(secret), "{what} shows a secret: {text}");
    }
}

#[test]
fn the_connect_error_hides_the_secrets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let raw = serde_yaml::to_value(serde_json::json!({
        "type": "postgres_outbox", "id": "o", "state_dir": "state", "config": config(),
    }))
    .expect("raw");
    let entry = CustomSourceConfig {
        type_name: "postgres_outbox".into(),
        id: "o".into(),
        state_dir: Some("state".into()),
        raw,
    };
    let ctx = SourceBuildContext {
        base_dir: dir.path().to_path_buf(),
        inspect: false,
    };
    let err = factory(&entry, &ctx)
        .err()
        .expect("an unparseable url is refused");
    assert_hidden("Display", &err.to_string());
    assert_hidden("Debug", &format!("{err:?}"));
}

#[test]
fn the_config_debug_hides_the_secrets() {
    let cfg: OutboxConfig = serde_json::from_value(config()).expect("config");
    let debug = format!("{cfg:?}");
    assert!(debug.contains("***"), "{debug}");
    assert_hidden("Debug", &debug);
}
