//! Each first-party backend's `type:` must resolve through
//! `register_default_backends`. An unparseable URL makes the factory reject
//! the entry before any connection is attempted, so no database is needed:
//! a rejection naming the backend proves its factory was found and ran.

use std::collections::HashMap;

use kulyrax_core::KulyraxRuntime;
use kulyrax_core::runtime::config::KulyraxConfig;

fn build_error_for(type_name: &str, config_block: &str) -> String {
    let yaml = format!(
        "version: 1\nrouting:\n  file: routing.json\nadapters:\n  - type: {type_name}\n    id: a1\n    priority: 10\n    config: {config_block}\n"
    );
    let config: KulyraxConfig = match serde_yaml::from_str(&yaml) {
        Ok(c) => c,
        Err(e) => return format!("config did not parse: {e}"),
    };
    let mut runtime = KulyraxRuntime::from_parts(
        config,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    kulyrax_backends::register_default_backends(&mut runtime);
    match runtime.build_adapters() {
        Ok(()) => "built without error".to_string(),
        Err(e) => e.to_string(),
    }
}

fn assert_factory_ran(type_name: &str, config_block: &str) {
    let msg = build_error_for(type_name, config_block);
    assert!(
        msg.contains(&format!("{type_name} adapter \"a1\"")),
        "expected {type_name}'s own factory to reject the entry, got: {msg}"
    );
}

#[test]
fn postgres_resolves_through_its_factory() {
    assert_factory_ran("postgres", "{url: 'not a url'}");
}

#[test]
fn elasticsearch_resolves_through_its_factory() {
    assert_factory_ran("elasticsearch", "{url: 'not a url'}");
}

#[test]
fn mysql_resolves_through_its_factory() {
    assert_factory_ran("mysql", "{url: 'not a url'}");
}

#[test]
fn redis_resolves_through_its_factory() {
    assert_factory_ran("redis", "{url: 'not a url'}");
}
