//! Step 5's format marker on Elasticsearch (decision 2026-11-28-format-versions;
//! the owner chose one marker per cluster): the document `guards` in the
//! index `kulyrax_format`, written with `_create` so concurrent first starts
//! leave one; a newer or unrecognised one is refused.
//!
//! The refusal of a user who may not write the marker (401/403) is not
//! tested: the local and CI clusters run with security disabled.
//!
//! **Gated on `KULYRAX_ELASTICSEARCH_URL`.** The marker is one per cluster,
//! so this binary owns the `kulyrax_format` index while it runs: each test
//! deletes it before and after, one test at a time. Cargo runs a crate's test
//! binaries one after another, and the other suites' tests do not use it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kulyrax_adapter_elasticsearch::ElasticsearchAdapter;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, StorageAdapter};
use kulyrax_core::format::FormatProblem;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;
use serde_json::{Value, json};

type R = Result<(), Box<dyn std::error::Error>>;

static TURN: Mutex<()> = Mutex::new(());

fn adapter(url: &str) -> ElasticsearchAdapter {
    ElasticsearchAdapter::new(
        "es".into(),
        url,
        None,
        60_000,
        10,
        DocumentRuntimeBuilder::new(HashMap::new()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn http() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .expect("client")
}

fn delete_marker(url: &str) -> R {
    let status = http()
        .delete(format!("{url}/kulyrax_format"))
        .send()?
        .status();
    assert!(
        status.is_success() || status == reqwest::StatusCode::NOT_FOUND,
        "delete kulyrax_format: {status}"
    );
    Ok(())
}

/// The marker's `_source`, or `None`.
fn marker(url: &str) -> Result<Option<Value>, Box<dyn std::error::Error>> {
    let resp = http()
        .get(format!("{url}/kulyrax_format/_doc/guards"))
        .send()?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let body: Value = resp.json()?;
    Ok(Some(body["_source"].clone()))
}

/// Run `test` with the cluster's marker removed before and after.
fn without_marker(test: impl FnOnce(&str) -> R) -> R {
    let Ok(url) = std::env::var("KULYRAX_ELASTICSEARCH_URL") else {
        eprintln!("SKIP: KULYRAX_ELASTICSEARCH_URL not set");
        return Ok(());
    };
    let url = url.trim_end_matches('/').to_string();
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    delete_marker(&url)?;
    let result = test(&url);
    delete_marker(&url)?;
    result
}

#[test]
fn a_new_cluster_gets_one_marker() -> R {
    without_marker(|url| {
        adapter(url).prepare()?;
        adapter(url).prepare()?;
        assert_eq!(marker(url)?, Some(json!({ "version": 1 })));
        Ok(())
    })
}

#[test]
fn a_newer_or_unknown_marker_is_refused() -> R {
    without_marker(|url| {
        for (version, newer) in [(json!(2), true), (json!("two"), false)] {
            // A fresh index each time: `version` is mapped by the first
            // value written, and a string would not fit a number field.
            delete_marker(url)?;
            let put = http()
                .put(format!("{url}/kulyrax_format/_doc/guards?refresh=true"))
                .json(&json!({ "version": version }))
                .send()?;
            assert!(put.status().is_success(), "{}", put.status());
            let err = adapter(url).prepare().expect_err("refused");
            let AdapterError::Format(e) = err else {
                panic!("not a format refusal: {err}");
            };
            assert_eq!(
                matches!(
                    e.problem,
                    FormatProblem::Newer {
                        found: 2,
                        supported: 1
                    }
                ),
                newer,
                "{e}"
            );
            assert!(e.store.starts_with("Elasticsearch "), "{}", e.store);
            assert_eq!(marker(url)?, Some(json!({ "version": version })), "kept");
        }
        Ok(())
    })
}

/// Change 3: concurrent first starts leave one marker, one version.
#[test]
fn concurrent_first_starts_leave_one_marker() -> R {
    without_marker(|url| {
        let results: Vec<Result<(), String>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|_| s.spawn(|| adapter(url).prepare().map_err(|e| e.to_string())))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
                .collect()
        });
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(marker(url)?, Some(json!({ "version": 1 })));
        let count: Value = http()
            .get(format!("{url}/kulyrax_format/_count"))
            .send()?
            .json()?;
        assert_eq!(count["count"], 1, "{count}");
        Ok(())
    })
}
