//! Step 5's format marker on Redis (decision 2026-11-28-format-versions): the
//! hash `__kulyrax:format`, written with `HSETNX` so concurrent first starts
//! leave one value; a newer or unrecognised one is refused; an ACL user who
//! may not write it is refused with the rights it needs; a user who may not
//! run `CONFIG` still starts.
//!
//! **Gated on `KULYRAX_REDIS_URL`.** The marker is one key per database, so
//! these tests use a database of their own ([`DB`]), flushed before and after
//! each test and never used by another suite; the tests take turns on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kulyrax_adapter_redis::RedisAdapter;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, StorageAdapter};
use kulyrax_core::format::FormatProblem;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

/// This binary's own database number.
const DB: u32 = 13;

/// The tests share [`DB`]: one at a time.
static TURN: Mutex<()> = Mutex::new(());

/// `KULYRAX_REDIS_URL` as `redis://[user:pw@]host:port/DB`.
fn url_for(base: &str, credentials: Option<(&str, &str)>) -> Option<String> {
    let (scheme, rest) = base.split_once("://")?;
    let host_part = rest.split('/').next()?;
    let host = host_part.rsplit_once('@').map_or(host_part, |(_, h)| h);
    let auth = credentials.map_or(String::new(), |(u, p)| format!("{u}:{p}@"));
    Some(format!("{scheme}://{auth}{host}/{DB}"))
}

fn admin(url: &str) -> Result<redis::Connection, redis::RedisError> {
    redis::Client::open(url)?.get_connection()
}

fn adapter(url: &str) -> RedisAdapter {
    RedisAdapter::new(
        "kv".into(),
        url,
        2,
        10,
        KvRuntimeBuilder::new(HashMap::new()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn marker(url: &str) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    Ok(redis::cmd("HGETALL")
        .arg("__kulyrax:format")
        .query(&mut admin(url)?)?)
}

/// Run `test` on a flushed [`DB`], flushing it again afterwards.
fn on_fresh_db(test: impl FnOnce(&str, &str) -> R) -> R {
    let Ok(base) = std::env::var("KULYRAX_REDIS_URL") else {
        eprintln!("SKIP: KULYRAX_REDIS_URL not set");
        return Ok(());
    };
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let url = url_for(&base, None).ok_or("KULYRAX_REDIS_URL is not a redis:// URL")?;
    let mut c = admin(&url)?;
    redis::cmd("FLUSHDB").query::<()>(&mut c)?;
    let size: i64 = redis::cmd("DBSIZE").query(&mut c)?;
    assert_eq!(size, 0, "database {DB} is not empty");
    let result = test(&base, &url);
    redis::cmd("FLUSHDB").query::<()>(&mut c)?;
    result
}

#[test]
fn a_new_database_gets_one_marker() -> R {
    on_fresh_db(|_, url| {
        adapter(url).prepare()?;
        adapter(url).prepare()?;
        assert_eq!(
            marker(url)?,
            HashMap::from([("guards".to_string(), "1".to_string())])
        );
        Ok(())
    })
}

#[test]
fn a_newer_or_unknown_marker_is_refused() -> R {
    on_fresh_db(|_, url| {
        for (value, newer) in [("2", true), ("one", false)] {
            redis::cmd("HSET")
                .arg("__kulyrax:format")
                .arg("guards")
                .arg(value)
                .query::<()>(&mut admin(url)?)?;
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
            assert!(e.store.contains(&format!("/{DB}")), "{}", e.store);
            assert_eq!(marker(url)?["guards"], value, "left as it is");
        }
        Ok(())
    })
}

/// Change 3: concurrent first starts leave one marker, one version.
#[test]
fn concurrent_first_starts_leave_one_marker() -> R {
    on_fresh_db(|_, url| {
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
        assert_eq!(
            marker(url)?,
            HashMap::from([("guards".to_string(), "1".to_string())])
        );
        Ok(())
    })
}

/// An ACL user for this test, removed afterwards: `rules` after `on`.
fn with_acl_user(url: &str, user: &str, rules: &[&str], test: impl FnOnce() -> R) -> R {
    let mut c = admin(url)?;
    let mut set = redis::cmd("ACL");
    set.arg("SETUSER")
        .arg(user)
        .arg("reset")
        .arg("on")
        .arg(">kx-acl-pw");
    for r in rules {
        set.arg(*r);
    }
    set.query::<()>(&mut c)?;
    let result = test();
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(user)
        .query::<()>(&mut c)?;
    result
}

/// Change 4: an ACL user who may not write the marker is refused, naming the
/// store and the rights; the message holds no password.
#[test]
fn a_user_who_may_not_write_the_marker_names_the_rights() -> R {
    on_fresh_db(|base, url| {
        let user = format!("kx_format_{}", std::process::id());
        with_acl_user(url, &user, &["~*", "+@all", "-hsetnx"], || {
            let app = url_for(base, Some((&user, "kx-acl-pw"))).ok_or("url")?;
            let err = adapter(&app).prepare().expect_err("refused");
            let AdapterError::Format(e) = &err else {
                panic!("not a format refusal: {err}");
            };
            let FormatProblem::MarkerNotCreated { grant, .. } = &e.problem else {
                panic!("not MarkerNotCreated: {e}");
            };
            assert!(
                grant.contains("HSETNX") && grant.contains("__kulyrax:format"),
                "{grant}"
            );
            assert!(!e.to_string().contains("kx-acl-pw"), "no password: {e}");
            assert!(marker(url)?.is_empty());
            Ok(())
        })
    })
}

/// A user who may not run `CONFIG` (as on managed Redis) still starts; the
/// info line it prints is checked through the binary in the CLI's
/// `redis_eviction_notice` test.
#[test]
fn a_user_without_config_still_starts() -> R {
    on_fresh_db(|base, url| {
        let user = format!("kx_noconfig_{}", std::process::id());
        with_acl_user(url, &user, &["~*", "+@all", "-config"], || {
            let app = url_for(base, Some((&user, "kx-acl-pw"))).ok_or("url")?;
            let blocked = redis::cmd("CONFIG")
                .arg("GET")
                .arg("maxmemory-policy")
                .query::<Vec<String>>(&mut admin(&app)?);
            assert!(
                blocked.is_err(),
                "CONFIG is blocked for {user}: {blocked:?}"
            );
            adapter(&app).prepare()?;
            assert_eq!(marker(url)?["guards"], "1");
            Ok(())
        })
    })
}
