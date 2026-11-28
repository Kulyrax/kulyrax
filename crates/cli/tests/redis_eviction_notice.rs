//! A Redis adapter whose user may not run `CONFIG` (as on managed Redis)
//! starts, projects, and prints one info line saying what eviction policy
//! Kulyrax requires (decision 2026-11-28-format-versions, the owner's Redis
//! additions). No warning and no refusal.
//!
//! **Gated on `KULYRAX_REDIS_URL`.** It uses database [`DB`], which no other
//! suite uses, flushed before and after, and an ACL user deleted afterwards.

use std::path::{Path, PathBuf};
use std::process::Command;

/// This test's own database number.
const DB: u32 = 14;

fn write(path: PathBuf, s: &str) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, s).expect("write");
}

/// `KULYRAX_REDIS_URL` as `redis://[user:pw@]host:port/DB`, and its
/// `host:port`.
fn url_for(base: &str, credentials: Option<(&str, &str)>) -> (String, String) {
    let (scheme, rest) = base.split_once("://").expect("a redis:// URL");
    let host_part = rest.split('/').next().expect("a host");
    let host = host_part.rsplit_once('@').map_or(host_part, |(_, h)| h);
    let auth = credentials.map_or(String::new(), |(u, p)| format!("{u}:{p}@"));
    (format!("{scheme}://{auth}{host}/{DB}"), host.to_string())
}

fn project(dir: &Path, url: &str) {
    write(
        dir.join("config.yaml"),
        &format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: redis\n    id: kv\n    \
             priority: 10\n    config:\n      url: {url}\nsources:\n  - type: directory\n    id: in\n    \
             path: events\n    state_dir: state\n"
        ),
    );
    write(dir.join("routing.yaml"), "U: [kv]\n");
    write(
        dir.join("mappings/keyvalue/u.yaml"),
        "event: U\nnamespace: users\nkey: payload.id\nversion: 1\nvalue:\n  name: payload.name\n",
    );
    for kind in ["sql", "document"] {
        std::fs::create_dir_all(dir.join("mappings").join(kind)).expect("mkdir");
    }
    write(
        dir.join("events/000.json"),
        r#"{"event_id":"e1","event_type":"U","payload":"{\"id\":\"u1\",\"name\":\"Ada\"}","metadata":{},"version":1,"sequence":1}"#,
    );
}

#[test]
fn a_blocked_config_still_starts_and_says_what_is_required() {
    let Ok(base) = std::env::var("KULYRAX_REDIS_URL") else {
        eprintln!("SKIP: KULYRAX_REDIS_URL not set");
        return;
    };
    let (admin_url, host) = url_for(&base, None);
    let mut admin = redis::Client::open(admin_url.as_str())
        .and_then(|c| c.get_connection())
        .expect("connect");
    redis::cmd("FLUSHDB")
        .query::<()>(&mut admin)
        .expect("flush");
    let user = format!("kx_notice_{}", std::process::id());
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("reset")
        .arg("on")
        .arg(">kx-acl-pw")
        .arg("~*")
        .arg("+@all")
        .arg("-config")
        .query::<()>(&mut admin)
        .expect("ACL user");

    let dir = tempfile::tempdir().expect("tempdir");
    let (app_url, _) = url_for(&base, Some((&user, "kx-acl-pw")));
    project(dir.path(), &app_url);
    let out = Command::new(env!("CARGO_BIN_EXE_kulyrax"))
        .args([
            "run",
            "--config",
            "config.yaml",
            "--mappings",
            "mappings",
            "--once",
        ])
        .current_dir(dir.path())
        .output()
        .expect("run kulyrax");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let value: Option<String> = redis::cmd("GET")
        .arg("users:u1")
        .query(&mut admin)
        .expect("GET");

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&user)
        .query::<()>(&mut admin)
        .expect("delete the ACL user");
    redis::cmd("FLUSHDB")
        .query::<()>(&mut admin)
        .expect("flush");

    assert_eq!(out.status.code(), Some(0), "{stderr}");
    assert!(value.is_some(), "the event was projected: {stderr}");
    let line = format!(
        "redis adapter kv ({host}/{DB}): could not read maxmemory-policy; \
         Kulyrax requires noeviction (or volatile-* with no TTLs on Kulyrax keys)"
    );
    assert_eq!(
        stderr.lines().filter(|l| *l == line).count(),
        1,
        "exactly one info line:\n{stderr}"
    );
    assert!(!stderr.contains("WARNING"), "no warning:\n{stderr}");
    assert!(!stderr.contains("kx-acl-pw"), "no password:\n{stderr}");
}
