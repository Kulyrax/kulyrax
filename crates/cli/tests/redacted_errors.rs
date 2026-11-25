//! A database password never reaches what `kulyrax` prints or writes, even
//! when the URL that holds it is refused. The password comes from an
//! environment variable, as operators keep it, so the URL in the config only
//! names `${KX_TEST_DB_PASSWORD}`: the leak this guards against was the
//! expanded URL in the error.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const PASSWORD: &str = "Hunter2Secret";
const TOKEN: &str = "TokenS3cretValue";

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kulyrax"))
}

fn write(path: PathBuf, s: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, s).unwrap();
}

/// A project with one adapter (`adapter` is its YAML), the mapping kind it
/// needs (`sql` or `document`), and an empty event directory.
fn project(p: &Path, adapter: &str, kind: &str) {
    write(
        p.join("kulyrax.yaml"),
        &format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n{adapter}sources:\n  - type: directory\n    id: events\n    path: ./events\n    state_dir: ./state\n"
        ),
    );
    write(p.join("routing.yaml"), "U: [store]\n");
    let _ = std::fs::remove_dir_all(p.join("mappings"));
    std::fs::create_dir_all(p.join("mappings/sql")).unwrap();
    std::fs::create_dir_all(p.join("mappings/document")).unwrap();
    if kind == "sql" {
        write(
            p.join("mappings/sql/u.yaml"),
            "event: U\ntable: users\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
        );
    } else {
        write(
            p.join("mappings/document/u.yaml"),
            "event: U\ncollection: users\nid: payload.id\nversion: 1\ndocument:\n  v: payload.v\n",
        );
    }
    std::fs::create_dir_all(p.join("events")).unwrap();
}

/// A Postgres adapter whose URL is refused (the port), with the password from
/// the environment and a secret query parameter.
const BAD_PG: &str = "  - type: postgres\n    id: store\n    priority: 10\n    config:\n      url: postgres://app:${KX_TEST_DB_PASSWORD}@db.internal:54x32/app?password=TokenS3cretValue\n";

const FILE_STORE: &str =
    "  - type: file\n    id: store\n    priority: 10\n    config:\n      root: ./store\n";

fn assert_hidden(what: &str, text: &str) {
    for secret in [PASSWORD, TOKEN] {
        assert!(!text.contains(secret), "{what} shows a secret:\n{text}");
    }
}

#[test]
fn a_refused_url_is_reported_without_its_password() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    project(p, BAD_PG, "sql");
    let out = Command::new(bin())
        .current_dir(p)
        .args([
            "run",
            "--config",
            "kulyrax.yaml",
            "--mappings",
            "mappings",
            "--once",
        ])
        .env("KX_TEST_DB_PASSWORD", PASSWORD)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "{text}");
    assert!(
        text.contains("does not parse") && text.contains("app:***@"),
        "{text}"
    );
    assert_hidden("kulyrax run", &text);
}

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_rejected_reload_is_reported_and_recorded_without_the_password() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    project(p, FILE_STORE, "document");
    let _run = Running(
        Command::new(bin())
            .current_dir(p)
            .args(["run", "--config", "kulyrax.yaml", "--mappings", "mappings"])
            .args(["--poll-interval-ms", "50"])
            .env("KX_TEST_DB_PASSWORD", PASSWORD)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    // The running process has built its store once it holds the lock.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !p.join("store/.kulyrax/lock").exists() {
        assert!(Instant::now() < deadline, "kulyrax run did not start");
        std::thread::sleep(Duration::from_millis(50));
    }

    project(p, BAD_PG, "sql");
    let out = Command::new(bin())
        .current_dir(p)
        .args(["reload", "--config", "kulyrax.yaml"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("rejected") && text.contains("app:***@"),
        "{text}"
    );
    assert_hidden("kulyrax reload", &text);

    let result_path = p.join(".kulyrax/control/reload-result");
    let result = std::fs::read_to_string(&result_path).unwrap();
    assert!(result.contains("rejected"), "{result}");
    assert_hidden("reload-result", &result);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&result_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "reload-result is readable by others");
    }
}
