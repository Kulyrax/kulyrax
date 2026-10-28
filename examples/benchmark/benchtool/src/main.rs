//! Measures projection lag: how long after an event file appears in a watched
//! directory its batch is committed by a running `kulyrax run`.
//!
//!   benchtool lag --dir <events dir> --checkpoint <state>/<source id>.json \
//!                 --events N --per-file K --rate R [--id-prefix p] [--log file] -- <kulyrax run …>
//!
//! It starts the given command, writes one warm-up file and waits for its
//! commit (so connection setup isn't counted), then writes N events as files
//! of K events at R events/s. Each file is published with write-then-rename,
//! so the source never reads half a file. A watcher reads the checkpoint every
//! millisecond; a file's lag is the time from its rename to the first
//! checkpoint at or past it. Every event in a file gets its file's lag. When
//! the last file is committed the command is killed (everything it did is
//! committed by then) and one JSON line of results is printed.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

struct Args {
    dir: PathBuf,
    checkpoint: PathBuf,
    events: usize,
    per_file: usize,
    rate: f64,
    id_prefix: String,
    /// Where the command's stdout goes (its run report); discarded if unset.
    log: Option<PathBuf>,
    command: Vec<String>,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    assert_eq!(it.next().as_deref(), Some("lag"), "usage: benchtool lag …");
    let (mut dir, mut checkpoint, mut events, mut per_file, mut rate) = (None, None, None, None, None);
    let mut id_prefix = "lag".to_string();
    let mut log = None;
    let mut command = Vec::new();
    while let Some(a) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{a} needs a value"));
        match a.as_str() {
            "--dir" => dir = Some(PathBuf::from(val())),
            "--checkpoint" => checkpoint = Some(PathBuf::from(val())),
            "--events" => events = Some(val().parse().expect("--events")),
            "--per-file" => per_file = Some(val().parse().expect("--per-file")),
            "--rate" => rate = Some(val().parse().expect("--rate")),
            "--id-prefix" => id_prefix = val(),
            "--log" => log = Some(PathBuf::from(val())),
            "--" => {
                command = it.by_ref().collect();
                break;
            }
            other => panic!("unknown argument {other}"),
        }
    }
    assert!(!command.is_empty(), "missing `-- <kulyrax command>`");
    Args {
        dir: dir.expect("--dir"),
        checkpoint: checkpoint.expect("--checkpoint"),
        events: events.expect("--events"),
        per_file: per_file.expect("--per-file"),
        rate: rate.expect("--rate"),
        id_prefix,
        log,
        command,
    }
}

/// Same event shape as `run.sh`: one new entity per event, sequence 1.
fn event_line(id: &str) -> String {
    let payload = serde_json::json!({
        "id": id,
        "name": format!("User {id}"),
        "email": format!("{id}@example.com"),
        "bio": format!("bio {id}"),
        "followers": 1,
    });
    serde_json::json!({
        "event_id": format!("e-{id}"),
        "event_type": "UserRegistered",
        "payload": payload.to_string(),
        "metadata": {},
        "version": 1,
        "sequence": 1,
    })
    .to_string()
}

fn publish(dir: &Path, index: usize, ids: impl Iterator<Item = String>) {
    let body: String = ids.map(|id| event_line(&id) + "\n").collect();
    // A dot-file with a non-event extension: the directory source ignores it.
    let tmp = dir.join(format!(".{index:06}.tmp"));
    std::fs::write(&tmp, body).expect("write event file");
    std::fs::rename(&tmp, dir.join(format!("{index:06}.ndjson"))).expect("publish event file");
}

/// The file index the checkpoint has committed through, if any.
fn committed_index(checkpoint: &Path) -> Option<usize> {
    // A read can race the checkpoint's own rename; the next poll retries.
    let text = std::fs::read_to_string(checkpoint).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let pos = v.get("position")?.as_str()?;
    pos.strip_suffix(".ndjson")?.parse().ok()
}

fn wait_for_commit(checkpoint: &Path, index: usize, child: &mut Child, timeout: Duration) {
    let start = Instant::now();
    while committed_index(checkpoint).is_none_or(|c| c < index) {
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!("kulyrax exited early with {status}");
        }
        assert!(start.elapsed() < timeout, "file {index} not committed within {timeout:?}");
        thread::sleep(Duration::from_millis(1));
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.dir).expect("create events dir");
    let files = a.events.div_ceil(a.per_file);

    let mut child = Command::new(&a.command[0])
        .args(&a.command[1..])
        .stdout(match &a.log {
            Some(path) => Stdio::from(std::fs::File::create(path).expect("create --log file")),
            None => Stdio::null(),
        })
        .spawn()
        .expect("start kulyrax");

    // Warm-up: file 0, not measured.
    publish(&a.dir, 0, (0..a.per_file).map(|i| format!("{}-warm{i}", a.id_prefix)));
    wait_for_commit(&a.checkpoint, 0, &mut child, Duration::from_secs(120));

    let published: Arc<Mutex<Vec<Option<Instant>>>> = Arc::new(Mutex::new(vec![None; files + 1]));
    let committed: Arc<Mutex<Vec<Option<Instant>>>> = Arc::new(Mutex::new(vec![None; files + 1]));

    let watcher = {
        let checkpoint = a.checkpoint.clone();
        let committed = Arc::clone(&committed);
        thread::spawn(move || {
            let mut seen = 0;
            while seen < files {
                if let Some(c) = committed_index(&checkpoint) {
                    let now = Instant::now();
                    let mut slots = committed.lock().unwrap();
                    for slot in slots.iter_mut().take(c.min(files) + 1).skip(seen + 1) {
                        *slot = Some(now);
                    }
                    seen = seen.max(c.min(files));
                }
                thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let start = Instant::now();
    let interval = Duration::from_secs_f64(a.per_file as f64 / a.rate);
    for f in 1..=files {
        let due = start + interval * (f as u32 - 1);
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            thread::sleep(wait);
        }
        let first = (f - 1) * a.per_file;
        let last = (first + a.per_file).min(a.events);
        publish(&a.dir, f, (first..last).map(|i| format!("{}{i}", a.id_prefix)));
        published.lock().unwrap()[f] = Some(Instant::now());
    }
    let produce_secs = start.elapsed().as_secs_f64();

    let deadline = Instant::now() + Duration::from_secs(1800);
    while !watcher.is_finished() {
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!("kulyrax exited early with {status}");
        }
        assert!(Instant::now() < deadline, "projection did not catch up within 30 min");
        thread::sleep(Duration::from_millis(50));
    }
    watcher.join().expect("watcher");
    let _ = child.kill();
    let _ = child.wait();

    let (published, committed) = (published.lock().unwrap(), committed.lock().unwrap());
    let mut lags_ms: Vec<f64> = Vec::with_capacity(a.events);
    for f in 1..=files {
        let (p, c) = (published[f].expect("published"), committed[f].expect("committed"));
        // A commit seen before the publish timestamp was taken is a zero-lag race.
        let lag = c.saturating_duration_since(p).as_secs_f64() * 1000.0;
        let n = (a.per_file).min(a.events - (f - 1) * a.per_file);
        lags_ms.extend(std::iter::repeat_n(lag, n));
    }
    lags_ms.sort_by(f64::total_cmp);
    let mean = lags_ms.iter().sum::<f64>() / lags_ms.len() as f64;
    println!(
        "{}",
        serde_json::json!({
            "events": a.events,
            "per_file": a.per_file,
            "target_rate": a.rate,
            "achieved_rate": (a.events as f64 / produce_secs).round(),
            "lag_ms": {
                "p50": percentile(&lags_ms, 50.0).round(),
                "p90": percentile(&lags_ms, 90.0).round(),
                "p99": percentile(&lags_ms, 99.0).round(),
                "max": lags_ms[lags_ms.len() - 1].round(),
                "mean": mean.round(),
            },
        })
    );
}
