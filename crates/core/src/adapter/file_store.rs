//! Phase 3: crash-safe, batched writes for the file-backed adapters.
//!
//! The document, key-value and graph file stores keep one JSON file per key.
//! They all write through a [`FileStore`]. It collects a batch's writes in
//! memory, keeping only the final content of each path, and makes them durable
//! with one fsynced batch log (`.kulyrax/wal/<seq>.log`) before applying them to
//! the per-key files without fsync. A sync point in the background later fsyncs
//! the touched files and deletes the logs they cover. `prepare` replays any log
//! still on disk before anything is ingested.
//!
//! Decision record: `docs/ledger/decisions/2026-10-30-file-store-batch-log.md`.
//!
//! A store's internal files live in `<root>/.kulyrax/`, in the entries
//! [`kulyrax_dir::STORE_ENTRIES`] lists and nowhere else. A store last opened by
//! Kulyrax 0.12 keeps them under the old name; the first start moves them (see
//! [`crate::adapter::legacy`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::adapter::{AdapterError, file_names};
use crate::crash::crash_point;
use crate::execution::stages::{self, Stage, timed};
use crate::kulyrax_dir::{self, DIR, LAYOUT, LOCK, WAL};

/// The content of a layout marker (`.kulyrax/layout/<kind>`): version 2 is the
/// `.kulyrax/` layout. Version 1 (0.12.1) was the same layout under the old name.
pub(crate) const LAYOUT_VERSION: &[u8] = b"2
";

/// Batch log layout: magic, body length (u64 LE), CRC-32 of the body (u32 LE),
/// then the body: per entry, a tag byte (1 write, 0 remove), the path length
/// (u32 LE) and path, and for a write the content length (u32 LE) and content.
const MAGIC: &[u8; 8] = b"KLXWAL01";
const HEADER_LEN: usize = 8 + 8 + 4;

/// A sync point runs after this many batches or this long since the last one,
/// whichever comes first.
const SYNC_EVERY_BATCHES: u32 = 32;
const SYNC_INTERVAL: Duration = Duration::from_secs(1);

/// A sync point waits until its newest log is this old before it fsyncs
/// anything. By then the OS has written most of the files back on its own,
/// and an fsync with nothing left to flush is cheap. Measured on NTFS, 4,000
/// files on 8 threads: 584 µs per file fsynced while dirty, 42 µs 2 s later.
/// It runs in the background, so the wait delays only the deletion of logs.
const SYNC_DELAY: Duration = Duration::from_secs(2);

/// Sync jobs that may wait for the sync thread before a commit blocks on it,
/// which bounds the batch logs kept on disk.
const SYNC_QUEUE: usize = 4;

/// Past this many cached `.kulyrax/` files the cache is cleared rather than
/// grown (about 100 bytes of guard per entry, plus the path).
const CACHE_LIMIT: usize = 1_000_000;

/// File writes and fsyncs are spread over this many threads when there are at
/// least `PARALLEL_MIN` of them. Measured: creating files scales to about 4
/// threads on NTFS, fsyncs a little further.
const IO_THREADS: usize = 8;
const PARALLEL_MIN: usize = 64;

fn io_err(e: io::Error) -> AdapterError {
    AdapterError::WriteFailed(e.to_string())
}

fn other(msg: String) -> io::Error {
    io::Error::other(msg)
}

// ---------------------------------------------------------------------------
// Pending writes
// ---------------------------------------------------------------------------

/// Writes not yet on disk, by path relative to the store root: `Some(content)`
/// replaces the whole file, `None` removes it. Keyed by path, so a key written
/// several times keeps only its final state.
#[derive(Debug, Default)]
struct Pending {
    files: BTreeMap<String, Option<String>>,
    /// The parent directory of every path in `files`, so directory listings
    /// can see directories that exist only in memory so far.
    dirs: BTreeSet<String>,
}

impl Pending {
    fn set(&mut self, path: &str, content: Option<String>) {
        if let Some((dir, _)) = path.rsplit_once('/')
            && !self.dirs.contains(dir)
        {
            self.dirs.insert(dir.to_string());
        }
        self.files.insert(path.to_string(), content);
    }

    fn merge(&mut self, other: Pending) {
        self.files.extend(other.files);
        self.dirs.extend(other.dirs);
    }

    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The names of `dir`'s child directories that have pending files.
    fn child_dirs(&self, dir: &str, out: &mut BTreeSet<String>) {
        let prefix = format!("{dir}/");
        for d in self.dirs.range(prefix.clone()..) {
            let Some(rest) = d.strip_prefix(&prefix) else {
                break;
            };
            let name = rest.split('/').next().unwrap_or(rest);
            if !name.is_empty() {
                out.insert(name.to_string());
            }
        }
    }
}

/// Only `.kulyrax/` files (guards, graph incident indexes) are cached: they
/// are what every event reads. Data files are read only for facet merges.
fn is_cached(path: &str) -> bool {
    kulyrax_dir::is_store_path(path)
}

// ---------------------------------------------------------------------------
// Batch log encoding
// ---------------------------------------------------------------------------

fn encode_log(pending: &Pending) -> Vec<u8> {
    let mut body = Vec::new();
    for (path, content) in &pending.files {
        body.push(u8::from(content.is_some()));
        body.extend_from_slice(&(path.len() as u32).to_le_bytes());
        body.extend_from_slice(path.as_bytes());
        if let Some(c) = content {
            body.extend_from_slice(&(c.len() as u32).to_le_bytes());
            body.extend_from_slice(c.as_bytes());
        }
    }
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// `None` when the bytes are not a complete, intact batch log: too short,
/// wrong magic, a length that disagrees with the file, a checksum mismatch,
/// or an entry that runs past the end.
fn decode_log(bytes: &[u8]) -> Option<Pending> {
    let header = bytes.get(..HEADER_LEN)?;
    if &header[..8] != MAGIC {
        return None;
    }
    let len = u64::from_le_bytes(header[8..16].try_into().ok()?) as usize;
    let crc = u32::from_le_bytes(header[16..20].try_into().ok()?);
    let body = bytes.get(HEADER_LEN..)?;
    if body.len() != len || crc32fast::hash(body) != crc {
        return None;
    }
    let mut pending = Pending::default();
    let mut rest = body;
    let take = |rest: &mut &[u8], n: usize| -> Option<Vec<u8>> {
        let (a, b) = (rest.get(..n)?, rest.get(n..)?);
        *rest = b;
        Some(a.to_vec())
    };
    let take_u32 = |rest: &mut &[u8]| -> Option<usize> {
        Some(u32::from_le_bytes(take(rest, 4)?.try_into().ok()?) as usize)
    };
    while !rest.is_empty() {
        let tag = take(&mut rest, 1)?[0];
        let n = take_u32(&mut rest)?;
        let path = String::from_utf8(take(&mut rest, n)?).ok()?;
        let content = match tag {
            0 => None,
            1 => {
                let n = take_u32(&mut rest)?;
                Some(String::from_utf8(take(&mut rest, n)?).ok()?)
            }
            _ => return None,
        };
        pending.set(&path, content);
    }
    Some(pending)
}

fn log_name(seq: u64) -> String {
    format!("{seq:016}.log")
}

/// Every `<seq>.log` in the log directory, oldest first. Leftover `.tmp`
/// files (a crash before their rename) are deleted: their batch never
/// committed.
fn list_logs(wal: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut logs = Vec::new();
    let entries = match fs::read_dir(wal) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(logs),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.ends_with(".tmp") {
            remove_if_exists(&path)?;
        } else if let Some(seq) = name.strip_suffix(".log").and_then(|s| s.parse().ok()) {
            logs.push((seq, path));
        }
    }
    logs.sort();
    Ok(logs)
}

// ---------------------------------------------------------------------------
// File-system helpers
// ---------------------------------------------------------------------------

pub(crate) fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Fsync a directory, so the names created, renamed or removed in it are
/// durable. Windows has no portable equivalent (a directory cannot be opened
/// as a file), so this is a no-op there; NTFS journals those changes in order
/// (see the decision record).
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        match File::open(dir) {
            Ok(f) => f.sync_all(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Make one file's data and metadata durable for a sync point, if it still
/// exists (a later batch may have removed it).
///
/// Unix: `fsync`. Windows: the file is written to the device without the
/// device cache flush that `FlushFileBuffers` sends for every file;
/// [`run_sync`] sends one for all of them afterwards. Measured on NTFS, 4,000
/// dirty files on 8 threads: 315 µs per file with `FlushFileBuffers`, 32 µs
/// with the flush deferred, plus 48 ms once.
fn sync_file(path: &Path) -> io::Result<()> {
    match OpenOptions::new().write(true).open(path) {
        #[cfg(windows)]
        Ok(f) => windows::flush_without_device_sync(&f),
        #[cfg(not(windows))]
        Ok(f) => f.sync_all(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    /// `IO_STATUS_BLOCK`: a status/pointer union, then a `ULONG_PTR`.
    #[repr(C)]
    struct IoStatusBlock {
        status: usize,
        information: usize,
    }

    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn NtFlushBuffersFileEx(
            file: *mut c_void,
            flags: u32,
            parameters: *const c_void,
            parameters_size: u32,
            io_status: *mut IoStatusBlock,
        ) -> i32;
        fn RtlNtStatusToDosError(status: i32) -> u32;
    }

    /// Write the file's data and metadata to the storage device, but do not
    /// ask the device to flush its own cache.
    const FLUSH_FLAGS_NO_SYNC: u32 = 0x2;

    /// `NtFlushBuffersFileEx(FLUSH_FLAGS_NO_SYNC)`. The result is durable only
    /// after a device cache flush, which any `FlushFileBuffers` on the same
    /// volume sends.
    pub(super) fn flush_without_device_sync(file: &File) -> io::Result<()> {
        let mut io_status = IoStatusBlock {
            status: 0,
            information: 0,
        };
        // SAFETY: the handle is a valid open file handle borrowed from `file`
        // for the duration of the call; no parameters are passed (null, 0);
        // `io_status` is a live, correctly laid out IO_STATUS_BLOCK.
        let status = unsafe {
            NtFlushBuffersFileEx(
                file.as_raw_handle(),
                FLUSH_FLAGS_NO_SYNC,
                std::ptr::null(),
                0,
                &mut io_status,
            )
        };
        if status >= 0 {
            Ok(())
        } else {
            // SAFETY: a pure status-code translation with no pointers.
            let code = unsafe { RtlNtStatusToDosError(status) };
            Err(io::Error::from_raw_os_error(code as i32))
        }
    }
}

/// Run `f` over `items`, on several threads when there are enough of them.
/// Returns the first error.
fn for_each_parallel<T: Sync>(
    items: &[T],
    f: impl Fn(&T) -> io::Result<()> + Sync,
) -> io::Result<()> {
    if items.len() < PARALLEL_MIN {
        return items.iter().try_for_each(&f);
    }
    let chunk = items.len().div_ceil(IO_THREADS);
    std::thread::scope(|s| {
        let handles: Vec<_> = items
            .chunks(chunk)
            .map(|c| s.spawn(|| c.iter().try_for_each(&f)))
            .collect();
        let mut result = Ok(());
        for h in handles {
            let r = h
                .join()
                .unwrap_or_else(|_| Err(other("file I/O thread panicked".to_string())));
            if result.is_ok() {
                result = r;
            }
        }
        result
    })
}

/// Write `pending` to the per-key files under `root`, without fsync.
/// `known_dirs` remembers directories already created, to skip the syscall.
fn apply(root: &Path, pending: &Pending, known_dirs: &mut HashSet<String>) -> io::Result<()> {
    for dir in &pending.dirs {
        if !known_dirs.contains(dir) {
            fs::create_dir_all(root.join(dir))?;
            known_dirs.insert(dir.clone());
        }
    }
    let entries: Vec<(&String, &Option<String>)> = pending.files.iter().collect();
    let half = entries.len() / 2;
    let (first, second) = entries.split_at(half);
    let write = |(path, content): &(&String, &Option<String>)| -> io::Result<()> {
        let full = root.join(path);
        match content {
            Some(c) => fs::write(full, c),
            None => remove_if_exists(&full),
        }
    };
    for_each_parallel(first, write)?;
    crash_point("mid-apply");
    for_each_parallel(second, write)
}

/// Every directory a sync must fsync for `files` to be durable by name: each
/// file's parent and that directory's ancestors, up to the store root.
fn dirs_to_sync(files: &[String]) -> BTreeSet<String> {
    let mut dirs = BTreeSet::new();
    dirs.insert(String::new());
    for f in files {
        let mut p = f.as_str();
        while let Some((dir, _)) = p.rsplit_once('/') {
            if !dirs.insert(dir.to_string()) {
                break;
            }
            p = dir;
        }
    }
    dirs
}

// ---------------------------------------------------------------------------
// The per-root handle: lock, log sequence, sync point
// ---------------------------------------------------------------------------

/// What a sync point covers: the files touched and the logs written since the
/// last one.
#[derive(Default)]
struct Unsynced {
    files: HashSet<String>,
    logs: Vec<PathBuf>,
    batches: u32,
}

struct SyncJob {
    files: Vec<String>,
    logs: Vec<PathBuf>,
    /// Not before this time (see `SYNC_DELAY`); `None` at shutdown and in
    /// recovery.
    not_before: Option<Instant>,
}

/// State shared by every [`FileStore`] on one root in this process.
struct WalState {
    next_seq: u64,
    unsynced: Unsynced,
    last_sync: Instant,
    /// Feeds the sync thread; at most `SYNC_QUEUE` jobs wait in the queue.
    jobs: Option<SyncSender<SyncJob>>,
    worker: Option<JoinHandle<()>>,
}

/// One store directory, held by this process: the OS lock on
/// `.kulyrax/lock`, and the batch logs written to it. Every adapter on the
/// same root in one process shares it (see [`acquire_root`]), so a sync point
/// covers every log on the root, oldest first.
struct Root {
    dir: PathBuf,
    /// Held for the lifetime of the handle; the OS releases the lock when it
    /// closes.
    _lock: File,
    wal: Mutex<WalState>,
    /// Set when applying a durable log or a background sync fails. From then
    /// on no log is deleted and every call fails, until a restart replays the
    /// logs still on disk.
    failed: Arc<Mutex<Option<String>>>,
    /// Background sync time not yet added to a run's stage timings.
    sync_us: Arc<AtomicU64>,
    sync_jobs: Arc<AtomicU64>,
    /// Set at shutdown: the sync thread stops waiting out `SYNC_DELAY`.
    hurry: Arc<AtomicBool>,
    /// The top-level directories each adapter on this root writes, by name
    /// lowercased, with the adapter kind and the name as written: two
    /// adapters must not share a directory, even one spelled differently
    /// only by case (one directory on a case-insensitive file system).
    claims: Mutex<HashMap<String, (&'static str, String)>>,
}

impl Root {
    fn claim(&self, kind: &'static str, names: &[String]) -> io::Result<()> {
        let mut claims = self
            .claims
            .lock()
            .map_err(|_| other("file store claims poisoned by an earlier panic".to_string()))?;
        // Checked against the root's earlier claims and against this
        // adapter's own names: `Users` and `users` are one directory on a
        // case-insensitive file system.
        let mut staged = claims.clone();
        let mut conflicts = Vec::new();
        for name in names {
            match staged.get(&name.to_lowercase()) {
                None => {}
                Some((k, n)) if *k == kind && n == name => {}
                Some((k, n)) => conflicts.push(format!(
                    "{kind} {name:?} and {k} {n:?} would share the directory {}",
                    self.dir.join(name).display()
                )),
            }
            staged.insert(name.to_lowercase(), (kind, name.clone()));
        }
        if !conflicts.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{}; give them different names", conflicts.join("; ")),
            ));
        }
        *claims = staged;
        Ok(())
    }

    fn wal_dir(&self) -> PathBuf {
        self.dir.join(DIR).join(WAL)
    }

    fn state(&self) -> io::Result<std::sync::MutexGuard<'_, WalState>> {
        self.wal
            .lock()
            .map_err(|_| other("file store lock poisoned by an earlier panic".to_string()))
    }

    fn check_failed(&self) -> io::Result<()> {
        let err = self.failed.lock().map(|e| e.clone()).unwrap_or(None);
        match err {
            Some(e) => Err(other(format!(
                "file store {} stopped after a failed write ({e}); restart to replay its batch logs",
                self.dir.display()
            ))),
            None => Ok(()),
        }
    }

    fn fail(&self, e: &io::Error) {
        if let Ok(mut slot) = self.failed.lock() {
            slot.get_or_insert_with(|| e.to_string());
        }
    }

    /// Write `pending` as the next batch log: temp file, fsync, rename, fsync
    /// the log directory. Returns once the log is durable.
    fn write_log(&self, state: &mut WalState, pending: &Pending) -> io::Result<PathBuf> {
        let seq = state.next_seq;
        state.next_seq += 1;
        let wal = self.wal_dir();
        let path = wal.join(log_name(seq));
        let tmp = wal.join(format!("{}.tmp", log_name(seq)));
        let mut f = File::create(&tmp)?;
        f.write_all(&encode_log(pending))?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &path)?;
        sync_dir(&wal)?;
        Ok(path)
    }

    /// Record a committed batch and start a sync point when one is due.
    fn committed(&self, state: &mut WalState, log: PathBuf, pending: &Pending) -> io::Result<()> {
        let u = &mut state.unsynced;
        u.logs.push(log);
        u.files.extend(pending.files.keys().cloned());
        u.batches += 1;
        if u.batches >= SYNC_EVERY_BATCHES || state.last_sync.elapsed() >= SYNC_INTERVAL {
            self.start_sync(state, Some(Instant::now() + SYNC_DELAY))?;
        }
        Ok(())
    }

    /// Hand everything unsynced to the sync thread, to run no earlier than
    /// `not_before`. Blocks while `SYNC_QUEUE` jobs are already waiting.
    fn start_sync(&self, state: &mut WalState, not_before: Option<Instant>) -> io::Result<()> {
        let u = std::mem::take(&mut state.unsynced);
        state.last_sync = Instant::now();
        if u.logs.is_empty() {
            return Ok(());
        }
        let job = SyncJob {
            files: u.files.into_iter().collect(),
            logs: u.logs,
            not_before,
        };
        let tx = match &state.jobs {
            Some(tx) => tx.clone(),
            None => {
                let (tx, rx) = mpsc::sync_channel(SYNC_QUEUE);
                state.worker = Some(self.spawn_worker(rx)?);
                state.jobs = Some(tx.clone());
                tx
            }
        };
        match tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(job)) => tx
                .send(job)
                .map_err(|_| other("file store sync thread stopped".to_string())),
            Err(TrySendError::Disconnected(_)) => {
                Err(other("file store sync thread stopped".to_string()))
            }
        }
    }

    fn spawn_worker(&self, rx: Receiver<SyncJob>) -> io::Result<JoinHandle<()>> {
        let dir = self.dir.clone();
        let error = Arc::clone(&self.failed);
        let (us, jobs) = (Arc::clone(&self.sync_us), Arc::clone(&self.sync_jobs));
        let hurry = Arc::clone(&self.hurry);
        std::thread::Builder::new()
            .name("kulyrax-file-sync".to_string())
            .spawn(move || {
                for job in rx {
                    if let Some(t) = job.not_before {
                        while !hurry.load(Ordering::Relaxed) && Instant::now() < t {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                    }
                    if error.lock().map(|e| e.is_some()).unwrap_or(true) {
                        continue;
                    }
                    let start = Instant::now();
                    if let Err(e) = run_sync(&dir, &job)
                        && let Ok(mut slot) = error.lock()
                    {
                        slot.get_or_insert_with(|| e.to_string());
                    }
                    us.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                    jobs.fetch_add(1, Ordering::Relaxed);
                }
            })
    }

    /// Add the background sync time finished since the last call to this
    /// thread's stage timings.
    fn drain_sync_timings(&self) {
        let us = self.sync_us.swap(0, Ordering::Relaxed);
        let jobs = self.sync_jobs.swap(0, Ordering::Relaxed);
        if jobs > 0 {
            stages::record(Stage::Sync, jobs, Duration::from_micros(us));
        }
    }
}

/// A sync point: fsync each touched file, then the directories holding them,
/// then delete the logs, oldest first. A log is deleted only once everything
/// it wrote is durable; deleting a newer log while an older one remains would
/// let recovery replay stale content over newer state.
fn run_sync(root: &Path, job: &SyncJob) -> io::Result<()> {
    crash_point("before-sync");
    for_each_parallel(&job.files, |f| sync_file(&root.join(f)))?;
    // Windows: `sync_file` deferred the device cache flush. One
    // `FlushFileBuffers` sends it for the whole volume, so every file above
    // is durable before any log is deleted. NTFS sends it even for a file
    // with nothing left to write (measured: 48 ms on a clean file).
    #[cfg(windows)]
    if let Some(newest) = job.logs.last() {
        OpenOptions::new().write(true).open(newest)?.sync_all()?;
    }
    if cfg!(unix) {
        let dirs: Vec<String> = dirs_to_sync(&job.files).into_iter().collect();
        for_each_parallel(&dirs, |d| sync_dir(&root.join(d)))?;
    }
    for (i, log) in job.logs.iter().enumerate() {
        remove_if_exists(log)?;
        if i == 0 {
            crash_point("mid-sync");
        }
    }
    if let Some(wal) = job.logs.first().and_then(|l| l.parent()) {
        sync_dir(wal)?;
    }
    Ok(())
}

impl Drop for Root {
    /// A clean shutdown syncs everything and deletes the logs, so the next
    /// start has nothing to replay. Errors are left to recovery.
    fn drop(&mut self) {
        let Ok(mut state) = self.wal.lock() else {
            return;
        };
        self.hurry.store(true, Ordering::Relaxed);
        let _ = self.start_sync(&mut state, None);
        state.jobs = None;
        if let Some(worker) = state.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Every root this process holds, so adapters on the same root share one
/// [`Root`]. `kulyrax reload` prepares the new adapters while the old ones
/// are alive: they must join the old handle, not fail on its lock.
static ROOTS: Mutex<BTreeMap<PathBuf, Weak<Root>>> = Mutex::new(BTreeMap::new());

/// The [`Root`] for `dir`, and whether this call took the OS lock (so this
/// process has not been writing to the root, and leftover logs must be
/// replayed).
fn acquire_root(dir: &Path) -> io::Result<(Arc<Root>, bool)> {
    let internal = dir.join(DIR);
    fs::create_dir_all(internal.join(WAL))?;
    let key = fs::canonicalize(dir)?;
    let mut roots = ROOTS
        .lock()
        .map_err(|_| other("file store registry poisoned by an earlier panic".to_string()))?;
    if let Some(root) = roots.get(&key).and_then(Weak::upgrade) {
        return Ok((root, false));
    }
    let lock = take_lock(dir, &internal.join(LOCK), true)?;
    let root = Arc::new(Root {
        dir: dir.to_path_buf(),
        _lock: lock,
        wal: Mutex::new(WalState {
            next_seq: 1,
            unsynced: Unsynced::default(),
            last_sync: Instant::now(),
            jobs: None,
            worker: None,
        }),
        failed: Arc::new(Mutex::new(None)),
        sync_us: Arc::new(AtomicU64::new(0)),
        sync_jobs: Arc::new(AtomicU64::new(0)),
        hurry: Arc::new(AtomicBool::new(false)),
        claims: Mutex::new(HashMap::new()),
    });
    roots.retain(|_, w| w.strong_count() > 0);
    roots.insert(key, Arc::downgrade(&root));
    Ok((root, true))
}

/// Open `lock_path` and take its OS lock, or say which process-level lock is
/// held. `create`: make the file if it is missing.
pub(crate) fn take_lock(dir: &Path, lock_path: &Path, create: bool) -> io::Result<File> {
    let lock = OpenOptions::new()
        .create(create)
        .truncate(false)
        .write(true)
        .open(lock_path)?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(fs::TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "file store {} is in use by another process (lock {} is held); \
                 only one Kulyrax process may write to a file store",
                dir.display(),
                lock_path.display()
            ),
        )),
        Err(fs::TryLockError::Error(e)) => Err(e),
    }
}

/// Replay every batch log under the root, oldest first, then fsync what they
/// touched and delete them. Returns the next free log sequence number.
fn recover(root: &Root, known_dirs: &mut HashSet<String>) -> io::Result<u64> {
    replay_logs(&root.dir, &root.wal_dir(), known_dirs)
}

/// Replay every batch log in `wal`, oldest first, applying it under `dir`
/// (log entries name paths relative to the store root), then fsync what they
/// touched and delete them. Returns the next free log sequence number.
pub(crate) fn replay_logs(
    dir: &Path,
    wal: &Path,
    known_dirs: &mut HashSet<String>,
) -> io::Result<u64> {
    let logs = list_logs(wal)?;
    let next_seq = logs.last().map_or(1, |(seq, _)| seq + 1);
    let mut decoded = Vec::with_capacity(logs.len());
    let mut valid = Vec::with_capacity(logs.len());
    for (i, (_, path)) in logs.iter().enumerate() {
        match decode_log(&fs::read(path)?) {
            Some(p) => {
                decoded.push(p);
                valid.push(path.clone());
            }
            // The newest log torn: its batch never finished committing, so
            // the source checkpoint never moved past it. Its events are read
            // again from the source.
            None if i + 1 == logs.len() => remove_if_exists(path)?,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "batch log {} is corrupt and newer batch logs exist; \
                         a crash cannot cause this, so the logs are left for inspection",
                        path.display()
                    ),
                ));
            }
        }
    }
    let mut touched = HashSet::new();
    for pending in &decoded {
        apply(dir, pending, known_dirs)?;
        touched.extend(pending.files.keys().cloned());
    }
    if !decoded.is_empty() {
        let job = SyncJob {
            files: touched.into_iter().collect(),
            logs: valid,
            not_before: None,
        };
        run_sync(dir, &job)?;
    }
    Ok(next_seq)
}

// ---------------------------------------------------------------------------
// FileStore: what an adapter holds
// ---------------------------------------------------------------------------

struct StoreState {
    root: Option<Arc<Root>>,
    /// The open batch, between `begin` and `commit`/`rollback`.
    batch: Option<Pending>,
    /// `.kulyrax/` files as they are on disk (`None`: absent).
    cache: HashMap<String, Option<String>>,
    /// Every `.kulyrax/` file this store reads is in `cache`, so a miss means
    /// absent. True when the store was empty at `prepare`.
    cache_complete: bool,
    known_dirs: HashSet<String>,
}

/// What one file adapter keeps under a store root, checked when the store
/// is prepared, before anything is read or written:
/// - every mapping name it puts in a path is a plain directory name
///   ([`file_names::check_config_name`]);
/// - its top-level directories are its own on the root ([`Root::claim`]);
/// - until it has passed once (a marker under `.kulyrax/layout/`),
///   `upgrade` moves an older layout into place and lists the files an older
///   scheme wrote that this one cannot read. Any listed file refuses the
///   start: re-keying it silently would lose its guard.
pub struct Layout {
    /// The adapter kind, naming its marker: `kv`, `document` or `graph`.
    pub kind: &'static str,
    /// Its top-level directories under the root.
    pub claims: Vec<String>,
    /// The mapping names it puts in paths: (what, name).
    pub names: Vec<(&'static str, String)>,
    /// Runs on the store root while the store's lock is held.
    #[allow(clippy::type_complexity)]
    pub upgrade: Box<dyn Fn(&Path) -> io::Result<Vec<String>> + Send + Sync>,
}

/// The durable write path shared by the file-backed adapters. See the module
/// docs.
pub struct FileStore {
    dir: PathBuf,
    layout: Option<Layout>,
    state: Mutex<StoreState>,
}

/// At most this many refused paths are named in the error.
const SHOW_REFUSED: usize = 20;

impl FileStore {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            layout: None,
            state: Mutex::new(StoreState {
                root: None,
                batch: None,
                cache: HashMap::new(),
                cache_complete: false,
                known_dirs: HashSet::new(),
            }),
        }
    }

    /// A store whose adapter's [`Layout`] is checked when it is prepared.
    pub fn with_layout(dir: PathBuf, layout: Layout) -> Self {
        Self {
            layout: Some(layout),
            ..Self::new(dir)
        }
    }

    /// The checks of [`Layout`], after the store's lock is taken and its
    /// batch logs are replayed.
    fn check_layout(&self, root: &Root) -> io::Result<()> {
        let Some(layout) = &self.layout else {
            return Ok(());
        };
        root.claim(layout.kind, &layout.claims)?;
        let marker = self.dir.join(DIR).join(LAYOUT).join(layout.kind);
        if marker.exists() {
            return Ok(());
        }
        let refused = (layout.upgrade)(&self.dir)?;
        if !refused.is_empty() {
            let shown: Vec<&str> = refused
                .iter()
                .take(SHOW_REFUSED)
                .map(String::as_str)
                .collect();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "file store {} holds {} path(s) this version cannot open as they are: \
                     files an earlier version named from ids this version names differently \
                     (uppercase letters, `/`, a leading or trailing `.`, other special \
                     characters, ids too long for a file name), or a guard under both the old \
                     and the new layout: {}{}. \
                     Rebuild the store: move it aside and project the full event history \
                     into an empty directory",
                    self.dir.display(),
                    refused.len(),
                    shown.join(", "),
                    if refused.len() > shown.len() {
                        ", …"
                    } else {
                        ""
                    }
                ),
            ));
        }
        if let Some(dir) = marker.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut f = File::create(&marker)?;
        f.write_all(b"1\n")?;
        f.sync_all()?;
        if let Some(dir) = marker.parent() {
            sync_dir(dir)?;
        }
        Ok(())
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, StoreState>, AdapterError> {
        self.state.lock().map_err(|_| {
            AdapterError::WriteFailed("file store state poisoned by an earlier panic".to_string())
        })
    }

    /// Take the store's lock and replay any batch log left by a crash. Safe
    /// to call again.
    pub fn prepare(&self) -> Result<(), AdapterError> {
        let mut st = self.state()?;
        self.prepare_locked(&mut st)
    }

    fn prepare_locked(&self, st: &mut StoreState) -> Result<(), AdapterError> {
        if st.root.is_some() {
            return Ok(());
        }
        if let Some(layout) = &self.layout {
            let bad: Vec<String> = layout
                .names
                .iter()
                .filter_map(|(what, name)| file_names::check_config_name(what, name).err())
                .collect();
            if !bad.is_empty() {
                return Err(AdapterError::WriteFailed(bad.join("; ")));
            }
        }
        let (root, fresh) = acquire_root(&self.dir).map_err(io_err)?;
        if fresh {
            crate::adapter::legacy::migrate_file_store(&self.dir, &mut st.known_dirs)
                .map_err(io_err)?;
            let next_seq = recover(&root, &mut st.known_dirs).map_err(io_err)?;
            root.state().map_err(io_err)?.next_seq = next_seq;
        }
        // After recovery (logs replay to the paths they were written with),
        // and before emptiness is judged (an upgrade may move files).
        self.check_layout(&root).map_err(io_err)?;
        st.cache_complete = store_is_empty(&self.dir).map_err(io_err)?;
        st.root = Some(root);
        Ok(())
    }

    /// The store's root, once it is prepared, unless it has stopped on a
    /// failure. A store used without `prepare` (code that embeds an adapter
    /// and calls `handle` directly, as file stores always allowed) prepares
    /// itself here, before its first write.
    fn ready<'a>(&self, st: &'a mut StoreState) -> Result<&'a Arc<Root>, AdapterError> {
        self.prepare_locked(st)?;
        let Some(root) = st.root.as_ref() else {
            return Err(AdapterError::WriteFailed(
                "file store not prepared".to_string(),
            ));
        };
        root.check_failed().map_err(io_err)?;
        Ok(root)
    }

    pub fn begin(&self) -> Result<(), AdapterError> {
        let mut st = self.state()?;
        self.ready(&mut st)?;
        if st.batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        st.batch = Some(Pending::default());
        Ok(())
    }

    pub fn commit(&self) -> Result<(), AdapterError> {
        let mut st = self.state()?;
        let Some(pending) = st.batch.take() else {
            return Ok(());
        };
        self.commit_pending(&mut st, pending)
    }

    pub fn rollback(&self) {
        if let Ok(mut st) = self.state() {
            st.batch = None;
        }
    }

    /// Run one event's reads and writes in `f`. Its writes join the open
    /// batch if `f` succeeds and are discarded if it fails. Without an open
    /// batch they are committed on their own, as a batch of one.
    pub fn with_txn<T, E>(
        &self,
        f: impl FnOnce(&mut FileTxn<'_>) -> Result<T, E>,
    ) -> Result<Result<T, E>, AdapterError> {
        let mut st = self.state()?;
        self.ready(&mut st)?;
        let StoreState {
            batch,
            cache,
            cache_complete,
            ..
        } = &mut *st;
        let mut txn = FileTxn {
            root: &self.dir,
            batch: batch.as_ref(),
            cache,
            cache_complete: *cache_complete,
            local: Pending::default(),
        };
        let out = f(&mut txn);
        let local = txn.local;
        if out.is_err() || local.is_empty() {
            return Ok(out);
        }
        match st.batch.as_mut() {
            Some(batch) => batch.merge(local),
            None => self.commit_pending(&mut st, local)?,
        }
        Ok(out)
    }

    fn commit_pending(&self, st: &mut StoreState, pending: Pending) -> Result<(), AdapterError> {
        if pending.is_empty() {
            return Ok(());
        }
        check_paths(&pending)?;
        let root = Arc::clone(self.ready(st)?);
        let mut wal = root.state().map_err(io_err)?;
        let log = timed(Stage::Log, || root.write_log(&mut wal, &pending)).map_err(io_err)?;
        crash_point("after-log");
        // From here the batch is committed. If applying it fails, the store
        // stops before any sync can delete its log, and the next start
        // replays it.
        if let Err(e) = timed(Stage::Apply, || {
            apply(&self.dir, &pending, &mut st.known_dirs)
        }) {
            root.fail(&e);
            return Err(io_err(e));
        }
        let recorded = root.committed(&mut wal, log, &pending);
        drop(wal);
        root.drain_sync_timings();
        if st.cache.len() + pending.files.len() > CACHE_LIMIT {
            st.cache.clear();
            st.cache_complete = false;
        }
        for (path, content) in pending.files {
            if is_cached(&path) {
                st.cache.insert(path, content);
            }
        }
        crash_point("before-checkpoint");
        recorded.map_err(io_err)
    }
}

/// Refuse a batch that writes under `.kulyrax/` outside the store's own
/// entries: `control` and `state` belong to other owners (see [`kulyrax_dir`]).
fn check_paths(pending: &Pending) -> Result<(), AdapterError> {
    let prefix = format!("{DIR}/");
    let bad: Vec<&str> = pending
        .files
        .keys()
        .map(String::as_str)
        .filter(|p| (*p == DIR || p.starts_with(&prefix)) && !kulyrax_dir::is_store_path(p))
        .collect();
    if bad.is_empty() {
        return Ok(());
    }
    Err(AdapterError::WriteFailed(format!(
        "file store refused to write {}: under {DIR}/ a file store writes only its own \
         entries ({})",
        bad.join(", "),
        kulyrax_dir::STORE_ENTRIES.join(", ")
    )))
}

/// True when no guard has ever been written here: none of the store's guard
/// entries exists under `.kulyrax/`. Only the store's own entries are looked
/// at; `.kulyrax/` may hold other owners' entries (see [`kulyrax_dir`]).
fn store_is_empty(dir: &Path) -> io::Result<bool> {
    let internal = dir.join(DIR);
    for entry in kulyrax_dir::STORE_ENTRIES {
        if [WAL, LOCK, LAYOUT].contains(entry) {
            continue;
        }
        match fs::symlink_metadata(internal.join(entry)) {
            Ok(_) => return Ok(false),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// One event's view of the store: its own writes, then the open batch's, then
/// the cache, then disk.
pub struct FileTxn<'a> {
    root: &'a Path,
    batch: Option<&'a Pending>,
    cache: &'a mut HashMap<String, Option<String>>,
    cache_complete: bool,
    local: Pending,
}

impl FileTxn<'_> {
    /// The file at `path` (relative to the store root, `/`-separated), or
    /// `None` if it does not exist.
    pub fn read(&mut self, path: &str) -> io::Result<Option<String>> {
        if let Some(c) = self.local.files.get(path) {
            return Ok(c.clone());
        }
        if let Some(c) = self.batch.and_then(|b| b.files.get(path)) {
            return Ok(c.clone());
        }
        let cached = is_cached(path);
        if cached {
            if let Some(c) = self.cache.get(path) {
                return Ok(c.clone());
            }
            if self.cache_complete {
                return Ok(None);
            }
        }
        let content = match fs::read_to_string(self.root.join(path)) {
            Ok(s) => Some(s),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        if cached && self.cache.len() < CACHE_LIMIT {
            self.cache.insert(path.to_string(), content.clone());
        }
        Ok(content)
    }

    /// Replace the whole file at `path`.
    pub fn write(&mut self, path: &str, content: String) {
        self.local.set(path, Some(content));
    }

    /// Remove the file at `path`, if any.
    pub fn remove(&mut self, path: &str) {
        self.local.set(path, None);
    }

    /// The names of `dir`'s child directories, on disk or pending.
    pub fn child_dirs(&self, dir: &str) -> io::Result<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        match fs::read_dir(self.root.join(dir)) {
            Ok(entries) => {
                for e in entries {
                    let e = e?;
                    if e.file_type()?.is_dir()
                        && let Ok(name) = e.file_name().into_string()
                    {
                        out.insert(name);
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if let Some(b) = self.batch {
            b.child_dirs(dir, &mut out);
        }
        self.local.child_dirs(dir, &mut out);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(entries: &[(&str, Option<&str>)]) -> Pending {
        let mut p = Pending::default();
        for (path, c) in entries {
            p.set(path, c.map(str::to_string));
        }
        p
    }

    /// `.kulyrax/control` and `.kulyrax/state` belong to `kulyrax reload` and
    /// the checkpoints: a store never writes there, and nothing is written
    /// when a batch tries.
    #[test]
    fn a_store_never_writes_another_owner_s_entries() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = FileStore::new(dir.path().to_path_buf());
        for path in [
            ".kulyrax/state/events.json",
            ".kulyrax/control/reload-request",
            ".kulyrax/unknown",
            ".kulyrax",
        ] {
            let err = store
                .with_txn(|t| {
                    t.write(path, "x".to_string());
                    Ok::<(), ()>(())
                })
                .expect_err(path);
            assert!(
                err.to_string().contains("refused to write"),
                "{path}: {err}"
            );
            assert!(!dir.path().join(path).is_file(), "{path} written");
        }
        store
            .with_txn(|t| {
                t.write(".kulyrax/kv/users/u1.guard.json", "{}".to_string());
                t.write("users/u1.json", "{}".to_string());
                Ok::<(), ()>(())
            })
            .expect("own entries")
            .expect("own entries");
    }

    #[test]
    fn log_round_trips() {
        let p = pending(&[
            ("a/b.json", Some("{\"x\":1}")),
            ("c.json", None),
            ("e/f", Some("")),
        ]);
        let back = decode_log(&encode_log(&p)).expect("valid log");
        assert_eq!(back.files, p.files);
        assert_eq!(back.dirs, p.dirs);
    }

    #[test]
    fn torn_or_corrupt_logs_are_rejected() {
        let bytes = encode_log(&pending(&[("a/b.json", Some("hello")), ("c", None)]));
        for n in 0..bytes.len() {
            assert!(
                decode_log(&bytes[..n]).is_none(),
                "truncated at {n} accepted"
            );
        }
        let mut flipped = bytes.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 1;
        assert!(decode_log(&flipped).is_none());
        let mut longer = bytes;
        longer.push(0);
        assert!(decode_log(&longer).is_none());
    }

    #[test]
    fn child_dirs_sees_pending_directories() {
        let p = pending(&[
            ("g/nodes/User/1.json", Some("{}")),
            ("g/nodes/Org/2.json", None),
        ]);
        let mut out = BTreeSet::new();
        p.child_dirs("g/nodes", &mut out);
        assert_eq!(out.into_iter().collect::<Vec<_>>(), ["Org", "User"]);
    }

    #[test]
    fn dirs_to_sync_covers_every_ancestor() {
        let dirs = dirs_to_sync(&["a/b/c.json".to_string(), "a/d.json".to_string()]);
        assert_eq!(dirs.into_iter().collect::<Vec<_>>(), ["", "a", "a/b"]);
    }
}
