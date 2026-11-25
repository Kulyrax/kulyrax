//! Crash points for Kulyrax's own crash tests. Not API.
//!
//! With the `crash-points` feature, `KULYRAX_CRASH_AT=<point>:<n>` aborts the
//! process the n-th time it reaches `point`, simulating a crash there. Only
//! test builds enable the feature (`kulyrax-cli`'s crash tests, and backend
//! crates through their own `crash-points` feature); without it
//! [`crash_point`] is empty.

/// Abort here if `KULYRAX_CRASH_AT` names this point and this is its n-th hit.
#[cfg(feature = "crash-points")]
pub fn crash_point(point: &str) {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU32, Ordering};
    static TARGET: OnceLock<Option<(String, u32)>> = OnceLock::new();
    static HITS: AtomicU32 = AtomicU32::new(0);
    let target = TARGET.get_or_init(|| {
        let spec = std::env::var("KULYRAX_CRASH_AT").ok()?;
        let (p, n) = spec.split_once(':')?;
        Some((p.to_string(), n.parse().ok()?))
    });
    if let Some((p, n)) = target
        && p == point
        && HITS.fetch_add(1, Ordering::SeqCst) + 1 == *n
    {
        eprintln!("crash point {point} reached, aborting");
        std::process::abort();
    }
}

/// Without the `crash-points` feature: nothing.
#[cfg(not(feature = "crash-points"))]
#[inline(always)]
pub fn crash_point(_point: &str) {}

/// With the `crash-points` feature, the value of the environment variable
/// `KULYRAX_TEST_<name>`, so a crash test can shrink a batch size to reach a
/// crash point between batches. Without the feature: always `None`.
pub fn test_knob(name: &str) -> Option<String> {
    #[cfg(feature = "crash-points")]
    {
        std::env::var(format!("KULYRAX_TEST_{name}")).ok()
    }
    #[cfg(not(feature = "crash-points"))]
    {
        let _ = name;
        None
    }
}
