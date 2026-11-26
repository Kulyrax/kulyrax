/// The document error type.
pub mod adapter;
pub mod exec;
/// The file-backed document store (`type: file`).
pub mod file;
pub(crate) mod loader;
/// The document mapping format.
pub mod mapping;
/// Resolving a document mapping against an event.
pub mod runtime;

// The extension surface for a new document backend (Phase 25.1) is `exec`:
// its traits, plans and guards, and the functions that drive them. 0.12 also
// re-exported its types here; since 1.0 each has one path (decision
// 2026-11-26-canonical-paths).
