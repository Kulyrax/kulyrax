//! The ingest crate's errors are `std::error::Error + Send + Sync + 'static`
//! and chain what they hold (decision 2026-11-26-error-chains).

use std::error::Error;

use kulyrax_ingest::{IngestError, ServeError};

fn is_error<E: Error + Send + Sync + 'static>() {}

#[test]
fn every_public_error_is_send_sync_static() {
    is_error::<IngestError>();
    is_error::<ServeError>();
}

#[test]
fn a_serve_error_chains_to_what_it_holds() {
    let io = std::io::Error::other("runtime failed");
    let err = ServeError::Ingest(IngestError::Runtime(io));
    let ingest = err
        .source()
        .and_then(|e| e.downcast_ref::<IngestError>())
        .expect("an IngestError");
    let cause = ingest.source().expect("a source");
    assert!(
        cause.downcast_ref::<std::io::Error>().is_some(),
        "{cause:?}"
    );
}
