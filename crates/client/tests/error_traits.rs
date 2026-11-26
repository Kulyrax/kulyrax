//! The client's errors are `std::error::Error + Send + Sync + 'static`
//! (decision 2026-11-26-error-chains).

use kulyrax_client::{ClientError, RpcError};

fn is_error<E: std::error::Error + Send + Sync + 'static>() {}

#[test]
fn every_public_error_is_send_sync_static() {
    is_error::<ClientError>();
    is_error::<RpcError>();
}
