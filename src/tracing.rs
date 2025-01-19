//! This module contains helper methods to set up tracing.
use tracing_subscriber::EnvFilter;

/// Constructs a tracing subscriber that prints formatted traces to stdout.
/// Default level is `error` but can be configured via the `RUST_LOG` environment variable.
/// Registers that subscriber to process traces emitted after this point.
pub fn setup() {
    let filter = EnvFilter::from_default_env();
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    tracing::info!("Tracing initialized");
}