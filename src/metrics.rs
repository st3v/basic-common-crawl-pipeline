//! This module contains methods to:
//! * set up metrics endpoint for Prometheus
//! * maintain metric for filtered docs
use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec};

lazy_static! {
    static ref DOCS_FILTERED: IntCounterVec = register_int_counter_vec!(
        "docs_filtered",
        "Number of filtered docs",
        &["included", "excluded"],
    )
    .unwrap();

    static ref DOCS_STORED: IntCounter = register_int_counter!(
        "docs_stored",
        "Number of docs uploaded to object store",
    )
    .unwrap();
}

// Increment metrics counter for filtered docs. Takes into account whether the
// doc was dropped or not. 
pub fn doc_filtered(dropped: bool) {
    DOCS_FILTERED.with_label_values(&[&*(!dropped).to_string(), &*dropped.to_string()]).inc();
}

// Increment metrics counter for stored docs
pub fn doc_stored() {
    DOCS_STORED.inc();
}

// A small API server that exposes metrics on `/metrics` using `axum`.
pub async fn run_server(port: u16) {
    async fn metrics() -> String {
        let encoder = prometheus::TextEncoder::new();
        let metrics = prometheus::gather();
        encoder.encode_to_string(&metrics).unwrap()
    }

    let app = axum::Router::new().route("/metrics", axum::routing::get(metrics));
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    tracing::info!("Starting metrics server on port {}", port);
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap()
}