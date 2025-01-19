//! This module contains a method to set up a metrics endpoint for Prometheus.

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