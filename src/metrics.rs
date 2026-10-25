//! Prometheus metrics and health check exporter for RustDesk Server (`hbbs` and `hbbr`).
//!
//! Provides zero-lock atomic metrics counters and an embedded Axum HTTP server
//! exposing `/metrics` (OpenMetrics format) and `/health` / `/livez` endpoints.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use lazy_static::lazy_static;
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Instant,
};

lazy_static! {
    static ref SERVER_START_TIME: Instant = Instant::now();
}

// ========================================================
// ZERO-LOCK ATOMIC COUNTERS (High Performance / No Lock Contention)
// ========================================================
pub static ACTIVE_PEERS: AtomicUsize = AtomicUsize::new(0);
pub static TOTAL_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_DISCONNECTIONS: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_BYTES_RELAYED: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_HEARTBEATS: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_PACKETS_DROPPED: AtomicU64 = AtomicU64::new(0);

// ========================================================
// ATOMIC COUNTER HELPERS
// ========================================================

/// Increments the active peers gauge and total connections counter.
#[inline]
pub fn inc_active_peers() {
    ACTIVE_PEERS.fetch_add(1, Ordering::Relaxed);
    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Decrements the active peers gauge and increments total disconnections counter.
#[inline]
pub fn dec_active_peers() {
    ACTIVE_PEERS.fetch_sub(1, Ordering::Relaxed);
    TOTAL_DISCONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Adds relayed payload size in bytes to the total forwarded bytes counter.
#[inline]
pub fn add_relayed_bytes(bytes: usize) {
    TOTAL_BYTES_RELAYED.fetch_add(bytes as u64, Ordering::Relaxed);
}

/// Increments total heartbeat packets received.
#[inline]
pub fn inc_heartbeat() {
    TOTAL_HEARTBEATS.fetch_add(1, Ordering::Relaxed);
}

/// Increments dropped / unroutable packets counter.
#[inline]
pub fn inc_packet_dropped() {
    TOTAL_PACKETS_DROPPED.fetch_add(1, Ordering::Relaxed);
}

/// RAII guard ensuring active peer gauge is safely decremented when a session drops or terminates.
pub struct ActivePeerGuard;

impl ActivePeerGuard {
    pub fn new() -> Self {
        inc_active_peers();
        Self
    }
}

impl Default for ActivePeerGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ActivePeerGuard {
    fn drop(&mut self) {
        dec_active_peers();
    }
}

// ========================================================
// PROMETHEUS / OPENMETRICS EXPOSITION FORMAT
// ========================================================

/// Formats the current in-memory counters into OpenMetrics text format.
pub fn generate_prometheus_metrics(service_name: &str) -> String {
    let uptime = SERVER_START_TIME.elapsed().as_secs();
    let active = ACTIVE_PEERS.load(Ordering::Relaxed);
    let total_conn = TOTAL_CONNECTIONS.load(Ordering::Relaxed);
    let total_disc = TOTAL_DISCONNECTIONS.load(Ordering::Relaxed);
    let total_bytes = TOTAL_BYTES_RELAYED.load(Ordering::Relaxed);
    let heartbeats = TOTAL_HEARTBEATS.load(Ordering::Relaxed);
    let drops = TOTAL_PACKETS_DROPPED.load(Ordering::Relaxed);

    format!(
        "# HELP rustdesk_server_uptime_seconds Total server uptime in seconds.\n\
         # TYPE rustdesk_server_uptime_seconds counter\n\
         rustdesk_server_uptime_seconds{{service=\"{service_name}\"}} {uptime}\n\n\
         # HELP rustdesk_active_peers Current active peer sessions.\n\
         # TYPE rustdesk_active_peers gauge\n\
         rustdesk_active_peers{{service=\"{service_name}\"}} {active}\n\n\
         # HELP rustdesk_connections_total Total connections accepted.\n\
         # TYPE rustdesk_connections_total counter\n\
         rustdesk_connections_total{{service=\"{service_name}\"}} {total_conn}\n\n\
         # HELP rustdesk_disconnections_total Total connections closed.\n\
         # TYPE rustdesk_disconnections_total counter\n\
         rustdesk_disconnections_total{{service=\"{service_name}\"}} {total_disc}\n\n\
         # HELP rustdesk_bytes_relayed_total Total bytes forwarded by relay.\n\
         # TYPE rustdesk_bytes_relayed_total counter\n\
         rustdesk_bytes_relayed_total{{service=\"{service_name}\"}} {total_bytes}\n\n\
         # HELP rustdesk_heartbeats_total Total heartbeat pings processed.\n\
         # TYPE rustdesk_heartbeats_total counter\n\
         rustdesk_heartbeats_total{{service=\"{service_name}\"}} {heartbeats}\n\n\
         # HELP rustdesk_packet_drops_total Total unroutable or dropped packets.\n\
         # TYPE rustdesk_packet_drops_total counter\n\
         rustdesk_packet_drops_total{{service=\"{service_name}\"}} {drops}\n",
        service_name = service_name,
        uptime = uptime,
        active = active,
        total_conn = total_conn,
        total_disc = total_disc,
        total_bytes = total_bytes,
        heartbeats = heartbeats,
        drops = drops
    )
}

// ========================================================
// HTTP ROUTE HANDLERS
// ========================================================

async fn metrics_handler(service_name: String) -> Response {
    let body = generate_prometheus_metrics(&service_name);
    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn health_handler() -> Response {
    (
        StatusCode::OK,
        [("Content-Type", "application/json")],
        r#"{"status":"healthy","service":"rustdesk-server"}"#,
    )
        .into_response()
}

// ========================================================
// ASYNC HTTP SERVER LAUNCHER
// ========================================================

/// Spawns an asynchronous Axum HTTP server on the specified port.
pub async fn start_metrics_server(port: u16, service_name: &'static str) {
    let app = Router::new()
        .route(
            "/metrics",
            get({
                let svc = service_name.to_string();
                move || metrics_handler(svc)
            }),
        )
        .route("/health", get(health_handler))
        .route("/livez", get(health_handler));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    hbb_common::log::info!("Prometheus metrics server listening on http://{addr}/metrics");

    hbb_common::tokio::spawn(async move {
        if let Err(e) = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
        {
            hbb_common::log::error!("Prometheus metrics server failed on port {port}: {e}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_formatting_and_atomics() {
        let initial_active = ACTIVE_PEERS.load(Ordering::Relaxed);
        {
            let _guard = ActivePeerGuard::new();
            assert_eq!(ACTIVE_PEERS.load(Ordering::Relaxed), initial_active + 1);
            add_relayed_bytes(1024);
            inc_heartbeat();

            let output = generate_prometheus_metrics("test_service");
            assert!(output.contains("rustdesk_active_peers{service=\"test_service\"}"));
            assert!(output.contains("rustdesk_bytes_relayed_total{service=\"test_service\"}"));
            assert!(output.contains("rustdesk_heartbeats_total{service=\"test_service\"}"));
            assert!(output.contains("rustdesk_server_uptime_seconds{service=\"test_service\"}"));
        }
        assert_eq!(ACTIVE_PEERS.load(Ordering::Relaxed), initial_active);
    }
}
