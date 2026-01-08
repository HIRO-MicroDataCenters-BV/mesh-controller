//! Co-locating all of the iroh metrics structs
#[cfg(feature = "test-utils")]
pub use iroh_relay::server::Metrics as RelayMetrics;
#[cfg(not(wasm_browser))]
pub use portmapper::Metrics as PortmapMetrics;

pub use crate::{magicsock::Metrics as MagicsockMetrics, net_report::Metrics as NetReportMetrics};

use iroh_metrics::{
    core::{Counter, Metric},
    struct_iterable::Iterable,
};

#[allow(missing_docs)]
#[derive(Debug, Clone, Iterable)]
#[non_exhaustive]
pub struct ConnectionMetrics {
    pub connections_created: Counter,
    pub connections_dropped: Counter,
}

impl Default for ConnectionMetrics {
    fn default() -> Self {
        Self {
            connections_created: Counter::new("created"),
            connections_dropped: Counter::new("dropped"),
        }
    }
}

impl Metric for ConnectionMetrics {
    fn name() -> &'static str {
        "connections"
    }
}
