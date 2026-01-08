use iroh_metrics::{
    core::{Counter, Metric},
    struct_iterable::Iterable,
};

#[allow(missing_docs)]
#[derive(Debug, Clone, Iterable)]
#[non_exhaustive]
pub struct QuinnConnectionMetrics {
    pub connections_created: Counter,
    pub connections_dropped: Counter,
}

impl Default for QuinnConnectionMetrics {
    fn default() -> Self {
        Self {
            connections_created: Counter::new("created"),
            connections_dropped: Counter::new("dropped"),
        }
    }
}

impl Metric for QuinnConnectionMetrics {
    fn name() -> &'static str {
        "quinn"
    }
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Iterable)]
#[non_exhaustive]
pub struct ConnectionDriverMetrics {
    pub connections_created: Counter,
    pub connections_dropped: Counter,
}

impl Default for ConnectionDriverMetrics {
    fn default() -> Self {
        Self {
            connections_created: Counter::new("created"),
            connections_dropped: Counter::new("dropped"),
        }
    }
}

impl Metric for ConnectionDriverMetrics {
    fn name() -> &'static str {
        "connection_driver"
    }
}
