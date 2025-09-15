use axum_prometheus::metrics;

// Labels
pub const LABEL_MSG_TYPE: &str = "msg_type";
pub const LABEL_MSG_TYPE_SNAPSHOT: &str = "snapshot";
pub const LABEL_MSG_TYPE_EVENT: &str = "event";
pub const LABEL_ZONE: &str = "zone";
pub const LABEL_NAME: &str = "name";
pub const LABEL_NAMESPACE: &str = "namespace";

/// Metrics
pub const LAST_TIMESTAMP: &str = "last_message_timestamp";
pub const MESSAGE_RECEIVE_TOTAL: &str = "message_receive_total";
pub const RESOURCE_COUNT: &str = "resource_count";
pub const ACTIVE_PEER_COUNT: &str = "active_peers_count";
pub const MEMBERSHIP_CHANGE_TOTAL: &str = "membership_change_total";

pub fn set_last_timestamp(
    zone: &'static str,
    name: &'static str,
    namespace: &'static str,
    timestamp: f64,
) {
    metrics::gauge!(
        LAST_TIMESTAMP,
        LABEL_ZONE => zone,
        LABEL_NAME => name,
        LABEL_NAMESPACE => namespace,
    )
    .set(timestamp);
}

pub fn increment_message_receive_snapshot_total(
    zone: &'static str,
    name: &'static str,
    namespace: &'static str,
) {
    metrics::counter!(
        MESSAGE_RECEIVE_TOTAL,
        LABEL_ZONE => zone,
        LABEL_NAME => name,
        LABEL_NAMESPACE => namespace,
        LABEL_MSG_TYPE => LABEL_MSG_TYPE_SNAPSHOT,
    )
    .increment(1);
}

pub fn increment_message_receive_event_total(
    zone: &'static str,
    name: &'static str,
    namespace: &'static str,
) {
    metrics::counter!(
        MESSAGE_RECEIVE_TOTAL,
        LABEL_ZONE => zone,
        LABEL_NAME => name,
        LABEL_NAMESPACE => namespace,
    )
    .increment(1);
}

pub fn set_resource_count(zone: &'static str, count: u32) {
    metrics::gauge!(
        RESOURCE_COUNT,
        LABEL_ZONE => zone,
    )
    .set(count);
}

pub fn set_active_peer_count(zone: &'static str, count: u32) {
    metrics::gauge!(
        ACTIVE_PEER_COUNT,
        LABEL_ZONE => zone,
    )
    .set(count);
}

pub fn increment_membership_change(zone: &'static str) {
    metrics::counter!(
        MEMBERSHIP_CHANGE_TOTAL,
        LABEL_ZONE => zone,
    )
    .increment(1);
}
