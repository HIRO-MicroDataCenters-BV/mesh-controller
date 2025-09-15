use axum_prometheus::metrics;

// Labels
pub const LABEL_MSG_TYPE: &str = "msg_type";
pub const LABEL_MSG_TYPE_SNAPSHOT: &str = "snapshot";
pub const LABEL_MSG_TYPE_EVENT: &str = "event";
pub const LABEL_ZONE: &str = "zone";
pub const LABEL_DISCOVERED_ZONE: &str = "discovered_zone";
pub const LABEL_SRC_ZONE: &str = "src_zone";
pub const LABEL_NAME: &str = "name";
pub const LABEL_NAMESPACE: &str = "namespace";

/// Metrics
pub const NEW_LOG_DISCOVERED_TOTAL: &str = "new_log_discovered_total";
pub const LAST_MESSAGE_TIMESTAMP: &str = "last_message_timestamp"; //TODO
pub const OPERATION_RECEIVED_SEQNR: &str = "operation_received_seqnr";
pub const OPERATION_APPLIED_SEQNR: &str = "operation_applied_seqnr";
pub const NETWORK_MESSAGE_RECEIVED_TOTAL: &str = "network_message_received_total";
pub const NETWORK_MESSAGE_BROADCASTED_TOTAL: &str = "network_message_broadcasted_total";
pub const APPLIED_EVENT_TOTAL: &str = "applied_event_total";
pub const APPLIED_SNAPSHOT_TOTAL: &str = "applied_shapshot_total";
pub const RESOURCES_TOTAL: &str = "resources_total"; // TODO
pub const ACTIVE_PEER_TOTAL: &str = "active_peers_total"; // TODO
pub const MEMBERSHIP_CHANGE_TOTAL: &str = "membership_change_total";

pub fn set_operation_received_seqnr(zone: &str, src_zone: &str, seq_nr: u64) {
    metrics::gauge!(
        OPERATION_RECEIVED_SEQNR,
        LABEL_ZONE => zone.to_owned(),
        LABEL_SRC_ZONE => src_zone.to_owned(),
    )
    .set(seq_nr as f64);
}

pub fn set_operation_applied_seqnr(zone: &str, src_zone: &str, seq_nr: u64) {
    metrics::gauge!(
        OPERATION_APPLIED_SEQNR,
        LABEL_ZONE => zone.to_owned(),
        LABEL_SRC_ZONE => src_zone.to_owned(),
    )
    .set(seq_nr as f64);
}

pub fn increment_new_log_discovered_total(zone: &str, discovered_zone: &str) {
    metrics::counter!(
        NEW_LOG_DISCOVERED_TOTAL,
        LABEL_ZONE => zone.to_owned(),
        LABEL_DISCOVERED_ZONE => discovered_zone.to_owned(),
    )
    .increment(1);
}

pub fn set_last_message_timestamp(
    zone: &str,
    src_zone: &str,
    name: &str,
    namespace: &str,
    timestamp: f64,
) {
    metrics::gauge!(
        LAST_MESSAGE_TIMESTAMP,
        LABEL_ZONE => zone.to_owned(),
        LABEL_SRC_ZONE => src_zone.to_owned(),
        LABEL_NAME => name.to_owned(),
        LABEL_NAMESPACE => namespace.to_owned(),
    )
    .set(timestamp);
}

pub fn increment_network_message_received_total(zone: &str, src_zone: &str) {
    metrics::counter!(
        NETWORK_MESSAGE_RECEIVED_TOTAL,
        LABEL_ZONE => zone.to_owned(),
        LABEL_SRC_ZONE => src_zone.to_owned(),
    )
    .increment(1);
}

pub fn increment_network_message_broadcasted_total(zone: &str) {
    metrics::counter!(
        NETWORK_MESSAGE_BROADCASTED_TOTAL,
        LABEL_ZONE => zone.to_owned(),
    )
    .increment(1);
}

pub fn increment_applied_event_total(zone: &str, src_zone: &str, name: &str, namespace: &str) {
    metrics::counter!(
        APPLIED_EVENT_TOTAL,
        LABEL_MSG_TYPE => LABEL_MSG_TYPE_SNAPSHOT,
        LABEL_ZONE => zone.to_owned(),
        LABEL_SRC_ZONE => src_zone.to_owned(),
        LABEL_NAME => name.to_owned(),
        LABEL_NAMESPACE => namespace.to_owned(),
    )
    .increment(1);
}

pub fn increment_applied_snapshot_total(zone: &str, src_zone: &str) {
    metrics::counter!(
        APPLIED_SNAPSHOT_TOTAL,
        LABEL_MSG_TYPE => LABEL_MSG_TYPE_EVENT,
        LABEL_ZONE => zone.to_owned(),
        LABEL_SRC_ZONE => src_zone.to_owned(),
    )
    .increment(1);
}

pub fn set_resources_total(zone: &str, count: u32) {
    metrics::gauge!(
        RESOURCES_TOTAL,
        LABEL_ZONE => zone.to_owned(),
    )
    .set(count);
}

pub fn set_active_peers_total(zone: &str, count: u32) {
    metrics::gauge!(
        ACTIVE_PEER_TOTAL,
        LABEL_ZONE => zone.to_owned(),
    )
    .set(count);
}

pub fn increment_membership_change_total(zone: &str) {
    metrics::counter!(
        MEMBERSHIP_CHANGE_TOTAL,
        LABEL_ZONE => zone.to_owned(),
    )
    .increment(1);
}
