#[derive(Debug, Clone)]
pub struct PeerState {
    pub peer_id: String,
    pub state: String,
    pub instance: InstanceId,
    pub update_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct InstanceId {
    pub zone: String,
    pub zone_start_time: u64,
}
