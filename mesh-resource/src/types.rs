#[derive(Debug, Clone)]
pub struct PeerUpdate {
    pub peer_id: String,
    pub state: String,
    pub timestamp: u64,
}
