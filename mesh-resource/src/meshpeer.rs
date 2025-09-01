use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;
use stackable_operator::status::condition::ClusterCondition;
use stackable_operator::status::condition::HasStatusCondition;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "MeshPeer",
    group = "dcp.hiro.io",
    version = "v1",
    status = "MeshPeerStatus",
    shortname = "meshpeer",
    plural = "meshpeers",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeerSpec {
    pub identity: PeerIdentity,
    pub status: Option<MeshPeerStatus>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PeerIdentity {
    pub public_key: String,
    pub endpoints: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct MeshPeerStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
    pub status: PeerStatus,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub enum PeerStatus {
    Ready,
    NotReady,
    #[default]
    Unavailable,
}

impl HasStatusCondition for MeshPeer {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}
