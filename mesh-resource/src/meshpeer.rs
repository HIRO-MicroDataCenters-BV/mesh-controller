use anyhow::{Context, Result};
use kube::api::DynamicObject;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
mod prelude {
    pub use kube::CustomResource;
    pub use schemars::JsonSchema;
    pub use serde::{Deserialize, Serialize};
}
use self::prelude::*;

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[kube(
    kind = "MeshPeer",
    group = "dcp.hiro.io",
    version = "v1",
    status = "MeshPeerStatus",
    shortname = "meshpeer",
    plural = "meshpeers",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeerSpec {
    pub identity: PeerIdentity,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PeerIdentity {
    pub public_key: String,
    pub endpoints: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeerStatus {
    pub status: PeerStatus,
    pub instance: Option<MeshPeerInstance>,
    pub update_time: u64,
    pub conditions: Vec<MeshPeerStatusCondition>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct MeshPeerInstance {
    pub zone: String,
    pub start_time: u64,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, Copy)]
pub enum PeerStatus {
    Ready,
    NotReady,
    #[default]
    Unavailable,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeerStatusCondition {
    pub r#type: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_time: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
}

pub trait MeshPeerExt {
    fn to_object(self) -> Result<DynamicObject>;
}

impl MeshPeerExt for MeshPeer {
    fn to_object(self) -> Result<DynamicObject>
    where
        Self: Sized + Serialize,
    {
        let value = serde_json::to_value(self).context("Failed to serialize merged object")?;
        let object: DynamicObject = serde_json::from_value(value)?;
        Ok(object)
    }
}
