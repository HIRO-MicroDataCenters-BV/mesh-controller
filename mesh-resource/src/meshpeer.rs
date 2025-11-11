use anyhow::{Context, Result};
use chrono::TimeZone;
use chrono::Utc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
pub use kube::CustomResource;
use kube::api::DynamicObject;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeerStatus {
    pub status: PeerStatus,
    pub instance: Option<MeshPeerInstance>,
    pub update_time: Time,
    pub conditions: Vec<MeshPeerStatusCondition>,
}

impl Default for MeshPeerStatus {
    fn default() -> Self {
        MeshPeerStatus {
            status: PeerStatus::default(),
            instance: None,
            update_time: 0.to_time(),
            conditions: vec![],
        }
    }
}

impl MeshPeerStatus {
    pub fn set_condition_if(
        &mut self,
        types: &[&str],
        match_status: &str,
        timestamp: u64,
        target_status: &str,
    ) {
        for condition in &mut self.conditions {
            if types.contains(&condition.r#type.as_str()) && match_status == condition.status {
                condition.last_transition_time = Some(timestamp.to_time());
                condition.status = target_status.into();
            }
        }
    }

    pub fn add_or_update(&mut self, condition: MeshPeerStatusCondition) {
        let mut found = false;
        for existing in &mut self.conditions {
            if existing.r#type == condition.r#type {
                found = true;
                if existing != &condition {
                    *existing = condition;
                    return;
                }
            }
        }
        if !found {
            self.conditions.push(condition);
        }
    }

    pub fn remove_condition(&mut self, r#type: &str) {
        self.conditions.retain_mut(|c| c.r#type != r#type);
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct MeshPeerInstance {
    pub zone: String,
    pub start_time: Time,
    pub start_timestamp: u64,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, Copy)]
pub enum PeerStatus {
    Ready,
    NotReady,
    Unknown,
    #[default]
    Unavailable,
}

impl std::fmt::Display for PeerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            PeerStatus::Ready => write!(f, "Ready"),
            PeerStatus::NotReady => write!(f, "NotReady"),
            PeerStatus::Unavailable => write!(f, "Unavailable"),
            PeerStatus::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeerStatusCondition {
    pub r#type: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_time: Option<Time>,
}

impl MeshPeerStatusCondition {
    pub fn new(r#type: String, last_transition_timestamp: u64) -> MeshPeerStatusCondition {
        MeshPeerStatusCondition {
            r#type,
            status: "True".into(),
            reason: None,
            message: None,
            last_transition_time: Some(last_transition_timestamp.to_time()),
        }
    }
}

pub trait IntoTimeExt {
    fn to_time(self) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
}

impl IntoTimeExt for u64 {
    fn to_time(self) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::Time {
        Time(Utc.timestamp_millis_opt(self as i64).unwrap())
    }
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
