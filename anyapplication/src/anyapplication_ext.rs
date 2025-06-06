use std::collections::HashSet;

use super::anyapplication::AnyApplication;
use crate::anyapplication::AnyApplicationStatusConditions;
use anyhow::Context;
use anyhow::{Result, anyhow};
use kube::api::DynamicObject;
use serde::Serialize;

pub const OWNER_VERSION: &str = "dcp.hiro.io/owner-version";

pub type Version = u64;

pub trait AnyApplicationExt {
    fn get_owner_version(&self) -> Result<Version>;
    fn set_owner_version(&mut self, version: Version);
    fn get_owner_zone(&self) -> String;
    fn to_object(self) -> Result<DynamicObject>
    where
        Self: Sized + Serialize,
    {
        let value = serde_json::to_value(self).context("Failed to serialize merged object")?;
        let object: DynamicObject = serde_json::from_value(value)?;
        Ok(object)
    }

    fn get_placement_zones(&self) -> HashSet<String>;

    fn set_condition_version(&mut self, zone: &str, version: Version);
}

impl AnyApplicationExt for AnyApplication {
    fn get_owner_version(&self) -> Result<Version> {
        self.metadata
            .labels
            .as_ref()
            .ok_or(anyhow!("{} label not set", OWNER_VERSION))?
            .get(OWNER_VERSION)
            .map(|v| {
                v.parse::<Version>()
                    .map_err(|e| anyhow!("unable to parse version from label. {e}"))
            })
            .unwrap_or(Err(anyhow!("{} label not set", OWNER_VERSION)))
    }

    fn set_owner_version(&mut self, version: Version) {
        let labels = self.metadata.labels.get_or_insert_default();
        labels.insert(OWNER_VERSION.into(), version.to_string());
    }

    fn get_owner_zone(&self) -> String {
        self.status
            .as_ref()
            .map(|s| s.owner.to_owned())
            .unwrap_or("unknown".to_string())
    }
    fn get_placement_zones(&self) -> HashSet<String> {
        self.status
            .as_ref()
            .map(|s| {
                s.placements
                    .as_ref()
                    .map(|p| p.iter().map(|p| p.zone.to_owned()).collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    fn set_condition_version(&mut self, zone: &str, version: Version) {
        if let Some(conditions) = self.status.as_mut().and_then(|s| s.conditions.as_mut()) {
            for condition in conditions.iter_mut() {
                if condition.zone_id == zone {
                    condition.zone_version = version.to_string();
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct AnyApplicationStatusConditionId {
    pub zone_id: String,
    pub r#type: String,
}

pub trait AnyApplicationStatusConditionsExt {
    fn identity(&self) -> AnyApplicationStatusConditionId;
    fn is_equal(&self, other: &AnyApplicationStatusConditions) -> bool;
}

impl AnyApplicationStatusConditionsExt for AnyApplicationStatusConditions {
    fn identity(&self) -> AnyApplicationStatusConditionId {
        AnyApplicationStatusConditionId {
            zone_id: self.zone_id.clone(),
            r#type: self.r#type.clone(),
        }
    }

    fn is_equal(&self, other: &AnyApplicationStatusConditions) -> bool {
        self.zone_id == other.zone_id
            && self.r#type == other.r#type
            && self.status == other.status
            && self.reason == other.reason
            && self.msg == other.msg
            && self.last_transition_time == other.last_transition_time
    }
}
