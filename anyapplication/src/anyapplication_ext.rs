use std::collections::{HashMap, HashSet};

use crate::anyapplication::{AnyApplicationStatusZones, AnyApplicationStatusZonesConditions};

use super::anyapplication::AnyApplication;
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

    fn set_resource_version(&mut self, version: Version);
    fn get_resource_version(&self) -> Option<Version>;

    fn get_status_zone_ids(&self) -> HashSet<String>;

    fn is_acceptable_zone(&self, incoming_zone: &str) -> bool;

    fn is_owned_zone(&self, incoming_zone: &str) -> bool;
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

    fn set_resource_version(&mut self, version: Version) {
        self.metadata.resource_version = Some(version.to_string());
    }

    fn get_resource_version(&self) -> Option<Version> {
        self.metadata
            .resource_version
            .as_ref()
            .and_then(|version_str| version_str.parse::<Version>().ok())
    }

    fn get_status_zone_ids(&self) -> HashSet<String> {
        self.status
            .as_ref()
            .and_then(|s| s.zones.as_ref())
            .map(|zones| {
                zones
                    .iter()
                    .map(|zone| zone.zone_id.to_owned())
                    .collect::<HashSet<String>>()
            })
            .unwrap_or_default()
    }

    fn is_acceptable_zone(&self, incoming_zone: &str) -> bool {
        let placements_zones = self.get_placement_zones();
        let status_zones = self.get_status_zone_ids();

        // The updates are distributed only from owning zone, status zone or placement zones
        let is_owned_zone = self.is_owned_zone(incoming_zone);
        let is_placement_zone = placements_zones.contains(incoming_zone);
        let is_status_zone = status_zones.contains(incoming_zone);

        is_owned_zone || is_placement_zone || is_status_zone
    }

    fn is_owned_zone(&self, incoming_zone: &str) -> bool {
        let is_owned_zone =
            self.get_owner_zone() == incoming_zone || self.get_owner_zone() == "unknown";
        is_owned_zone
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct AnyApplicationStatusZonesConditionsId {
    pub zone_id: String,
    pub r#type: String,
}

pub trait AnyApplicationStatusZonesConditionsExt {
    fn identity(&self) -> AnyApplicationStatusZonesConditionsId;
    fn is_equal(&self, other: &AnyApplicationStatusZonesConditions) -> bool;
}

impl AnyApplicationStatusZonesConditionsExt for AnyApplicationStatusZonesConditions {
    fn identity(&self) -> AnyApplicationStatusZonesConditionsId {
        AnyApplicationStatusZonesConditionsId {
            zone_id: self.zone_id.clone(),
            r#type: self.r#type.clone(),
        }
    }

    fn is_equal(&self, other: &AnyApplicationStatusZonesConditions) -> bool {
        self.zone_id == other.zone_id
            && self.r#type == other.r#type
            && self.status == other.status
            && self.reason == other.reason
            && self.msg == other.msg
            && self.last_transition_time == other.last_transition_time
    }
}

pub trait AnyApplicationStatusZonesExt {
    fn is_equal(&self, other: &AnyApplicationStatusZones) -> bool;
}

impl AnyApplicationStatusZonesExt for AnyApplicationStatusZones {
    fn is_equal(&self, other: &AnyApplicationStatusZones) -> bool {
        self.zone_id == other.zone_id
            && conditions_equal(
                self.conditions.as_ref().unwrap_or(&vec![]),
                other.conditions.as_ref().unwrap_or(&vec![]),
            )
    }
}

fn conditions_equal(
    current: &[AnyApplicationStatusZonesConditions],
    incoming: &[AnyApplicationStatusZonesConditions],
) -> bool {
    if current.len() != incoming.len() {
        false
    } else {
        let current_map: HashMap<
            AnyApplicationStatusZonesConditionsId,
            &AnyApplicationStatusZonesConditions,
        > = current.iter().map(|v| (v.identity(), v)).collect();
        let incoming_map: HashMap<
            AnyApplicationStatusZonesConditionsId,
            &AnyApplicationStatusZonesConditions,
        > = incoming.iter().map(|v| (v.identity(), v)).collect();

        incoming_map
            .iter()
            .map(|(id, new_item)| (current_map.get(id), new_item))
            .all(|(old_item, new_item)| old_item.is_some() && new_item.is_equal(old_item.unwrap()))
    }
}
