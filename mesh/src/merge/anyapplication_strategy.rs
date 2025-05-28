use super::{
    anyapplication::{AnyApplication, AnyApplicationSpec, AnyApplicationStatus},
    anyapplication_ext::AnyApplicationExt,
    types::{MergeResult, MergeStrategy},
};
use crate::kube::{cache::Version, dynamic_object_ext::DynamicObjectExt};
use anyhow::{Context, Result, anyhow};
use kube::api::DynamicObject;

pub struct AnyApplicationMerge {}

impl AnyApplicationMerge {
    pub fn new() -> Self {
        AnyApplicationMerge {}
    }

    fn merge_update_internal(
        &self,
        current: DynamicObject,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let mut current: AnyApplication = current.try_parse()?;
        let current_owner_version = current.get_owner_version()?;

        let incoming: AnyApplication = incoming.try_parse()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone();

        let merged_spec = self.merge_spec(
            current_owner_version,
            &incoming.spec,
            incoming_owner_version,
            incoming_zone,
            &incoming_owner_zone,
        );
        let merged_status = self.merge_status(
            &current.status,
            current_owner_version,
            &incoming.status,
            incoming_owner_version,
            incoming_zone,
            &incoming_owner_zone,
        );
        let mut updated = false;
        if let Some(spec) = merged_spec {
            current.spec = spec;
            updated = true;
        }
        if let Some(status) = merged_status {
            current.status = Some(status);
            updated = true;
        }

        if !updated {
            return Ok(MergeResult::DoNothing);
        }
        current.set_owner_version(incoming_owner_version);

        let value = serde_json::to_value(current).context("Failed to serialize merged object")?;
        let object: DynamicObject = serde_json::from_value(value)?;
        Ok(MergeResult::Update { object })
    }

    fn merge_spec(
        &self,
        current_owner_version: Version,
        incoming: &AnyApplicationSpec,
        incoming_owner_version: Version,
        incoming_zone: &str,
        incoming_owner_zone: &str,
    ) -> Option<AnyApplicationSpec> {
        let acceptable_zone = incoming_zone == incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;
        if acceptable_zone && new_change {
            Some(incoming.to_owned())
        } else {
            None
        }
    }

    fn merge_status(
        &self,
        current: &Option<AnyApplicationStatus>,
        current_owner_version: Version,
        incoming: &Option<AnyApplicationStatus>,
        incoming_owner_version: Version,
        incoming_zone: &str,
        incoming_owner_zone: &str,
    ) -> Option<AnyApplicationStatus> {
        let acceptable_zone = incoming_zone == incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;

        if acceptable_zone && new_change {
            None
        } else {
            None
        }
    }

    fn merge_create_internal(&self, mut incoming: DynamicObject) -> Result<MergeResult> {
        incoming.metadata.managed_fields = None;
        incoming.metadata.uid = None;
        Ok(MergeResult::Create { object: incoming })
    }

    fn merge_delete_internal(
        &self,
        current: DynamicObject,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let current: AnyApplication = current.try_parse()?;
        let current_owner_version = current.get_owner_version()?;

        let name = incoming.get_namespaced_name();
        let incoming: AnyApplication = incoming.to_owned().try_parse()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone();

        let acceptable_zone = incoming_zone == &incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;

        if acceptable_zone && new_change {
            Ok(MergeResult::Delete { name })
        } else {
            Ok(MergeResult::DoNothing)
        }
    }
}

impl MergeStrategy for AnyApplicationMerge {
    fn merge_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming = incoming.clone();
        if let Some(current) = current {
            self.merge_update_internal(current, incoming.clone(), &incoming_zone)
        } else {
            self.merge_create_internal(incoming)
        }
    }

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        if let Some(current) = current {
            self.merge_delete_internal(current, incoming, incoming_zone)
        } else {
            Ok(MergeResult::DoNothing)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnyApplicationOwnership {
    pub owner: String,
    pub placements: Vec<String>,
}

#[cfg(test)]
pub mod tests {

    #[test]
    pub fn test_any_application_merge() {}
}
