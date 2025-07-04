use super::types::{MergeResult, MergeStrategy, UpdateResult};
use crate::{
    kube::{
        dynamic_object_ext::{DynamicObjectExt, dump_zones},
        subscriptions::Version,
    },
    merge::types::{Tombstone, VersionedObject},
};
use anyapplication::{
    anyapplication::{
        AnyApplication, AnyApplicationSpec, AnyApplicationStatus, AnyApplicationStatusZones,
    },
    anyapplication_ext::AnyApplicationExt,
};
use anyhow::{Result, anyhow};
use kube::api::{DynamicObject, GroupVersionKind};
use tracing::error;

pub struct AnyApplicationMerge {
    gvk: GroupVersionKind,
}

impl Default for AnyApplicationMerge {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeStrategy for AnyApplicationMerge {
    fn mesh_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Result<MergeResult> {
        match current {
            VersionedObject::Object(current) => {
                self.mesh_update_update(current, incoming, incoming_zone, current_zone)
            }
            VersionedObject::NonExisting => self.mesh_update_create(incoming, incoming_zone),
            VersionedObject::Tombstone(tombstone) => {
                self.mesh_update_tombstone(tombstone.owner_version, incoming, incoming_zone)
            }
        }
    }

    fn mesh_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        match current {
            VersionedObject::Object(current) => {
                self.mesh_delete_internal(current, incoming, incoming_zone)
            }
            VersionedObject::NonExisting => Ok(MergeResult::Tombstone(Tombstone {
                gvk: self.gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: incoming.get_owner_version_or_fail()?,
                owner_zone: incoming_zone.into(),
                resource_version: 0,
                deletion_timestamp: 0, //TODO now
            })),
            VersionedObject::Tombstone(tombstone) => {
                if tombstone.owner_zone == incoming_zone {
                    let name = incoming.get_namespaced_name();
                    let incoming: AnyApplication = incoming.try_parse()?;
                    let incoming_owner_version = incoming.get_owner_version()?;

                    let version = Version::max(tombstone.owner_version, incoming_owner_version);
                    Ok(MergeResult::Tombstone(Tombstone {
                        gvk: self.gvk.to_owned(),
                        name,
                        owner_version: version,
                        owner_zone: incoming_zone.into(),
                        resource_version: tombstone.resource_version,
                        deletion_timestamp: 0, //TODO now
                    }))
                } else {
                    Ok(MergeResult::Skip)
                }
            }
        }
    }

    fn is_owner_zone(&self, current: &VersionedObject, zone: &str) -> bool {
        match current {
            VersionedObject::Object(current) => self.is_owner_zone_object(current, zone),
            VersionedObject::NonExisting => false,
            VersionedObject::Tombstone(tombstone) => tombstone.owner_zone == zone,
        }
    }

    fn is_owner_zone_object(&self, current: &DynamicObject, zone: &str) -> bool {
        let current: AnyApplication = current
            .clone()
            .try_parse()
            .expect("Unable to parse AnyApplication");
        let placements_zones = current.get_placement_zones();
        current.get_owner_zone() == zone || placements_zones.contains(zone)
    }

    fn kube_update(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        // Object is in deleting state
        if incoming.metadata.deletion_timestamp.is_some() {
            return Ok(UpdateResult::Skip);
        }

        let mut incoming: AnyApplication = incoming.clone().try_parse()?;

        let is_acceptable_zone = incoming.is_acceptable_zone(incoming_zone);
        if !is_acceptable_zone {
            return Ok(UpdateResult::Skip);
        }

        let is_owned_zone = incoming.is_owned_zone(incoming_zone);
        match current {
            VersionedObject::Object(current) => {
                let mut current: AnyApplication = current.clone().try_parse()?;
                let maybe_current_version = current
                    .get_resource_version()
                    .ok_or(anyhow!("resource version is absent"));
                if maybe_current_version.is_err() {
                    error!("current resource version is not available: {current:?}");
                }
                let current_version = maybe_current_version.unwrap();
                if current_version >= incoming_resource_version {
                    return Ok(UpdateResult::Skip);
                }
                let mut updated = false;
                if is_owned_zone
                    && current.spec != incoming.spec
                    && current_version < incoming_resource_version
                {
                    current.spec = incoming.spec.clone();
                    updated = true;
                }

                if let Some(status) =
                    self.local_update_status(&current.status, &incoming.status, is_owned_zone)
                {
                    current.status = Some(status);
                    updated = true;
                }

                if updated {
                    if is_owned_zone {
                        current.set_owner_version(incoming_resource_version);
                    }

                    let mut object = current.to_object()?;
                    if incoming_resource_version > current_version {
                        object.set_resource_version(incoming_resource_version);
                    }
                    Ok(UpdateResult::Update { object })
                } else {
                    Ok(UpdateResult::Skip)
                }
            }
            VersionedObject::NonExisting => {
                if is_owned_zone {
                    incoming.set_owner_version(incoming_resource_version);
                }
                let mut object = incoming.to_object()?;
                object.set_resource_version(incoming_resource_version);
                Ok(UpdateResult::Create { object })
            }
            VersionedObject::Tombstone(tombstone) => {
                if tombstone.owner_version >= incoming_resource_version {
                    Ok(UpdateResult::Skip)
                } else {
                    if is_owned_zone {
                        incoming.set_owner_version(incoming_resource_version);
                    }
                    let mut object = incoming.to_object()?;
                    object.set_resource_version(incoming_resource_version);
                    Ok(UpdateResult::Create { object })
                }
            }
        }
    }

    fn kube_delete(
        &self,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        let name = incoming.get_namespaced_name();
        let mut incoming: AnyApplication = incoming.clone().try_parse()?;
        // Delete is allowed only from owning zone
        let is_owning_zone = incoming.get_owner_zone() == incoming_zone;
        if !is_owning_zone {
            return Ok(UpdateResult::Skip);
        }

        match current {
            VersionedObject::Object(_current) => {
                incoming.set_owner_version(incoming_version);
                let mut object = incoming.to_object()?;

                object.set_resource_version(incoming_version);
                Ok(UpdateResult::Delete {
                    object,
                    tombstone: Tombstone {
                        gvk: self.gvk.to_owned(),
                        name,
                        owner_version: incoming_version,
                        owner_zone: incoming_zone.to_owned(),
                        resource_version: incoming_version,
                        deletion_timestamp: 0, //TODO now
                    },
                })
            }
            VersionedObject::NonExisting => Ok(UpdateResult::Tombstone(Tombstone {
                gvk: self.gvk.to_owned(),
                name,
                owner_version: incoming_version,
                owner_zone: incoming_zone.to_owned(),
                resource_version: incoming_version,
                deletion_timestamp: 0, // TODO now
            })),
            VersionedObject::Tombstone(tombstone) => {
                let max_version = Version::max(tombstone.owner_version, incoming_version);
                Ok(UpdateResult::Tombstone(Tombstone {
                    gvk: self.gvk.to_owned(),
                    name,
                    owner_version: max_version,
                    owner_zone: incoming_zone.to_owned(),
                    resource_version: tombstone.resource_version,
                    deletion_timestamp: 0, // TODO now
                }))
            }
        }
    }
}

impl AnyApplicationMerge {
    pub fn new() -> Self {
        AnyApplicationMerge {
            gvk: GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication"),
        }
    }

    fn mesh_update_update(
        &self,
        current: DynamicObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Result<MergeResult> {
        let mut current: AnyApplication = current.try_parse()?;
        let current_owner_version = current.get_owner_version()?;

        let incoming: AnyApplication = incoming.try_parse()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone();

        // Update is possible from owning zone, status zones and placement zones.
        let status_zones = incoming.get_status_zone_ids();
        let placement_zones = incoming.get_placement_zones();
        let acceptable_zone =
            status_zones.contains(incoming_zone) || placement_zones.contains(incoming_zone);

        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        let merged_spec = self.merge_spec(
            current_owner_version,
            &incoming.spec,
            incoming_owner_version,
            &incoming_owner_zone,
            incoming_zone,
        );
        let merged_status = self.merge_status(
            &current.status,
            current_owner_version,
            &incoming.status,
            incoming_owner_version,
            &incoming_owner_zone,
            incoming_zone,
            current_zone,
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
            return Ok(MergeResult::Skip);
        }

        if incoming_owner_zone == current.get_owner_zone() {
            current.set_owner_version(incoming_owner_version);
        }

        let object = current.to_object()?;
        object.dump_status("merge_update_internal");
        Ok(MergeResult::Update { object })
    }

    fn merge_spec(
        &self,
        current_owner_version: Version,
        incoming: &AnyApplicationSpec,
        incoming_owner_version: Version,
        incoming_owner_zone: &str,
        incoming_zone: &str,
    ) -> Option<AnyApplicationSpec> {
        let acceptable_zone = incoming_zone == incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;
        if acceptable_zone && new_change {
            Some(incoming.to_owned())
        } else {
            None
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_status(
        &self,
        current: &Option<AnyApplicationStatus>,
        current_owner_version: Version,
        incoming: &Option<AnyApplicationStatus>,
        incoming_owner_version: Version,
        incoming_owner_zone: &str,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Option<AnyApplicationStatus> {
        let maybe_status = self.merge_ownership_section(
            current,
            current_owner_version,
            incoming,
            incoming_owner_version,
            incoming_zone,
            incoming_owner_zone,
            current_zone,
        );

        // let should_merge_zones = incoming_zone == incoming_owner_zone || current_zone == incoming_owner_zone;
        let should_merge_zones = true;
        let maybe_zones = if should_merge_zones {
            self.merge_zone_section(
                current
                    .as_ref()
                    .and_then(|v| v.zones.as_ref())
                    .unwrap_or(&vec![]),
                incoming
                    .as_ref()
                    .and_then(|v| v.zones.as_ref())
                    .unwrap_or(&vec![]),
            )
        } else {
            None
        };

        if let Some(mut status) = maybe_status {
            if let Some(mut zones) = maybe_zones {
                zones.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
                status.zones = Some(zones);
            } else {
                status.zones = current.as_ref().and_then(|v| v.zones.clone());
            }
            Some(status)
        } else if let Some(mut zones) = maybe_zones {
            if let Some(status) = current {
                let mut status = status.clone();
                zones.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
                status.zones = Some(zones);
                Some(status)
            } else {
                None
            }
        } else {
            None
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_ownership_section(
        &self,
        current: &Option<AnyApplicationStatus>,
        current_owner_version: Version,
        incoming: &Option<AnyApplicationStatus>,
        incoming_owner_version: Version,
        incoming_zone: &str,
        incoming_owner_zone: &str,
        _current_zone: &str,
    ) -> Option<AnyApplicationStatus> {
        let acceptable_zone = incoming_zone == incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;

        if acceptable_zone && new_change {
            match (current, incoming) {
                (Some(_) | None, Some(incoming)) => {
                    let mut target = incoming.clone();
                    target.zones = None;
                    Some(target)
                }
                (_, None) => None,
            }
        } else {
            None
        }
    }

    fn merge_zone_section(
        &self,
        current: &[AnyApplicationStatusZones],
        incoming: &[AnyApplicationStatusZones],
    ) -> Option<Vec<AnyApplicationStatusZones>> {
        let mut updated = false;
        let mut target = vec![];
        for curr in current.iter() {
            let found = incoming.iter().find(|v| v.zone_id == curr.zone_id);
            if let Some(found) = found {
                if found.version > curr.version {
                    target.push(found.to_owned());
                    updated = true;
                } else {
                    target.push(curr.to_owned());
                }
            } else {
                target.push(curr.to_owned());
                updated = true;
            }
        }

        for incom in incoming.iter() {
            let found = current.iter().find(|v| v.zone_id == incom.zone_id);
            if found.is_none() {
                target.push(incom.to_owned());
                updated = true;
            }
        }

        if updated {
            dump_zones("merge_zone_section", &target);
        }

        if updated { Some(target) } else { None }
    }

    fn mesh_update_create(
        &self,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let mut incoming: AnyApplication = incoming.try_parse()?;
        let incoming_owner_zone = incoming.get_owner_zone();
        // Create is possible from owning zone
        let acceptable_zone = incoming_zone == incoming_owner_zone;
        if acceptable_zone {
            incoming.metadata.managed_fields = None;
            incoming.metadata.uid = None;
            let object = incoming.to_object()?;
            Ok(MergeResult::Create { object })
        } else {
            Ok(MergeResult::Skip)
        }
    }

    fn mesh_update_tombstone(
        &self,
        current_owner_version: Version,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming_app: AnyApplication = incoming.clone().try_parse()?;
        let incoming_owner_version = incoming_app.get_owner_version()?;
        let incoming_owner_zone = incoming_app.get_owner_zone();

        // tombstone update is possible only from owning zone
        let acceptable_zone = incoming_owner_zone == incoming_zone;
        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        if current_owner_version >= incoming_owner_version {
            // tombstone is newer than incoming => ignore
            Ok(MergeResult::Skip)
        } else {
            // tomstone is older than incoming => create
            self.mesh_update_create(incoming, incoming_zone)
        }
    }

    fn mesh_delete_internal(
        &self,
        current: DynamicObject,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let current_resource_version = current.get_resource_version();
        let current: AnyApplication = current.try_parse()?;
        let current_owner_version = current.get_owner_version()?;

        let name = incoming.get_namespaced_name();
        let incoming: AnyApplication = incoming.to_owned().try_parse()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone();

        let acceptable_zone = incoming_zone == incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;

        if acceptable_zone && new_change {
            Ok(MergeResult::Delete(Tombstone {
                gvk: self.gvk.to_owned(),
                name,
                owner_version: incoming_owner_version,
                owner_zone: incoming_owner_zone,
                resource_version: current_resource_version,
                deletion_timestamp: 0, // TODO now
            }))
        } else {
            Ok(MergeResult::Skip)
        }
    }

    fn local_update_status(
        &self,
        current: &Option<AnyApplicationStatus>,
        incoming: &Option<AnyApplicationStatus>,
        is_owned_zone: bool,
    ) -> Option<AnyApplicationStatus> {
        let maybe_updated_status = if is_owned_zone {
            self.update_ownership_section(current, incoming)
        } else {
            None
        };

        let maybe_updated_zones = self.local_update_zones(
            current
                .as_ref()
                .and_then(|v| v.zones.as_ref())
                .unwrap_or(&vec![]),
            incoming
                .as_ref()
                .and_then(|v| v.zones.as_ref())
                .unwrap_or(&vec![]),
        );

        if let Some(mut status) = maybe_updated_status {
            if let Some(mut zones) = maybe_updated_zones {
                zones.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
                status.zones = Some(zones);
            } else {
                status.zones = current.as_ref().and_then(|v| v.zones.clone());
            }

            Some(status)
        } else if let Some(mut zones) = maybe_updated_zones {
            if let Some(status) = current {
                let mut status = status.clone();
                zones.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
                status.zones = Some(zones);
                Some(status)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn update_ownership_section(
        &self,
        current: &Option<AnyApplicationStatus>,
        incoming: &Option<AnyApplicationStatus>,
    ) -> Option<AnyApplicationStatus> {
        match (current, incoming) {
            (Some(current), Some(incoming)) => {
                let owner_diff = current.owner != incoming.owner;
                let placements_diff = current.placements != incoming.placements;
                let state_diff = current.state != incoming.state;
                if owner_diff || placements_diff || state_diff {
                    let mut target = incoming.clone();
                    target.zones = None;
                    Some(target)
                } else {
                    None
                }
            }
            (None, Some(incoming)) => Some(incoming.to_owned()),
            (_, None) => None,
        }
    }

    fn local_update_zones(
        &self,
        current: &[AnyApplicationStatusZones],
        incoming: &[AnyApplicationStatusZones],
    ) -> Option<Vec<AnyApplicationStatusZones>> {
        self.merge_zone_section(current, incoming)
    }
}

#[cfg(test)]
pub mod tests {

    use crate::kube::dynamic_object_ext::DynamicObjectExt;
    use crate::merge::anyapplication_strategy::AnyApplicationMerge;
    use crate::merge::anyapplication_test_support::tests::anyapp;
    use crate::merge::anyapplication_test_support::tests::anycond;
    use crate::merge::anyapplication_test_support::tests::anycond_status;
    use crate::merge::anyapplication_test_support::tests::anyzone;
    use crate::merge::anyapplication_test_support::tests::make_anyapplication_with_conditions;
    use crate::merge::types::MergeResult;
    use crate::merge::types::MergeStrategy;
    use crate::merge::types::Tombstone;
    use crate::merge::types::UpdateResult;
    use crate::merge::types::VersionedObject;

    #[test]
    pub fn mesh_update_create_non_existing() {
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy
                .mesh_update(VersionedObject::NonExisting, incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_create_tombstone() {
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 0,
            owner_zone: "zone2".into(),
            resource_version: 1,
            deletion_timestamp: 0,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy
                .mesh_update(existing, incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_create_skip_if_tombstone_is_newer() {
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 2,
            owner_zone: "zone2".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(existing, incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_non_existing_other_zone() {
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(VersionedObject::NonExisting, incoming, "test", "test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_same_version() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(1, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(current.into(), incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_spec() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            strategy
                .mesh_update(current.clone().into(), incoming.clone(), "zone1", "zone1")
                .unwrap()
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(current.into(), incoming, "zone2", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_status_ownership() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            strategy
                .mesh_update(current.into(), incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_status_conditions() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            strategy
                .mesh_update(current.into(), incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_unacceptable_zone() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "unacceptable", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(current.into(), incoming, "zone1", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_non_existing_delete() {
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().unwrap(),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "zone1".into(),
                resource_version: 0,
                deletion_timestamp: 0,
            }),
            strategy
                .mesh_delete(VersionedObject::NonExisting, incoming, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone() {
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 0,
            owner_zone: "zone1".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().unwrap(),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "zone1".into(),
                resource_version: 5,
                deletion_timestamp: 0,
            }),
            strategy.mesh_delete(existing, incoming, "zone1").unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_is_newer() {
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 2,
            owner_zone: "zone1".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().expect("gvk expected"),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "zone1".into(),
                resource_version: 5,
                deletion_timestamp: 0
            }),
            strategy.mesh_delete(existing, incoming, "zone1").unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_skip_from_different_zone() {
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 0,
            owner_zone: "zone2".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy.mesh_delete(existing, incoming, "zone1").unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_the_same_version_delete() {
        let incoming = anyapp(1, "zone1", 1);
        let mut current = anyapp(1, "zone1", 0);
        current.set_resource_version(10);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_delete(current.into(), incoming, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_greater_version_delete() {
        let incoming = anyapp(2, "zone1", 1);
        let mut current = anyapp(1, "zone1", 0);
        current.set_resource_version(10);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Delete(Tombstone {
                gvk: current.get_gvk().unwrap(),
                name: current.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "zone1".into(),
                resource_version: 10,
                deletion_timestamp: 0,
            }),
            strategy
                .mesh_delete(current.into(), incoming, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_condition_from_replica_zone() {
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update { object: expected },
            strategy
                .mesh_update(current.into(), incoming, "zone2", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_create_condition_from_replica_zone() {
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[anyzone("zone1", 2, &[anycond("zone1", "type")])],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update { object: expected },
            strategy
                .mesh_update(current.into(), incoming, "zone2", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_condition_from_replica_zone() {
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 2, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update { object: expected },
            strategy
                .mesh_update(current.into(), incoming, "zone2", "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create_non_existing() {
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 0, &[]);

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .kube_update(VersionedObject::NonExisting, incoming, 1, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create_tombstone() {
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 0, &[]);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 0,
            owner_zone: "zone1".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .kube_update(existing, incoming, 1, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip_create_tombstone_is_newer() {
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 0, &[]);

        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 2,
            owner_zone: "zone1".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            UpdateResult::Skip,
            AnyApplicationMerge::new()
                .kube_update(existing, incoming, 1, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip_old_version() {
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 0, &[]);

        let existing = make_anyapplication_with_conditions(2, 2, "zone1", 0, &[]);

        assert_eq!(
            UpdateResult::Skip,
            AnyApplicationMerge::new()
                .kube_update(existing.into(), incoming, 1, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_incoming_has_diff() {
        let incoming = make_anyapplication_with_conditions(1, 2, "zone1", 2, &[]);
        let existing = make_anyapplication_with_conditions(1, 1, "zone1", 0, &[]);

        let mut expected = incoming.clone();
        expected.set_owner_version(2);

        assert_eq!(
            UpdateResult::Update { object: expected },
            AnyApplicationMerge::new()
                .kube_update(existing.into(), incoming, 2, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_skip_non_existing() {
        let incoming = anyapp(2, "zone1", 2);

        assert_eq!(
            UpdateResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().expect("expect gvk"),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "zone1".into(),
                resource_version: 2,
                deletion_timestamp: 0
            }),
            AnyApplicationMerge::new()
                .kube_delete(VersionedObject::NonExisting, incoming, 2, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_tombstone() {
        let incoming = anyapp(2, "zone1", 2);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 1,
            owner_zone: "zone1".into(),
            resource_version: 5,
            deletion_timestamp: 0,
        });

        assert_eq!(
            UpdateResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().expect("expect gvk"),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "zone1".into(),
                resource_version: 5,
                deletion_timestamp: 0,
            }),
            AnyApplicationMerge::new()
                .kube_delete(existing, incoming, 2, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_tombstone_other_zone() {
        let incoming = anyapp(2, "zone1", 2);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 1,
            owner_zone: "zone2".into(),
            resource_version: 3,
            deletion_timestamp: 0,
        });

        assert_eq!(
            UpdateResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().expect("expect gvk"),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "zone1".into(),
                resource_version: 3,
                deletion_timestamp: 0
            }),
            AnyApplicationMerge::new()
                .kube_delete(existing, incoming, 2, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_incoming_resource_version_greater() {
        let current = make_anyapplication_with_conditions(1, 1, "zone1", 1, &[]);
        let incoming = make_anyapplication_with_conditions(1, 2, "zone1", 1, &[]);

        let mut expected = incoming.clone();
        expected.set_owner_version(2);

        assert_eq!(
            UpdateResult::Delete {
                object: expected,
                tombstone: Tombstone {
                    gvk: incoming.get_gvk().expect("expect gvk"),
                    name: incoming.get_namespaced_name(),
                    owner_version: 2,
                    owner_zone: "zone1".into(),
                    resource_version: 2,
                    deletion_timestamp: 0
                },
            },
            AnyApplicationMerge::new()
                .kube_delete(current.into(), incoming, 2, "zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_from_placement_zone() {
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond_status("zone2", "type", "updated")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            5,
            "zone1",
            1,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond_status("zone2", "type", "updated")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update { object: expected },
            strategy
                .kube_update(current.into(), incoming, 5, "zone2")
                .unwrap()
        );
    }

    #[test]
    pub fn local_create_from_placement_zone() {
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[anyzone("zone1", 2, &[anycond("zone1", "type")])],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            5,
            "zone1",
            0,
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update { object: expected },
            strategy
                .kube_update(current.into(), incoming, 5, "zone2")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_from_placement_zone() {
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );

        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            4,
            "zone1",
            0,
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update { object: expected },
            strategy
                .kube_update(current.into(), incoming, 4, "zone2")
                .unwrap()
        );
    }
}
