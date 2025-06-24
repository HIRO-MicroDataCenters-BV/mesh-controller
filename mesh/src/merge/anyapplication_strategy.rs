use super::types::{MergeResult, MergeStrategy, UpdateResult};
use crate::kube::{
    dynamic_object_ext::{DynamicObjectExt, dump_zones},
    subscriptions::Version,
};
use anyapplication::{
    anyapplication::{
        AnyApplication, AnyApplicationSpec, AnyApplicationStatus, AnyApplicationStatusZones,
    },
    anyapplication_ext::AnyApplicationExt,
};
use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

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
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Result<MergeResult> {
        let incoming = incoming.clone();
        if let Some(current) = current {
            self.merge_update_internal(current, incoming.clone(), incoming_zone, current_zone)
        } else {
            self.merge_create_internal(incoming, incoming_zone)
        }
    }

    fn mesh_delete(
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

    fn is_owner_zone(&self, current: &DynamicObject, zone: &str) -> bool {
        let current: AnyApplication = current.clone().try_parse().unwrap(); // TODO fixme errors
        let placements_zones = current.get_placement_zones();
        current.get_owner_zone() == zone || placements_zones.contains(zone)
    }

    fn local_update(
        &self,
        current: Option<DynamicObject>,
        incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        let mut incoming: AnyApplication = incoming.clone().try_parse()?;
        let placements_zones = incoming.get_placement_zones();
        let status_zones = incoming.get_status_zone_ids();

        let is_owned_zone =
            incoming.get_owner_zone() == incoming_zone || incoming.get_owner_zone() == "unknown";
        let is_placement_zone = placements_zones.contains(incoming_zone);
        let is_status_zone = status_zones.contains(incoming_zone);

        let is_acceptable_zone = is_owned_zone || is_placement_zone || is_status_zone;

        if !is_acceptable_zone {
            return Ok(UpdateResult::DoNothing);
        }

        if let Some(current) = current {
            let current_version = current.get_resource_version();
            if current_version >= incoming_version {
                return Ok(UpdateResult::DoNothing);
            }
            let mut current: AnyApplication = current.clone().try_parse()?;
            let mut updated = false;
            if is_owned_zone && current.spec != incoming.spec && current_version < incoming_version
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
                    current.set_owner_version(incoming_version);
                }

                let mut object = current.to_object()?;
                if incoming_version > current_version {
                    object.set_resource_version(incoming_version);
                }
                Ok(UpdateResult::Update { object })
            } else {
                Ok(UpdateResult::DoNothing)
            }
        } else {
            if is_owned_zone {
                incoming.set_owner_version(incoming_version);
            }
            let mut object = incoming.to_object()?;
            object.metadata.resource_version = Some(incoming_version.to_string());
            Ok(UpdateResult::Create { object })
        }
    }

    fn local_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: DynamicObject,
        incoming_version: Version,
        incoming_zone: &str,
    ) -> Result<UpdateResult> {
        let mut incoming: AnyApplication = incoming.clone().try_parse()?;
        // Delete is allowed only from owning zone
        let is_owning_zone = incoming.get_owner_zone() == incoming_zone;
        if !is_owning_zone {
            return Ok(UpdateResult::DoNothing);
        }

        if current.is_some() {
            incoming.set_owner_version(incoming_version);
            let mut object = incoming.to_object()?;

            object.set_resource_version(incoming_version);
            Ok(UpdateResult::Delete { object })
        } else {
            Ok(UpdateResult::DoNothing)
        }
    }
}

impl AnyApplicationMerge {
    pub fn new() -> Self {
        AnyApplicationMerge {
            gvk: GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication"),
        }
    }

    fn merge_update_internal(
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
        let status_zones = incoming.get_status_zone_ids();
        let placement_zones = incoming.get_placement_zones();

        let acceptable_zone =
            status_zones.contains(incoming_zone) || placement_zones.contains(incoming_zone);

        if !acceptable_zone {
            return Ok(MergeResult::DoNothing);
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
            return Ok(MergeResult::DoNothing);
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

    fn merge_create_internal(
        &self,
        incoming: DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let mut incoming: AnyApplication = incoming.try_parse()?;
        let incoming_owner_zone = incoming.get_owner_zone();
        let acceptable_zone = incoming_zone == incoming_owner_zone;
        if acceptable_zone {
            incoming.metadata.managed_fields = None;
            incoming.metadata.uid = None;
            let object = incoming.to_object()?;
            Ok(MergeResult::Create { object })
        } else {
            Ok(MergeResult::DoNothing)
        }
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

        let acceptable_zone = incoming_zone == incoming_owner_zone;
        let new_change = incoming_owner_version > current_owner_version;

        if acceptable_zone && new_change {
            Ok(MergeResult::Delete {
                gvk: self.gvk.to_owned(),
                name,
            })
        } else {
            Ok(MergeResult::DoNothing)
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
    use crate::merge::types::UpdateResult;

    #[test]
    pub fn mesh_update_create_non_existing() {
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy
                .mesh_update(None, &incoming, &"zone1", &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_non_existing_other_zone() {
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_update(None, &incoming, &"test", &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_same_version() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(1, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_update(Some(current), &incoming, &"zone1", &"zone1")
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
                .mesh_update(Some(current.clone()), &incoming, &"zone1", &"zone1")
                .unwrap()
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_update(Some(current), &incoming, &"zone2", &"zone1")
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
                .mesh_update(Some(current), &incoming, &"zone1", &"zone1")
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
                .mesh_update(Some(current), &incoming, &"zone1", &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_unacceptable_zone() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "unacceptable", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_update(Some(current), &incoming, &"zone1", &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_non_existing_delete() {
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy.mesh_delete(None, &incoming, &"zone1").unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_the_same_version_delete() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(1, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_delete(Some(current), &incoming, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_greater_version_delete() {
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Delete {
                gvk: current.get_gvk().unwrap(),
                name: current.get_namespaced_name()
            },
            strategy
                .mesh_delete(Some(current), &incoming, &"zone1")
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
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 3, &vec![anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 3, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &vec![
                anyzone("zone1", 3, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update { object: expected },
            strategy
                .mesh_update(Some(current), &incoming, &"zone2", &"zone1")
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
            &vec![anyzone("zone1", 2, &vec![anycond("zone1", "type")])],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update { object: expected },
            strategy
                .mesh_update(Some(current), &incoming, &"zone2", &"zone1")
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
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 2, &vec![anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 3, &vec![]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            0,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 3, &vec![]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update { object: expected },
            strategy
                .mesh_update(Some(current), &incoming, &"zone2", &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create() {
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 0, &vec![]);

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .local_update(None, incoming, 1, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_ignore_version() {
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 0, &vec![]);

        let existing = make_anyapplication_with_conditions(2, 2, "zone1", 0, &vec![]);

        assert_eq!(
            UpdateResult::DoNothing,
            AnyApplicationMerge::new()
                .local_update(Some(existing), incoming, 1, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update() {
        let incoming = make_anyapplication_with_conditions(2, 2, "zone1", 2, &vec![]);

        let existing = make_anyapplication_with_conditions(1, 1, "zone1", 0, &vec![]);

        assert_eq!(
            UpdateResult::Update {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .local_update(Some(existing), incoming, 2, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_skip() {
        let incoming = anyapp(2, "zone1", 2);

        assert_eq!(
            UpdateResult::DoNothing,
            AnyApplicationMerge::new()
                .local_delete(None, incoming, 2, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete() {
        let current = make_anyapplication_with_conditions(1, 1, "zone1", 1, &vec![]);

        let incoming = make_anyapplication_with_conditions(2, 2, "zone1", 1, &vec![]);

        assert_eq!(
            UpdateResult::Delete {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .local_delete(Some(current), incoming, 2, &"zone1")
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
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 3, &vec![anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone(
                    "zone2",
                    4,
                    &vec![anycond_status("zone2", "type", "updated")],
                ),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            5,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone(
                    "zone2",
                    4,
                    &vec![anycond_status("zone2", "type", "updated")],
                ),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update { object: expected },
            strategy
                .local_update(Some(current), incoming, 5, &"zone2")
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
            &vec![anyzone("zone1", 2, &vec![anycond("zone1", "type")])],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 3, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            5,
            "zone1",
            0,
            &vec![
                anyzone("zone1", 3, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update { object: expected },
            strategy
                .local_update(Some(current), incoming, 5, &"zone2")
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
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 3, &vec![anycond("zone2", "type")]),
            ],
        );

        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            4,
            "zone1",
            0,
            &vec![
                anyzone("zone1", 2, &vec![anycond("zone1", "type")]),
                anyzone("zone2", 4, &vec![]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update { object: expected },
            strategy
                .local_update(Some(current), incoming, 4, &"zone2")
                .unwrap()
        );
    }
}
