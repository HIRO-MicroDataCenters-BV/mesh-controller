use super::{
    anyapplication::{
        AnyApplication, AnyApplicationSpec, AnyApplicationStatus, AnyApplicationStatusConditions,
        AnyApplicationStatusPlacements,
    },
    anyapplication_ext::AnyApplicationExt,
    types::{MergeResult, MergeStrategy, UpdateResult},
};
use crate::kube::{pool::Version, dynamic_object_ext::DynamicObjectExt};
use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

pub struct AnyApplicationMerge {
    gvk: GroupVersionKind,
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

        let object = current.to_object()?;
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
        let maybe_status = self.merge_ownership_section(
            current,
            current_owner_version,
            incoming,
            incoming_owner_version,
            incoming_zone,
            incoming_owner_zone,
        );

        let maybe_conditions = self.merge_conditions_section(
            current
                .as_ref()
                .and_then(|v| v.conditions.as_ref())
                .unwrap_or(&vec![]),
            incoming
                .as_ref()
                .and_then(|v| v.conditions.as_ref())
                .unwrap_or(&vec![]),
            incoming_zone,
            incoming
                .as_ref()
                .and_then(|v| v.placements.as_ref())
                .unwrap_or(&vec![]),
        );

        if let Some(mut status) = maybe_status {
            maybe_conditions.into_iter().for_each(|conditions| {
                status.conditions = Some(conditions);
            });
            Some(status)
        } else if let Some(conditions) = maybe_conditions {
            if let Some(status) = current {
                let mut status = status.clone();
                status.conditions = Some(conditions);
                Some(status)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn merge_ownership_section(
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
            match (current, incoming) {
                (Some(_) | None, Some(incoming)) => {
                    let mut target = incoming.clone();
                    target.conditions = None;
                    Some(target)
                }
                (_, None) => None,
            }
        } else {
            None
        }
    }
    // only owner should be allowed to merge condition section from placements section
    fn merge_conditions_section(
        &self,
        current: &Vec<AnyApplicationStatusConditions>,
        incoming: &Vec<AnyApplicationStatusConditions>,
        incoming_zone: &str,
        placements: &Vec<AnyApplicationStatusPlacements>,
    ) -> Option<Vec<AnyApplicationStatusConditions>> {
        let acceptable_zone = placements
            .iter()
            .find(|p| p.zone == incoming_zone)
            .is_some();
        if !acceptable_zone {
            return None;
        }

        let incoming_owned_by_zone = incoming.into_iter().filter(|v| v.zone_id == incoming_zone);
        let mut target = current.clone();
        let mut updated = false;
        for incoming_cond in incoming_owned_by_zone {
            let found = target
                .iter_mut()
                .find(|v| v.zone_id == incoming_cond.zone_id && v.r#type == incoming_cond.r#type);
            if let Some(current_cond) = found {
                if incoming_cond.zone_version > current_cond.zone_version {
                    *current_cond = incoming_cond.clone();
                    updated = true
                }
            } else {
                updated = true;
                target.push(incoming_cond.clone());
            }
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

        let acceptable_zone = incoming_zone == &incoming_owner_zone;
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
}

impl MergeStrategy for AnyApplicationMerge {
    fn mesh_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &str,
    ) -> Result<MergeResult> {
        let incoming = incoming.clone();
        if let Some(current) = current {
            self.merge_update_internal(current, incoming.clone(), &incoming_zone)
        } else {
            self.merge_create_internal(incoming, &incoming_zone)
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
        let is_acceptable_zone =
            incoming.get_owner_zone() == incoming_zone || placements_zones.contains(incoming_zone);

        if !is_acceptable_zone {
            return Ok(UpdateResult::DoNothing);
        }

        if let Some(current) = current {
            let mut current: AnyApplication = current.clone().try_parse()?;
            let mut updated = false;
            if incoming.get_owner_zone() == incoming_zone {
                current.set_owner_version(incoming_version);
                if current.spec != incoming.spec {
                    current.spec = incoming.spec;
                    updated = true;
                }
            }

            // TODO
            // for owner zone we diff - the spec, status and zone conditions
            // for placements zones we diff only conditions
            
            if updated {
                let object = current.to_object()?;                
                return Ok(UpdateResult::Update { object });
            } else {
                return Ok(UpdateResult::DoNothing);
            }
        } else {
            incoming.set_owner_version(incoming_version);
            let object = incoming.to_object()?;
            return Ok(UpdateResult::Create { object });
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
        let is_current_zone = incoming.get_owner_zone() == incoming_zone;
        if !is_current_zone {
            return Ok(UpdateResult::DoNothing);
        }

        if current.is_some() {
            incoming.set_owner_version(incoming_version);
            let object = incoming.to_object()?;
            return Ok(UpdateResult::Delete { object });
        } else {
            return Ok(UpdateResult::DoNothing);
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
    use crate::kube::dynamic_object_ext::DynamicObjectExt;
    use crate::merge::anyapplication_strategy::AnyApplicationMerge;
    use crate::merge::anyapplication_test_support::tests::make_anyapplication;
    use crate::merge::types::MergeResult;
    use crate::merge::types::MergeStrategy;
    use crate::merge::types::UpdateResult;

    #[test]
    pub fn non_existing_create() {
        let incoming = make_anyapplication(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy.mesh_update(None, &incoming, &"zone1").unwrap()
        );
    }

    #[test]
    pub fn non_existing_other_zone() {
        let incoming = make_anyapplication(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy.mesh_update(None, &incoming, &"test").unwrap()
        );
    }

    #[test]
    pub fn update_same_version() {
        let current = make_anyapplication(1, "zone1", 0);
        let incoming = make_anyapplication(1, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_update(Some(current), &incoming, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn update_greater_version_spec() {
        let current = make_anyapplication(1, "zone1", 0);
        let incoming = make_anyapplication(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            strategy
                .mesh_update(Some(current.clone()), &incoming, &"zone1")
                .unwrap()
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_update(Some(current), &incoming, &"zone2")
                .unwrap()
        );
    }

    #[test]
    pub fn update_greater_version_status_ownership() {
        let current = make_anyapplication(1, "zone1", 0);
        let incoming = make_anyapplication(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned()
            },
            strategy
                .mesh_update(Some(current), &incoming, &"zone1")
                .unwrap()
        );
    }

    // #[test]
    // pub fn update_greater_version_status_conditions() {
    //     let current = make_anyapplication(1, "zone1", 0);
    //     let incoming = make_anyapplication(2, "zone1", 1);

    //     let strategy = AnyApplicationMerge::new();
    //     assert_eq!(
    //         MergeResult::Update {
    //             object: incoming.to_owned()
    //         },
    //         strategy
    //             .merge_update(Some(current), &incoming, &"zone1")
    //             .unwrap()
    //     );
    // }

    #[test]
    pub fn update_other_zone() {}

    #[test]
    pub fn non_existing_delete() {
        let incoming = make_anyapplication(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy.mesh_delete(None, &incoming, &"zone1").unwrap()
        );
    }

    #[test]
    pub fn the_same_version_delete() {
        let current = make_anyapplication(1, "zone1", 0);
        let incoming = make_anyapplication(1, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .mesh_delete(Some(current), &incoming, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn greater_version_delete() {
        let current = make_anyapplication(1, "zone1", 0);
        let incoming = make_anyapplication(2, "zone1", 1);

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

    // TODO update condition from not an owner, but from placement
    #[test]
    pub fn update_from_replica_zone() {}

    #[test]
    pub fn local_create() {
        let incoming = make_anyapplication(1, "zone1", 0);

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .local_update(None, incoming, 2, &"test")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip() {
        let incoming = make_anyapplication(1, "zone1", 2);
        let existing = make_anyapplication(1, "zone1", 1);

        assert_eq!(
            UpdateResult::DoNothing,
            AnyApplicationMerge::new()
                .local_update(Some(existing), incoming, 1, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update() {
        let incoming = make_anyapplication(2, "zone1", 2);
        let existing = make_anyapplication(1, "zone1", 0);

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
        let incoming = make_anyapplication(2, "zone1", 2);

        assert_eq!(
            UpdateResult::DoNothing,
            AnyApplicationMerge::new()
                .local_delete(None, incoming, 2, &"zone1")
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete() {
        let incoming = make_anyapplication(2, "zone1", 2);

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone()
            },
            AnyApplicationMerge::new()
                .local_update(None, incoming, 2, &"zone1")
                .unwrap()
        );
    }
}
