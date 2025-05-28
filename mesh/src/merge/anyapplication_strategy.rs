use super::{
    anyapplication::{
        AnyApplication, AnyApplicationSpec, AnyApplicationStatus, AnyApplicationStatusConditions,
        AnyApplicationStatusPlacements,
    },
    anyapplication_ext::AnyApplicationExt,
    types::{MergeResult, MergeStrategy},
};
use crate::kube::{cache::Version, dynamic_object_ext::DynamicObjectExt};
use anyhow::Result;
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
            self.merge_create_internal(incoming, &incoming_zone)
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
    use crate::kube::dynamic_object_ext::DynamicObjectExt;
    use crate::merge::anyapplication_strategy::AnyApplicationMerge;
    use crate::merge::anyapplication_test_support::tests::make_anyapplication;
    use crate::merge::types::MergeResult;
    use crate::merge::types::MergeStrategy;

    #[test]
    pub fn non_existing_create() {
        let incoming = make_anyapplication(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy.merge_update(None, &incoming, &"zone1").unwrap()
        );
    }

    #[test]
    pub fn non_existing_other_zone() {
        let incoming = make_anyapplication(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy.merge_update(None, &incoming, &"test").unwrap()
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
                .merge_update(Some(current), &incoming, &"zone1")
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
                .merge_update(Some(current.clone()), &incoming, &"zone1")
                .unwrap()
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::DoNothing,
            strategy
                .merge_update(Some(current), &incoming, &"zone2")
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
                .merge_update(Some(current), &incoming, &"zone1")
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
            strategy.merge_delete(None, &incoming, &"zone1").unwrap()
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
                .merge_delete(Some(current), &incoming, &"zone1")
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
                name: current.get_namespaced_name()
            },
            strategy
                .merge_delete(Some(current), &incoming, &"zone1")
                .unwrap()
        );
    }

    // TODO update condition from not an owner, but from placement
}
