use std::collections::BTreeMap;

use super::types::{MergeResult, MergeStrategy, UpdateResult};

use crate::{
    merge::types::{Tombstone, VersionedObject},
    mesh::{event::MeshEvent, topic::InstanceId},
    network::discovery::types::Membership,
};
use anyapplication::{
    anyapplication::{
        AnyApplication, AnyApplicationSpec, AnyApplicationStatus,
        AnyApplicationStatusOwnershipPlacements, AnyApplicationStatusZones,
    },
    anyapplication_ext::{AnyApplicationExt, AnyApplicationStatusOwnershipExt},
};
use anyhow::{Context, Result};
use kube::api::{DynamicObject, GroupVersionKind};
use meshkube::kube::{
    dynamic_object_ext::{DynamicObjectExt, dump_zones},
    subscriptions::Version,
    types::NamespacedName,
};
use tracing::{Span, debug, warn};

pub struct AnyApplicationMerge {
    gvk: GroupVersionKind,
}

impl MergeStrategy for AnyApplicationMerge {
    fn mesh_update(
        &self,
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        received_from_zone: &str,
        node_zone: &str,
        membership: &Membership,
    ) -> Result<MergeResult> {
        match current {
            VersionedObject::Object(current) => self.mesh_update_update(
                span,
                current,
                incoming,
                received_from_zone,
                node_zone,
                membership,
            ),
            VersionedObject::NonExisting => {
                self.mesh_update_create(span, incoming, received_from_zone)
            }
            VersionedObject::Tombstone(tombstone) => {
                self.mesh_update_tombstone(span, tombstone, incoming, received_from_zone)
            }
        }
    }

    fn mesh_delete(
        &self,
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_zone: &str,
        now_millis: u64,
    ) -> Result<MergeResult> {
        match current {
            VersionedObject::Object(current) => {
                self.mesh_delete_internal(span, current, incoming, incoming_zone, now_millis)
            }
            VersionedObject::NonExisting => Ok(MergeResult::Tombstone(Tombstone {
                gvk: self.gvk.to_owned(),
                name: incoming.get_namespaced_name(),
                owner_version: incoming.get_owner_version_or_fail()?,
                owner_zone: incoming_zone.into(),
                resource_version: 0,
                deletion_timestamp: now_millis,
            })),
            VersionedObject::Tombstone(tombstone) => {
                let name = incoming.get_namespaced_name();
                let incoming: AnyApplication = incoming.try_parse()?;
                let incoming_owner_version = incoming.get_owner_version()?;
                let incoming_timestamp = now_millis;

                let is_same_zone_greater_version = tombstone.owner_zone == incoming_zone
                    && tombstone.owner_version < incoming_owner_version;
                let is_other_zone_greater_timestamp = tombstone.owner_zone != incoming_zone
                    && tombstone.deletion_timestamp < incoming_timestamp;

                if is_same_zone_greater_version || is_other_zone_greater_timestamp {
                    if is_same_zone_greater_version {
                        debug!(parent: span, %name, "mesh delete over tombstone - new version received");
                    } else {
                        debug!(parent: span, %name, "mesh delete over tombstone - new epoch received");
                    }

                    Ok(MergeResult::Tombstone(Tombstone {
                        gvk: self.gvk.to_owned(),
                        name,
                        owner_version: incoming_owner_version,
                        owner_zone: incoming_zone.into(),
                        resource_version: tombstone.resource_version,
                        deletion_timestamp: now_millis,
                    }))
                } else {
                    debug!(parent: span, %name, "mesh delete over tombstone, skipping");
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
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_resource_version: Version,
        node_zone: &str,
        now_millis: u64,
    ) -> Result<UpdateResult> {
        let name = incoming.get_namespaced_name();
        // Object is in deleting state
        if incoming.metadata.deletion_timestamp.is_some() {
            debug!(parent: span, %name, "deletion timestamp is set, skipping");
            return Ok(UpdateResult::Skip);
        }

        let mut incoming: AnyApplication = incoming.clone().try_parse()?;

        let is_acceptable_zone = incoming.is_acceptable_zone(node_zone);
        if !is_acceptable_zone {
            debug!(parent: span, %name, "not acceptable zone, skipping");
            return Ok(UpdateResult::Skip);
        }
        let is_owned_zone = incoming.is_owned_zone(node_zone);
        match current {
            VersionedObject::Object(current) => {
                let mut current: AnyApplication = current.clone().try_parse()?;
                let maybe_current_version = current.get_resource_version();
                if maybe_current_version.is_none() {
                    warn!(parent: span, %name, "resource version is not set, probably new resource");
                }
                // version=0 typically means that resource is new and it did not get the resource_version yet
                let current_version = maybe_current_version.unwrap_or(0);
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
                    debug!(parent: span, %name, "resource updated");
                    Ok(UpdateResult::Update {
                        object,
                        version: incoming_resource_version,
                    })
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
                debug!(parent: span, %name, "resource created, previous non-existing");
                Ok(UpdateResult::Create {
                    object,
                    version: incoming_resource_version,
                })
            }
            VersionedObject::Tombstone(tombstone) => {
                let incoming_timestamp = now_millis;
                let incoming_owner_version = incoming_resource_version;

                let is_same_zone_greater_version = tombstone.owner_zone == node_zone
                    && tombstone.owner_version < incoming_owner_version;
                let is_other_zone_greater_timestamp = tombstone.owner_zone != node_zone
                    && tombstone.deletion_timestamp < incoming_timestamp;

                if is_same_zone_greater_version || is_other_zone_greater_timestamp {
                    if is_same_zone_greater_version {
                        debug!(parent: span, %name, "mesh update over tombstone - creating new as new version is greater");
                    } else {
                        debug!(parent: span, %name, "mesh update over tombstone - creating new as new epoch is received");
                    }

                    if is_owned_zone {
                        incoming.set_owner_version(incoming_resource_version);
                    }
                    let mut object = incoming.to_object()?;
                    object.set_resource_version(incoming_resource_version);
                    debug!(parent: span, %name, "resource created, previous tombstone");
                    Ok(UpdateResult::Create {
                        object,
                        version: incoming_resource_version,
                    })
                } else {
                    debug!(parent: span, %name, "update over tombstone, tombstone.owner version is high than incoming version, skipping");
                    Ok(UpdateResult::Skip)
                }
            }
        }
    }

    fn kube_delete(
        &self,
        span: &Span,
        current: VersionedObject,
        incoming: DynamicObject,
        incoming_version: Version,
        node_zone: &str,
        now_millis: u64,
    ) -> Result<UpdateResult> {
        let name = incoming.get_namespaced_name();
        let mut incoming: AnyApplication = incoming.clone().try_parse()?;
        // Delete is allowed only from owning zone
        let is_owned_zone = incoming.is_owned_zone(node_zone);
        if !is_owned_zone {
            return Ok(UpdateResult::Skip);
        }

        match current {
            VersionedObject::Object(_) => {
                incoming.set_owner_version(incoming_version);
                let mut object = incoming.to_object()?;

                object.set_resource_version(incoming_version);
                debug!(parent: span, %name, "resource deleted, previous existing");
                Ok(UpdateResult::Delete {
                    object,
                    version: incoming_version,
                    tombstone: Tombstone {
                        gvk: self.gvk.to_owned(),
                        name,
                        owner_version: incoming_version,
                        owner_zone: node_zone.to_owned(),
                        resource_version: incoming_version,
                        deletion_timestamp: now_millis,
                    },
                })
            }
            VersionedObject::NonExisting => {
                debug!(parent: span, %name, "resource tombstone created, previous non-existing");
                Ok(UpdateResult::Tombstone(Tombstone {
                    gvk: self.gvk.to_owned(),
                    name,
                    owner_version: incoming_version,
                    owner_zone: node_zone.to_owned(),
                    resource_version: incoming_version,
                    deletion_timestamp: now_millis,
                }))
            }
            VersionedObject::Tombstone(tombstone) => {
                let max_version = Version::max(tombstone.owner_version, incoming_version);
                debug!(parent: span, %name, "resource tombstone updated, previous tombstone");
                Ok(UpdateResult::Tombstone(Tombstone {
                    gvk: self.gvk.to_owned(),
                    name,
                    owner_version: max_version,
                    owner_zone: node_zone.to_owned(),
                    resource_version: tombstone.resource_version,
                    deletion_timestamp: now_millis,
                }))
            }
        }
    }

    fn tombstone(&self, current: VersionedObject, now_millis: u64) -> Result<Option<Tombstone>> {
        match current {
            VersionedObject::Object(current) => {
                let name = current.get_namespaced_name();
                let resource_version = current.get_resource_version();

                let current: AnyApplication = current.try_parse()?;
                let owner_zone = current.get_owner_zone();
                let owner_version = current.get_owner_version().unwrap_or(0);
                let tombstone = Tombstone {
                    gvk: self.gvk.to_owned(),
                    name,
                    owner_version,
                    owner_zone: owner_zone.to_owned(),
                    resource_version,
                    deletion_timestamp: now_millis,
                };
                Ok(Some(tombstone))
            }
            VersionedObject::NonExisting => Ok(None),
            VersionedObject::Tombstone(tombstone) => Ok(Some(tombstone.clone())),
        }
    }

    fn mesh_membership_change(
        &self,
        span: &Span,
        current: VersionedObject,
        membership: &Membership,
        node_zone: &str,
    ) -> Result<Vec<MergeResult>> {
        match current {
            VersionedObject::Object(current) => {
                let resource_version = current.get_resource_version();
                let name = current.get_namespaced_name();
                let mut current: AnyApplication = current.try_parse()?;
                let maybe_instance = membership.get_instance(&current.get_owner_zone());
                if maybe_instance.is_some() {
                    return Ok(vec![]);
                };

                let mut placement_zones: Vec<&InstanceId> = current
                    .get_placement_zones()
                    .iter()
                    .flat_map(|zone| membership.get_instance(zone))
                    .collect();
                placement_zones.sort_by_key(|x| x.start_time);
                if let Some(instance) = placement_zones
                    .first()
                    .cloned()
                    .or(membership.default_owner())
                    && instance.zone == node_zone
                        && let Some(status) = &mut current.status {
                            status.ownership.owner = instance.zone.to_owned();
                            status.ownership.epoch += 1;
                            debug!(parent: span, %name, "taking over ownership: new epoch = {}", status.ownership.epoch);
                            let merge_result = current.clone().to_object()?;
                            let mut event_object = current.to_object()?;
                            event_object.set_owner_version(resource_version);
                            event_object.unset_resource_version();
                            return Ok(vec![MergeResult::Update {
                                object: merge_result,
                                event: Some(MeshEvent::Update {
                                    object: event_object,
                                    version: 0,
                                }),
                            }]);
                        }
                Ok(vec![])
            }
            VersionedObject::NonExisting | VersionedObject::Tombstone(_) => Ok(vec![]),
        }
    }

    fn construct_remote_versions(
        &self,
        span: &Span,
        snapshot: &BTreeMap<NamespacedName, DynamicObject>,
        node_zone: &str,
    ) -> Result<BTreeMap<String, Version>> {
        let mut remote_zone_versions: BTreeMap<String, Version> = BTreeMap::new();
        for object in snapshot.values() {
            let remote_version = object.get_owner_version();
            let name = object.get_namespaced_name();
            let application: AnyApplication = object
                .to_owned()
                .try_parse()
                .context("parsing AnyApplication resource from snapshot")?;
            let zone = application.get_owner_zone();
            if zone != node_zone {
                if let Some(remote_version) = remote_version {
                    let current = remote_zone_versions.entry(zone).or_insert(remote_version);
                    *current = Version::max(*current, remote_version);
                } else {
                    warn!(parent: span, %name, "kube apply snapshot (initial), resource has no owner version. Skipping...")
                }
            }
        }
        Ok(remote_zone_versions)
    }
}

impl Default for AnyApplicationMerge {
    fn default() -> Self {
        Self::new()
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
        span: &Span,
        current: DynamicObject,
        incoming: DynamicObject,
        received_from_zone: &str,
        node_zone: &str,
        membership: &Membership,
    ) -> Result<MergeResult> {
        let name = current.get_namespaced_name();
        let mut current: AnyApplication = current.try_parse()?;
        let current_owner_version = current.get_owner_version()?;
        let current_owner_zone = current.get_owner_zone();
        let current_owner_epoch = current.get_owner_epoch();

        let incoming: AnyApplication = incoming.try_parse()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone();
        let incoming_owner_epoch = incoming.get_owner_epoch();
        let incoming_zones = incoming.get_status_zone_ids();

        debug!(parent: span, %name, "merge-update, current{{version = {current_owner_version}, zone = {current_owner_zone}, epoch = {current_owner_epoch} }}, incoming {{ version = {incoming_owner_version}, zone = {incoming_owner_zone}, epoch = {incoming_owner_epoch} }}");

        let mut event: Option<MeshEvent> = None;

        // 1. Owner is merging statuses from local zones
        // if current.ownership.zone == incoming.ownership.zone
        //    && incoming.ownership.zone == node_zone
        //    && zone_statuses.contain(receive_from_zone)
        // then if incoming.ownership.epoch == current.ownership.epoch
        //    merge zone statuses if zone status version is higher than current

        if current_owner_zone == incoming_owner_zone
            && incoming_owner_zone == node_zone
            && incoming_zones.contains(received_from_zone)
            && incoming_owner_epoch == current_owner_epoch
        {
            let mut updated = false;
            let maybe_merged_zones = self.merge_zone_statuses(&current.status, &incoming.status);
            if let Some(zones) = maybe_merged_zones
                && let Some(status) = current.status.as_mut() {
                    status.zones = Some(zones);
                    updated = true;
                };
            if updated {
                debug!(parent: span, %name, "owner merge local zone statuses");
                let object = current.to_object()?;
                object.dump_status("merge_update_internal - merging local zone statuses");
                return Ok(MergeResult::Update { object, event });
            }
        }

        // 2.1 Non owners are receiving changes from owner
        // if current.ownership.zone == incoming.ownership.zone
        //    && received_from_zone == incoming.ownership.zone
        //    && incoming.ownership.owner != node_zone
        //    if incoming ownership epoch is HIGH_OR_EQUAL to current one
        //      merge ownership section from incoming if owner version is higher than current
        //      merge zone statuses if zone status version is higher than current
        //      merge spec from incoming if owner version is higher than current
        // 2.2 Non owners are receiving changes from new owner
        // if current.ownership.zone != incoming.ownership.zone
        //    && received_from_zone == incoming.ownership.zone
        //    && incoming.ownership.owner != node_zone
        //    if incoming ownership epoch is HIGH to current one
        //      merge ownership section from incoming
        //      merge zone statuses from incoming
        //      merge spec from incoming

        if incoming_owner_zone == received_from_zone && incoming_owner_zone != node_zone {
            let mut updated = false;
            if current_owner_zone == incoming_owner_zone
                && incoming_owner_epoch >= current_owner_epoch
            {
                let merged_spec = self.merge_spec(
                    current_owner_version,
                    &incoming.spec,
                    incoming_owner_version,
                );
                let merged_status = self.merge_status(
                    &current.status,
                    current_owner_version,
                    &incoming.status,
                    incoming_owner_version,
                );
                if let Some(spec) = merged_spec {
                    current.spec = spec;
                    updated = true;
                }
                if let Some(status) = merged_status {
                    current.status = Some(status);
                    updated = true;
                }
                current.set_owner_version(incoming_owner_version);
                debug!(parent: span, %name, "non owner receives update from owner");
            } else if current_owner_zone != incoming_owner_zone
                && incoming_owner_epoch > current_owner_epoch
            {
                current.spec = incoming.spec.clone();
                current.status = incoming.status.clone();
                current.set_owner_version(incoming_owner_version);
                updated = true;
                debug!(parent: span, %name, "non owner receives update from owner with new epoch");
            }

            if updated {
                let object = current.to_object()?;
                object.dump_status("merge_update_internal - non owner merge");
                return Ok(MergeResult::Update { object, event });
            }
        }

        // 3. Merge owners
        // if current.ownership.zone != incoming.ownership.zone
        //    && current.ownership.zone == node_zone
        //    && received_from_zone == incoming.ownership.zone
        //    && incoming.ownership.zone != node_zone
        //   if current.ownership.epoch > incoming.ownership.epoch
        //      merge ownership section from current
        //        - merge placements from current and incoming
        //      merge spec from current
        //      merge zone statuses from both if zone status version is higher than current
        //
        //   if current.ownership.epoch == incoming.ownership.epoch
        //      if current.startTime is greater then incoming.startTime
        //          merge ownership section from current
        //          - merge placements from current and incoming
        //          - increment epoch
        //      merge zone statuses from both if zone status version is higher than current
        //
        //   if current.ownership.epoch < incoming.ownership.epoch
        //      merge ownership section from incoming
        //      merge zone statuses if zone status version is higher than current
        //      merge spec from incoming

        if current_owner_zone != incoming_owner_zone
            && current_owner_zone == node_zone
            && received_from_zone == incoming_owner_zone
            && incoming_owner_zone != node_zone
        {
            let mut updated = false;
            match current_owner_epoch.cmp(&incoming_owner_epoch) {
                std::cmp::Ordering::Greater => {
                    if let Some(merged_current) =
                        self.merge_owners_current_greater(&current, &incoming)
                    {
                        current = merged_current;
                    }
                    updated = true;
                    debug!(parent: span, %name, "owner conflict: current owner confirms ownership");
                    current.set_owner_version(current_owner_version);

                    // Sending event to confirm the ownership
                    let mut event_object = current.clone().to_object()?;
                    event_object.set_owner_version(current_owner_version);
                    event_object.unset_resource_version();
                    debug!(parent: span, %name, "confirm ownership event");
                    event = Some(MeshEvent::Update {
                        object: event_object,
                        version: 0,
                    });
                }
                std::cmp::Ordering::Equal => {
                    //      if current.startTime is greater then incoming.startTime
                    //          merge ownership section from current
                    //          - merge placements from current and incoming
                    //          - increment epoch
                    //      merge zone statuses from both if zone status version is higher than current
                    let current_instance = membership.get_instance(&current_owner_zone);
                    let incoming_instance = membership.get_instance(&incoming_owner_zone);
                    let current_has_priority = match (current_instance, incoming_instance) {
                        (Some(current), Some(incoming)) => current.start_time < incoming.start_time,
                        (Some(_), None) | (None, None) => true,
                        (None, Some(_)) => false,
                    };
                    if current_has_priority {
                        if let Some(merged_current) =
                            self.merge_owners_current_greater(&current, &incoming)
                        {
                            current = merged_current;
                        }
                        if let Some(status) = current.status.as_mut() {
                            status.ownership.epoch += 1;
                        }
                        updated = true;
                        debug!(parent: span, %name, "owner conflict equal epoch: current owner takes ownership");
                        let mut event_object = current.clone().to_object()?;
                        event_object.set_owner_version(current_owner_version);
                        event_object.unset_resource_version();
                        debug!(parent: span, "establish ownership event");
                        event = Some(MeshEvent::Update {
                            object: event_object,
                            version: 0,
                        });
                    } else {
                        debug!(parent: span, %name, "owner conflict equal epoch: current owner follows");
                    }
                }
                std::cmp::Ordering::Less => {
                    current.spec = incoming.spec.clone();
                    current.status = incoming.status.clone();
                    current.set_owner_version(current_owner_version);
                    updated = true;
                    debug!(parent: span, %name, "owner conflict: current owner becomes follower, following_leader = {incoming_owner_zone}");
                }
            }

            if updated {
                let object = current.to_object()?;
                object.dump_status("merge_update_internal - owner merge");
                return Ok(MergeResult::Update { object, event });
            }
        }

        debug!(parent: span, %name, "skipping update");
        Ok(MergeResult::Skip)
    }

    fn merge_owners_current_greater(
        &self,
        current: &AnyApplication,
        incoming: &AnyApplication,
    ) -> Option<AnyApplication> {
        //      merge ownership section from current
        //        - merge placements from current and incoming
        //      merge spec from current
        //      merge zone statuses from both if zone status version is higher than current

        let mut updated = false;
        let mut current = current.clone();

        let maybe_merged_zones = self.merge_zone_statuses(&current.status, &incoming.status);

        let merged_placements =
            self.merge_placements_into_current(&current.status, &incoming.status);

        if let Some(zones) = maybe_merged_zones
            && let Some(status) = current.status.as_mut() {
                status.zones = Some(zones);
                updated = true;
            };

        if let Some(placements) = merged_placements
            && let Some(status) = current.status.as_mut() {
                status.ownership.placements = Some(placements);
                updated = true;
            };

        if updated { Some(current) } else { None }
    }

    fn merge_spec(
        &self,
        current_owner_version: Version,
        incoming: &AnyApplicationSpec,
        incoming_owner_version: Version,
    ) -> Option<AnyApplicationSpec> {
        if incoming_owner_version > current_owner_version {
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
    ) -> Option<AnyApplicationStatus> {
        let maybe_status = self.merge_ownership_section(
            current,
            current_owner_version,
            incoming,
            incoming_owner_version,
        );

        let maybe_zones = self.merge_zone_section(
            current
                .as_ref()
                .and_then(|v| v.zones.as_ref())
                .unwrap_or(&vec![]),
            incoming
                .as_ref()
                .and_then(|v| v.zones.as_ref())
                .unwrap_or(&vec![]),
        );

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

    fn merge_ownership_section(
        &self,
        current: &Option<AnyApplicationStatus>,
        current_owner_version: Version,
        incoming: &Option<AnyApplicationStatus>,
        incoming_owner_version: Version,
    ) -> Option<AnyApplicationStatus> {
        let new_change = incoming_owner_version > current_owner_version;
        if new_change {
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

    fn merge_zone_statuses(
        &self,
        current: &Option<AnyApplicationStatus>,
        incoming: &Option<AnyApplicationStatus>,
    ) -> Option<Vec<AnyApplicationStatusZones>> {
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
    }

    fn merge_placements_into_current(
        &self,
        current: &Option<AnyApplicationStatus>,
        incoming: &Option<AnyApplicationStatus>,
    ) -> Option<Vec<AnyApplicationStatusOwnershipPlacements>> {
        let empty_vec = Vec::new();
        let current_placements = current
            .as_ref()
            .and_then(|v| v.ownership.placements.as_ref())
            .unwrap_or(&empty_vec);
        let incoming_placements = incoming
            .as_ref()
            .and_then(|v| v.ownership.placements.as_ref())
            .unwrap_or(&empty_vec);
        let mut updated = false;
        let mut target = current_placements.clone();

        for incom in incoming_placements.iter() {
            let found = current_placements.iter().find(|v| v.zone == incom.zone);
            if found.is_none() {
                target.push(incom.to_owned());
                updated = true;
            }
        }
        if updated { Some(target) } else { None }
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
        span: &Span,
        incoming: DynamicObject,
        received_from_zone: &str,
    ) -> Result<MergeResult> {
        let name = incoming.get_namespaced_name();
        let mut incoming: AnyApplication = incoming.try_parse()?;
        let incoming_owner_zone = incoming.get_owner_zone();
        // Create is possible from owning zone
        let acceptable_zone = received_from_zone == incoming_owner_zone;
        if acceptable_zone {
            incoming.metadata.managed_fields = None;
            incoming.metadata.uid = None;
            debug!(parent: span, %name, "merge update over non existing: create new");
            let object = incoming.to_object()?;
            Ok(MergeResult::Create { object })
        } else {
            debug!(parent: span, %name, "merge update over non existing: skipping update");
            Ok(MergeResult::Skip)
        }
    }

    fn mesh_update_tombstone(
        &self,
        span: &Span,
        current: Tombstone,
        incoming: DynamicObject,
        received_from_zone: &str,
    ) -> Result<MergeResult> {
        let name = current.name;
        let current_owner_version = current.owner_version;
        let current_owner_zone = current.owner_zone;
        let current_delete_timestamp = current.deletion_timestamp as i64;

        let incoming_app: AnyApplication = incoming.clone().try_parse()?;
        let incoming_owner_version = incoming_app.get_owner_version()?;
        let incoming_owner_zone = incoming_app.get_owner_zone();
        let incoming_owner_epoch = incoming_app.get_owner_epoch();
        let incoming_create_timestamp = incoming_app.get_create_timestamp();

        debug!(parent: span, %name, "merge-update-tombstone, current{{version = {current_owner_version}, zone = {current_owner_zone}, delete_timestamp = {current_delete_timestamp} }}, incoming {{ version = {incoming_owner_version}, zone = {incoming_owner_zone}, epoch = {incoming_owner_epoch}, create_timestamp = {incoming_create_timestamp} }}");
        // tombstone update is possible only from owning zone
        let acceptable_zone = incoming_owner_zone == received_from_zone;
        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        let is_same_zone_greater_version = incoming_owner_zone == current_owner_zone
            && current_owner_version < incoming_owner_version;
        let is_other_zone_greater_timestamp = incoming_owner_zone != current_owner_zone
            && current_delete_timestamp < incoming_create_timestamp;
        if is_other_zone_greater_timestamp || is_same_zone_greater_version {
            // tomstone is older than incoming =>
            debug!(parent: span, %name, "merge update over tombstone: creating resource");
            self.mesh_update_create(span, incoming, received_from_zone)
        } else {
            debug!(parent: span, %name, "merge update over tombstone: skipping update");
            // tombstone is newer than incoming => ignore
            Ok(MergeResult::Skip)
        }
    }

    fn mesh_delete_internal(
        &self,
        span: &Span,
        current: DynamicObject,
        incoming: DynamicObject,
        received_from_zone: &str,
        now_millis: u64,
    ) -> Result<MergeResult> {
        let current_resource_version = current.get_resource_version();
        let current: AnyApplication = current.try_parse()?;
        let current_owner_version = current.get_owner_version()?;
        let current_owner_zone = current.get_owner_zone();
        let current_owner_epoch = current.get_owner_epoch();

        let name = incoming.get_namespaced_name();
        let incoming: AnyApplication = incoming.to_owned().try_parse()?;
        let incoming_owner_version = incoming.get_owner_version()?;
        let incoming_owner_zone = incoming.get_owner_zone();
        let incoming_owner_epoch = incoming.get_owner_epoch();

        let acceptable_zone = received_from_zone == incoming_owner_zone;
        if !acceptable_zone {
            return Ok(MergeResult::Skip);
        }

        let is_same_zone_greater_version = incoming_owner_zone == current_owner_zone
            && current_owner_version < incoming_owner_version;
        let is_other_zone_greater_epoch =
            incoming_owner_zone != current_owner_zone && current_owner_epoch < incoming_owner_epoch;

        if is_same_zone_greater_version || is_other_zone_greater_epoch {
            let deletion_timestamp = now_millis;
            if is_same_zone_greater_version {
                debug!(parent: span, %name, "mesh delete - new version received");
            } else {
                debug!(parent: span, %name, "mesh delete - new epoch received");
            }
            Ok(MergeResult::Delete(Tombstone {
                gvk: self.gvk.to_owned(),
                name,
                owner_version: incoming_owner_version,
                owner_zone: incoming_owner_zone,
                resource_version: current_resource_version,
                deletion_timestamp,
            }))
        } else {
            debug!(parent: span, %name, "skipping delete");
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
                let ownership_diff = !current.ownership.is_equal(&incoming.ownership);
                if ownership_diff {
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

    use tracing::Level;
    use tracing::span;

    use super::*;
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
    use crate::mesh::event::MeshEvent;
    use crate::mesh::topic::InstanceId;

    #[test]
    pub fn mesh_update_create_non_existing() {
        let span = span!(Level::DEBUG, "mesh_update_create_non_existing");
        let membership = Membership::default();
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy
                .mesh_update(
                    &span,
                    VersionedObject::NonExisting,
                    incoming,
                    "zone1",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_create_tombstone_different_zone() {
        let span = span!(Level::DEBUG, "mesh_update_create_tombstone_different_zone");
        let membership = Membership::default();
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
                .mesh_update(&span, existing, incoming, "zone1", "zone1", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_create_tombstone_same_zone() {
        let span = span!(Level::DEBUG, "mesh_update_create_tombstone_same_zone");
        let membership = Membership::default();
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 0,
            owner_zone: "zone1".into(),
            resource_version: 1,
            deletion_timestamp: 0,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Create {
                object: incoming.clone()
            },
            strategy
                .mesh_update(&span, existing, incoming, "zone1", "zone2", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_create_skip_if_tombstone_is_newer() {
        let span = span!(
            Level::DEBUG,
            "mesh_update_create_skip_if_tombstone_is_newer"
        );
        let membership = Membership::default();
        let incoming = anyapp(1, "zone1", 0);
        let existing = VersionedObject::Tombstone(Tombstone {
            gvk: incoming.get_gvk().expect("gvk expected"),
            name: incoming.get_namespaced_name(),
            owner_version: 2,
            owner_zone: "zone2".into(),
            resource_version: 5,
            deletion_timestamp: 1000000,
        });

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(&span, existing, incoming, "zone1", "zone3", &membership)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_non_existing_other_zone() {
        let span = span!(Level::DEBUG, "mesh_update_non_existing_other_zone");
        let membership = Membership::default();
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(
                    &span,
                    VersionedObject::NonExisting,
                    incoming,
                    "test",
                    "test",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_same_version() {
        let span = span!(Level::DEBUG, "");
        let membership = Membership::default();
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(1, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone1",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_spec() {
        let span = span!(Level::DEBUG, "mesh_update_greater_version_spec");
        let membership = Membership::default();
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned(),
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.clone().into(),
                    incoming.clone(),
                    "zone1",
                    "zone2",
                    &membership
                )
                .unwrap()
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone2",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_status_ownership() {
        let span = span!(Level::DEBUG, "mesh_update_greater_version_status_ownership");
        let membership = Membership::default();
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned(),
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone1",
                    "zone2",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_status_conditions() {
        let span = span!(
            Level::DEBUG,
            "mesh_update_greater_version_status_conditions"
        );
        let membership = Membership::default();
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "zone1", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: incoming.to_owned(),
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone1",
                    "zone2",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_greater_version_unacceptable_zone() {
        let span = span!(
            Level::DEBUG,
            "mesh_update_greater_version_unacceptable_zone"
        );
        let membership = Membership::default();
        let current = anyapp(1, "zone1", 0);
        let incoming = anyapp(2, "unacceptable", 1);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone1",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_the_same_owner_greater_epoch() {
        let span = span!(Level::DEBUG, "mesh_update_the_same_owner_greater_epoch");
        let membership = Membership::default();

        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            2,
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            2,
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone1",
                    "zone3",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_different_owner_greater_epoch() {
        let span = span!(Level::DEBUG, "mesh_update_different_owner_greater_epoch");
        let membership = Membership::default();

        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            2,
            3,
            "zone2",
            2,
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            2,
            1,
            "zone2",
            2,
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone2",
                    "zone3",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_ownership_conflict_greater_epoch() {
        let span = span!(Level::DEBUG, "mesh_update_ownership_conflict_greater_epoch");
        let membership = Membership::default();

        let current = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            2,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            3,
            "zone3",
            1,
            1,
            &["zone2", "zone3"],
            &[
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            2,
            0,
            &["zone1", "zone2", "zone3"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );
        let mut expected_event = expected.clone();
        expected_event.unset_resource_version();

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: Some(MeshEvent::Update {
                    object: expected_event,
                    version: 0,
                }),
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone3",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_ownership_conflict_equal_epoch_current_owner() {
        let span = span!(Level::DEBUG, "");
        let mut membership = Membership::default();
        membership.add(InstanceId {
            zone: "zone1".into(),
            start_time: 1,
        });
        membership.add(InstanceId {
            zone: "zone3".into(),
            start_time: 2,
        });

        let current = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            3,
            "zone3",
            1,
            1,
            &["zone2", "zone3"],
            &[
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            2,
            0,
            &["zone1", "zone2", "zone3"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );
        let mut expected_event = expected.clone();
        expected_event.unset_resource_version();

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: Some(MeshEvent::Update {
                    object: expected_event,
                    version: 0,
                }),
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone3",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_ownership_conflict_equal_epoch_incoming_owner() {
        let span = span!(
            Level::DEBUG,
            "mesh_update_ownership_conflict_equal_epoch_incoming_owner"
        );
        let mut membership = Membership::default();
        membership.add(InstanceId {
            zone: "zone1".into(),
            start_time: 2,
        });
        membership.add(InstanceId {
            zone: "zone3".into(),
            start_time: 1,
        });

        let current = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            3,
            "zone3",
            1,
            1,
            &["zone2", "zone3"],
            &[
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone3",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_ownership_conflict_less_epoch() {
        let span = span!(Level::DEBUG, "mesh_update_ownership_conflict_less_epoch");
        let membership = Membership::default();

        let current = make_anyapplication_with_conditions(
            2,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[anycond("zone2", "type")]),
            ],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            3,
            "zone3",
            2,
            1,
            &["zone2", "zone3"],
            &[
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone3",
            2,
            1,
            &["zone2", "zone3"],
            &[
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
                anyzone("zone3", 4, &[anycond("zone3", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone3",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_non_existing_delete() {
        let span = span!(Level::DEBUG, "mesh_delete_non_existing_delete");
        let incoming = anyapp(1, "zone1", 0);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().unwrap(),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "zone1".into(),
                resource_version: 0,
                deletion_timestamp: 17,
            }),
            strategy
                .mesh_delete(&span, VersionedObject::NonExisting, incoming, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_from_the_same_zone() {
        let span = span!(Level::DEBUG, "");
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
                deletion_timestamp: 17,
            }),
            strategy
                .mesh_delete(&span, existing, incoming, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_existing_tombstone_has_higher_version() {
        let span = span!(
            Level::DEBUG,
            "mesh_delete_existing_tombstone_has_higher_version"
        );
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
            MergeResult::Skip,
            strategy
                .mesh_delete(&span, existing, incoming, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_tombstone_different_zone_newer_timestamp() {
        let span = span!(
            Level::DEBUG,
            "mesh_delete_tombstone_different_zone_newer_timestamp"
        );
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
            MergeResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().expect("gvk expected"),
                name: incoming.get_namespaced_name(),
                owner_version: 1,
                owner_zone: "zone1".into(),
                resource_version: 5,
                deletion_timestamp: 17
            }),
            strategy
                .mesh_delete(&span, existing, incoming, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_the_same_version_delete() {
        let span = span!(Level::DEBUG, "mesh_delete_the_same_version_delete");
        let incoming = anyapp(1, "zone1", 1);
        let mut current = anyapp(1, "zone1", 0);
        current.set_resource_version(10);

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Skip,
            strategy
                .mesh_delete(&span, current.into(), incoming, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_greater_version_delete() {
        let span = span!(Level::DEBUG, "mesh_delete_greater_version_delete");
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
                deletion_timestamp: 17,
            }),
            strategy
                .mesh_delete(&span, current.into(), incoming, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_update_condition_from_replica_zone() {
        let span = span!(Level::DEBUG, "");
        let membership = Membership::default();
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
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
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone2",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_create_condition_from_replica_zone() {
        let span = span!(Level::DEBUG, "mesh_create_condition_from_replica_zone");
        let membership = Membership::default();
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[anyzone("zone1", 2, &[anycond("zone1", "type")])],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone2",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    pub fn mesh_delete_condition_from_replica_zone() {
        let span = span!(Level::DEBUG, "mesh_delete_condition_from_replica_zone");
        let membership = Membership::default();
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
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
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 3, &[]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            MergeResult::Update {
                object: expected,
                event: None,
            },
            strategy
                .mesh_update(
                    &span,
                    current.into(),
                    incoming,
                    "zone2",
                    "zone1",
                    &membership
                )
                .unwrap()
        );
    }

    #[test]
    fn mesh_membership_change() {
        let span = span!(Level::DEBUG, "mesh_membership_change");
        let mut membership = Membership::default();
        membership.add(InstanceId::new("zone2".into()));
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 2, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            1,
            "zone2",
            2,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 2, &[anycond("zone2", "type")]),
            ],
        );
        let mut expected_event = expected.clone();
        expected_event.unset_resource_version();

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            vec![MergeResult::Update {
                object: expected,
                event: Some(MeshEvent::Update {
                    version: 0,
                    object: expected_event
                }),
            }],
            strategy
                .mesh_membership_change(&span, current.into(), &membership, "zone2")
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create_non_existing() {
        let span = span!(Level::DEBUG, "local_update_create_non_existing");
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 1, 0, &["zone1"], &[]);

        assert_eq!(
            UpdateResult::Create {
                object: incoming.clone(),
                version: 1,
            },
            AnyApplicationMerge::new()
                .kube_update(&span, VersionedObject::NonExisting, incoming, 1, "zone1", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_create_tombstone() {
        let span = span!(Level::DEBUG, "");
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 1, 0, &["zone1"], &[]);
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
                object: incoming.clone(),
                version: 1,
            },
            AnyApplicationMerge::new()
                .kube_update(&span, existing, incoming, 1, "zone1", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip_create_tombstone_is_newer() {
        let span = span!(Level::DEBUG, "local_update_skip_create_tombstone_is_newer");
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 1, 0, &["zone1"], &[]);

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
                .kube_update(&span, existing, incoming, 1, "zone1", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_skip_old_version() {
        let span = span!(Level::DEBUG, "");
        let incoming = make_anyapplication_with_conditions(1, 1, "zone1", 1, 0, &["zone1"], &[]);

        let existing = make_anyapplication_with_conditions(2, 2, "zone1", 1, 0, &["zone1"], &[]);

        assert_eq!(
            UpdateResult::Skip,
            AnyApplicationMerge::new()
                .kube_update(&span, existing.into(), incoming, 1, "zone1", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_incoming_has_diff() {
        let span = span!(Level::DEBUG, "local_update_incoming_has_diff");
        let incoming = make_anyapplication_with_conditions(1, 2, "zone1", 1, 2, &["zone1"], &[]);
        let existing = make_anyapplication_with_conditions(1, 1, "zone1", 1, 0, &["zone1"], &[]);

        let mut expected = incoming.clone();
        expected.set_owner_version(2);

        assert_eq!(
            UpdateResult::Update {
                object: expected,
                version: 2
            },
            AnyApplicationMerge::new()
                .kube_update(&span, existing.into(), incoming, 2, "zone1", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_skip_non_existing() {
        let span = span!(Level::DEBUG, "local_delete_skip_non_existing");
        let incoming = anyapp(2, "zone1", 2);

        assert_eq!(
            UpdateResult::Tombstone(Tombstone {
                gvk: incoming.get_gvk().expect("expect gvk"),
                name: incoming.get_namespaced_name(),
                owner_version: 2,
                owner_zone: "zone1".into(),
                resource_version: 2,
                deletion_timestamp: 17
            }),
            AnyApplicationMerge::new()
                .kube_delete(
                    &span,
                    VersionedObject::NonExisting,
                    incoming,
                    2,
                    "zone1",
                    17
                )
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_tombstone() {
        let span = span!(Level::DEBUG, "local_delete_tombstone");
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
                deletion_timestamp: 17,
            }),
            AnyApplicationMerge::new()
                .kube_delete(&span, existing, incoming, 2, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_tombstone_other_zone() {
        let span = span!(Level::DEBUG, "local_delete_tombstone_other_zone");
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
                deletion_timestamp: 17
            }),
            AnyApplicationMerge::new()
                .kube_delete(&span, existing, incoming, 2, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_incoming_resource_version_greater() {
        let span = span!(
            Level::DEBUG,
            "local_delete_incoming_resource_version_greater"
        );
        let current = make_anyapplication_with_conditions(1, 1, "zone1", 1, 1, &["zone1"], &[]);
        let incoming = make_anyapplication_with_conditions(1, 2, "zone1", 1, 1, &["zone1"], &[]);

        let mut expected = incoming.clone();
        expected.set_owner_version(2);

        assert_eq!(
            UpdateResult::Delete {
                version: 2,
                object: expected,
                tombstone: Tombstone {
                    gvk: incoming.get_gvk().expect("expect gvk"),
                    name: incoming.get_namespaced_name(),
                    owner_version: 2,
                    owner_zone: "zone1".into(),
                    resource_version: 2,
                    deletion_timestamp: 17
                },
            },
            AnyApplicationMerge::new()
                .kube_delete(&span, current.into(), incoming, 2, "zone1", 17)
                .unwrap()
        );
    }

    #[test]
    pub fn local_update_from_placement_zone() {
        let span = span!(Level::DEBUG, "local_update_from_placement_zone");
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            1,
            &["zone1", "zone2"],
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
            1,
            &["zone1", "zone2"],
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
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond_status("zone2", "type", "updated")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update {
                object: expected,
                version: 5
            },
            strategy
                .kube_update(&span, current.into(), incoming, 5, "zone2", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_create_from_placement_zone() {
        let span = span!(Level::DEBUG, "");
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[anyzone("zone1", 2, &[anycond("zone1", "type")])],
        );
        let incoming = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            5,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 3, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[anycond("zone2", "type")]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update {
                object: expected,
                version: 5
            },
            strategy
                .kube_update(&span, current.into(), incoming, 5, "zone2", 0)
                .unwrap()
        );
    }

    #[test]
    pub fn local_delete_from_placement_zone() {
        let span = span!(Level::DEBUG, "local_delete_from_placement_zone");
        let current = make_anyapplication_with_conditions(
            1,
            1,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
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
            1,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[]),
            ],
        );

        let expected = make_anyapplication_with_conditions(
            1,
            4,
            "zone1",
            1,
            0,
            &["zone1", "zone2"],
            &[
                anyzone("zone1", 2, &[anycond("zone1", "type")]),
                anyzone("zone2", 4, &[]),
            ],
        );

        let strategy = AnyApplicationMerge::new();
        assert_eq!(
            UpdateResult::Update {
                object: expected,
                version: 4
            },
            strategy
                .kube_update(&span, current.into(), incoming, 4, "zone2", 0)
                .unwrap()
        );
    }
}
