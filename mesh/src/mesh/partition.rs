use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use super::event::MeshEvent;
use crate::{
    kube::subscriptions::Version,
    merge::types::{Tombstone, VersionedObject},
    utils::types::Clock,
};
use crate::{
    kube::{dynamic_object_ext::DynamicObjectExt, event::KubeEvent, types::NamespacedName},
    merge::types::{MergeResult, MergeStrategy, UpdateResult},
};
use anyhow::Result;
use kube::api::DynamicObject;
use tracing::{debug, warn};

pub struct Partition {
    resources: BTreeMap<NamespacedName, VersionedObject>,
    merge_strategy: Arc<dyn MergeStrategy>,
    clock: Arc<dyn Clock>,
    initialized: bool,
}

impl Partition {
    pub fn new<M>(merge_strategy: M, clock: Arc<dyn Clock>) -> Partition
    where
        M: MergeStrategy + 'static,
    {
        Partition {
            resources: BTreeMap::new(),
            merge_strategy: Arc::new(merge_strategy),
            clock,
            initialized: false,
        }
    }

    pub fn mesh_apply(
        &mut self,
        incoming: MeshEvent,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Result<Vec<MergeResult>> {
        match incoming {
            MeshEvent::Update { object: incoming } => {
                let name = incoming.get_namespaced_name();
                let current = self
                    .resources
                    .get(&name)
                    .cloned()
                    .unwrap_or(VersionedObject::NonExisting);

                let result = self.merge_strategy.mesh_update(
                    current,
                    incoming,
                    incoming_zone,
                    current_zone,
                )?;

                self.mesh_update_partition(&result);
                Ok(vec![result])
            }
            MeshEvent::Delete { object: incoming } => {
                let name = incoming.get_namespaced_name();
                let current = self
                    .resources
                    .get(&name)
                    .cloned()
                    .unwrap_or(VersionedObject::NonExisting);
                let result = self.merge_strategy.mesh_delete(
                    current,
                    incoming,
                    incoming_zone,
                    self.clock.now_millis(),
                )?;
                self.mesh_update_partition(&result);
                Ok(vec![result])
            }
            MeshEvent::Snapshot { snapshot } => {
                self.mesh_apply_snapshot(snapshot, incoming_zone, current_zone)
            }
        }
    }

    fn mesh_update_partition(&mut self, result: &MergeResult) {
        match &result {
            MergeResult::Create { object } | MergeResult::Update { object } => {
                self.resources.insert(
                    object.get_namespaced_name(),
                    VersionedObject::Object(object.clone()),
                );
            }
            MergeResult::Delete(tombstone) => {
                let tombstone = tombstone.to_owned();
                self.resources.insert(
                    tombstone.name.to_owned(),
                    VersionedObject::Tombstone(tombstone),
                );
            }
            MergeResult::Tombstone(tombstone) => {
                let tombstone = tombstone.to_owned();
                self.resources.insert(
                    tombstone.name.to_owned(),
                    VersionedObject::Tombstone(tombstone),
                );
            }
            MergeResult::Skip => {}
        }
    }

    pub fn mesh_apply_snapshot(
        &mut self,
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
        incoming_zone: &str,
        current_zone: &str,
    ) -> Result<Vec<MergeResult>> {
        let in_partition: HashSet<NamespacedName> = self
            .resources
            .iter()
            .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, incoming_zone))
            .map(|(k, _)| k.to_owned())
            .collect();

        let incoming: HashSet<NamespacedName> = snapshot
            .iter()
            .filter(|(_, v)| self.merge_strategy.is_owner_zone_object(v, incoming_zone))
            .map(|(k, _)| k.to_owned())
            .collect();

        let to_update = incoming.clone();
        let to_delete_non_existing: HashSet<&NamespacedName> =
            in_partition.difference(&incoming).collect();

        let mut results = vec![];

        for name in to_delete_non_existing {
            let current = self
                .resources
                .remove(name)
                .unwrap_or(VersionedObject::NonExisting);
            let Some(tombstone) = self
                .merge_strategy
                .tombstone(current, self.clock.now_millis())?
            else {
                continue;
            };
            self.resources
                .insert(name.to_owned(), VersionedObject::Tombstone(tombstone));
        }
        for name in to_update {
            let current = self
                .resources
                .get(&name)
                .cloned()
                .unwrap_or(VersionedObject::NonExisting);
            let incoming = snapshot.get(&name).cloned().unwrap();
            let result =
                self.merge_strategy
                    .mesh_update(current, incoming, incoming_zone, current_zone)?;
            self.mesh_update_partition(&result);
            results.push(result);
        }
        Ok(results)
    }

    pub fn get_mesh_snapshot(&self, current_zone: &str) -> MeshEvent {
        let owned: HashSet<NamespacedName> = self
            .resources
            .iter()
            .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, current_zone))
            .map(|(k, _)| k.to_owned())
            .collect();

        let mut snapshot = BTreeMap::new();

        for name in owned {
            let VersionedObject::Object(object) = self
                .resources
                .get(&name)
                .unwrap_or(&VersionedObject::NonExisting)
            else {
                continue;
            };
            snapshot.insert(name, object.to_owned());
        }
        MeshEvent::Snapshot { snapshot }
    }

    pub fn kube_apply(&mut self, event: KubeEvent, current_zone: &str) -> Result<UpdateResult> {
        match event {
            KubeEvent::Update {
                version, object, ..
            } => {
                let name = object.get_namespaced_name();
                let current = self
                    .resources
                    .get(&name)
                    .cloned()
                    .unwrap_or(VersionedObject::NonExisting);
                let result =
                    self.merge_strategy
                        .kube_update(current, object, version, current_zone)?;
                self.kube_update_partition(&result)?;
                Ok(result)
            }
            KubeEvent::Delete {
                version, object, ..
            } => {
                let name = object.get_namespaced_name();
                let current = self
                    .resources
                    .get(&name)
                    .cloned()
                    .unwrap_or(VersionedObject::NonExisting);
                let result = self.merge_strategy.kube_delete(
                    current,
                    object,
                    version,
                    current_zone,
                    self.clock.now_millis(),
                )?;
                self.kube_update_partition(&result)?;
                Ok(result)
            }
            KubeEvent::Snapshot {
                version, snapshot, ..
            } => {
                if !self.initialized {
                    let snapshot_result =
                        self.kube_apply_snapshot(version, snapshot, current_zone, true)?;
                    self.initialized = true;
                    Ok(snapshot_result)
                } else {
                    let snapshot_result =
                        self.kube_apply_snapshot(version, snapshot, current_zone, false)?;
                    Ok(snapshot_result)
                }
            }
        }
    }

    fn kube_update_partition(&mut self, result: &UpdateResult) -> Result<()> {
        match result {
            UpdateResult::Create { object } => {
                debug!(
                    "kube_update_partition: create: version set to {:?}",
                    object.metadata.resource_version
                );
                self.resources.insert(
                    object.get_namespaced_name(),
                    VersionedObject::Object(object.clone()),
                );
            }
            UpdateResult::Update { object } => {
                debug!(
                    "kube_update_partition: update: version set to {:?}",
                    object.metadata.resource_version
                );
                self.resources.insert(
                    object.get_namespaced_name(),
                    VersionedObject::Object(object.clone()),
                );
            }
            UpdateResult::Delete { object, tombstone } => {
                debug!(
                    "kube_update_partition: delete: version set to {:?}",
                    object.metadata.resource_version
                );
                let tombstone = tombstone.to_owned();
                self.resources.insert(
                    object.get_namespaced_name(),
                    VersionedObject::Tombstone(tombstone),
                );
            }
            UpdateResult::Tombstone(tombstone) => {
                debug!(
                    "kube_update_partition: tombstone: version set to {:?}",
                    tombstone.resource_version
                );
                let tombstone = tombstone.to_owned();
                self.resources.insert(
                    tombstone.name.to_owned(),
                    VersionedObject::Tombstone(tombstone),
                );
            }
            UpdateResult::Snapshot { .. } => {
                warn!("Snapshot is not applied in kube_update_partition. Algorithm error.")
            }
            UpdateResult::Skip => {}
        }
        Ok(())
    }

    fn kube_apply_snapshot(
        &mut self,
        version: Version,
        mut snapshot: BTreeMap<NamespacedName, DynamicObject>,
        current_zone: &str,
        initial: bool,
    ) -> Result<UpdateResult> {
        let owned_resources: HashSet<NamespacedName> = snapshot
            .iter()
            .filter(|(_, v)| self.merge_strategy.is_owner_zone_object(v, current_zone))
            .map(|(k, _)| k.to_owned())
            .collect();

        let mut owned_snapshot = BTreeMap::new();
        for name in owned_resources.into_iter() {
            let object = snapshot
                .remove(&name)
                .expect("Invariant failure. expected object in snapshot");
            let result = self.merge_strategy.kube_update(
                VersionedObject::NonExisting,
                object,
                version,
                current_zone,
            )?;
            self.kube_update_partition(&result)?;
            match result {
                UpdateResult::Create { object } | UpdateResult::Update { object } => {
                    owned_snapshot.insert(name.to_owned(), object.clone());
                }
                UpdateResult::Delete { .. } | UpdateResult::Snapshot { .. } => {
                    panic!("unexpected delete or snapshot update result")
                }
                UpdateResult::Skip | UpdateResult::Tombstone { .. } => (),
            }
        }
        let snapshot_result = UpdateResult::Snapshot {
            snapshot: owned_snapshot,
        };

        // partition should reflect the state of the kubernetes for not owned resources as well
        // we must keep partition in sync with kubernetes snapshot, for not owned resources
        snapshot
            .iter()
            .filter(|(_, v)| !self.merge_strategy.is_owner_zone_object(v, current_zone))
            .for_each(|(name, object)| {
                if !self.resources.contains_key(name) {
                    self.resources
                        .insert(name.to_owned(), VersionedObject::Object(object.to_owned()));
                }
            });

        if !initial {
            let owned_by_current_zone_not_in_snapshot: HashSet<NamespacedName> = self
                .resources
                .iter()
                .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, current_zone))
                .filter(|(name, _)| !snapshot.contains_key(name))
                .map(|(k, _)| k.to_owned())
                .collect();

            // Inserting tombstone if partition contains object which is absent in kube snapshot
            for name in owned_by_current_zone_not_in_snapshot {
                match self
                    .resources
                    .get(&name)
                    .unwrap_or(&VersionedObject::NonExisting)
                {
                    VersionedObject::Object(object) => {
                        let resource_version = object.get_resource_version();
                        let owner_version = object.get_owner_version().unwrap_or(resource_version);
                        let tombstone = Tombstone {
                            gvk: object.get_gvk()?,
                            name: name.to_owned(),
                            owner_version,
                            owner_zone: current_zone.to_owned(),
                            resource_version,
                            deletion_timestamp: self.clock.now_millis(),
                        };
                        self.resources
                            .insert(name.to_owned(), VersionedObject::Tombstone(tombstone));
                    }
                    VersionedObject::NonExisting | VersionedObject::Tombstone(_) => (),
                }
            }
        }
        Ok(snapshot_result)
    }

    pub fn update_resource_version(&mut self, name: &NamespacedName, version: Version) {
        if let Some(object) = self.resources.get_mut(name) {
            match object {
                VersionedObject::Object(object) => {
                    object.set_resource_version(version);
                }
                VersionedObject::Tombstone(Tombstone {
                    resource_version, ..
                }) => {
                    *resource_version = version;
                }
                _ => (),
            }
        }
    }

    pub fn get(&self, name: &NamespacedName) -> Option<DynamicObject> {
        self.resources
            .get(name)
            .map(|v| match v {
                VersionedObject::Object(obj) => Some(obj.to_owned()),
                VersionedObject::NonExisting | VersionedObject::Tombstone(_) => None,
            })
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub fn get_tombstone(&self, name: &NamespacedName) -> Option<Tombstone> {
        self.resources
            .get(name)
            .map(|v| match v {
                VersionedObject::Tombstone(tombstone) => Some(tombstone.to_owned()),
                VersionedObject::NonExisting | VersionedObject::Object(_) => None,
            })
            .unwrap_or_default()
    }

    pub fn drop_tombstones(&mut self, retention_interval_seconds: u64) {
        let now = self.clock.now_millis();
        let cutover_millis = now.saturating_sub(retention_interval_seconds * 1000);

        self.resources.retain(|_, obj| match obj {
            VersionedObject::Tombstone(Tombstone {
                deletion_timestamp, ..
            }) => *deletion_timestamp > cutover_millis,
            VersionedObject::NonExisting => false,
            VersionedObject::Object(_) => true,
        });
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use anyapplication::{anyapplication::*, anyapplication_ext::*};
    use kube::api::{DynamicObject, ObjectMeta};
    use maplit::btreemap;

    use crate::{
        kube::{
            dynamic_object_ext::DynamicObjectExt, event::KubeEvent, subscriptions::Version,
            types::NamespacedName,
        },
        merge::{
            anyapplication_strategy::AnyApplicationMerge,
            anyapplication_test_support::tests::{anycond, anyplacements, anyspec, anystatus},
            types::{MergeResult, Tombstone, UpdateResult},
        },
        mesh::{event::MeshEvent, partition::Partition},
        utils::clock::FakeClock,
    };

    #[test]
    fn snapshot_handling() {
        let name_a1 = NamespacedName::new("default".into(), "nginx-app-a1".into());
        let mut app_a1 = anyapp(
            &name_a1,
            1,
            anyspec(1),
            Some(anystatus("A", anyplacements("A", None), None)),
        );
        app_a1.set_resource_version(1);
        let name_a2 = NamespacedName::new("default".into(), "nginx-app-a2".into());
        let mut app_a2 = anyapp(
            &name_a2,
            1,
            anyspec(1),
            Some(anystatus("A", anyplacements("A", None), None)),
        );
        app_a2.set_resource_version(1);

        let name_b1 = NamespacedName::new("default".into(), "nginx-app-b1".into());
        let mut app_b1 = anyapp(
            &name_b1,
            1,
            anyspec(1),
            Some(anystatus("B", anyplacements("B", None), None)),
        );
        app_b1.set_resource_version(1);
        app_b1.set_owner_zone("B".into());
        let name_b2 = NamespacedName::new("default".into(), "nginx-app-b2".into());
        let mut app_b2 = anyapp(
            &name_b2,
            1,
            anyspec(1),
            Some(anystatus("B", anyplacements("B", None), None)),
        );
        app_b2.set_resource_version(1);
        app_b2.set_owner_zone("B".into());
        let name_b3 = NamespacedName::new("default".into(), "nginx-app-b3".into());
        let mut app_b3 = anyapp(
            &name_b3,
            1,
            anyspec(1),
            Some(anystatus("B", anyplacements("B", None), None)),
        );
        app_b3.set_resource_version(1);
        app_b3.set_owner_zone("B".into());

        let mut partition = Partition::new(AnyApplicationMerge::new(), Arc::new(FakeClock::new()));

        // 1. Outgoing initial snapshot
        let actual_snapshot = kube_apply_snapshot(
            &mut partition,
            btreemap! {
                name_a1.clone() => app_a1.to_owned(),
                name_a2.clone() => app_a2.to_owned(),
                name_b1.clone() => app_b1.to_owned(),
            },
            1,
        );

        // app_a1 and appa2 should be in the snapshot
        assert_eq!(actual_snapshot.get(&name_a1).unwrap(), &app_a1);
        assert_eq!(actual_snapshot.get(&name_a2).unwrap(), &app_a2);

        // app_b1 should not be in snapshot, but it should be registered in partition
        assert_eq!(actual_snapshot.get(&name_b1), None);
        assert_eq!(partition.get(&name_b1).unwrap(), app_b1);

        // 2. Outgoing incremental snapshot
        app_a1.set_resource_version(2);
        let snapshot = btreemap! {
            name_a1.clone() => app_a1.to_owned(),
            name_b1.clone() => app_b1.to_owned(),
            name_b2.clone() => app_b2.to_owned(),
        };

        let actual_snapshot = kube_apply_snapshot(&mut partition, snapshot, 2);

        // app_a1 should be in the snapshot with updated version
        app_a1.set_owner_version(2);
        assert_eq!(actual_snapshot.get(&name_a1).unwrap(), &app_a1);

        // app_a2 should not be in the snapshot, but partition will get tombstone for a2
        assert_eq!(actual_snapshot.get(&name_a2), None);
        assert_eq!(partition.get(&name_a1), None);

        // app_b1 should not be in snapshot, but it should be registered in partition
        assert_eq!(actual_snapshot.get(&name_b1), None);
        assert_eq!(partition.get(&name_b1).unwrap(), app_b1);

        // app_b2 should not be in snapshot, and it should be registered in partition
        assert_eq!(actual_snapshot.get(&name_b2), None);
        assert_eq!(partition.get(&name_b2).unwrap(), app_b2);

        // 3. Incoming snapshot

        app_b1.set_owner_version(2);
        app_b1.unset_resource_version();
        app_b3.unset_resource_version();
        let snapshot = btreemap! {
            name_b1.clone() => app_b1.to_owned(),
            name_b3.clone() => app_b3.to_owned(),
        };

        let mut actual_merge_result = partition
            .mesh_apply_snapshot(snapshot, "B", "A")
            .expect("Incoming snapshot should be applied");
        sort_merge_results(&mut actual_merge_result);

        app_b1.set_resource_version(1);
        let expected_merge_result = vec![
            MergeResult::Create {
                object: app_b3.to_owned(),
            },
            MergeResult::Update {
                object: app_b1.to_owned(),
            },
        ];
        assert_eq!(
            actual_merge_result, expected_merge_result,
            "Expected merge result for incoming snapshot"
        );

        app_b1.set_resource_version(1);
        assert_eq!(partition.get(&name_b1).unwrap(), app_b1);

        assert_eq!(partition.get(&name_b2), None);

        assert_eq!(partition.get(&name_b3).unwrap(), app_b3);
    }

    fn kube_apply_snapshot(
        partition: &mut Partition,
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
        version: Version,
    ) -> BTreeMap<NamespacedName, DynamicObject> {
        let update_result = partition
            .kube_apply(KubeEvent::Snapshot { version, snapshot }, "A")
            .expect("Snapshot should be applied");

        let UpdateResult::Snapshot {
            snapshot: actual_snapshot,
        } = update_result
        else {
            panic!("Expected snapshot result");
        };

        actual_snapshot
    }

    #[test]
    fn should_drop_tombstones_periodically() {
        let name_a1 = NamespacedName::new("default".into(), "nginx-app-a1".into());
        let mut app_a1 = anyapp(
            &name_a1,
            1,
            anyspec(1),
            Some(anystatus("A", anyplacements("A", None), None)),
        );
        app_a1.set_resource_version(1);

        let clock = Arc::new(FakeClock::new());
        clock.set_time_millis(10000);
        let mut partition = Partition::new(AnyApplicationMerge::new(), clock.to_owned());

        // 1. Outgoing initial snapshot
        let actual_snapshot = kube_apply_snapshot(
            &mut partition,
            btreemap! {
                name_a1.clone() => app_a1.to_owned(),
            },
            1,
        );

        assert_eq!(actual_snapshot.get(&name_a1).unwrap(), &app_a1);

        // 1. Applying empty snapshot and deleting object a1
        kube_apply_snapshot(&mut partition, btreemap! {}, 1);

        assert_eq!(
            partition.get_tombstone(&name_a1).unwrap(),
            Tombstone {
                gvk: app_a1.get_gvk().unwrap(),
                name: name_a1.clone(),
                owner_version: app_a1.get_owner_version().unwrap_or(1),
                owner_zone: "A".into(),
                resource_version: 1,
                deletion_timestamp: 10000,
            }
        );

        clock.set_time_millis(19000);
        partition.drop_tombstones(10);

        assert!(partition.get_tombstone(&name_a1).is_some());

        // Fast forward clock to 11 seconds
        clock.set_time_millis(20000);
        partition.drop_tombstones(10);

        assert!(partition.get_tombstone(&name_a1).is_none());
    }

    #[test]
    fn single_source_replication() {
        let mut runner = ReplicationTestRunner::new_anyapp("A", "B");
        let mut anyapp_a = AnyApplicationStore::new("A");

        // 1.1 Do not replicate object that has no status (new resource)
        runner.kube_partition_a(
            &anyapp_a.kube_snap(),
            &mesh_snap(vec![]),
            Vec::<MergeResult>::new(),
        );

        // 1.2 persistence step and update of partition
        anyapp_a.inc_version();
        runner.post_merge_update_version_a(&mut anyapp_a);

        // initializing partition_a
        runner.init_partition_a(anyapp_a.kube_upd());

        // 2.1 Replicate object update with status set
        anyapp_a.inc_version();
        anyapp_a.with_initial_state("A", "New");
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_a_with_version.merge_cre()],
        );

        // 2.2 persistence step and update of partition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_a_with_version);

        // 3.1 Replicate object update with placements
        anyapp_a.inc_version();
        anyapp_a.set_placements(anyplacements("A", None));
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();
        let mut anyapp_b = anyapp_a_with_version.with_resource_version(3);

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_b.merge_upd()],
        );

        // 3.2 persistence step and update of partition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_b = anyapp_b.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_b);

        // 3.1 Replicate object update with placements and new condition
        anyapp_a.inc_version();
        anyapp_a.set_conditions(1, "A", vec![anycond("A", "type")]);

        let anyapp_a_updated = anyapp_a.with_updated_owner_version();
        let mut anyapp_b = anyapp_a_updated.with_resource_version(anyapp_b.resource_version);

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_b.merge_upd()],
        );

        // 3.2 persistence step and update of partition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_b = anyapp_b.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_b);

        // 4.1 Replicate object - condition update
        anyapp_a.inc_version();
        anyapp_a.set_conditions(2, "A", vec![anycond("A", "type2")]);

        let anyapp_a_updated = anyapp_a.with_updated_owner_version();
        let mut anyapp_b = anyapp_a_updated.with_resource_version(anyapp_b.resource_version);

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_b.merge_upd()],
        );

        // 4.2 persistence step and update of partition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_b = anyapp_b.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_b);

        // 4.1 Replicate object - condition delete
        anyapp_a.inc_version();
        anyapp_a.set_conditions(3, "A", vec![]);

        let mut anyapp_a_updated = anyapp_a.with_updated_owner_version();
        let mut anyapp_b = anyapp_a_updated.with_resource_version(anyapp_b.resource_version);

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_b.merge_upd()],
        );

        // 4.2 persistence step and update of partition
        anyapp_a = anyapp_a_updated.with_incremented_version();
        anyapp_b = anyapp_b.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_b);

        // 5 Replicate object delete
        let anyapp_a_updated = anyapp_a.with_updated_owner_version();
        let anyapp_b = anyapp_a_updated.with_resource_version(anyapp_b.resource_version);

        runner.kube_partition_a(
            &anyapp_a.kube_del(),
            &anyapp_a_updated.mesh_del(),
            vec![anyapp_b.merge_del()],
        );
    }

    #[test]
    fn two_zones_interaction() {
        let mut runner = ReplicationTestRunner::new_anyapp("A", "B");
        let mut anyapp_a = AnyApplicationStore::new("A");

        // 1.1 Do not replicate object that has no status (new resource)
        runner.kube_partition_a(
            &anyapp_a.kube_snap(),
            &mesh_snap(vec![]),
            Vec::<MergeResult>::new(),
        );

        // 1.2 persistence step and update of partition
        anyapp_a.inc_version();
        runner.post_merge_update_version_a(&mut anyapp_a);

        // Initialize partition_a
        runner.init_partition_a(anyapp_a.kube_upd());

        // 2.1 Replicate object update with status set
        anyapp_a.inc_version();
        anyapp_a.with_initial_state("A", "New");
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_a_with_version.merge_cre()],
        );

        // 2.2 persistence step and update of partition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_a_with_version);

        dbg!(
            runner
                .partition_a
                .get(&anyapp_a.get_namespaced_name())
                .unwrap()
                .metadata
                .resource_version
        );
        dbg!(
            runner
                .partition_b
                .get(&anyapp_a.get_namespaced_name())
                .unwrap()
                .metadata
                .resource_version
        );

        // 3.1 Replicate object to zone B update with placements
        anyapp_a.inc_version();
        anyapp_a.set_placements(anyplacements("A", Some("B")));
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();
        let mut anyapp_b = anyapp_a_with_version.with_resource_version(3);

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_b.merge_upd()],
        );

        // 3.2 persistence step and update of partition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_b = anyapp_b.with_incremented_version();
        runner.post_merge_update_version_a(&mut anyapp_a);
        runner.post_merge_update_version_b(&mut anyapp_b);

        // 4.1 conditions of A replicate to B
        anyapp_a.inc_version();
        anyapp_a.add_condition(1, anycond("A", "type"));

        let mut anyapp_a_updated = anyapp_a.with_updated_owner_version();
        let mut anyapp_b = anyapp_a_updated.with_resource_version(anyapp_b.resource_version);

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_b.merge_upd()],
        );

        // 4.2 persistence step and update of partition
        anyapp_b = anyapp_a_updated.as_zone("B", 1);
        runner.post_merge_update_version_b(&mut anyapp_b);

        // 5.1 conditions of B replicate to A
        anyapp_b.add_condition(1, anycond("B", "type"));
        anyapp_b.inc_version();
        anyapp_b = anyapp_b.with_update_resource_version();

        let mut anyapp_b_updated = anyapp_b.clone();
        anyapp_a_updated = anyapp_b_updated
            .as_zone("A", anyapp_a_updated.resource_version)
            .with_update_resource_version();

        runner.kube_partition_b(
            &anyapp_b.kube_upd(),
            &anyapp_b_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // 5.2 persistence step and update of partition
        runner.post_merge_update_version_a(&mut anyapp_a_updated);
        runner.post_merge_update_version_b(&mut anyapp_b_updated);
        anyapp_a = anyapp_a_updated;

        // 6.1 update of condition of A replicate to B
        anyapp_a.update_condition(2, "type", "A", anycond("A", "type2"));
        anyapp_a.inc_version();
        anyapp_a = anyapp_a.with_update_resource_version();

        anyapp_a_updated = anyapp_a.with_updated_owner_version();

        anyapp_b_updated = anyapp_a_updated
            .as_zone("B", anyapp_b_updated.resource_version)
            .with_update_resource_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_b_updated.merge_upd()],
        );

        // 6.2 persistence step and update of partition
        runner.post_merge_update_version_b(&mut anyapp_b_updated);
        runner.post_merge_update_version_a(&mut anyapp_a_updated);
        anyapp_b = anyapp_b_updated.clone();

        // 7.1 update of condition of B replicate to A
        anyapp_b.update_condition(3, "type", "B", anycond("B", "type3"));
        anyapp_b.inc_version();
        anyapp_b = anyapp_b.with_update_resource_version();

        anyapp_b_updated = anyapp_b.clone();

        anyapp_a_updated = anyapp_b_updated
            .as_zone("A", anyapp_a_updated.resource_version)
            .with_update_resource_version()
            .with_updated_owner_version();

        runner.kube_partition_b(
            &anyapp_b.kube_upd(),
            &anyapp_b_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // 7.2 persistence step and update of partition
        runner.post_merge_update_version_a(&mut anyapp_a_updated);
        runner.post_merge_update_version_b(&mut anyapp_b_updated);
        anyapp_a = anyapp_a_updated;

        // 8.1 delete of condition of A replicate to B
        anyapp_a.inc_version();
        anyapp_a.delete_condition(4, "type2", "A");
        anyapp_a = anyapp_a.with_update_resource_version();

        anyapp_a_updated = anyapp_a.with_updated_owner_version();

        anyapp_b_updated = anyapp_a_updated
            .as_zone("B", anyapp_b_updated.resource_version)
            .with_update_resource_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_b_updated.merge_upd()],
        );

        // 8.2 persistence step and update of partition
        runner.post_merge_update_version_a(&mut anyapp_a_updated);
        runner.post_merge_update_version_b(&mut anyapp_b_updated);
        anyapp_b = anyapp_b_updated.clone();

        // delete of condition of B replicate to A
        anyapp_b.delete_condition(5, "type3", "B");
        anyapp_b.inc_version();
        anyapp_b = anyapp_b.with_update_resource_version();

        anyapp_b_updated = anyapp_b.clone();

        anyapp_a_updated = anyapp_b_updated
            .as_zone("A", anyapp_a_updated.resource_version)
            .with_update_resource_version()
            .with_updated_owner_version();

        runner.kube_partition_b(
            &anyapp_b.kube_upd(),
            &anyapp_b_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );
    }

    fn mesh_snap(objects: Vec<&DynamicObject>) -> MeshEvent {
        let mut snapshot = BTreeMap::new();
        objects.into_iter().for_each(|o| {
            let name = o.get_namespaced_name();
            snapshot.insert(name, o.clone());
        });
        MeshEvent::Snapshot { snapshot }
    }

    pub fn anyapp(
        name: &NamespacedName,
        owner_version: Version,
        spec: AnyApplicationSpec,
        status: Option<AnyApplicationStatus>,
    ) -> DynamicObject {
        let resource = AnyApplication {
            metadata: ObjectMeta {
                name: Some(name.name.to_owned()),
                namespace: Some(name.namespace.to_owned()),
                labels: Some(BTreeMap::from([(
                    OWNER_VERSION.into(),
                    owner_version.to_string(),
                )])),
                ..Default::default()
            },
            spec,
            status,
        };
        let resource_str = serde_json::to_value(&resource).expect("Resource is not serializable");
        let object: DynamicObject =
            serde_json::from_value(resource_str).expect("Cannot parse dynamic object");
        object
    }

    struct ReplicationTestRunner {
        partition_a: Partition,
        partition_b: Partition,
        zone_a: String,
        zone_b: String,
    }

    impl ReplicationTestRunner {
        pub fn new_anyapp(zone_a: &str, zone_b: &str) -> ReplicationTestRunner {
            let clock = Arc::new(FakeClock::new());
            clock.set_time_millis(0);
            ReplicationTestRunner {
                partition_a: Partition::new(AnyApplicationMerge::new(), clock.clone()),
                partition_b: Partition::new(AnyApplicationMerge::new(), clock),
                zone_a: zone_a.into(),
                zone_b: zone_b.into(),
            }
        }

        pub fn init_partition_a(&mut self, event: KubeEvent) {
            self.partition_a
                .kube_apply(event, &self.zone_a)
                .expect("init partition a should succeed");
        }

        pub fn kube_partition_a(
            &mut self,
            kube_event_a: &KubeEvent,
            mesh_event_a: &MeshEvent,
            merge_result_b: Vec<MergeResult>,
        ) {
            let actual_mesh_event: Option<MeshEvent> = self
                .partition_a
                .kube_apply(kube_event_a.to_owned(), &self.zone_a)
                .expect("partition_a.kube_apply() succeeds")
                .into();
            assert_eq!(
                mesh_event_a,
                actual_mesh_event.as_ref().unwrap(),
                "mesh event"
            );

            let actual_merge_result = self
                .partition_b
                .mesh_apply(actual_mesh_event.unwrap(), &self.zone_a, &self.zone_b)
                .expect("partition_b.mesh_apply() succeeds");
            assert_eq!(actual_merge_result, merge_result_b, "merge result");
        }

        pub fn kube_partition_b(
            &mut self,
            kube_event_b: &KubeEvent,
            mesh_event_b: &MeshEvent,
            merge_result_a: Vec<MergeResult>,
        ) {
            let actual_mesh_event: Option<MeshEvent> = self
                .partition_b
                .kube_apply(kube_event_b.to_owned(), &self.zone_b)
                .expect("partition_b.kube_apply() succeeds")
                .into();
            assert_eq!(
                mesh_event_b,
                actual_mesh_event.as_ref().unwrap(),
                "mesh event"
            );

            let actual_merge_result = self
                .partition_a
                .mesh_apply(actual_mesh_event.unwrap(), &self.zone_b, &self.zone_a)
                .expect("partition_a.mesh_apply() succeeds");
            assert_eq!(actual_merge_result, merge_result_a, "merge result");
        }

        pub fn post_merge_update_version_a(&mut self, controller: &mut AnyApplicationStore) {
            controller
                .object
                .set_resource_version(controller.resource_version);
            let name = controller.get_namespaced_name();
            self.partition_a
                .update_resource_version(&name, controller.resource_version);
        }

        pub fn post_merge_update_version_b(&mut self, controller: &mut AnyApplicationStore) {
            controller
                .object
                .set_resource_version(controller.resource_version);
            let version = controller.resource_version;
            let name = controller.get_namespaced_name();
            self.partition_b.update_resource_version(&name, version);
        }
    }

    #[derive(Clone)]
    struct AnyApplicationStore {
        object: AnyApplication,
        zone: String,
        resource_version: Version,
    }

    impl AnyApplicationStore {
        pub fn new(zone: &str) -> AnyApplicationStore {
            let resource_version = 1;
            let object = AnyApplication {
                metadata: ObjectMeta {
                    name: Some("nginx-app".into()),
                    namespace: Some("default".into()),
                    labels: None,
                    resource_version: Some(resource_version.to_string()),
                    ..Default::default()
                },
                spec: anyspec(1),
                status: None,
            };

            AnyApplicationStore {
                object,
                zone: zone.into(),
                resource_version,
            }
        }

        pub fn get_namespaced_name(&self) -> NamespacedName {
            NamespacedName::new(
                self.object
                    .metadata
                    .namespace
                    .as_ref()
                    .cloned()
                    .expect("namespace is expected"),
                self.object
                    .metadata
                    .name
                    .as_ref()
                    .cloned()
                    .expect("name is expected"),
            )
        }

        pub fn with_initial_state(&mut self, owner: &str, state: &str) {
            match self.object.status.as_mut() {
                Some(status) => {
                    status.owner = owner.into();
                    status.state = state.into();
                }
                None => {
                    self.object.status = Some(AnyApplicationStatus {
                        owner: owner.into(),
                        state: state.into(),
                        zones: None,
                        placements: None,
                    })
                }
            }
        }

        pub fn set_placements(&mut self, placements: Vec<AnyApplicationStatusPlacements>) {
            match self.object.status.as_mut() {
                Some(status) => {
                    status.placements = Some(placements);
                }
                None => {
                    self.object.status = Some(AnyApplicationStatus {
                        owner: "".into(),
                        state: "".into(),
                        zones: None,
                        placements: Some(placements),
                    })
                }
            }
        }

        pub fn set_conditions(
            &mut self,
            version: i64,
            zone: &str,
            to_set: Vec<AnyApplicationStatusZonesConditions>,
        ) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                zones: None,
                placements: None,
            });
            let zones = status.zones.get_or_insert(vec![]);
            match zones.iter_mut().find(|z| z.zone_id == zone) {
                Some(zone) => {
                    zone.version = version;
                    let conditions = zone.conditions.get_or_insert(vec![]);
                    *conditions = to_set;
                }
                None => {
                    let zone = AnyApplicationStatusZones {
                        zone_id: zone.into(),
                        version,
                        chart_version: Some("1.0.0".into()),
                        conditions: Some(to_set),
                    };
                    zones.push(zone);
                }
            }
        }

        pub fn add_condition(
            &mut self,
            version: i64,
            condition: AnyApplicationStatusZonesConditions,
        ) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                zones: None,
                placements: None,
            });
            let zones = status.zones.get_or_insert(vec![]);
            match zones.iter_mut().find(|v| v.zone_id == condition.zone_id) {
                Some(zone) => {
                    let conditions = zone.conditions.get_or_insert(vec![]);
                    conditions.push(condition);
                    conditions.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
                }
                None => {
                    zones.push(AnyApplicationStatusZones {
                        zone_id: condition.zone_id.to_owned(),
                        version,
                        chart_version: Some("1.0.0".into()),
                        conditions: Some(vec![condition]),
                    });
                }
            };
        }

        pub fn update_condition(
            &mut self,
            version: i64,
            cond_type: &str,
            zone: &str,
            cond: AnyApplicationStatusZonesConditions,
        ) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                zones: None,
                placements: None,
            });
            let mut updated = false;
            let zones = status.zones.get_or_insert(vec![]);
            let zone = match zones.iter_mut().find(|z| z.zone_id == zone) {
                Some(zone) => {
                    zone.version = version;
                    zone
                }
                None => {
                    panic!("no zone status exists for zone '{zone}'");
                }
            };

            for existing in zone.conditions.get_or_insert(vec![]).iter_mut() {
                if existing.r#type == cond_type {
                    *existing = cond;
                    updated = true;
                    break;
                }
            }

            if !updated {
                panic!("condition '{cond_type}' is not updated");
            }
            zone.conditions
                .get_or_insert(vec![])
                .sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
        }

        pub fn delete_condition(&mut self, version: i64, cond_type: &str, zone_id: &str) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                zones: None,
                placements: None,
            });
            let zones = status.zones.get_or_insert(vec![]);
            let Some(zone) = zones.iter_mut().find(|z| z.zone_id == zone_id) else {
                panic!("zone '{zone_id}' is missing in status");
            };
            if let Some(conditions) = zone.conditions.as_mut() {
                if let Some(pos) = conditions
                    .iter()
                    .position(|c| c.r#type == cond_type && c.zone_id == zone_id)
                {
                    zone.version = version;
                    conditions.remove(pos);
                    return;
                }
            }
            panic!("condition '{cond_type}' is not removed");
        }

        pub fn object(&self) -> DynamicObject {
            let resource_str =
                serde_json::to_value(&self.object).expect("Resource is not serializable");
            let object: DynamicObject =
                serde_json::from_value(resource_str).expect("Cannot parse dynamic object");
            object
        }

        pub fn kube_snap(&self) -> KubeEvent {
            let mut object = self.object();
            object.set_resource_version(self.resource_version);
            let mut snapshot = BTreeMap::new();
            let name = object.get_namespaced_name();
            snapshot.insert(name, object.clone());
            KubeEvent::Snapshot {
                version: self.resource_version,
                snapshot,
            }
        }

        fn kube_upd(&self) -> KubeEvent {
            let mut object = self.object();
            object.set_resource_version(self.resource_version);
            KubeEvent::Update {
                version: self.resource_version,
                object,
            }
        }

        fn kube_del(&self) -> KubeEvent {
            let mut object = self.object();
            object.set_resource_version(self.resource_version);
            KubeEvent::Delete {
                version: self.resource_version,
                object,
            }
        }

        fn mesh_upd(&self) -> MeshEvent {
            let mut object = self.object();
            object.unset_resource_version();
            MeshEvent::Update { object }
        }

        fn mesh_del(&self) -> MeshEvent {
            let mut object = self.object();
            object.unset_resource_version();
            MeshEvent::Delete { object }
        }

        fn merge_cre(&self) -> MergeResult {
            let mut object = self.object();
            object.unset_resource_version();
            // object.set_resource_version(self.resource_version);
            MergeResult::Create { object }
        }

        fn merge_upd(&self) -> MergeResult {
            let mut object = self.object();
            object.set_resource_version(self.resource_version);
            MergeResult::Update {
                object: object.to_owned(),
            }
        }

        fn merge_del(&self) -> MergeResult {
            let owner_version = self
                .object
                .get_owner_version()
                .expect("owner version is expected");
            let object = self.object();
            MergeResult::Delete(Tombstone {
                gvk: object.get_gvk().unwrap(),
                name: object.get_namespaced_name(),
                owner_version,
                owner_zone: self.zone.clone(),
                resource_version: self.resource_version,
                deletion_timestamp: 0,
            })
        }

        fn inc_version(&mut self) -> Version {
            self.resource_version += 1;
            self.resource_version
        }

        pub fn as_zone(&self, zone: &str, version: Version) -> Self {
            let mut copy = self.clone();
            copy.zone = zone.into();
            copy.resource_version = version;
            copy
        }

        fn with_updated_owner_version(&mut self) -> Self {
            let mut copy = self.clone();
            copy.object.set_owner_version(self.resource_version);
            copy
        }

        fn with_incremented_version(&mut self) -> Self {
            let mut copy = self.clone();
            copy.inc_version();
            copy
        }

        fn with_update_resource_version(&mut self) -> Self {
            let mut copy = self.clone();
            copy.object.set_resource_version(self.resource_version);
            copy
        }

        fn with_resource_version(&self, version: Version) -> Self {
            let mut copy = self.clone();
            copy.resource_version = version;
            copy
        }
    }

    fn sort_merge_results(data: &mut Vec<MergeResult>) {
        data.sort_by_key(|result| match result {
            MergeResult::Skip => 0,
            MergeResult::Create { .. } => 1,
            MergeResult::Update { .. } => 2,
            MergeResult::Delete(_) => 3,
            MergeResult::Tombstone(_) => 4,
        });
    }
}
