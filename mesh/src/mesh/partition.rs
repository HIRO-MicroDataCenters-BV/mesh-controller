use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use super::event::MeshEvent;
use crate::{
    kube::{dynamic_object_ext::DynamicObjectExt, event::KubeEvent, types::NamespacedName},
    merge::types::{MergeResult, MergeStrategy, UpdateResult},
};
use anyhow::Result;
use kube::api::DynamicObject;
use tracing::error;

pub struct Partition {
    resources: BTreeMap<NamespacedName, DynamicObject>,
    merge_strategy: Arc<dyn MergeStrategy>,
    initialized: bool,
}

impl Partition {
    pub fn new<M>(merge_strategy: M) -> Partition
    where
        M: MergeStrategy + 'static,
    {
        Partition {
            resources: BTreeMap::new(),
            merge_strategy: Arc::new(merge_strategy),
            initialized: false,
        }
    }

    pub fn mesh_apply(
        &mut self,
        incoming: MeshEvent,
        incoming_zone: &str,
    ) -> Result<Vec<MergeResult>> {
        match incoming {
            MeshEvent::Update { object } => {
                let name = object.get_namespaced_name();
                let current = self.resources.get(&name).cloned();
                let result = self
                    .merge_strategy
                    .mesh_update(current, &object, incoming_zone)?;
                self.mesh_update_partition(&result);
                Ok(vec![result])
            }
            MeshEvent::Delete { object } => {
                let name = object.get_namespaced_name();
                let current = self.resources.get(&name).cloned();
                let result = self
                    .merge_strategy
                    .mesh_delete(current, &object, incoming_zone)?;
                self.mesh_update_partition(&result);
                Ok(vec![result])
            }
            MeshEvent::Snapshot { snapshot } => {
                let existing: HashSet<NamespacedName> = self
                    .resources
                    .iter()
                    .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, incoming_zone))
                    .map(|(k, _)| k.to_owned())
                    .collect();

                let incoming: HashSet<NamespacedName> = snapshot
                    .iter()
                    .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, incoming_zone))
                    .map(|(k, _)| k.to_owned())
                    .collect();

                let to_update = incoming.clone();
                let to_delete: HashSet<&NamespacedName> = existing.difference(&incoming).collect();

                let mut results = vec![];
                for name in to_delete {
                    let current = self.resources.get(name).cloned();
                    if let Some(object) = current.clone() {
                        let result =
                            self.merge_strategy
                                .mesh_delete(current, &object, incoming_zone)?;
                        self.mesh_update_partition(&result);
                        results.push(result);
                    }
                }
                for name in to_update {
                    let current = self.resources.get(&name).cloned();
                    let object = snapshot.get(&name).unwrap();
                    let result = self
                        .merge_strategy
                        .mesh_update(current, object, incoming_zone)?;
                    self.mesh_update_partition(&result);
                    results.push(result);
                }
                Ok(results)
            }
        }
    }

    fn mesh_update_partition(&mut self, result: &MergeResult) {
        match &result {
            MergeResult::Create { object } | MergeResult::Update { object } => {
                self.resources
                    .insert(object.get_namespaced_name(), object.clone());
            }
            MergeResult::Delete { gvk: _, name } => {
                self.resources.remove(name);
            }
            MergeResult::DoNothing => {}
            MergeResult::Conflict { msg } => {
                error!("conflict while merging update from network {msg}")
            }
        }
    }

    pub fn mesh_snapshot(&self, current_zone: &str) -> MeshEvent {
        let owned: HashSet<NamespacedName> = self
            .resources
            .iter()
            .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, current_zone))
            .map(|(k, _)| k.to_owned())
            .collect();

        let mut snapshot = BTreeMap::new();
        for name in owned {
            if let Some(current) = self.resources.get(&name) {
                snapshot.insert(name, current.to_owned());
            };
        }
        MeshEvent::Snapshot { snapshot }
    }

    pub fn kube_apply(&mut self, event: &KubeEvent, current_zone: &str) -> Result<UpdateResult> {
        match event {
            KubeEvent::Update {
                version, object, ..
            } => {
                let name = object.get_namespaced_name();
                let current = self.resources.get(&name).cloned();
                let result = self.merge_strategy.local_update(
                    current,
                    object.to_owned(),
                    *version,
                    current_zone,
                )?;
                self.kube_update_partition(&result, current_zone)?;
                Ok(result)
            }
            KubeEvent::Delete {
                version, object, ..
            } => {
                let name = object.get_namespaced_name();
                let current = self.resources.get(&name).cloned();
                let result = self.merge_strategy.local_delete(
                    current,
                    object.to_owned(),
                    *version,
                    current_zone,
                )?;
                self.kube_update_partition(&result, current_zone)?;
                Ok(result)
            }
            KubeEvent::Snapshot {
                version, snapshot, ..
            } => {
                if !self.initialized {
                    let owned: HashSet<NamespacedName> = snapshot
                        .iter()
                        .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, current_zone))
                        .map(|(k, _)| k.to_owned())
                        .collect();

                    let not_owned: HashSet<NamespacedName> = snapshot
                        .iter()
                        .filter(|(_, v)| !self.merge_strategy.is_owner_zone(v, current_zone))
                        .map(|(k, _)| k.to_owned())
                        .collect();

                    let mut filtered_snapshot = BTreeMap::new();
                    for name in owned.into_iter() {
                        let object = snapshot.get(&name).unwrap();
                        let result = self.merge_strategy.local_update(
                            None,
                            object.clone(),
                            *version,
                            current_zone,
                        )?;
                        self.kube_update_partition(&result, current_zone)?;
                        match result {
                            UpdateResult::Create { object } | UpdateResult::Update { object } => {
                                filtered_snapshot.insert(name.to_owned(), object.clone());
                            }
                            UpdateResult::Delete { .. } | UpdateResult::Snapshot { .. } => {
                                panic!("unexpected delete or snapshot")
                            }
                            UpdateResult::DoNothing => (),
                        }
                    }
                    let snapshot_result = UpdateResult::Snapshot {
                        snapshot: filtered_snapshot,
                    };
                    for name in not_owned.into_iter() {
                        let value = snapshot.get(&name).unwrap();
                        self.resources.insert(name.to_owned(), value.to_owned());
                    }
                    self.initialized = true;
                    Ok(snapshot_result)
                } else {
                    let incoming: HashSet<&NamespacedName> = snapshot
                        .iter()
                        .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, current_zone))
                        .map(|(k, _)| k)
                        .collect();

                    let mut filtered_snapshot = BTreeMap::new();
                    for name in incoming {
                        let object = snapshot.get(name).unwrap();
                        let result = self.merge_strategy.local_update(
                            None,
                            object.clone(),
                            *version,
                            current_zone,
                        )?;
                        self.kube_update_partition(&result, current_zone)?;
                        match result {
                            UpdateResult::Create { object } | UpdateResult::Update { object } => {
                                filtered_snapshot.insert(name.to_owned(), object.clone());
                            }
                            UpdateResult::Delete { .. } | UpdateResult::Snapshot { .. } => {
                                panic!("unexpected delete or snapshot")
                            }
                            UpdateResult::DoNothing => (),
                        }
                    }
                    let result = UpdateResult::Snapshot {
                        snapshot: filtered_snapshot,
                    };
                    self.kube_update_partition(&result, current_zone)?;
                    Ok(result)
                }
            }
        }
    }

    fn kube_update_partition(&mut self, result: &UpdateResult, current_zone: &str) -> Result<()> {
        match result {
            UpdateResult::Create { object } | UpdateResult::Update { object } => {
                self.resources
                    .insert(object.get_namespaced_name(), object.clone());
            }
            UpdateResult::Delete { object } => {
                self.resources.remove(&object.get_namespaced_name());
            }
            UpdateResult::Snapshot { snapshot } => {
                let existing: HashSet<NamespacedName> = self
                    .resources
                    .iter()
                    .filter(|(_, v)| self.merge_strategy.is_owner_zone(v, current_zone))
                    .map(|(k, _)| k.to_owned())
                    .collect();

                for name in existing {
                    match snapshot.get(&name) {
                        Some(snapshot_object) => {
                            self.resources.insert(name, snapshot_object.clone());
                        }
                        None => {
                            self.resources.remove(&name);
                        }
                    }
                }
            }
            UpdateResult::DoNothing => {}
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeMap;

    use kube::api::{DynamicObject, ObjectMeta};

    use crate::{
        kube::{dynamic_object_ext::DynamicObjectExt, event::KubeEvent, pool::Version},
        merge::{
            anyapplication::{
                AnyApplication, AnyApplicationSpec, AnyApplicationStatus,
                AnyApplicationStatusConditions, AnyApplicationStatusPlacements,
            },
            anyapplication_ext::{AnyApplicationExt, OWNER_VERSION},
            anyapplication_strategy::AnyApplicationMerge,
            anyapplication_test_support::tests::{anycond, anyplacements, anyspec},
            types::MergeResult,
        },
        mesh::{event::MeshEvent, partition::Partition},
    };

    #[test]
    fn single_source_replication() {
        let mut runner = ReplicationTestRunner::new_anyapp("A", "B");
        let mut anyapp_a = AnyApplicationTestController::new("A");

        // Do not replicate object that has no status (new resource)
        runner.kube_partition_a(
            &anyapp_a.kube_snap(),
            &mesh_snap(vec![]),
            Vec::<MergeResult>::new(),
        );

        // Replicate object update with status set
        anyapp_a.inc_version();
        anyapp_a.with_initial_state("A", "New");
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_a_with_version.merge_cre()],
        );

        // Replicate object update with placements
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_a.set_placements(anyplacements("A", None));
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_a_with_version.merge_upd()],
        );

        // Replicate object update with placements and new condition
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_a.set_conditions(Some(vec![anycond(0, "A", "type")]));

        let anyapp_a_updated = anyapp_a
            .with_updated_owner_version()
            .with_updated_zone_conditions();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // Replicate object - condition update
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_a.set_conditions(Some(vec![anycond(0, "A", "type2")]));

        let anyapp_a_updated = anyapp_a
            .with_updated_owner_version()
            .with_updated_zone_conditions();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // Replicate object - condition delete
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_a.set_conditions(Some(vec![]));

        let mut anyapp_a_updated = anyapp_a
            .with_updated_owner_version()
            .with_updated_zone_conditions();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // Replicate object delete
        anyapp_a = anyapp_a_updated.with_incremented_version();

        let anyapp_a_updated = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_del(),
            &anyapp_a_updated.mesh_del(),
            vec![anyapp_a_updated.merge_del()],
        );
    }

    #[test]
    fn two_zones_interaction() {
        let mut runner = ReplicationTestRunner::new_anyapp("A", "B");
        let mut anyapp_a = AnyApplicationTestController::new("A");
        // let mut anyapp_b = AnyApplicationTestController::new("B");

        // Do not replicate object that has no status (new resource)
        runner.kube_partition_a(
            &anyapp_a.kube_snap(),
            &mesh_snap(vec![]),
            Vec::<MergeResult>::new(),
        );

        // Replicate object update with status set
        anyapp_a.inc_version();
        anyapp_a.with_initial_state("A", "New");
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_a_with_version.merge_cre()],
        );

        // Replicate object to zone B update with placements
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_a.set_placements(anyplacements("A", Some("B")));
        let mut anyapp_a_with_version = anyapp_a.with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_with_version.mesh_upd(),
            vec![anyapp_a_with_version.merge_upd()],
        );

        // conditions of A replicate to B
        anyapp_a = anyapp_a_with_version.with_incremented_version();
        anyapp_a.add_condition(anycond(0, "A", "type"));

        let anyapp_a_updated = anyapp_a
            .with_updated_owner_version()
            .with_updated_zone_conditions();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );
        let mut anyapp_b = anyapp_a_updated.as_zone("B", 1);

        // conditions of B replicate to A
        anyapp_b.add_condition(anycond(0, "B", "type"));

        let anyapp_b_updated = anyapp_b.with_updated_zone_conditions();

        runner.kube_partition_b(
            &anyapp_b.kube_upd(),
            &anyapp_b_updated.mesh_upd(),
            vec![anyapp_b_updated.merge_upd()],
        );

        // update of condition of A replicate to B
        anyapp_a = anyapp_b_updated.as_zone("A", anyapp_a_updated.incoming_version);
        anyapp_a.inc_version();
        anyapp_a.update_condition("type", "A", anycond(0, "A", "type2"));

        let anyapp_a_updated = anyapp_a
            .with_updated_zone_conditions()
            .with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // update of condition of B replicate to A
        let mut anyapp_b = anyapp_a_updated.as_zone("B", anyapp_b_updated.incoming_version);
        anyapp_b.inc_version();
        anyapp_b.update_condition("type", "B", anycond(0, "B", "type3"));

        let anyapp_b_updated = anyapp_b.with_updated_zone_conditions();
        runner.kube_partition_b(
            &anyapp_b.kube_upd(),
            &anyapp_b_updated.mesh_upd(),
            vec![anyapp_b_updated.merge_upd()],
        );

        // delete of condition of A replicate to B
        anyapp_a = anyapp_b_updated.as_zone("A", anyapp_a_updated.incoming_version);
        anyapp_a.inc_version();
        anyapp_a.delete_condition("type2", "A");

        let anyapp_a_updated = anyapp_a
            .with_updated_zone_conditions()
            .with_updated_owner_version();

        runner.kube_partition_a(
            &anyapp_a.kube_upd(),
            &anyapp_a_updated.mesh_upd(),
            vec![anyapp_a_updated.merge_upd()],
        );

        // delete of condition of B replicate to A
        let mut anyapp_b = anyapp_a_updated.as_zone("B", anyapp_b_updated.incoming_version);
        anyapp_b.inc_version();
        anyapp_b.delete_condition("type3", "B");

        let anyapp_b_updated = anyapp_b.with_updated_zone_conditions();
        runner.kube_partition_b(
            &anyapp_b.kube_upd(),
            &anyapp_b_updated.mesh_upd(),
            vec![anyapp_b_updated.merge_upd()],
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
        owner_version: Version,
        spec: AnyApplicationSpec,
        status: Option<AnyApplicationStatus>,
    ) -> DynamicObject {
        let resource = AnyApplication {
            metadata: ObjectMeta {
                name: Some("nginx-app".into()),
                namespace: Some("default".into()),
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
            ReplicationTestRunner {
                partition_a: Partition::new(AnyApplicationMerge::new()),
                partition_b: Partition::new(AnyApplicationMerge::new()),
                zone_a: zone_a.into(),
                zone_b: zone_b.into(),
            }
        }

        pub fn kube_partition_a(
            &mut self,
            kube_event_a: &KubeEvent,
            mesh_event_a: &MeshEvent,
            merge_result_b: Vec<MergeResult>,
        ) {
            let actual_mesh_event: Option<MeshEvent> = self
                .partition_a
                .kube_apply(kube_event_a, &self.zone_a)
                .unwrap()
                .into();
            assert_eq!(
                mesh_event_a,
                actual_mesh_event.as_ref().unwrap(),
                "mesh event"
            );

            let actual_merge_result = self
                .partition_b
                .mesh_apply(actual_mesh_event.unwrap(), &self.zone_a)
                .unwrap();
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
                .kube_apply(kube_event_b, &self.zone_b)
                .unwrap()
                .into();
            assert_eq!(
                mesh_event_b,
                actual_mesh_event.as_ref().unwrap(),
                "mesh event"
            );

            let actual_merge_result = self
                .partition_a
                .mesh_apply(actual_mesh_event.unwrap(), &self.zone_b)
                .unwrap();
            assert_eq!(actual_merge_result, merge_result_a, "merge result");
        }
    }

    #[derive(Clone)]
    struct AnyApplicationTestController {
        object: AnyApplication,
        zone: String,
        incoming_version: Version,
    }

    impl AnyApplicationTestController {
        pub fn new(zone: &str) -> AnyApplicationTestController {
            let incoming_version = 1;
            let object = AnyApplication {
                metadata: ObjectMeta {
                    name: Some("nginx-app".into()),
                    namespace: Some("default".into()),
                    labels: None,
                    ..Default::default()
                },
                spec: anyspec(1),
                status: None,
            };

            AnyApplicationTestController {
                object,
                zone: zone.into(),
                incoming_version,
            }
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
                        conditions: None,
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
                        conditions: None,
                        placements: Some(placements),
                    })
                }
            }
        }

        pub fn set_conditions(&mut self, conditions: Option<Vec<AnyApplicationStatusConditions>>) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                conditions: None,
                placements: None,
            });
            status.conditions = conditions;
        }

        pub fn add_condition(&mut self, condition: AnyApplicationStatusConditions) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                conditions: None,
                placements: None,
            });
            status.conditions.get_or_insert(vec![]).push(condition);
            status
                .conditions
                .get_or_insert(vec![])
                .sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
        }

        pub fn update_condition(
            &mut self,
            cond_type: &str,
            zone: &str,
            cond: AnyApplicationStatusConditions,
        ) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                conditions: None,
                placements: None,
            });
            let mut updated = false;
            for existing in status.conditions.get_or_insert(vec![]).iter_mut() {
                if existing.r#type == cond_type && existing.zone_id == zone {
                    *existing = cond;
                    updated = true;
                    break;
                }
            }
            if !updated {
                panic!("condition '{cond_type}' is not updated");
            }
            status
                .conditions
                .get_or_insert(vec![])
                .sort_by(|a, b| a.zone_id.cmp(&b.zone_id));
        }

        pub fn delete_condition(&mut self, cond_type: &str, zone: &str) {
            let status = self.object.status.get_or_insert(AnyApplicationStatus {
                owner: "".into(),
                state: "".into(),
                conditions: None,
                placements: None,
            });
            if let Some(conditions) = status.conditions.as_mut() {
                if let Some(pos) = conditions
                    .iter()
                    .position(|c| c.r#type == cond_type && c.zone_id == zone)
                {
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
            let object = self.object();
            let mut snapshot = BTreeMap::new();
            let name = object.get_namespaced_name();
            snapshot.insert(name, object.clone());
            KubeEvent::Snapshot {
                version: self.incoming_version,
                snapshot,
            }
        }

        fn kube_upd(&self) -> KubeEvent {
            let object = self.object();
            KubeEvent::Update {
                version: self.incoming_version,
                object,
            }
        }

        fn kube_del(&self) -> KubeEvent {
            let object = self.object();
            KubeEvent::Delete {
                version: self.incoming_version,
                object,
            }
        }

        fn mesh_upd(&self) -> MeshEvent {
            let object = self.object();
            MeshEvent::Update { object }
        }

        fn mesh_del(&self) -> MeshEvent {
            let object = self.object();
            MeshEvent::Delete { object }
        }

        fn merge_cre(&self) -> MergeResult {
            let object = self.object();
            MergeResult::Create { object }
        }

        fn merge_upd(&self) -> MergeResult {
            let object = self.object();
            MergeResult::Update {
                object: object.to_owned(),
            }
        }

        fn merge_del(&self) -> MergeResult {
            let object = self.object();
            MergeResult::Delete {
                gvk: object.get_gvk().unwrap(),
                name: object.get_namespaced_name(),
            }
        }

        fn inc_version(&mut self) {
            self.incoming_version += 1;
        }

        pub fn as_zone(&self, zone: &str, version: Version) -> Self {
            let mut copy = self.clone();
            copy.zone = zone.into();
            copy.incoming_version = version;
            copy
        }

        fn with_updated_owner_version(&mut self) -> Self {
            let mut copy = self.clone();
            copy.object.set_owner_version(self.incoming_version);
            copy
        }

        fn with_incremented_version(&mut self) -> Self {
            let mut copy = self.clone();
            copy.inc_version();
            copy
        }

        fn with_updated_zone_conditions(&mut self) -> Self {
            let mut copy = self.clone();
            copy.object
                .set_condition_version(&self.zone, self.incoming_version);
            copy
        }
    }
}
