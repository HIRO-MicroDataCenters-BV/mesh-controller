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

    pub fn apply_mesh(
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
                self.update_partition(&result);
                Ok(vec![result])
            }
            MeshEvent::Delete { object } => {
                let name = object.get_namespaced_name();
                let current = self.resources.get(&name).cloned();
                let result = self
                    .merge_strategy
                    .mesh_delete(current, &object, incoming_zone)?;
                self.update_partition(&result);
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
                        self.update_partition(&result);
                        results.push(result);
                    }
                }
                for name in to_update {
                    let current = self.resources.get(&name).cloned();
                    let object = snapshot.get(&name).unwrap();
                    let result = self
                        .merge_strategy
                        .mesh_update(current, object, incoming_zone)?;
                    self.update_partition(&result);
                    results.push(result);
                }
                Ok(results)
            }
        }
    }

    fn update_partition(&mut self, result: &MergeResult) {
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

    pub fn snapshot(&self, current_zone: &str) -> MeshEvent {
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

    pub fn apply_kube(&mut self, event: &KubeEvent, current_zone: &str) -> Result<UpdateResult> {
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
                self.local_update_partition(&result, current_zone)?;
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
                self.local_update_partition(&result, current_zone)?;
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
                        self.local_update_partition(&result, current_zone)?;
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
                        self.local_update_partition(&result, current_zone)?;
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
                    self.local_update_partition(&result, current_zone)?;
                    Ok(result)
                }
            }
        }
    }

    fn local_update_partition(&mut self, result: &UpdateResult, current_zone: &str) -> Result<()> {
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

    #[test]
    fn test() {}
}
