use super::types::{MergeResult, MergeStrategy};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::Result;
use kube::api::DynamicObject;

pub struct DefaultMerge {}

impl DefaultMerge {
    pub fn new() -> Self {
        DefaultMerge {}
    }

}

impl MergeStrategy for DefaultMerge {
    fn merge_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &String,
    ) -> Result<MergeResult> {
        let incoming = incoming.clone();
        if let Some(current) = current {
            let current_owner_version = current.get_owner_version()?;

            let incoming_owner_version = incoming.get_owner_version()?;
            let incoming_owner_zone = incoming.get_owner_zone()?;

            let acceptable_zone = &incoming_owner_zone == incoming_zone;
            let new_version = incoming_owner_version > current_owner_version;

            if acceptable_zone && new_version {
                let mut object = incoming.clone();
                object.metadata.managed_fields = None;
                object.metadata.uid = None;
                Ok(MergeResult::Update { object })
            } else {
                Ok(MergeResult::DoNothing)
            }
        } else {
            let mut object = incoming.clone();
            object.metadata.managed_fields = None;
            object.metadata.uid = None;
            Ok(MergeResult::Create { object })
        }
    }

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &String,
    ) -> Result<MergeResult> {
        if let Some(current) = current {
            let current_owner_version = current.get_owner_version()?;

            let incoming_owner_version = incoming.get_owner_version()?;
            let incoming_owner_zone = incoming.get_owner_zone()?;

            let acceptable_zone = &incoming_owner_zone == incoming_zone;
            let new_version = incoming_owner_version > current_owner_version;

            if acceptable_zone && new_version {
                Ok(MergeResult::Delete {
                    name: incoming.get_namespaced_name(),
                })
            } else {
                Ok(MergeResult::DoNothing)
            }
        } else {
            Ok(MergeResult::DoNothing)
        }
    }
}

#[cfg(test)]
pub mod tests {

    #[test]
    pub fn test_default_merge() {}
}
