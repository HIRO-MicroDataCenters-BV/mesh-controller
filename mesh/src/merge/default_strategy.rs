use super::{
    anyapplication::{AnyApplication, AnyApplicationSpec, AnyApplicationStatus},
    types::{MergeResult, MergeStrategy},
};
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::{Context, Result, anyhow};
use kube::api::DynamicObject;
use serde::de::DeserializeOwned;

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
            Ok(MergeResult::Update { object: current })
        } else {
            Ok(MergeResult::Create { object: incoming })
        }
    }

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
        incoming_zone: &String,
    ) -> Result<MergeResult> {
        if let Some(_) = current {
            Ok(MergeResult::Delete {
                name: incoming.get_namespaced_name(),
            })
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
