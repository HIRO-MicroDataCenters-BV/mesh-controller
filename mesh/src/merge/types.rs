use anyhow::Result;
use kube::api::DynamicObject;

use crate::kube::types::NamespacedName;

pub enum MergeResult {
    Create { object: DynamicObject },
    Update { object: DynamicObject },
    Delete { name: NamespacedName },
    DoNothing,
    Conflict { msg: String },
}

pub trait MergeStrategy {
    fn merge_update(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
    ) -> Result<MergeResult>;

    fn merge_delete(
        &self,
        current: Option<DynamicObject>,
        incoming: &DynamicObject,
    ) -> Result<MergeResult>;
}
