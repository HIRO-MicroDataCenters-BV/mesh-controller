use super::types::{MergeResult, MergeStrategy};
use crate::client::kube_client::KubeClient;
use crate::kube::dynamic_object_ext::DynamicObjectExt;
use anyhow::Result;
use kube::api::{DynamicObject, GroupVersionKind};

pub struct Merger {
    client: KubeClient,
}

impl Merger {
    pub fn new(client: KubeClient) -> Self {
        Merger { client }
    }

    pub async fn merge_update<M>(
        &self,
        source_zone: &String,
        incoming: &DynamicObject,
        strategy: &M,
    ) -> Result<()>
    where
        M: MergeStrategy,
    {
        let gvk = incoming.get_gvk()?;
        let current = self
            .client
            .direct_get(&gvk, &incoming.get_namespaced_name())
            .await?;
        let result = strategy.merge_update(current, &incoming, source_zone)?;
        self.handle_result(result, &gvk).await?;
        Ok(())
    }

    pub async fn merge_delete<M>(
        &self,
        source_zone: &String,
        incoming: &DynamicObject,
        strategy: &M,
    ) -> Result<()>
    where
        M: MergeStrategy,
    {
        let gvk = incoming.get_gvk()?;
        let current = self
            .client
            .direct_get(&gvk, &incoming.get_namespaced_name())
            .await?;
        let result = strategy.merge_delete(current, &incoming, source_zone)?;
        self.handle_result(result, &gvk).await?;
        Ok(())
    }

    pub async fn handle_result(
        &self,
        merge_result: MergeResult,
        gvk: &GroupVersionKind,
    ) -> Result<()> {
        match merge_result {
            MergeResult::Create { object } => {
                self.client.direct_patch_apply(object).await?;
            }
            MergeResult::Update { object } => {
                self.client.direct_patch_apply(object).await?;
            }
            MergeResult::Delete { name } => {
                self.client.direct_delete(gvk, &name).await?;
            }
            MergeResult::Conflict { msg } => {
                tracing::warn!("Conflict detected: {}", msg);
            }
            MergeResult::DoNothing => (),
        }
        Ok(())
    }
}
