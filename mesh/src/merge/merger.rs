use super::types::{MergeResult, MergeStrategy};
use crate::kube::{
    cache::object_not_found, dynamic_object_ext::DynamicObjectExt, types::NamespacedName,
};
use anyhow::Result;
use either::Either;
use kube::api::Patch;
use kube::{
    Api,
    api::{DeleteParams, DynamicObject, GroupVersionKind, PatchParams},
};

pub struct Merger {
    client: kube::Client,
}

impl Merger {
    pub fn new(client: kube::Client) -> Self {
        Merger { client }
    }

    pub async fn merge_update<M>(&self, incoming: &DynamicObject, strategy: &M) -> Result<()>
    where
        M: MergeStrategy,
    {
        let gvk = incoming.get_gvk()?;
        let current = self.get(&incoming.get_namespaced_name(), &gvk).await?;
        let result = strategy.merge_update(current, &incoming)?;
        self.handle_result(result, &gvk).await?;
        Ok(())
    }

    pub async fn merge_delete<M>(&self, incoming: &DynamicObject, strategy: &M) -> Result<()>
    where
        M: MergeStrategy,
    {
        let gvk = incoming.get_gvk()?;
        let current = self.get(&incoming.get_namespaced_name(), &gvk).await?;
        let result = strategy.merge_delete(current, &incoming)?;
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
                self.create_or_update(&object, &gvk).await?;
            }
            MergeResult::Update { object } => {
                self.create_or_update(&object, &gvk).await?;
            }
            MergeResult::Delete { name } => {
                self.delete(&name, &gvk).await?;
            }
            MergeResult::Conflict { msg } => {
                tracing::warn!("Conflict detected: {}", msg);
            }
            MergeResult::DoNothing => (),
        }
        Ok(())
    }

    pub async fn get(
        &self,
        name: &NamespacedName,
        gvk: &GroupVersionKind,
    ) -> Result<Option<DynamicObject>> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), &name.namespace, &ar);
        let results = api.get(&name.name).await;
        if object_not_found(&results) {
            Ok(None)
        } else {
            Ok(Some(results?))
        }
    }

    pub async fn create_or_update(
        &self,
        object: &DynamicObject,
        gvk: &GroupVersionKind,
    ) -> Result<()> {
        let ns_name = object.get_namespaced_name();

        let (ar, _) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), &ns_name.namespace, &ar);
        let patch_params = PatchParams::apply(&ns_name.name).force();

        api.patch(&ns_name.name, &patch_params, &Patch::Apply(object))
            .await?;
        Ok(())
    }

    pub async fn delete(&self, name: &NamespacedName, gvk: &GroupVersionKind) -> Result<()> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(self.client.clone(), &name.namespace, &ar);
        let params = DeleteParams::default();
        let status = api.delete(&name.name, &params).await?;
        match status {
            Either::Left(_obj) => {
                tracing::info!("Deleted object: {}", name);
            }
            Either::Right(status) => {
                tracing::info!("Deleted object: {} with status: {:?}", name, status);
            }
        }
        Ok(())
    }
}
