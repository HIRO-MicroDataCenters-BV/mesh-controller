use crate::client::kube_client::watcher::Event;
use crate::{
    config::configuration::{Config, KubeConfiguration},
    kube::{dynamic_object_ext::DynamicObjectExt, types::NamespacedName},
};
use anyhow::{Context, Result};
use either::Either;
use fake_kube_api::service::FakeKubeApiService;
use futures::Stream;
use kube::Error;
use kube::{
    Api,
    api::{DeleteParams, DynamicObject, GroupVersionKind, Patch, PatchParams},
    core::Status,
    runtime::watcher,
};

#[derive(Clone)]
pub struct KubeClient {
    client: kube::Client,
}

impl KubeClient {
    pub async fn build(config: &Config) -> Result<KubeClient> {
        let kube_config = KubeClient::to_kube_config(config).await?;
        let client = kube::Client::try_from(kube_config).context("failed to create kube client")?;
        Ok(KubeClient { client })
    }

    pub fn build_fake(svc: FakeKubeApiService) -> KubeClient {
        let client = kube::Client::new(svc, "default");
        KubeClient { client }
    }

    async fn to_kube_config(config: &Config) -> Result<kube::config::Config> {
        match config.kubernetes.as_ref() {
            Some(kube_config) => match kube_config {
                KubeConfiguration::InCluster => kube::config::Config::infer()
                    .await
                    .context("failed to infer kube config"),
                KubeConfiguration::External(external_config) => {
                    let kube_context = external_config
                        .kube_context
                        .to_owned()
                        .unwrap_or("default".into());
                    let options = kube::config::KubeConfigOptions {
                        context: Some(kube_context),
                        ..Default::default()
                    };
                    kube::config::Config::from_kubeconfig(&options)
                        .await
                        .context("failed to create kube config from path")
                }
            },
            None => kube::config::Config::infer()
                .await
                .context("failed to infer kube config"),
        }
    }

    pub async fn direct_get(
        &self,
        gvk: &GroupVersionKind,
        namspaced_name: &NamespacedName,
    ) -> Result<Option<DynamicObject>> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(
            self.client.clone(),
            &namspaced_name.namespace,
            &ar,
        );
        let results = api.get(&namspaced_name.name).await;
        if object_not_found(&results) {
            Ok(None)
        } else {
            Ok(Some(results?))
        }
    }

    pub async fn direct_delete(
        &self,
        gvk: &GroupVersionKind,
        namspaced_name: &NamespacedName,
    ) -> Result<Either<DynamicObject, Status>> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        let api = Api::<DynamicObject>::namespaced_with(
            self.client.clone(),
            &namspaced_name.namespace,
            &ar,
        );
        let params = DeleteParams::default();
        let status = api.delete(&namspaced_name.name, &params).await?;
        Ok(status)
    }

    pub async fn direct_patch_apply(&self, resource: DynamicObject) -> Result<()> {
        let gvk = resource.get_gvk()?;
        let ns_name = resource.get_namespaced_name();

        let (ar, _) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), &ns_name.namespace, &ar);
        let patch_params = PatchParams::apply(&ns_name.name).force();

        api.patch(&ns_name.name, &patch_params, &Patch::Apply(resource))
            .await?;

        Ok(())
    }

    pub async fn event_stream(
        &self,
        gvk: &GroupVersionKind,
    ) -> Result<impl Stream<Item = Result<Event<DynamicObject>, watcher::Error>> + Send> {
        let (ar, _caps) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        let api = Api::<DynamicObject>::all_with(self.client.clone(), &ar);
        let wc = watcher::Config::default();
        let stream = watcher::watcher(api, wc);
        Ok(stream)
    }
}

pub fn object_not_found(results: &Result<DynamicObject, Error>) -> bool {
    match results {
        Err(Error::Api(response)) => response.reason == "NotFound",
        _ => false,
    }
}
