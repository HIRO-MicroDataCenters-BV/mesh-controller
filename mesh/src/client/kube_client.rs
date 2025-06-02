use crate::client::kube_client::watcher::Event;
use crate::{
    config::configuration::{Config, KubeConfiguration},
    kube::{dynamic_object_ext::DynamicObjectExt, types::NamespacedName},
};
use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use either::Either;
use fake_kube_api::service::FakeKubeApiService;
use futures::Stream;
use kube::Error;
use kube::api::ApiResource;
use kube::{
    Api,
    api::{DeleteParams, DynamicObject, GroupVersionKind, Patch, PatchParams},
    core::Status,
    runtime::watcher,
};

#[derive(Clone)]
pub struct KubeClient {
    client: kube::Client,
    resources: DashMap<GroupVersionKind, ApiResource>,
}

impl KubeClient {
    pub async fn build(config: &Config) -> Result<KubeClient> {
        let kube_config = KubeClient::to_kube_config(config).await?;
        let client = kube::Client::try_from(kube_config).context("failed to create kube client")?;
        Ok(KubeClient {
            client,
            resources: DashMap::new(),
        })
    }

    pub fn build_fake(svc: FakeKubeApiService) -> KubeClient {
        let client = kube::Client::new(svc, "default");
        KubeClient {
            client,
            resources: DashMap::new(),
        }
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
        name: &NamespacedName,
    ) -> Result<Option<DynamicObject>> {
        let results = self
            .get_or_resolve_namespaced_api(gvk, &name.namespace)
            .await?
            .get(&name.name)
            .await;
        if object_not_found(&results) {
            Ok(None)
        } else {
            Ok(Some(results?))
        }
    }

    pub async fn direct_delete(
        &self,
        gvk: &GroupVersionKind,
        name: &NamespacedName,
    ) -> Result<Either<DynamicObject, Status>> {
        let params = DeleteParams::default();
        let status = self
            .get_or_resolve_namespaced_api(gvk, &name.namespace)
            .await?
            .delete(&name.name, &params)
            .await?;
        Ok(status)
    }

    pub async fn direct_patch_apply(&self, resource: DynamicObject) -> Result<()> {
        let gvk = resource.get_gvk()?;
        let name = resource.get_namespaced_name();

        let patch_params = PatchParams::apply(&name.name).force();
        self.get_or_resolve_namespaced_api(&gvk, &name.namespace)
            .await?
            .patch(&name.name, &patch_params, &Patch::Apply(resource))
            .await?;

        Ok(())
    }

    async fn get_or_resolve_namespaced_api(
        &self,
        gvk: &GroupVersionKind,
        namespace: &str,
    ) -> Result<Api<DynamicObject>> {
        if !self.resources.contains_key(gvk) {
            let (ar, _) = kube::discovery::pinned_kind(&self.client, gvk).await?;
            self.resources.insert(gvk.clone(), ar);
        }
        self.resources
            .get(gvk)
            .ok_or(anyhow!(
                "Should not happend, resource not found in cache: {:?}",
                gvk
            ))
            .map(|resource| {
                Api::<DynamicObject>::namespaced_with(
                    self.client.clone(),
                    namespace,
                    resource.value(),
                )
            })
    }

    pub async fn event_stream_for(
        &self,
        gvk: &GroupVersionKind,
        namespace: &Option<String>,
    ) -> Result<impl Stream<Item = Result<Event<DynamicObject>, watcher::Error>> + Send> {
        let api = match namespace {
            Some(namespace) => self.get_or_resolve_namespaced_api(gvk, namespace).await?,
            None => self.get_or_resolve_all_api(gvk).await?,
        };

        let wc = watcher::Config::default();
        Ok(watcher::watcher(api, wc))
    }

    async fn get_or_resolve_all_api(&self, gvk: &GroupVersionKind) -> Result<Api<DynamicObject>> {
        if !self.resources.contains_key(gvk) {
            let (ar, _) = kube::discovery::pinned_kind(&self.client, gvk).await?;
            self.resources.insert(gvk.clone(), ar);
        }
        self.resources
            .get(gvk)
            .ok_or(anyhow!(
                "Should not happend, resource not found in cache: {:?}",
                gvk
            ))
            .map(|resource| Api::<DynamicObject>::all_with(self.client.clone(), resource.value()))
    }
}

pub fn object_not_found(results: &Result<DynamicObject, Error>) -> bool {
    match results {
        Err(Error::Api(response)) => response.reason == "NotFound",
        _ => false,
    }
}
