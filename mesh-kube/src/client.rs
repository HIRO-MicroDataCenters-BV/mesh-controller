use crate::client::watcher::Event;
use crate::config::KubeConfiguration;
use crate::kube::subscriptions::Version;
use crate::kube::{dynamic_object_ext::DynamicObjectExt, types::NamespacedName};
use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use either::Either;
use futures::Stream;
use kube::Error;
use kube::api::{ApiResource, ListParams};
use kube::config::{InferConfigError, KubeconfigError};
use kube::core::dynamic::ParseDynamicObjectError;
use kube::{
    Api,
    api::{DeleteParams, DynamicObject, GroupVersionKind, Patch, PatchParams},
    core::Status,
    runtime::watcher,
};

use serde_json::json;
use tracing::error;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("Configuration Error: {0}")]
    Configuration(#[from] BoxedError),

    #[error("Kube Error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Invariant Error: {0}")]
    Invariant(#[from] anyhow::Error),

    #[error("Resource Format Error: {0}")]
    ResourceFormatError(#[from] ParseDynamicObjectError),

    #[error("Version conflict")]
    VersionConflict,
}

impl From<InferConfigError> for ClientError {
    fn from(value: InferConfigError) -> Self {
        ClientError::Configuration(Box::new(value))
    }
}

impl From<KubeconfigError> for ClientError {
    fn from(value: KubeconfigError) -> Self {
        ClientError::Configuration(Box::new(value))
    }
}

#[derive(Clone)]
pub struct KubeClient {
    client: kube::Client,
    resources: DashMap<GroupVersionKind, ApiResource>,
}

impl KubeClient {
    pub async fn build(config: &KubeConfiguration) -> Result<KubeClient, ClientError> {
        let kube_config = KubeClient::to_kube_config(config).await?;
        let client = kube::Client::try_from(kube_config).context("failed to create kube client")?;
        Ok(KubeClient {
            client,
            resources: DashMap::new(),
        })
    }

    pub fn build_fake(svc: fake_kube_api::service::FakeEtcdServiceWrapper) -> KubeClient {
        let client = kube::Client::new(svc, "default");
        KubeClient {
            client,
            resources: DashMap::new(),
        }
    }

    async fn to_kube_config(
        config: &KubeConfiguration,
    ) -> Result<kube::config::Config, ClientError> {
        match &config {
            KubeConfiguration::InCluster => {
                kube::config::Config::infer().await.map_err(|e| e.into())
            }
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
                    .map_err(|e| e.into())
            }
        }
    }

    pub async fn get(
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

    pub async fn list(
        &self,
        gvk: &GroupVersionKind,
        namespace: &Option<String>,
    ) -> Result<Vec<DynamicObject>> {
        let namespace: &str = namespace
            .as_ref()
            .map(|ns| ns.as_str())
            .unwrap_or("default");
        let list_params = ListParams::default();
        let results = self
            .get_or_resolve_namespaced_api(gvk, namespace)
            .await?
            .list(&list_params)
            .await
            .map(|list| list.items)?;
        Ok(results)
    }

    pub async fn delete(
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

    pub async fn patch_apply(&self, mut resource: DynamicObject) -> Result<Version, ClientError> {
        let gvk = resource.get_gvk()?;
        let name = resource.get_namespaced_name();
        let status = resource.get_status();

        resource.metadata.managed_fields = None;

        let api = self
            .get_or_resolve_namespaced_api(&gvk, &name.namespace)
            .await?;

        let mut result_version = api
            .patch(
                &name.name,
                &PatchParams::apply(&name.name),
                &Patch::Apply(resource),
            )
            .await
            .map(|obj| obj.get_resource_version())
            .map_err(|err| {
                if is_conflict(&err) {
                    ClientError::VersionConflict
                } else {
                    error!("patch spec error {err:?}");
                    err.into()
                }
            })?;

        if let Some(mut status) = status {
            status["metadata"] = json!({ "resourceVersion": Some(result_version.to_string()) });

            result_version = api
                .patch_status(&name.name, &PatchParams::default(), &Patch::Merge(&status))
                .await
                .map(|obj| obj.get_resource_version())
                .map_err(|err| {
                    if is_conflict(&err) {
                        ClientError::VersionConflict
                    } else {
                        error!("patch spec error {err:?}");
                        err.into()
                    }
                })?;
        }

        Ok(result_version)
    }

    async fn get_or_resolve_namespaced_api(
        &self,
        gvk: &GroupVersionKind,
        namespace: &str,
    ) -> Result<Api<DynamicObject>, ClientError> {
        if !self.resources.contains_key(gvk) {
            let (ar, _) = kube::discovery::pinned_kind(&self.client, gvk).await?;
            self.resources.insert(gvk.clone(), ar);
        }
        self.resources
            .get(gvk)
            .ok_or(ClientError::Invariant(anyhow!(
                "Should not happend, resource not found in cache: {:?}",
                gvk
            )))
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
    ) -> Result<impl Stream<Item = Result<Event<DynamicObject>, watcher::Error>> + Send, ClientError>
    {
        let api = match namespace {
            Some(namespace) => self.get_or_resolve_namespaced_api(gvk, namespace).await?,
            None => self.get_or_resolve_all_api(gvk).await?,
        };

        let wc = watcher::Config::default();
        Ok(watcher::watcher(api, wc))
    }

    async fn get_or_resolve_all_api(
        &self,
        gvk: &GroupVersionKind,
    ) -> Result<Api<DynamicObject>, ClientError> {
        if !self.resources.contains_key(gvk) {
            let (ar, _) = kube::discovery::pinned_kind(&self.client, gvk).await?;
            self.resources.insert(gvk.clone(), ar);
        }
        self.resources
            .get(gvk)
            .ok_or(ClientError::Invariant(anyhow!(
                "Should not happend, resource not found in cache: {:?}",
                gvk
            )))
            .map(|resource| Api::<DynamicObject>::all_with(self.client.clone(), resource.value()))
    }

    pub async fn get_latest_version(&self) -> Result<Version> {
        use k8s_openapi::api::core::v1::Pod;
        let pods: Api<Pod> = Api::all(self.client.clone());

        let lp = ListParams {
            resource_version: Some("0".to_string()),
            // 0 - value is interpreted as "set server default".
            // To effectively limit we have to set value to 1.
            limit: Some(1),
            ..Default::default()
        };

        let pod_list = pods.list_metadata(&lp).await?;

        let latest_version_str = pod_list.metadata.resource_version.unwrap_or("0".into());
        let latest = latest_version_str.parse::<Version>().unwrap_or_default();
        Ok(latest)
    }

    pub async fn emit_event(&self, event: k8s_openapi::api::core::v1::Event) -> Result<()> {
        use k8s_openapi::api::core::v1::Event;

        let ns = event.metadata.namespace.as_deref().unwrap_or("default");
        let events: kube::Api<Event> = kube::Api::namespaced(self.client.clone(), ns);

        events.create(&Default::default(), &event).await?;
        Ok(())
    }
}

pub fn object_not_found(results: &Result<DynamicObject, Error>) -> bool {
    match results {
        Err(Error::Api(response)) => response.reason == "NotFound",
        _ => false,
    }
}

fn is_conflict(error: &Error) -> bool {
    if let Error::Api(response) = error {
        response.code == 409
    } else {
        false
    }
}
