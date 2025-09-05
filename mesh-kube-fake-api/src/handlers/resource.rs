use anyhow::Context;
use anyhow::anyhow;
use bytes::Bytes;
use http::StatusCode;
use kube::api::{ApiResource, DynamicObject, ListMeta, ObjectList, TypeMeta};
use kube::client::Status;
use kube::core::ErrorResponse;
use kube::core::response::StatusDetails;
use kube::core::response::StatusSummary;
use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};
use tracing::info;

use crate::dynamic_object_ext::DynamicObjectExt;
use crate::dynamic_object_ext::NamespacedName;
use crate::response::ApiResponse;
use crate::storage::Storage;
use crate::types::ApiHandler;
use crate::types::ApiHandlerResponse;
use crate::types::ApiHandlerWatchResponse;

pub struct ResourceHandler {
    storage: Arc<Storage>,
}

impl ResourceHandler {
    pub fn new(storage: Arc<Storage>) -> ResourceHandler {
        ResourceHandler { storage }
    }
}

#[derive(Clone, Debug)]
pub struct ResourceArgs {
    pub group: String,
    pub version: String,
    pub kind_plural: String,
    pub subresource: Option<String>,
    pub resource_name: Option<NamespacedName>,
    pub input: Bytes,
    pub params: BTreeMap<String, String>,
}

impl ResourceArgs {
    pub fn try_new(
        path_params: HashMap<&str, &str>,
        query_params: BTreeMap<String, String>,
        input: Bytes,
    ) -> Result<Self, anyhow::Error> {
        let group = path_params
            .get("group")
            .ok_or_else(|| anyhow::anyhow!("Missing 'group' path parameter"))?
            .to_string();
        let version = path_params
            .get("version")
            .ok_or_else(|| anyhow::anyhow!("Missing 'version' path parameter"))?
            .to_string();
        let kind_plural = path_params
            .get("pluralkind")
            .ok_or_else(|| anyhow::anyhow!("Missing 'pluralkind' path parameter"))?
            .to_string();
        let subresource = path_params.get("subresource").cloned().map(|s| s.into());

        let resource_name = path_params.get("name").map(|n| {
            NamespacedName::new(
                path_params
                    .get("namespace")
                    .cloned()
                    .map(|s| s.into())
                    .unwrap_or_default(),
                (*n).into(),
            )
        });
        Ok(ResourceArgs {
            group,
            version,
            kind_plural,
            input,
            resource_name,
            subresource,
            params: query_params,
        })
    }
}

impl ApiHandler for ResourceHandler {
    type Req = ResourceArgs;

    fn get(&self, request: ResourceArgs) -> ApiHandlerResponse {
        let storage = self.storage.clone();
        Box::pin(async move {
            let ResourceArgs {
                group,
                version,
                kind_plural,
                resource_name,
                ..
            } = request;
            info!(
                "ApiHandler: Fetching resource: {group} {version} {kind_plural} {resource_name:?}"
            );
            if let Some((ar, objects)) = storage.find_objects(&group, &version, &kind_plural) {
                if let Some(resource_name) = resource_name {
                    let resource = objects
                        .into_iter()
                        .find(|obj| obj.get_namespaced_name() == resource_name);
                    resource
                        .map(|r| ApiResponse::try_from(StatusCode::OK, r))
                        .unwrap_or_else(|| {
                            ApiResponse::try_from(
                                StatusCode::NOT_FOUND,
                                to_not_found(&group, &version, &kind_plural),
                            )
                        })
                } else {
                    ApiResponse::try_from(StatusCode::OK, to_object_list(&ar, objects))
                }
            } else {
                ApiResponse::try_from(
                    StatusCode::NOT_FOUND,
                    to_not_found(&group, &version, &kind_plural),
                )
            }
        })
    }

    fn watch(&self, request: ResourceArgs) -> ApiHandlerWatchResponse {
        let storage = self.storage.clone();
        Box::pin(async move {
            let ResourceArgs {
                group,
                version,
                kind_plural,
                params,
                ..
            } = request;

            let resource_version = params
                .get("resourceVersion")
                .map(|v| v.parse::<u64>().unwrap_or(0))
                .unwrap_or(0);

            storage
                .watch_events(&group, &version, &kind_plural, resource_version)
                .await
        })
    }

    fn patch(&self, request: ResourceArgs) -> ApiHandlerResponse {
        let storage = self.storage.clone();

        Box::pin(async move {
            let ResourceArgs {
                group,
                version,
                kind_plural,
                input,
                resource_name,
                subresource,
                ..
            } = request;
            let name = resource_name.ok_or(anyhow!("Resource name is not set"))?;
            let object: DynamicObject =
                serde_json::from_slice(&input).context("Failed to deserialize input body")?;
            let maybe_response = if let Some(subresource) = subresource {
                storage
                    .patch_subresource(&group, &version, &kind_plural, &subresource, &name, object)
                    .await
            } else {
                storage
                    .patch(&group, &version, &kind_plural, &name, object)
                    .await
            };

            maybe_response.and_then(|response| ApiResponse::try_from(StatusCode::OK, response))
        })
    }

    fn post(&self, request: ResourceArgs) -> ApiHandlerResponse {
        let storage = self.storage.clone();

        Box::pin(async move {
            let ResourceArgs {
                group,
                version,
                kind_plural,
                input,
                resource_name,
                ..
            } = request;

            let mut object: DynamicObject =
                serde_json::from_slice(&input).context("Failed to deserialize input body")?;

            if resource_name.is_none() && object.metadata.name.is_none() {
                object.generate_name();
            }

            let name = resource_name.unwrap_or(object.get_namespaced_name());

            let maybe_response = storage
                .patch(&group, &version, &kind_plural, &name, object)
                .await;

            maybe_response.and_then(|response| ApiResponse::try_from(StatusCode::OK, response))
        })
    }

    fn delete(&self, request: ResourceArgs) -> ApiHandlerResponse {
        let storage = self.storage.clone();
        Box::pin(async move {
            let ResourceArgs {
                group,
                version,
                kind_plural,
                resource_name,
                ..
            } = request;
            let name = resource_name.ok_or(anyhow!("Resource name is not set"))?;

            let maybe_response = storage
                .delete(&group, &version, &kind_plural, &name)
                .await
                .map(|_o| to_delete_status_success(&name.name, &group, &kind_plural));

            maybe_response.and_then(|response| ApiResponse::try_from(StatusCode::OK, response))
        })
    }
}

fn to_object_list(ar: &ApiResource, objects: Vec<DynamicObject>) -> ObjectList<DynamicObject> {
    let resource_version = objects
        .iter()
        .flat_map(|obj| &obj.metadata.resource_version)
        .map(|v| v.parse::<u64>().unwrap_or(0))
        .max()
        .map(|v| v.to_string())
        .unwrap_or("0".into());

    ObjectList {
        types: TypeMeta {
            api_version: ar.api_version.to_owned(),
            kind: format!("{}List", ar.kind),
        },
        metadata: ListMeta {
            resource_version: Some(resource_version),
            continue_: Some("".into()),
            ..Default::default()
        },
        items: objects,
    }
}

fn to_not_found(group: &str, version: &str, kind: &str) -> ErrorResponse {
    ErrorResponse {
        status: "Failure".into(),
        message: format!("{group} {version} {kind} not found"),
        reason: "NotFound".into(),
        code: 404,
    }
}

fn to_delete_status_success(name: &str, group: &str, kind: &str) -> Status {
    Status {
        status: Some(StatusSummary::Success),
        details: Some(StatusDetails {
            name: name.into(),
            group: group.into(),
            kind: kind.into(),
            uid: "test".into(),
            causes: vec![],
            retry_after_seconds: 0,
        }),
        ..Default::default()
    }
}
