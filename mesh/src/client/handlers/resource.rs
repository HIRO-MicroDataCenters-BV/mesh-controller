use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use anyhow::anyhow;
use bytes::Bytes;
use http::StatusCode;
use kube::api::{ApiResource, DynamicObject, ListMeta, ObjectList, TypeMeta};
use kube::client::Status;
use kube::core::ErrorResponse;
use kube::core::response::StatusDetails;
use kube::core::response::StatusSummary;

use crate::kube::dynamic_object_ext::DynamicObjectExt;
use crate::{
    client::{
        response::ApiResponse,
        storage::Storage,
        types::{ApiHandler, ApiHandlerResponse, ApiHandlerWatchResponse},
    },
    kube::types::NamespacedName,
};

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
    pub resource_name: Option<NamespacedName>,
    pub input: Bytes,
    pub params: BTreeMap<String, String>,
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
                    return ApiResponse::try_from(StatusCode::OK, to_object_list(&ar, objects));
                }
            } else {
                return ApiResponse::try_from(
                    StatusCode::NOT_FOUND,
                    to_not_found(&group, &version, &kind_plural),
                );
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
                .subscribe(&group, &version, &kind_plural, resource_version)
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
                ..
            } = request;
            let name = resource_name.ok_or(anyhow!("Resource name is not set"))?;
            let object: DynamicObject =
                serde_json::from_slice(&input).context("Failed to deserialize input body")?;
            let maybe_response = storage
                .create_or_update(&group, &version, &kind_plural, &name, object)
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
