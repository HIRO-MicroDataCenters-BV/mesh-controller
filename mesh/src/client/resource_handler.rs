use std::{pin::Pin, sync::Arc};

use super::{request::ApiRequest, response::ApiResponse, storage::Storage, types::ApiHandler};
use anyhow::anyhow;
use http::{Response, StatusCode};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList;
use kube::{
    api::{ApiResource, DynamicObject, ListMeta, ObjectList, TypeMeta},
    client::Body,
};

pub struct CustomResourceHandler {
    storage: Arc<Storage>,
}

impl CustomResourceHandler {
    pub fn new(storage: Arc<Storage>) -> CustomResourceHandler {
        CustomResourceHandler { storage }
    }
}

impl ApiHandler for CustomResourceHandler {
    type Fut = Pin<Box<dyn Future<Output = Result<ApiResponse, anyhow::Error>> + Send + Sync>>;

    fn get(&mut self, request: ApiRequest) -> Self::Fut {
        let maybe_response = self
            .storage
            .find_objects(
                &request.group,
                &request.version,
                &request.kind_plural.unwrap(),
            )
            .map(|(ar, objects)| to_object_list(&ar, objects));

        Box::pin(async {
            maybe_response
                .map(|response| ApiResponse::try_from(StatusCode::OK, response))
                .unwrap_or_else(|| Ok(ApiResponse::new(StatusCode::NOT_FOUND, String::new())))
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
