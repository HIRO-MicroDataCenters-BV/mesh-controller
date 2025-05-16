use std::{pin::Pin, sync::Arc};

use super::{request::ApiRequest, response::ApiResponse, storage::Storage, types::ApiHandler};
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
    fn object_list(ar: &ApiResource, objects: Vec<DynamicObject>) -> ObjectList<DynamicObject> {
        let result: ObjectList<DynamicObject> = ObjectList {
            types: TypeMeta {
                api_version: ar.api_version.to_owned(),
                kind: format!("{}List", ar.kind),
            },
            metadata: ListMeta {
                resource_version: Some("1".into()),
                ..Default::default()
            },
            items: objects,
        };
        result
    }
}

impl ApiHandler for CustomResourceHandler {
    type Fut = Pin<Box<dyn Future<Output = Result<ApiResponse, anyhow::Error>> + Send + Sync>>;

    fn get(&mut self, request: ApiRequest) -> Self::Fut {
        let response = self
            .storage
            .metadata
            .iter()
            .find(|entry| {
                entry.key().group == request.group && entry.key().version == request.version
                // && &entry.key().kind == request.kind.as_ref().unwrap()
            })
            .map(|v| {
                let gvk = v.key();
                let resources = self
                    .storage
                    .resources
                    .get(gvk)
                    .map(|v| {
                        v.value()
                            .iter()
                            .map(|v| v.value().resource.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                let ar = v.value().first().clone().unwrap();
                let list = CustomResourceHandler::object_list(&ar, resources);
                list
            })
            .unwrap();

        Box::pin(async {
            let api_response = ApiResponse::try_from(StatusCode::OK, response);
            api_response
        })
    }
}
