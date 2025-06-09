use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{APIResource, APIResourceList};
use kube::api::ApiResource;

use crate::{
    response::ApiResponse,
    storage::Storage,
    types::{ApiHandler, ApiHandlerResponse},
};

pub struct ApiResourceHandler {
    storage: Arc<Storage>,
}

impl ApiResourceHandler {
    pub fn new(storage: Arc<Storage>) -> ApiResourceHandler {
        ApiResourceHandler { storage }
    }
}

#[derive(Clone, Debug)]
pub struct ApiResourceArgs {
    pub group: String,
    pub version: String,
    #[allow(dead_code)]
    pub input: Bytes,
}

impl ApiHandler for ApiResourceHandler {
    type Req = ApiResourceArgs;

    fn get(&self, request: ApiResourceArgs) -> ApiHandlerResponse {
        let storage = self.storage.clone();
        Box::pin(async move {
            let ApiResourceArgs { group, version, .. } = &request;
            let resources = storage.get_api_resources(group, version);

            ApiResponse::try_from(
                StatusCode::OK,
                api_resource_list_to_response(group, version, &resources),
            )
        })
    }

    fn call(&self, method: &http::Method, request: Self::Req) -> crate::types::ApiHandlerResponse {
        match *method {
            http::Method::GET => self.get(request),
            http::Method::DELETE => self.delete(request),
            http::Method::PATCH => self.patch(request),
            _ => std::unimplemented!("{}", method),
        }
    }

    fn watch_method(
        &self,
        method: &http::Method,
        request: Self::Req,
    ) -> crate::types::ApiHandlerWatchResponse {
        match method {
            &http::Method::GET => self.watch(request),
            _ => std::unimplemented!("{}", method),
        }
    }

    fn watch(&self, _request: Self::Req) -> crate::types::ApiHandlerWatchResponse {
        std::unimplemented!()
    }
}

fn api_resource_list_to_response(
    group: &str,
    version: &str,
    ar: &[ApiResource],
) -> APIResourceList {
    let resources = ar
        .iter()
        .flat_map(api_resource_to_response)
        .collect::<Vec<APIResource>>();
    APIResourceList {
        group_version: format!("{}/{}", group, version),
        resources,
    }
}

fn api_resource_to_response(ar: &ApiResource) -> Vec<APIResource> {
    vec![
        APIResource {
            name: ar.plural.clone(),
            singular_name: ar.kind.to_lowercase(),
            kind: ar.kind.clone(),
            namespaced: true,
            verbs: vec![
                "delete".into(),
                "deletecollection".into(),
                "get".into(),
                "list".into(),
                "patch".into(),
                "create".into(),
                "update".into(),
                "watch".into(),
            ],
            ..Default::default()
        },
        APIResource {
            name: format!("{}/status", ar.plural),
            singular_name: String::new(),
            kind: ar.kind.clone(),
            namespaced: true,
            verbs: vec!["get".into(), "patch".into(), "update".into()],
            ..Default::default()
        },
    ]
}
