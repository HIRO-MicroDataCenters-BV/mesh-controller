use std::{pin::Pin, sync::Arc};

use super::{request::ApiRequest, response::ApiResponse, storage::Storage, types::ApiHandler};
use http::{Response, StatusCode};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList;
use kube::{api::ApiResource, client::Body};

pub struct ApiResourceHandler {
    storage: Arc<Storage>,
}

impl ApiResourceHandler {
    pub fn new(storage: Arc<Storage>) -> ApiResourceHandler {
        ApiResourceHandler { storage }
    }

    fn api_resource_list_response(
        &self,
        group: &str,
        version: &str,
        ar: Vec<ApiResource>,
    ) -> APIResourceList {
        let resources = ar
            .iter()
            .flat_map(|r| ApiResourceHandler::api_resources(r))
            .collect::<Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource>>();
        let object = APIResourceList {
            group_version: format!("{}/{}", group, version),
            resources,
        };
        object
    }

    fn api_resources(
        ar: &ApiResource,
    ) -> Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource> {
        vec![
            k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource {
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
            k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource {
                name: format!("{}/status", ar.plural),
                singular_name: String::new(),
                kind: ar.kind.clone(),
                namespaced: true,
                verbs: vec!["get".into(), "patch".into(), "update".into()],
                ..Default::default()
            },
        ]
    }
}

impl ApiHandler for ApiResourceHandler {
    type Fut = Pin<Box<dyn Future<Output = Result<ApiResponse, anyhow::Error>> + Send + Sync>>;

    fn get(&mut self, request: ApiRequest) -> Self::Fut {
        let result = self
            .storage
            .metadata
            .iter()
            .filter(|entry| {
                entry.key().group == request.group && entry.key().version == request.version
            })
            .flat_map(|v| v.value().clone())
            .collect::<Vec<ApiResource>>();
        let response = self.api_resource_list_response("dcp.hiro.io", "v1", result);
        Box::pin(async {
            let api_response = ApiResponse::try_from(StatusCode::OK, response);
            api_response
        })
    }
}
