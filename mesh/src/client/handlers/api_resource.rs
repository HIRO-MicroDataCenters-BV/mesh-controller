use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{APIResource, APIResourceList};
use kube::api::ApiResource;

use crate::client::{
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
    pub input: Bytes,
}

impl ApiHandler for ApiResourceHandler {
    type Req = ApiResourceArgs;

    fn get(&self, request: ApiResourceArgs) -> ApiHandlerResponse {
        let ApiResourceArgs { group, version, .. } = request;
        let resources = self.storage.get_api_resources(&group, &version);

        Box::pin(async move {
            ApiResponse::try_from(
                StatusCode::OK,
                api_resource_list_to_response(&group, &version, &resources),
            )
        })
    }
}

fn api_resource_list_to_response(
    group: &str,
    version: &str,
    ar: &Vec<ApiResource>,
) -> APIResourceList {
    let resources = ar
        .iter()
        .flat_map(api_resource_to_response)
        .collect::<Vec<APIResource>>();
    let object = APIResourceList {
        group_version: format!("{}/{}", group, version),
        resources,
    };
    object
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
