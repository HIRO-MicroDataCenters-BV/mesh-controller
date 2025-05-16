use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use kube::api::{ApiResource, DynamicObject, ListMeta, ObjectList, TypeMeta};

use crate::client::{
    response::ApiResponse,
    storage::Storage,
    types::{ApiHandler, ApiHandlerResponse},
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
    pub input: Bytes,
}

impl ApiHandler for ResourceHandler {
    type Req = ResourceArgs;

    fn get(&self, request: ResourceArgs) -> ApiHandlerResponse {
        let ResourceArgs {
            group,
            version,
            kind_plural,
            ..
        } = request;

        let maybe_response = self
            .storage
            .find_objects(&group, &version, &kind_plural)
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
