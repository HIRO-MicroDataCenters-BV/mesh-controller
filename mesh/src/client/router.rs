use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;

use super::{
    request::ApiRequest,
    response::ApiResponse,
    types::{ApiHandler, ApiServiceType},
};

pub struct ApiRequestRouter {
    // handlers: HashMap<ApiServiceType, Arc<dyn ApiHandler<Fut = F> + Send + Sync>>,
}

impl ApiRequestRouter {
    pub fn new() -> ApiRequestRouter {
        ApiRequestRouter {
            // handlers: HashMap::new()
        }
    }

    pub fn handler<F>(
        mut self,
        handler_type: ApiServiceType,
        handler: Arc<dyn ApiHandler<Fut = F> + Send + Sync>,
    ) -> Self {
        // self.handlers.insert(handler_type, handler);
        self
    }

    // pub async fn handle(&self, req: ApiRequest) -> anyhow::Result<ApiResponse> {
    //     // if let Some(handler) = self.handlers.get(&req.service) {
    //     //     let response = Box::new(handler.call(req));
    //     //     return response.and_then(|v| v.to_http_response());
    //     // }
    //     // unimplemented!()
    //     // Ok(api_request) => match api_request.service {
    //     //     ApiServiceType::ApiResources => {
    //     //         let mut handler = ApiResourceHandler::new(storage);
    //     //         let response = handler.call(api_request).await;
    //     //         return response.and_then(|v| v.to_http_response());
    //     //     }
    //     //     ApiServiceType::CustomResource => {
    //     //         let mut handler = CustomResourceHandler::new(storage);
    //     //         let response = handler.call(api_request).await;
    //     //         return response.and_then(|v| v.to_http_response());
    //     //     }
    //     // },

    // }
}
