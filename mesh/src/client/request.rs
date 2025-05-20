use std::collections::BTreeMap;

use crate::kube::types::NamespacedName;

use super::handlers::api_resource::ApiResourceArgs;
use super::handlers::resource::ResourceArgs;
use super::types::ApiServiceType;
use anyhow::Result;
use anyhow::bail;
use http::Method;
use http::Request;
use http::request::Parts;
use http::uri::PathAndQuery;
use kube::client::Body;
use regex::Regex;
use strum::Display;
use tracing::info;
use url::form_urlencoded;

#[derive(Debug, Clone, Display)]
pub enum Args {
    ApiResource(ApiResourceArgs),
    Resource(ResourceArgs),
}

#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub parts: Parts,

    pub service: ApiServiceType,

    pub args: Args,

    pub is_watch: bool,
}

impl ApiRequest {
    pub async fn try_from(req: Request<Body>) -> Result<ApiRequest> {
        let (parts, body) = req.into_parts();

        let path_and_query = parts
            .uri
            .path_and_query()
            .ok_or(anyhow::anyhow!("path and query are not in request"))?;

        let (args, service, is_watch) = ApiRequest::parse_uri(path_and_query, body).await?;

        Ok(ApiRequest {
            parts,
            args,
            service,
            is_watch,
        })
    }

    async fn parse_uri(
        path_and_query: &PathAndQuery,
        body: Body,
    ) -> Result<(Args, ApiServiceType, bool)> {
        let input = body.collect_bytes().await?;

        let query = path_and_query.query().unwrap_or("");
        let params = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect::<BTreeMap<String, String>>();

        let is_watch = params
            .get("watch")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);

        let path = path_and_query.path();
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)/namespaces/(?P<namespace>[^/]+)/(?P<pluralkind>[^/]+)/(?P<name>[^/]+)")
            .unwrap(); // TODO
        if let Some(captures) = re.captures(path) {
            let group = captures
                .name("group")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let version = captures
                .name("version")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let kind_plural = captures
                .name("pluralkind")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let namespace = captures
                .name("namespace")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let name = captures
                .name("name")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();

            let resource_name = Some(NamespacedName::new(namespace, name));
            let service = ApiServiceType::Resource;

            return Ok((
                Args::Resource(ResourceArgs {
                    group,
                    version,
                    kind_plural,
                    input,
                    resource_name,
                    params,
                }),
                service,
                is_watch,
            ));
        }
        let path = path_and_query.path();
        info!("path {path}");
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)/(?P<pluralkind>[^/]+)")
            .unwrap(); // TDOO
        if let Some(captures) = re.captures(path) {
            info!("parsed {path}");
            let group = captures
                .name("group")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let version = captures
                .name("version")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let kind_plural = captures
                .name("pluralkind")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let service = ApiServiceType::Resource;
            return Ok((
                Args::Resource(ResourceArgs {
                    group,
                    version,
                    kind_plural,
                    input,
                    resource_name: None,
                    params,
                }),
                service,
                is_watch,
            ));
        }

        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/|?]+)").unwrap();
        if let Some(captures) = re.captures(path) {
            let group = captures
                .name("group")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let version = captures
                .name("version")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let service = ApiServiceType::ApiResources;
            return Ok((
                Args::ApiResource(ApiResourceArgs {
                    group,
                    version,
                    input,
                }),
                service,
                is_watch,
            ));
        }

        bail!("unknown path {path}")
    }

    pub fn method(&self) -> &Method {
        &self.parts.method
    }
}
