use std::collections::BTreeMap;

use crate::kube::types::NamespacedName;

use super::handlers::api_resource::ApiResourceArgs;
use super::handlers::resource::ResourceArgs;
use super::types::ApiServiceType;
use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;
use http::Method;
use http::Request;
use http::request::Parts;
use http::uri::PathAndQuery;
use kube::client::Body;
use regex::Regex;
use strum::Display;
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

        if let Some(result) = ApiRequest::parse_resource(path_and_query, &input, &params, is_watch)
        {
            return result;
        }

        if let Some(result) =
            ApiRequest::parse_resource_list(path_and_query, &input, &params, is_watch)
        {
            return result;
        }

        if let Some(result) = ApiRequest::parse_api_resources(path_and_query, &input, is_watch) {
            return result;
        }

        bail!("unknown path {}", path_and_query.path())
    }

    fn parse_resource(
        path_and_query: &PathAndQuery,
        input: &Bytes,
        params: &BTreeMap<String, String>,
        is_watch: bool,
    ) -> Option<Result<(Args, ApiServiceType, bool)>> {
        let path = path_and_query.path();
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)/namespaces/(?P<namespace>[^/]+)/(?P<pluralkind>[^/]+)/(?P<name>[^/]+)").unwrap();
        re.captures(path).map(|captures| {
            let extract_capture =
                |name: &str| captures.name(name).map(|m| m.as_str()).unwrap_or("").into();

            let group = extract_capture("group");
            let version = extract_capture("version");
            let kind_plural = extract_capture("pluralkind");
            let namespace = extract_capture("namespace");
            let name = extract_capture("name");

            let resource_name = Some(NamespacedName::new(namespace, name));
            let service = ApiServiceType::Resource;

            Ok((
                Args::Resource(ResourceArgs {
                    group,
                    version,
                    kind_plural,
                    input: input.clone(),
                    resource_name,
                    params: params.clone(),
                }),
                service,
                is_watch,
            ))
        })
    }

    fn parse_resource_list(
        path_and_query: &PathAndQuery,
        input: &Bytes,
        params: &BTreeMap<String, String>,
        is_watch: bool,
    ) -> Option<Result<(Args, ApiServiceType, bool)>> {
        let path = path_and_query.path();
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)/(?P<pluralkind>[^/]+)")
            .unwrap();
        re.captures(path).map(|captures| {
            let extract_capture =
                |name: &str| captures.name(name).map(|m| m.as_str()).unwrap_or("").into();
            let group = extract_capture("group");
            let version = extract_capture("version");
            let kind_plural = extract_capture("pluralkind");

            let service = ApiServiceType::Resource;

            Ok((
                Args::Resource(ResourceArgs {
                    group,
                    version,
                    kind_plural,
                    input: input.clone(),
                    resource_name: None,
                    params: params.clone(),
                }),
                service,
                is_watch,
            ))
        })
    }

    fn parse_api_resources(
        path_and_query: &PathAndQuery,
        input: &Bytes,
        is_watch: bool,
    ) -> Option<Result<(Args, ApiServiceType, bool)>> {
        let path = path_and_query.path();
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/|?]+)").unwrap();
        re.captures(path).map(|captures| {
            let extract_capture =
                |name: &str| captures.name(name).map(|m| m.as_str()).unwrap_or("").into();
            let group = extract_capture("group");
            let version = extract_capture("version");

            let service = ApiServiceType::ApiResources;

            Ok((
                Args::ApiResource(ApiResourceArgs {
                    group,
                    version,
                    input: input.clone(),
                }),
                service,
                is_watch,
            ))
        })
    }

    pub fn method(&self) -> &Method {
        &self.parts.method
    }
}
