use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::LinkedList;

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

        let query_params = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect::<BTreeMap<String, String>>();

        let is_watch = query_params
            .get("watch")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);

        match PathParser::new(path_and_query.path()).parse()? {
            (ApiServiceType::ApiResources, path_params) => Ok((
                Args::ApiResource(ApiResourceArgs::try_new(path_params, input)?),
                ApiServiceType::ApiResources,
                is_watch,
            )),
            (ApiServiceType::Resource, path_params) => Ok((
                Args::Resource(ResourceArgs::try_new(path_params, query_params, input)?),
                ApiServiceType::Resource,
                is_watch,
            )),
        }
    }

    pub fn method(&self) -> &Method {
        &self.parts.method
    }
}

struct PathParser<'a> {
    segments: LinkedList<&'a str>,
}

impl<'a> PathParser<'a> {
    pub fn new(path: &'a str) -> PathParser<'a> {
        PathParser {
            segments: path.split("/").filter(|s| !s.is_empty()).collect(),
        }
    }

    pub fn parse(mut self) -> Result<(ApiServiceType, HashMap<&'static str, &'a str>)> {
        if Self::expected(&mut self.segments, "apis").is_ok() {
            return self.parse_apis();
        }

        self.parse_api()
    }

    pub fn parse_apis(mut self) -> Result<(ApiServiceType, HashMap<&'static str, &'a str>)> {
        let mut params = HashMap::new();
        params.insert("group", Self::segment(&mut self.segments)?);
        params.insert("version", Self::segment(&mut self.segments)?);
        if self.segments.is_empty() {
            return Ok((ApiServiceType::ApiResources, params));
        }
        let has_namespace = Self::opt_expected(&mut self.segments, "namespaces").is_some();
        if has_namespace {
            params.insert("namespace", Self::segment(&mut self.segments)?);
        }
        params.insert("pluralkind", Self::segment(&mut self.segments)?);
        if let Some(name) = Self::opt_segment(&mut self.segments) {
            params.insert("name", name);
        }
        if let Some(subresource) = Self::opt_segment(&mut self.segments) {
            params.insert("subresource", subresource);
        }

        Ok((ApiServiceType::Resource, params))
    }

    pub fn parse_api(mut self) -> Result<(ApiServiceType, HashMap<&'static str, &'a str>)> {
        let mut params = HashMap::new();
        params.insert("group", "");
        params.insert("version", Self::segment(&mut self.segments)?);
        if self.segments.is_empty() {
            return Ok((ApiServiceType::ApiResources, params));
        }
        let has_namespace = Self::opt_expected(&mut self.segments, "namespaces").is_some();
        if has_namespace {
            params.insert("namespace", Self::segment(&mut self.segments)?);
        }
        params.insert("pluralkind", Self::segment(&mut self.segments)?);
        if let Some(name) = Self::opt_segment(&mut self.segments) {
            params.insert("name", name);
        }
        if let Some(subresource) = Self::opt_segment(&mut self.segments) {
            params.insert("subresource", subresource);
        }

        Ok((ApiServiceType::Resource, params))
    }

    fn expected(segments: &mut LinkedList<&'a str>, expected: &'static str) -> Result<&'a str> {
        let Some(segment) = segments.pop_front() else {
            bail!("expected segment '{}', but no segments left", expected)
        };
        if segment != expected {
            bail!(
                "expected segment '{}', but actual '{:?}'",
                expected,
                segments.front()
            )
        } else {
            Ok(segment)
        }
    }

    fn opt_expected(segments: &mut LinkedList<&'a str>, expected: &'static str) -> Option<&'a str> {
        let segment = segments.pop_front()?;
        if segment == expected {
            Some(segment)
        } else {
            segments.push_front(segment);
            None
        }
    }

    fn segment(segments: &mut LinkedList<&'a str>) -> Result<&'a str> {
        let Some(segment) = segments.pop_front() else {
            bail!("expected segment, but no segments left");
        };
        Ok(segment)
    }

    fn opt_segment(segments: &mut LinkedList<&'a str>) -> Option<&'a str> {
        segments.pop_front()
    }
}
