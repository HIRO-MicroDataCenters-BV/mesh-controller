use std::sync::Arc;

use crate::config::private_key::load_private_key_from_file;
use crate::context::Context;
use crate::http::api::MeshApiImpl;

use crate::api::server::MeshHTTPServer;
use crate::kube::cache::KubeCache;
use crate::node::mesh::MeshNode;
use anyhow::{Context as AnyhowContext, Result};
use kube::Client;
use p2panda_core::{PrivateKey, PublicKey};

use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::config::configuration::PRIVATE_KEY_ENV;
use crate::config::configuration::{Config, load_config};
use crate::tracing::setup_tracing;
use figment::providers::Env;

#[derive(Debug)]
pub struct ContextBuilder {
    config: Config,
    _private_key: PrivateKey,
    public_key: PublicKey,
}

impl ContextBuilder {
    pub fn new(config: Config, private_key: PrivateKey) -> Self {
        setup_tracing(config.log_level.clone());
        ContextBuilder {
            public_key: private_key.public_key(),
            config,
            _private_key: private_key,
        }
    }
    /// Load the configuration from the environment and initializes context builder
    pub fn from_cli() -> Result<Self> {
        let config = load_config()?;
        setup_tracing(config.log_level.clone());

        // Load the private key from either an environment variable _or_ a file specified in the
        // config. The environment variable takes priority.
        let private_key = match Env::var(PRIVATE_KEY_ENV) {
            Some(private_key_hex) => PrivateKey::try_from(&hex::decode(&private_key_hex)?[..])?,
            None => load_private_key_from_file(&config.node.private_key_path).context(format!(
                "could not load private key from file {}",
                config.node.private_key_path.display(),
            ))?,
        };

        let public_key = private_key.public_key();
        Ok(ContextBuilder {
            config,
            _private_key: private_key,
            public_key,
        })
    }

    pub fn try_build_and_start(&self) -> Result<Context> {
        let mesh_runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("mesh")
            .build()
            .expect("Mesh Controller tokio runtime");

        let (mesh_node, cache) = mesh_runtime.block_on(async {
            let client = ContextBuilder::build_kube_client().await?;
            let cache = KubeCache::new(client);

            let node = ContextBuilder::init(self.config.clone(), self._private_key.clone())
                .await
                .context("failed to initialize mesh node")?;
            Ok::<_, anyhow::Error>((node, cache))
        })?;

        let http_runtime = Builder::new_multi_thread()
            .enable_io()
            .thread_name("http-server")
            .build()
            .expect("http server tokio runtime");
        let cancellation_token = CancellationToken::new();
        let http_handle = self.start_http_server(&http_runtime, cancellation_token.clone())?;

        Ok(Context::new(
            self.config.clone(),
            cache,
            mesh_node,
            self.public_key,
            http_handle,
            http_runtime,
            cancellation_token,
            mesh_runtime,
        ))
    }

    async fn init(_config: Config, _private_key: PrivateKey) -> Result<MeshNode> {
        Ok(MeshNode {})
    }

    /// Starts the HTTP server with health endpoint.
    fn start_http_server(
        &self,
        runtime: &Runtime,
        cancellation_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let config = self.config.clone();
        let http_bind_port = self.config.node.http_bind_port;
        let api = Arc::new(MeshApiImpl::new(config).context("MeshAPIImpl initialization")?);
        let http_server = MeshHTTPServer::new(http_bind_port, api);
        Ok(runtime.spawn(async move {
            let result = http_server
                .run()
                .await
                .context("failed to start mesh http server")
                .inspect_err(|e| error!("http result {}", e));
            cancellation_token.cancel();
            result
        }))
    }

    async fn build_kube_client() -> Result<Client> {
        #[cfg(not(test))]
        {
            let config = kube::config::Config::incluster().context("failed to load kube config")?;
            let client = kube::Client::try_from(config).context("failed to create kube client")?;
            Ok(client)
        }
        #[cfg(test)]
        {
            let svc = crate::client::kube::FakeKubeApiService::new();
            let client = kube::Client::new(svc, "default");
            Ok(client)
        }
    }
}
