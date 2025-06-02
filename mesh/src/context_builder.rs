use std::sync::Arc;

use crate::client::kube_client::KubeClient;
use crate::config::private_key::load_private_key_from_file;
use crate::context::Context;
use crate::http::api::MeshApiImpl;

use crate::api::server::MeshHTTPServer;
use crate::kube::pool::ObjectPool;
use crate::mesh::mesh::Mesh;
use crate::mesh::operations::Extensions;
use crate::mesh::peer_discovery::PeerDiscovery;
use crate::mesh::topic::{InstanceId, MeshLogId};
use crate::mesh::topic::{MeshTopic, MeshTopicLogMap};
use crate::network::Panda;
use crate::network::membership::Membership;
use crate::node::mesh::{MeshNode, NodeOptions};
use anyhow::{Context as AnyhowContext, Result, anyhow};
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::{NetworkBuilder, ResyncConfiguration, SyncConfiguration};
use p2panda_store::MemoryStore;
use p2panda_sync::log_sync::LogSyncProtocol;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
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

        let mesh_node = mesh_runtime.block_on(async {
            let client = ContextBuilder::build_kube_client(&self.config).await?;
            let cache = ObjectPool::new(client);

            let node = ContextBuilder::init(self.config.clone(), self._private_key.clone(), cache)
                .await
                .context("failed to initialize mesh node")?;
            Ok::<_, anyhow::Error>(node)
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
            mesh_node,
            self.public_key,
            http_handle,
            http_runtime,
            cancellation_token,
            mesh_runtime,
        ))
    }

    async fn init(config: Config, private_key: PrivateKey, pool: ObjectPool) -> Result<MeshNode> {
        let (node_config, p2p_network_config) = MeshNode::configure_p2p_network(&config).await?;

        let resync_config = ContextBuilder::to_resync_config(&config);
        let instance_id = InstanceId::new(config.mesh.zone.to_owned());
        let topic_log_map =
            MeshTopicLogMap::new(private_key.public_key(), MeshLogId(instance_id.clone()));
        let log_store = MemoryStore::<MeshLogId, Extensions>::new();

        let (mesh_tx, network_rx) = mpsc::channel(512);
        let (network_tx, mesh_rx) = mpsc::channel(512);

        let mesh = Mesh::new(
            private_key.clone(),
            &config.mesh.resource,
            instance_id,
            pool,
            topic_log_map.clone(),
            log_store.clone(),
            network_tx,
            network_rx,
        )
        .await?;

        let sync_protocol = LogSyncProtocol::new(topic_log_map.clone(), log_store);
        let sync_config = SyncConfiguration::new(sync_protocol).resync(resync_config);

        let mut builder = NetworkBuilder::from_config(p2p_network_config)
            .private_key(private_key.clone())
            .sync(sync_config)
            .discovery(Membership::new(
                &config.node.known_nodes,
                config.node.discovery.to_owned().unwrap_or_default(),
            ));
        if config.mesh.bootstrap {
            builder = builder.bootstrap();
        }

        let network = builder.build().await?;
        let peer_discovery = PeerDiscovery::start(network.events().await?, topic_log_map.clone());

        let node_id = network.node_id();
        let direct_addresses = network
            .direct_addresses()
            .await
            .ok_or_else(|| anyhow!("socket is not bind to any interface"))?;
        let panda = Panda::new(network);

        let options = NodeOptions {
            public_key: node_id,
            private_key,
            direct_addresses,
            node_config,
        };

        let node = MeshNode::new(panda, mesh, peer_discovery, mesh_tx, options.clone()).await?;

        node.subscribe(MeshTopic::default()).await?;
        node.publish_operations(mesh_rx).await.ok();

        Ok(node)
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

    async fn build_kube_client(_config: &Config) -> Result<KubeClient> {
        #[cfg(not(test))]
        {
            let client = KubeClient::build(_config).await?;
            Ok(client)
        }
        #[cfg(test)]
        {
            let svc = fake_kube_api::service::FakeKubeApiService::new();
            let client = KubeClient::build_fake(svc);
            Ok(client)
        }
    }

    fn to_resync_config(config: &Config) -> ResyncConfiguration {
        config
            .node
            .protocol
            .as_ref()
            .map(|c| {
                ResyncConfiguration::new()
                    .poll_interval(c.poll_interval_seconds)
                    .interval(c.resync_interval_seconds)
            })
            .unwrap_or_default()
    }
}
