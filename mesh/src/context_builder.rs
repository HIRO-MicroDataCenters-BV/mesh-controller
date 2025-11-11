use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::config::private_key::load_private_key_from_file;
use crate::context::Context;
use crate::http::api::MeshApiImpl;

use crate::api::server::MeshHTTPServer;
use crate::mesh::mesh::Mesh;
use crate::mesh::operations::Extensions;
use crate::mesh::topic::{InstanceId, MeshLogId};
use crate::network::Panda;
use crate::network::discovery::nodes::Nodes;
use crate::network::discovery::static_lookup::StaticLookup;
use crate::node::mesh::{MeshNode, NodeOptions};
use crate::utils::clock::RealClock;
use crate::utils::types::Clock;
use anyhow::{Context as AnyhowContext, Result, anyhow};
use meshkube::client::KubeClient;
use meshkube::kube::subscriptions::Subscriptions;
use meshresource::mesh_status::MeshStatus;
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::{NetworkBuilder, ResyncConfiguration, SyncConfiguration};
use p2panda_store::MemoryStore;
use p2panda_sync::log_sync::LogSyncProtocol;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::config::configuration::PRIVATE_KEY_ENV;
use crate::config::configuration::{Config, load_config};
use crate::tracing::setup_tracing;
use figment::providers::Env;

#[derive(Debug)]
pub struct ContextBuilder {
    config: Config,
    private_key: PrivateKey,
    public_key: PublicKey,
}

impl ContextBuilder {
    pub fn new(config: Config, private_key: PrivateKey) -> Self {
        setup_tracing(config.log_level.clone());
        ContextBuilder {
            public_key: private_key.public_key(),
            config,
            private_key,
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
            private_key,
            public_key,
        })
    }

    pub fn try_build_and_start(&self) -> Result<Context> {
        let mesh_runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("mesh")
            .build()
            .expect("Mesh Controller tokio runtime");

        let cancellation = CancellationToken::new();
        let (mesh_node, client) = mesh_runtime.block_on(async {
            let client = ContextBuilder::build_kube_client(&self.config).await?;

            let node = ContextBuilder::init(
                self.config.clone(),
                self.private_key.clone(),
                client.clone(),
                cancellation.clone(),
            )
            .await
            .context("failed to initialize mesh node")?;
            Ok::<_, anyhow::Error>((node, client))
        })?;

        let subscriptions = Subscriptions::new(client);

        let http_runtime = Builder::new_multi_thread()
            .enable_io()
            .thread_name("http-server")
            .build()
            .expect("http server tokio runtime");

        let http_handle = self.start_http_server(&http_runtime, cancellation.clone())?;

        Ok(Context::new(
            self.config.clone(),
            mesh_node,
            subscriptions,
            self.public_key,
            http_handle,
            http_runtime,
            cancellation,
            mesh_runtime,
        ))
    }

    async fn init(
        config: Config,
        private_key: PrivateKey,
        client: KubeClient,
        cancelation: CancellationToken,
    ) -> Result<MeshNode> {
        let clock = Arc::new(RealClock::new());
        let now = clock.now_millis();

        let mesh_status = MeshStatus::new(client.clone(), cancelation.child_token()).await?;

        let peer_states = mesh_status.get_all().await?;
        let previously_known_peers: Vec<PublicKey> = peer_states
            .iter()
            .flat_map(|ps| {
                PublicKey::from_str(&ps.peer_id)
                    .inspect_err(|e| {
                        warn!(
                            "Unable to parse peer public key {}: {}. Skipping...",
                            ps.peer_id, e
                        )
                    })
                    .ok()
            })
            .collect();

        let (node_config, p2p_network_config) = MeshNode::configure_p2p_network(&config).await?;

        let resync_config = ContextBuilder::to_resync_config(&config);
        let instance_id = InstanceId::new(config.mesh.zone.to_owned());
        let log_store = MemoryStore::<MeshLogId, Extensions>::new();

        let nodes = Nodes::new(
            private_key.public_key(),
            MeshLogId(instance_id.clone()),
            Duration::from_secs(config.mesh.peer_timeout.peer_unavailable_after_seconds),
        );
        nodes.load_initial_state(previously_known_peers, now);

        let sync_protocol = LogSyncProtocol::new(nodes.clone(), log_store.clone());
        let sync_config = SyncConfiguration::new(sync_protocol).resync(resync_config);

        let mut builder = NetworkBuilder::from_config(p2p_network_config)
            .private_key(private_key.clone())
            .sync(sync_config)
            .discovery(StaticLookup::new(
                &config.node.known_nodes,
                config.node.discovery.to_owned().unwrap_or_default(),
            ));
        if config.mesh.bootstrap {
            builder = builder.bootstrap();
        }

        let network = builder.build().await?;

        let mesh = Mesh::new(
            private_key.clone(),
            config.mesh.to_owned(),
            instance_id,
            cancelation,
            client,
            clock,
            nodes.clone(),
            mesh_status,
            log_store.clone(),
            network.events().await?,
        )
        .await?;

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

        MeshNode::new(panda, mesh, options.clone()).await
    }

    /// Starts the HTTP server with health endpoint.
    fn start_http_server(
        &self,
        runtime: &Runtime,
        cancellation_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let http_bind_port = self.config.node.http_bind_port;
        let api = Arc::new(MeshApiImpl::new().context("MeshAPIImpl initialization")?);
        let http_server = MeshHTTPServer::new(http_bind_port, api);
        Ok(runtime.spawn(async move {
            let result = http_server
                .run()
                .await
                .inspect_err(|e| error!("http result {}", e))
                .context("failed to start mesh http server");
            cancellation_token.cancel();
            result
        }))
    }

    async fn build_kube_client(config: &Config) -> Result<KubeClient> {
        #[cfg(not(test))]
        {
            let client = KubeClient::build(&config.kubernetes).await?;
            Ok(client)
        }
        #[cfg(test)]
        {
            use crate::tests::fake_etcd_server::FakeEtcdServer;
            let client = FakeEtcdServer::get_client(&config.kubernetes);
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
