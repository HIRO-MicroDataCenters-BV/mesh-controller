use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use directories::ProjectDirs;
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Yaml};
use kube::api::GroupVersionKind;
use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};

/// Default file name of config.
const CONFIG_FILE_NAME: &str = "config.yaml";

/// Default file path to private key file.
const DEFAULT_PRIVATE_KEY_PATH: &str = "private.key";

/// Private key env arg.
pub const PRIVATE_KEY_ENV: &str = "PRIVATE_KEY";

/// Default port.
const DEFAULT_BIND_PORT: u16 = 9102;

/// Default HTTP port.
pub const DEFAULT_HTTP_BIND_PORT: u16 = 3000;

/// Default network id.
const DEFAULT_NETWORK_ID: &str = "default-network-1";

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub mesh: MeshConfig,
    pub kubernetes: KubeConfiguration,
    pub log_level: Option<String>,
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "mesh",
    about = "Mesh-controller",
    long_about = None,
    version
)]
struct Cli {
    /// Path to "config.yaml" file for further configuration.
    ///
    #[arg(short = 'c', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<PathBuf>,

    /// Path to file containing hexadecimal-encoded Ed25519 private key.
    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key_path: Option<PathBuf>,

    /// Set log verbosity. Use this for learning more about how your node behaves or for debugging.
    ///
    /// Possible log levels are: ERROR, WARN, INFO, DEBUG, TRACE.
    ///
    /// If you want to adjust the scope for deeper inspection use a filter value, for example
    /// "=TRACE" for logging _everything_ or "mesh=INFO" etc.
    #[arg(short = 'l', long, value_name = "LEVEL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,
}

/// Get configuration from 1. .yaml file, 2. environment variables and 3. command line arguments
/// (in that order, meaning that later configuration sources take precedence over the earlier
/// ones).
///
/// Returns a partly unchecked configuration object which results from all of these sources.
pub fn load_config() -> Result<Config> {
    // Parse command line arguments and CONFIG environment variable first to get optional config
    // file path
    let cli = Cli::parse();

    // Determine if a config file path was provided or if we should look for it in common locations
    let config_file_path: Option<PathBuf> = match &cli.config {
        Some(path) => {
            if !path.exists() {
                bail!("config file '{}' does not exist", path.display());
            }

            Some(path.clone())
        }
        None => try_determine_config_file_path(),
    };

    let mut figment = Figment::from(Serialized::defaults(Config::default()));

    if let Some(path) = &config_file_path {
        figment = figment.merge(Yaml::file(path));
    }

    let config: Config = figment
        .merge(Env::raw())
        .merge(Serialized::defaults(cli))
        .extract()?;

    Ok(config)
}

fn try_determine_config_file_path() -> Option<PathBuf> {
    // Find config file in current folder
    let mut current_dir = std::env::current_dir().expect("could not determine current directory");
    current_dir.push(CONFIG_FILE_NAME);

    // Find config file in XDG config folder
    let mut xdg_config_dir: PathBuf = ProjectDirs::from("", "", "mesh")
        .expect("could not determine valid config directory path from operating system")
        .config_dir()
        .to_path_buf();
    xdg_config_dir.push(CONFIG_FILE_NAME);

    [current_dir, xdg_config_dir]
        .iter()
        .find(|path| path.exists())
        .cloned()
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MeshConfig {
    pub zone: String,
    pub bootstrap: bool,
    pub snapshot: PeriodicSnapshotConfig,
    pub tombstone: TombstoneConfig,
    pub resource: ResourceConfig,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub namespace: Option<String>,
    #[serde(default)]
    pub merge_strategy: MergeStrategyType,
}

impl ResourceConfig {
    pub fn get_gvk(&self) -> GroupVersionKind {
        GroupVersionKind::gvk(&self.group, &self.version, &self.kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub enum MergeStrategyType {
    #[serde(rename = "default")]
    #[default]
    Default,
    #[serde(rename = "anyapplication")]
    AnyApplication,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, Default)]
pub enum KubeConfiguration {
    #[serde(rename = "incluster")]
    #[default]
    InCluster,
    #[serde(rename = "external")]
    External(KubeConfigurationExternal),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct KubeConfigurationExternal {
    pub kube_context: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct PeriodicSnapshotConfig {
    pub snapshot_interval_seconds: u64,
    pub snapshot_max_log: u64,
}

impl Default for PeriodicSnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_interval_seconds: 300,
            snapshot_max_log: 256,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct TombstoneConfig {
    pub tombstone_retention_interval_seconds: u64,
}

impl Default for TombstoneConfig {
    fn default() -> Self {
        Self {
            tombstone_retention_interval_seconds: 600,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct NodeConfig {
    pub bind_port: u16,
    pub http_bind_port: u16,
    #[serde(rename = "nodes")]
    pub known_nodes: Vec<KnownNode>,
    pub private_key_path: PathBuf,
    pub network_id: String,
    pub protocol: Option<ProtocolConfig>,
    pub discovery: Option<DiscoveryOptions>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            bind_port: DEFAULT_BIND_PORT,
            http_bind_port: DEFAULT_HTTP_BIND_PORT,
            known_nodes: vec![],
            private_key_path: DEFAULT_PRIVATE_KEY_PATH.into(),
            network_id: DEFAULT_NETWORK_ID.to_string(),
            protocol: None,
            discovery: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct KnownNode {
    pub public_key: PublicKey,
    #[serde(rename = "endpoints")]
    pub direct_addresses: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ProtocolConfig {
    #[serde(rename = "poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    #[serde(rename = "resync_interval_seconds")]
    pub resync_interval_seconds: u64,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 1,
            resync_interval_seconds: 60,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DiscoveryOptions {
    pub query_interval_seconds: u64,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            query_interval_seconds: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use figment::Figment;
    use figment::providers::{Format, Serialized, Yaml};

    use crate::config::configuration::{
        Config, DiscoveryOptions, KnownNode, KubeConfiguration, KubeConfigurationExternal,
        MergeStrategyType, MeshConfig, NodeConfig, PeriodicSnapshotConfig, ProtocolConfig,
        ResourceConfig, TombstoneConfig,
    };

    #[test]
    fn parse_minimal_config() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
                bind_port: 1112
                http_bind_port: 2223
                private_key_path: "/etc/dcp/mesh/private.key"
                network_id: "default"
                kubernetes: incluster

                mesh:
                    zone: "test"
                    bootstrap: false
                    resource:
                        group: ""
                        version: v2
                        kind: Secret
                        namespace: null
                        merge_strategy: default
            "#,
            )?;
            let config: Config = Figment::from(Serialized::defaults(Config::default()))
                .merge(Yaml::file("config.yaml"))
                .extract()
                .unwrap();

            assert_eq!(
                config,
                Config {
                    node: NodeConfig {
                        bind_port: 1112,
                        http_bind_port: 2223,
                        known_nodes: vec![],
                        private_key_path: PathBuf::from("/etc/dcp/mesh/private.key"),
                        network_id: "default".into(),
                        protocol: None,
                        discovery: None
                    },
                    mesh: MeshConfig {
                        zone: "test".into(),
                        bootstrap: false,
                        snapshot: PeriodicSnapshotConfig::default(),
                        tombstone: TombstoneConfig::default(),
                        resource: ResourceConfig {
                            group: "".into(),
                            version: "v2".into(),
                            kind: "Secret".into(),
                            namespace: None,
                            merge_strategy: MergeStrategyType::default()
                        }
                    },
                    kubernetes: KubeConfiguration::InCluster,
                    log_level: None,
                }
            );

            Ok(())
        })
    }

    #[test]
    fn parse_yaml_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
bind_port: 1112
http_bind_port: 2223
private_key_path: /etc/dcp/mesh/private.key
network_id: "default"
protocol:
    poll_interval_seconds: 1
    resync_interval_seconds: 60
discovery:
    query_interval_seconds: 5

nodes:
    - public_key: "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a"
      endpoints:
        - "some.hostname.org."
        - "192.168.178.100:1112"
        - "[2a02:8109:9c9a:4200:eb13:7c0a:4201:8128]:1113"
mesh:
    zone: "test"
    bootstrap: false
    snapshot:
        snapshot_interval_seconds: 100
        snapshot_max_log: 100
    tombstone:
        tombstone_retention_interval_seconds: 100
    resource:
        group: ""
        version: v2
        kind: Secret
        namespace: null
        merge_strategy: default

kubernetes:
    external:
        kube_context: "default"
"#,
            )?;

            let config: Config = Figment::from(Serialized::defaults(Config::default()))
                .merge(Yaml::file("config.yaml"))
                .extract()
                .unwrap();

            assert_eq!(
                config,
                Config {
                    node: NodeConfig {
                        bind_port: 1112,
                        http_bind_port: 2223,
                        known_nodes: vec![KnownNode {
                            public_key:
                                "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a"
                                    .parse()
                                    .unwrap(),
                            direct_addresses: vec![
                                "some.hostname.org.".into(),
                                "192.168.178.100:1112".parse().unwrap(),
                                "[2a02:8109:9c9a:4200:eb13:7c0a:4201:8128]:1113"
                                    .parse()
                                    .unwrap(),
                            ],
                        }],
                        private_key_path: PathBuf::from("/etc/dcp/mesh/private.key"),
                        network_id: "default".into(),
                        protocol: Some(ProtocolConfig::default()),
                        discovery: Some(DiscoveryOptions::default()),
                    },
                    kubernetes: KubeConfiguration::External(KubeConfigurationExternal {
                        kube_context: Some("default".into()),
                    }),
                    mesh: MeshConfig {
                        zone: "test".into(),
                        bootstrap: false,
                        snapshot: PeriodicSnapshotConfig {
                            snapshot_interval_seconds: 100,
                            snapshot_max_log: 100
                        },
                        tombstone: TombstoneConfig {
                            tombstone_retention_interval_seconds: 100,
                        },
                        resource: ResourceConfig {
                            group: "".into(),
                            version: "v2".into(),
                            kind: "Secret".into(),
                            namespace: None,
                            merge_strategy: MergeStrategyType::default()
                        }
                    },
                    log_level: None,
                }
            );

            Ok(())
        });
    }
}
