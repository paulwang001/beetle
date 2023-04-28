use std::path::PathBuf;

use anyhow::Result;
use config::{ConfigError, Map, Source, Value};
use libp2p::Multiaddr;
use luffa_metrics::config::Config as MetricsConfig;
use luffa_util::{insert_into_config_map, luffa_data_root};
use serde::{Deserialize, Serialize};

/// CONFIG_FILE_NAME is the name of the optional config file located in the luffa home directory
pub const CONFIG_FILE_NAME: &str = "luffa.config.toml";
/// ENV_PREFIX should be used along side the config field name to set a config field using
/// environment variables
/// For example, `LUFFA_MDNS=true` would set the value of the `Libp2pConfig.mdns` field
pub const ENV_PREFIX: &str = "LUFFA_";

/// Default bootstrap nodes
///
pub const DEFAULT_BOOTSTRAP: &[&str] = &[
    "/ip4/182.140.244.167/udp/8899/quic-v1/p2p/12D3KooWFQ6ifytHCU6EC2qFFebm6UMzLQN3NWz3XwbzSTD5V1xT",
    "/ip4/182.140.244.156/udp/8899/quic-v1/p2p/12D3KooWAvfMdfWBxu2Td8K9Cn1nsVRjAq3zfVBmMd7rgSv4Tcn1",
    "/ip4/182.140.244.175/udp/8899/quic-v1/p2p/12D3KooWHnu2Sr1LqRYbdaDEMmPtbNLWfkUvDqh42p4X9x89P8s6",
    // "/ip4/182.140.244.167/tcp/8866/p2p/12D3KooWFQ6ifytHCU6EC2qFFebm6UMzLQN3NWz3XwbzSTD5V1xT",
    // "/ip4/182.140.244.156/tcp/8866/p2p/12D3KooWAvfMdfWBxu2Td8K9Cn1nsVRjAq3zfVBmMd7rgSv4Tcn1"
];
// no udp support yet

// "/ip4/104.131.131.82/udp/4001/quic/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // i.luffa.io

/// The configuration for the p2p server.
///
/// This is the configuration which the p2p server binary needs to run.  It is a superset
/// from the configuration needed by the p2p service.
#[derive(PartialEq, Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub p2p: Config,
    pub path: PathBuf,
    pub metrics: MetricsConfig,
}

impl ServerConfig {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            p2p: Config::default_network(),
            path: luffa_util::luffa_data_path("luffa_db").unwrap(),
            metrics: Default::default(),
        }
    }
}

impl Source for ServerConfig {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();
        insert_into_config_map(&mut map, "p2p", self.p2p.collect()?);
        insert_into_config_map(&mut map, "metrics", self.metrics.collect()?);
        Ok(map)
    }
}

/// Libp2p config for the node.
#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Libp2pConfig {
    /// Local address.
    pub listening_multiaddrs: Vec<Multiaddr>,
    /// Bootstrap peer list.
    pub bootstrap_peers: Vec<Multiaddr>,
    /// Mdns discovery enabled.
    pub mdns: bool,
    /// Bitswap server mode enabled.
    pub bitswap_server: bool,
    /// Bitswap client mode enabled.
    pub bitswap_client: bool,
    /// Kademlia discovery enabled.
    pub kademlia: bool,
    /// Autonat holepunching enabled.
    pub autonat: bool,
    /// Relay server enabled.
    pub relay_server: bool,
    /// Relay client enabled.
    pub relay_client: bool,
    /// Gossipsub enabled.
    pub gossipsub: bool,
    pub max_conns_out: u32,
    pub max_conns_in: u32,
    pub max_conns_pending_out: u32,
    pub max_conns_pending_in: u32,
    pub max_conns_per_peer: u32,
    pub notify_handler_buffer_size: usize,
    pub connection_event_buffer_size: usize,
    pub dial_concurrency_factor: u8,
}

/// Configuration for the [`luffa-p2p`] node.
#[derive(PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Configuration for libp2p.
    pub libp2p: Libp2pConfig,

    /// Directory where cryptographic keys are stored.
    ///
    /// The p2p node needs to have an identity consisting of a cryptographic key pair.  As
    /// it is useful to have the same identity across restarts this is stored on disk in a
    /// format compatible with how ssh stores keys.  This points to a directory where these
    /// keypairs are stored.
    pub key_store_path: PathBuf,
}

impl From<ServerConfig> for Config {
    fn from(source: ServerConfig) -> Self {
        source.p2p
    }
}

impl Source for Libp2pConfig {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();
        // `config` package converts all unsigned integers into U64, which then has problems
        // downcasting to, in this case, u32. To get it to allow the convertion between the
        // config::Config and the p2p::Config, we need to cast it as a signed int
        insert_into_config_map(&mut map, "max_conns_in", self.max_conns_in as i64);
        insert_into_config_map(&mut map, "max_conns_out", self.max_conns_out as i64);
        insert_into_config_map(
            &mut map,
            "max_conns_pending_in",
            self.max_conns_pending_in as i64,
        );
        insert_into_config_map(
            &mut map,
            "max_conns_pending_out",
            self.max_conns_pending_out as i64,
        );
        insert_into_config_map(
            &mut map,
            "max_conns_per_peer",
            self.max_conns_per_peer as i64,
        );
        insert_into_config_map(
            &mut map,
            "notify_handler_buffer_size",
            self.notify_handler_buffer_size as i64,
        );
        insert_into_config_map(
            &mut map,
            "connection_event_buffer_size",
            self.connection_event_buffer_size as i64,
        );
        insert_into_config_map(
            &mut map,
            "dial_concurrency_factor",
            self.dial_concurrency_factor as i64,
        );

        insert_into_config_map(&mut map, "kademlia", self.kademlia);
        insert_into_config_map(&mut map, "autonat", self.autonat);
        insert_into_config_map(&mut map, "bitswap_client", self.bitswap_client);
        insert_into_config_map(&mut map, "bitswap_server", self.bitswap_server);
        insert_into_config_map(&mut map, "mdns", self.mdns);
        insert_into_config_map(&mut map, "relay_server", self.relay_server);
        insert_into_config_map(&mut map, "relay_client", self.relay_client);
        insert_into_config_map(&mut map, "gossipsub", self.gossipsub);
        let peers: Vec<String> = self.bootstrap_peers.iter().map(|b| b.to_string()).collect();
        insert_into_config_map(&mut map, "bootstrap_peers", peers);
        let addrs: Vec<String> = self
            .listening_multiaddrs
            .iter()
            .map(|b| b.to_string())
            .collect();
        insert_into_config_map(&mut map, "listening_multiaddrs", addrs);
        Ok(map)
    }
}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();
        insert_into_config_map(&mut map, "libp2p", self.libp2p.collect()?);
        insert_into_config_map(&mut map, "key_store_path", self.key_store_path.to_str());
        Ok(map)
    }
}

impl Default for Libp2pConfig {
    fn default() -> Self {
        let bootstrap_peers = DEFAULT_BOOTSTRAP
            .iter()
            .map(|node| node.parse::<Multiaddr>().unwrap())
            .collect::<Vec<_>>();
        Self {
            listening_multiaddrs: vec![
                // "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
                "/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap(),
            ],
            bootstrap_peers,
            mdns: false,
            kademlia: true,
            autonat: false,
            relay_server: true,
            relay_client: true,
            gossipsub: true,
            bitswap_client: true,
            bitswap_server: false,
            max_conns_pending_out: 1024 * 1024,
            max_conns_pending_in: 1024 * 1024,
            max_conns_in: 1024 * 1024,
            max_conns_out: 1024 * 1024,
            max_conns_per_peer: 16,
            notify_handler_buffer_size: 256,
            connection_event_buffer_size: 256,
            dial_concurrency_factor: 4,
        }
    }
}

impl Config {
    pub fn default_network() -> Self {
        Self {
            libp2p: Libp2pConfig::default(),
            key_store_path: luffa_data_root().unwrap(),
        }
    }
}
