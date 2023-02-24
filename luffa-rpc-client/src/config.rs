use config::{ConfigError, Map, Source, Value};
use luffa_rpc_types::{p2p::P2pAddr, store::StoreAddr};
use luffa_util::insert_into_config_map;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
/// Config for the rpc Client.
pub struct Config {
    /// P2p rpc address.
    pub p2p_addr: Option<P2pAddr>,
    /// Store rpc address.
    pub store_addr: Option<StoreAddr>,
    /// Number of concurent channels.
    ///
    /// If `None` defaults to `1`, not used for in-memory addresses.
    // TODO: Consider changing this to NonZeroUsize instead of Option<usize>.
    pub channels: Option<usize>,
}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();

        if let Some(addr) = &self.p2p_addr {
            insert_into_config_map(&mut map, "p2p_addr", addr.to_string());
        }
        if let Some(addr) = &self.store_addr {
            insert_into_config_map(&mut map, "store_addr", addr.to_string());
        }
        if let Some(channels) = &self.channels {
            insert_into_config_map(&mut map, "channels", channels.to_string());
        }
        Ok(map)
    }
}

impl Config {
    pub fn default_network() -> Self {
        Self {
            p2p_addr: Some("irpc://127.0.0.1:8887".parse().unwrap()),
            store_addr: Some("irpc://127.0.0.1:4402".parse().unwrap()),
            /// disable load balancing by default by just having 1 channel
            channels: Some(1),
        }
    }
}
