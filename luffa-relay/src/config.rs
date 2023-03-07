use anyhow::Result;
use config::{ConfigError, Map, Source, Value};

use luffa_metrics::config::Config as MetricsConfig;
use luffa_node::Libp2pConfig;
use luffa_store::config::config_data_path;
use luffa_util::insert_into_config_map;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// CONFIG_FILE_NAME is the name of the optional config file located in the luffa home directory
pub const CONFIG_FILE_NAME: &str = "luffa.relay.toml";
/// ENV_PREFIX should be used along side the config field name to set a config field using
/// environment variables
/// For example, `LUFFA_ONE_PORT=1000` would set the value of the `Config.port` field
pub const ENV_PREFIX: &str = "LUFFA_";
pub const DEFAULT_PORT: u16 = 8050;

/// Configuration for [`luffa`].
///
/// The configuration includes gateway, store and p2p specific items as well as the common
/// rpc & metrics ones.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Config {
    /// Store specific configuration.
    pub store: luffa_store::config::Config,
    /// P2P specific configuration.
    pub p2p: luffa_node::config::Config,
    /// metrics configuration
    pub metrics: MetricsConfig,
}

impl Config {
    pub fn new(
        store: luffa_store::config::Config,
        p2p: luffa_node::config::Config,
    ) -> Self {
        Self {
            store,
            p2p,
            metrics: MetricsConfig::default(),
        }
    }

}

impl Default for Config {
    fn default() -> Self {
        let metrics_config = MetricsConfig::default();
        let store_config = default_store_config(None).unwrap();
        let key_store_path = luffa_util::luffa_data_root().unwrap();
        Self {
            metrics: metrics_config,
            store: store_config,
            p2p: default_p2p_config( key_store_path),
        }
    }
}

fn default_store_config(
    store_path: Option<PathBuf>,
) -> Result<luffa_store::config::Config> {
    let path = config_data_path(store_path)?;
    Ok(luffa_store::config::Config {
        path,
    })
}

fn default_p2p_config(
    key_store_path: PathBuf,
) -> luffa_node::config::Config {
    let mut p2p_config = Libp2pConfig::default();
    p2p_config.bootstrap_peers = vec![];

    luffa_node::config::Config {
        key_store_path,
        libp2p: p2p_config,
    }
}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();

        insert_into_config_map(&mut map, "store", self.store.collect()?);
        insert_into_config_map(&mut map, "p2p", self.p2p.collect()?);
        insert_into_config_map(&mut map, "metrics", self.metrics.collect()?);
        Ok(map)
    }
}
