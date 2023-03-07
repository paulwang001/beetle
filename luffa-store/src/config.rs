use anyhow::{anyhow, Result};
use config::{ConfigError, Map, Source, Value};
use luffa_metrics::config::Config as MetricsConfig;
use luffa_util::{insert_into_config_map, luffa_data_path};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// CONFIG_FILE_NAME is the name of the optional config file located in the luffa home directory
pub const CONFIG_FILE_NAME: &str = "store.config.toml";
/// ENV_PREFIX should be used along side the config field name to set a config field using
/// environment variables
/// For example, `LUFFA_STORE_PATH=/path/to/config` would set the value of the `Config.path` field
pub const ENV_PREFIX: &str = "LUFFA_STORE";

/// the path to data directory. If arg_path is `None`, the default luffa_data_path()/store is used
/// luffa_data_path() returns an operating system-specific directory
pub fn config_data_path(arg_path: Option<PathBuf>) -> Result<PathBuf> {
    match arg_path {
        Some(p) => Ok(p),
        None => luffa_data_path("store").map_err(|e| anyhow!("{}", e)),
    }
}

/// The configuration for the store server.
///
/// This is the configuration which the store server binary needs to run.  This is a
/// superset from the configuration needed by the store service.
#[derive(PartialEq, Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    /// Configuration of the store service.
    pub store: Config,
    /// Configuration for metrics export.
    pub metrics: MetricsConfig,
}

impl ServerConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            store: Config::new(path),
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
        insert_into_config_map(&mut map, "store", self.store.collect()?);
        insert_into_config_map(&mut map, "metrics", self.metrics.collect()?);
        Ok(map)
    }
}

/// The configuration for the store service.
///
/// As opposed to the [`ServerConfig`] this is only the configuration needed to run the
/// store service.  It can still be deserialised from a file.
#[derive(PartialEq, Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    /// The location of the content database.
    pub path: PathBuf,
}

impl From<ServerConfig> for Config {
    fn from(source: ServerConfig) -> Self {
        source.store
    }
}

impl Config {
    /// Creates a new store config.
    ///
    /// This config will not have any RpcClientConfig, but that is fine because it is mostly
    /// unused: `luffa_rpc::rpc::new` which is used takes the RPC address as a separate
    /// argument.  Once #672 is merged we can probably remove the `rpc_client` field.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
        }
    }

}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();
        let path = self
            .path
            .to_str()
            .ok_or_else(|| ConfigError::Foreign("No `path` set. Path is required.".into()))?;
        insert_into_config_map(&mut map, "path", path);
        Ok(map)
    }
}
