use config::{ConfigError, Map, Source, Value};
use luffa_util::insert_into_config_map;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The name of the service. Should be the same as the Cargo package name.
    pub service_name: String,
    /// A unique identifier for this instance of the service.
    pub instance_id: String,
    /// The build version of the service (commit hash).
    pub build: String,
    /// The version of the service. Should be the same as the Cargo package version.
    pub version: String,
    /// The environment of the service.
    pub service_env: String,
    /// Flag to enable metrics collection.
    pub collect: bool,
    /// Flag to enable tracing collection.
    pub tracing: bool,
    /// The endpoint of the trace collector.
    pub collector_endpoint: String,
    /// The endpoint of the prometheus push gateway.
    #[serde(alias = "prom_gateway_endpoint")]
    pub prom_gateway_endpoint: String,
    /// log level valid value of (Trace，Debug，Info，Warn，Error)
    pub log_level: String,
}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map = Map::new();
        insert_into_config_map(&mut map, "service_name", self.service_name.clone());
        insert_into_config_map(&mut map, "instance_id", self.instance_id.clone());
        insert_into_config_map(&mut map, "build", self.build.clone());
        insert_into_config_map(&mut map, "version", self.version.clone());
        insert_into_config_map(&mut map, "service_env", self.service_env.clone());
        insert_into_config_map(&mut map, "collect", self.collect);
        insert_into_config_map(&mut map, "tracing", self.tracing);
        insert_into_config_map(
            &mut map,
            "collector_endpoint",
            self.collector_endpoint.clone(),
        );
        insert_into_config_map(
            &mut map,
            "prom_gateway_endpoint",
            self.prom_gateway_endpoint.clone(),
        );
        insert_into_config_map(&mut map, "log_level", self.log_level.clone());
        Ok(map)
    }
}

impl Config {
    pub fn with_service_name(mut self, name: String) -> Self {
        self.service_name = name;
        self
    }
    pub fn with_build(mut self, build: String) -> Self {
        self.build = build;
        self
    }
    pub fn with_version(mut self, version: String) -> Self {
        self.version = version;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            service_name: "unknown".to_string(),
            instance_id: names::Generator::default().next().unwrap(),
            build: "unknown".to_string(),
            version: "unknown".to_string(),
            service_env: "dev".to_string(),
            collect: true,
            tracing: true,
            collector_endpoint: "http://182.140.244.175:4317".to_string(),
            prom_gateway_endpoint: "http://182.140.244.175:9100".to_string(),
            log_level: "Info".to_string(),
        }
    }
}
