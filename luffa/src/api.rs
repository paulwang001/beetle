use crate::api_p2p::P2p as P2pApi;
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use cid::Cid;
use config::{ConfigError, Map, Source, Value};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use libipld::{
    cbor::DagCborCodec,
    prelude::{Codec, Decode, Encode},
    Ipld, IpldCodec,
};
use libp2p::gossipsub::{MessageId, TopicHash};
use libp2p::multihash::Code;
use luffa_bitswap::Block;
use luffa_metrics::config::Config as MetricsConfig;
use luffa_rpc_client::Config as RpcClientConfig;
use luffa_rpc_client::{Client, ClientStatus};
use multihash::MultihashDigest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use luffa_util::{insert_into_config_map, luffa_config_path, make_config};
use tokio::io::{AsyncRead, AsyncReadExt};

/// CONFIG_FILE_NAME is the name of the optional config file located in the luffa home directory
pub const CONFIG_FILE_NAME: &str = "ctl.config.toml";
/// ENV_PREFIX should be used along side the config field name to set a config field using
/// environment variables
pub const ENV_PREFIX: &str = "LUFFA_CTL";

/// Configuration for [`luffa-api`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub rpc_client: RpcClientConfig,
    pub metrics: MetricsConfig,
    pub http_resolvers: Option<Vec<String>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc_client: RpcClientConfig::default_network(),
            metrics: Default::default(),
            http_resolvers: None,
        }
    }
}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }
    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut map: Map<String, Value> = Map::new();
        insert_into_config_map(&mut map, "rpc_client", self.rpc_client.collect()?);
        insert_into_config_map(&mut map, "metrics", self.metrics.collect()?);
        if let Some(http_resolvers) = &self.http_resolvers {
            insert_into_config_map(&mut map, "http_resolvers", http_resolvers.clone());
        }

        Ok(map)
    }
}

/// API to interact with an luffa system.
///
/// This provides an API to use the luffa system consisting of several services working
/// together.  It offers both a higher level API as well as some lower-level APIs.
///
/// Unless working on luffa directly this should probably be constructed via the `luffa-embed`
/// crate rather then directly.
#[derive(Debug, Clone)]
pub struct Api {
    client: Client,
}

pub enum OutType {
    Dir,
    Reader(Box<dyn AsyncRead + Unpin + Send>),
    Symlink(PathBuf),
}

impl fmt::Debug for OutType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dir => write!(f, "Dir"),
            Self::Reader(_) => write!(f, "Reader(impl AsyncRead + Unpin>)"),
            Self::Symlink(arg0) => f.debug_tuple("Symlink").field(arg0).finish(),
        }
    }
}

impl Api {
    /// Creates a new instance from the luffa configuration.
    ///
    /// This loads configuration from an optional configuration file and environment
    /// variables.
    // The lifetime is needed for mocking.
    #[allow(clippy::needless_lifetimes)]
    pub async fn from_env<'a>(
        config_path: Option<&'a Path>,
        overrides_map: HashMap<String, String>,
    ) -> Result<Self> {
        let cfg_path = luffa_config_path(CONFIG_FILE_NAME)?;
        let sources = [Some(cfg_path.as_path()), config_path];
        let config = make_config(
            // default
            Config::default(),
            // potential config files
            &sources,
            // env var prefix for this config
            ENV_PREFIX,
            // map of present command line arguments
            overrides_map,
        )?;
        Self::new(config).await
    }

    /// Creates a new instance from the provided configuration.
    pub async fn new(config: Config) -> Result<Self> {
        let client = Client::new(config.rpc_client).await?;

        Ok(Self { client })
    }

    pub fn from_client(client: Client) -> Self {
        Self { client }
    }

    /// Announces to the DHT that this node can offer the given [`Cid`].
    ///
    /// This publishes a provider record for the [`Cid`] to the DHT, establishing the local
    /// node as provider for the [`Cid`].
    pub async fn provide(&self, cid: Cid) -> Result<()> {
        self.client.try_p2p()?.start_providing(&cid).await
    }

    pub fn p2p(&self) -> Result<P2pApi> {
        let p2p_client = self.client.try_p2p()?;
        Ok(P2pApi::new(p2p_client))
    }
    pub async fn check(&self) -> ClientStatus {
        self.client.check().await
    }

    pub async fn watch(&self) -> BoxStream<'static, ClientStatus> {
        self.client.clone().watch().await.boxed()
    }

    pub async fn publish(&self, topic: TopicHash, data: bytes::Bytes) -> Result<MessageId> {
        match self.client.try_p2p() {
            Ok(p2p) => p2p.gossipsub_publish(topic, data).await,
            Err(e) => Err(e),
        }
    }
    pub async fn subscribe(&self, topic: TopicHash) -> Result<bool> {
        match self.client.try_p2p() {
            Ok(p2p) => p2p.gossipsub_subscribe(topic).await,
            Err(e) => Err(e),
        }
    }
    pub async fn unsubscribe(&self, topic: TopicHash) -> Result<bool> {
        match self.client.try_p2p() {
            Ok(p2p) => p2p.gossipsub_unsubscribe(topic).await,
            Err(e) => Err(e),
        }
    }

    pub async fn fetch_data(&self, ctx: u64, cid: Cid) -> Result<Option<Bytes>> {
        match self.client.try_p2p() {
            Ok(p2p) => {
                let providers = p2p.fetch_providers_dht(&cid).await?;
                let providers = providers.collect::<Vec<_>>();
                let mut itr = providers.await.into_iter();
                while let Some(Ok(pp)) = itr.next() {
                    tracing::warn!("fetch from: {pp:?}");
                    if pp.is_empty() {
                        continue;
                    }
                    if let Ok(data) = p2p.fetch_bitswap(ctx, cid, pp).await {
                        return Ok(Some(data));
                    }
                }
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn put(&self, data: Bytes) -> Result<Cid> {
        let rsp = self.client.try_p2p().unwrap().push_data(data).await?;
        // let data = data.to_vec();
        // let cid = Cid::new_v1(DagCborCodec.into(), multihash::Code::Sha2_256.digest(&data));
        // self.client
        //     .try_p2p()
        //     .unwrap()
        //     .start_providing(&rsp.cid)
        //     .await?;
        // self.client
        //     .try_store()
        //     .unwrap()
        //     .put(cid, Bytes::from(data), vec![])
        //     .await?;

        Ok(rsp.cid)

    }

    pub async fn put_record(&self, data: Bytes, expires: Option<u64>) -> Result<Cid> {
        let data = data.to_vec();
        let cid = Cid::new_v1(DagCborCodec.into(), multihash::Code::Sha2_256.digest(&data));
        let publisher = match self.client.try_p2p().unwrap().local_peer_id().await {
            Ok(p) => Some(p),
            Err(_) => None,
        };
        let data = Bytes::from(data);
        self.client
            .try_p2p()
            .unwrap()
            .put_record(&cid, data, publisher, expires)
            .await?;
        Ok(cid)
    }
    pub async fn get_record(&self, key: &String) -> Result<Option<Bytes>> {
        let key = Cid::from_str(&key)?;
        if let Ok(rsp) = self.client.try_p2p().unwrap().get_record(&key).await {
            Ok(Some(rsp.data))
        } else {
            Ok(None)
        }
    }
}
