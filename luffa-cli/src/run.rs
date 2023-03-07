use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::{Parser, Subcommand};

use libp2p::gossipsub::TopicHash;
use luffa_metrics::config::Config as MetricsConfig;
use luffa_util::{luffa_config_path, make_config};

use crate::api::Api;
use crate::config::{Config, CONFIG_FILE_NAME, ENV_PREFIX};

use crate::p2p::{run_command as run_p2p_command, P2p};

#[derive(Parser, Debug, Clone)]
#[clap(version, long_about = None, propagate_version = true)]
#[clap(about = "A next Web3 IM")]
#[clap(after_help = "")]
pub struct Cli {
    #[clap(long)]
    cfg: Option<PathBuf>,
    /// Do not track metrics
    #[clap(long)]
    no_metrics: bool,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    P2p(P2p),
    #[clap(about = "Start local luffa services")]
    #[clap(after_help = "")]
    Start {
        service: Vec<String>,
        /// Start all services
        #[clap(short, long)]
        all: bool,
    },
    /// status checks the health of the different processes
    #[clap(about = "Check the health of the different luffa services")]
    #[clap(after_help = "")]
    Status {
        #[clap(short, long)]
        /// when true, updates the status table whenever a change in a process's status occurs
        watch: bool,
    },
    #[clap(about = "Stop local luffa services")]
    #[clap(after_help = "")]
    Stop {
        service: Vec<String>,
    },

    #[clap(about = "gossip publish")]
    #[clap(after_help = "")]
    Pub {
        topic: String,
        data: String,
        codec: u8,
    },

    #[clap(about = "gossip sub")]
    #[clap(after_help = "")]
    Sub {
        topic: String,
        codec: u8,
    },

    #[clap(about = "gossip unsub")]
    #[clap(after_help = "")]
    UnSub {
        topic: String,
        codec: u8,
    },

    #[clap(about = "DHT Put Record")]
    #[clap(after_help = "")]
    Put {
        data: String,
        codec: u8,
    },
    #[clap(about = "DHT Get Record")]
    #[clap(after_help = "")]
    Get {
        key: String,
        codec: u8,
    },
    #[clap(about = "Bitswap Put Block")]
    Push {
        data: String,
    },
    #[clap(about = "Bitswap Fetch Block")]
    Fetch {
        ctx: u64,
        cid: String,
    },
}

impl Cli {
    pub async fn run(&self) -> Result<()> {
        let config_path = luffa_config_path(CONFIG_FILE_NAME)?;
        let sources = [Some(config_path.as_path()), self.cfg.as_deref()];
        let config = make_config(
            // default
            Config::new(),
            // potential config files
            &sources,
            // env var prefix for this config
            ENV_PREFIX,
            // map of present command line arguments
            // args.make_overrides_map(),
            HashMap::<String, String>::new(),
        )
        .unwrap();

        let metrics_handler = luffa_metrics::MetricsHandle::new(MetricsConfig::default())
            .await
            .expect("failed to initialize metrics");

        let api = Api::from_env(self.cfg.as_deref(), self.make_overrides_map()).await?;

        self.cli_command(&config, &api).await?;

        metrics_handler.shutdown();

        Ok(())
    }

    fn make_overrides_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("metrics.debug".to_string(), (self.no_metrics).to_string());
        map
    }

    async fn cli_command(&self, config: &Config, api: &Api) -> Result<()> {
        match &self.command {
            Commands::P2p(p2p) => run_p2p_command(&api.p2p()?, p2p).await?,
            Commands::Start { service, all } => {
                let svc = match *all {
                    true => vec![String::from("store"), String::from("p2p")],
                    false => match service.is_empty() {
                        true => config.start_default_services.clone(),
                        false => service.clone(),
                    },
                };
                
                crate::services::start(api, &svc).await?;
            }
            Commands::Status { watch } => {
                crate::services::status(api, *watch).await?;
            }
            Commands::Stop { service } => {
                crate::services::stop(api, service).await?;
            }
            Commands::Put { data,.. } => {
                // let (_,data) = multibase::decode(data)?;
                let data = data.as_bytes();
                let expires = Some(60);
                let cid = api.put_record(bytes::Bytes::from(data.to_vec()),expires).await?;
                println!("Put success, Cid: {}",cid.to_string());
            }
            Commands::Pub { topic, data, .. } => {
                let data = data.as_bytes();
                let data = bytes::Bytes::from(data.to_vec());
                let msg_id = api.publish(TopicHash::from_raw(topic),data).await?;
                println!("Pub success, id: {}",msg_id.to_string());
            }
            Commands::Get { key, .. } => {
                match api.get_record(key).await? {
                    Some(data)=>{
                        println!("Get> {}",String::from_utf8(data.to_vec())?);
                    }
                    None=>{
                        println!("Get not found: {}",key);
                    }
                }
            }
            Commands::Sub { topic, .. } =>{
                let ret = api.subscribe(TopicHash::from_raw(topic)).await?;
                println!("Sub topic:{}",ret);
            }
            Commands::UnSub { topic, .. } =>{
                let ret = api.unsubscribe(TopicHash::from_raw(topic)).await?;
                
                println!("UnSub topic:{}",ret);
            }
            Commands::Push { data } =>{
                let cid = api.put(bytes::Bytes::from(data.as_bytes().to_vec())).await?;
                println!("Push: {}",cid.to_string());
            }
            Commands::Fetch { ctx,cid } =>{
                let cid = cid::Cid::from_str(&cid)?;
                match api.fetch_data(*ctx, cid).await {
                    Ok(Some(data))=>{
                        println!("Fetch> {cid} : {}",String::from_utf8(data.to_vec())?);
                    }
                    Ok(None)=>{
                        println!("Fetch> {cid} not found.");
                    }
                    Err(e)=>{
                        eprintln!("fetch error:{e:?}");
                    }
                }
            }
        };

        Ok(())
    }
}
