use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use console::style;
use crossterm::style::Stylize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use luffa_metrics::config::Config as MetricsConfig;
use luffa_util::{human, luffa_config_path, make_config};

use crate::api::Api;
use crate::config::{Config, CONFIG_FILE_NAME, ENV_PREFIX};

use crate::p2p::{run_command as run_p2p_command, P2p};
use crate::services::require_services;
use crate::size::size_stream;

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
    #[clap(about = "Check the health of the different iroh services")]
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
        };

        Ok(())
    }
}
