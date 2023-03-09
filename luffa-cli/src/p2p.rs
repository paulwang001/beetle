use anyhow::{Error, Result};
use clap::{Args, Subcommand};
use crossterm::style::Stylize;
use libp2p::{Multiaddr, PeerId};
use std::{collections::HashMap, fmt::Display, str::FromStr};


#[derive(Args, Debug, Clone)]
#[clap(about = "Peer-2-peer commands")]
#[clap(
    after_help = "p2p commands all relate to peer-2-peer connectivity. See subcommands for
additional details."
)]
pub struct P2p {
    #[clap(subcommand)]
    command: P2pCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum P2pCommands {
    #[clap(about = "Connect to a peer")]
    #[clap(after_help = "")]
    Connect {
        /// Multiaddr or peer ID of a peer to connect to
        addr: PeerIdOrAddrArg,
    },
    #[clap(about = "Retrieve info about a node")]
    #[clap(after_help = "")]
    Lookup {
        /// multiaddress or peer ID
        addr: Option<PeerIdOrAddrArg>,
    },
    #[clap(about = "List connected peers")]
    #[clap(after_help = "")]
    Peers {},

    #[clap(about = "List addresses")]
    #[clap(after_help = "")]
    Addresses {},

    #[clap(about = "Mesh peers")]
    #[clap(after_help = "")]
    Mesh { topic: String },

    #[clap(about = "Push ")]
    #[clap(after_help = "")]
    Push { data: String },

    #[clap(about = "Fetch ")]
    #[clap(after_help = "")]
    Fetch { ctx: u64, cid: String },
}

#[derive(Debug, Clone)]
pub struct PeerIdOrAddrArg(PeerIdOrAddr);

impl FromStr for PeerIdOrAddrArg {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(m) = Multiaddr::from_str(s) {
            return Ok(PeerIdOrAddrArg(PeerIdOrAddr::Multiaddr(m)));
        }
        if let Ok(p) = PeerId::from_str(s) {
            return Ok(PeerIdOrAddrArg(PeerIdOrAddr::PeerId(p)));
        }
        Err(anyhow::anyhow!("invalid peer id or multiaddress"))
    }
}

impl Display for PeerIdOrAddrArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let peer_id_or_addr = match &self.0 {
            PeerIdOrAddr::PeerId(p) => p.to_string(),
            PeerIdOrAddr::Multiaddr(a) => a.to_string(),
        };
        write!(f, "{peer_id_or_addr}")
    }
}

pub async fn run_command(p2p: &P2pApi, cmd: &P2p) -> Result<()> {
    match &cmd.command {
        P2pCommands::Connect { addr } => match p2p.connect(&addr.0).await {
            Ok(_) => {
                tracing::info!("Connected to {addr}!");
            }
            Err(e) => return Err(e),
        },
        P2pCommands::Lookup { addr } => {
            let lookup = match addr {
                Some(addr) => p2p.lookup(&addr.0).await?,
                None => p2p.lookup_local().await?,
            };
            
            display_lookup(&lookup);
        }
        P2pCommands::Peers {} => {
            let peers = p2p.peers().await?;
            display_peers(peers);
        }
        P2pCommands::Addresses {} => {  
            let addresses = p2p.addresses().await?;
            display_addresses(addresses);
        }
        P2pCommands::Mesh { topic } =>{
            let peers = p2p.mesh_peers(topic.clone()).await?;
            for (i,peer) in peers.into_iter().enumerate() {
                tracing::info!("{} [{}] {}",topic,i+1,peer.to_string());
            }
        }
        P2pCommands::Push { data } =>{
            let cid = p2p.push(bytes::Bytes::from(data.as_bytes().to_vec())).await?;
            tracing::info!("cid: {}",cid.to_string());
        }
        P2pCommands::Fetch { ctx, cid } => {
            let cid = cid::Cid::from_str(cid)?;
            let data = p2p.fetch(*ctx, cid).await?;
            match data {
                Some(data)=>{
                    tracing::info!("found: {}",String::from_utf8(data.to_vec()).unwrap());
                }
                None=>{
                    tracing::info!("not found: {cid}");
                }
            }
        }
    };
    Ok(())
}

fn display_lookup(l: &Lookup) {
    tracing::info!("{}\n  {}", "Peer ID:".bold().dim(), l.peer_id);
    tracing::info!("{}\n  {}", "Agent Version:".bold().dim(), l.agent_version);
    tracing::info!(
        "{}\n  {}",
        "Protocol Version:".bold().dim(),
        l.protocol_version
    );
    tracing::info!(
        "{} {}",
        "Observed Addresses".bold().dim(),
        format!("({}):", l.observed_addrs.len()).bold().dim()
    );
    l.observed_addrs
        .iter()
        .for_each(|addr| tracing::info!("  {addr}"));
    tracing::info!(
        "{} {}",
        "Listening Addresses".bold().dim(),
        format!("({}):", l.listen_addrs.len()).bold().dim()
    );
    l.listen_addrs.iter().for_each(|addr| tracing::info!("  {addr}"));
    tracing::info!(
        "{} {}\n  {}",
        "Protocols".bold().dim(),
        format!("({}):", l.protocols.len()).bold().dim(),
        l.protocols.join("\n  ")
    );
}

fn display_addresses(addresses: Vec<Multiaddr>) {
    for (i, addr) in addresses.into_iter().enumerate() {
        tracing::info!("{}:{}", i + 1, addr.to_string());
    }
}

fn display_peers(peers: HashMap<PeerId, Vec<Multiaddr>>) {
    // let mut pid_str: String;
    for (peer_id, addrs) in peers {
        if let Some(addr) = addrs.first() {
            tracing::info!("{addr}/p2p/{peer_id}");
        }
    }
}
