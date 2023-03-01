use anyhow::Result;
use futures_lite::future::block_on;
use luffa_sdk::{Callback, Client};
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

#[derive(Debug, Clone)]
struct Messager {
    // queue: VecDeque<Vec<u8>>,
}

impl Callback for Messager {
    fn on_message(&self, msg: Vec<u8>) {
        tracing::warn!("on>>>> {}", msg.len());
        // self.queue
        // self.sender.send(msg);
    }
}

impl Messager {
    pub fn new() -> Self {
        Self { 
            // queue:VecDeque::new() 
        }
    }
}

fn main() -> Result<()> {
    // let (tx, mut rx) = channel(1024);
    let msg = Messager::new();
    let client = Client::new();
    let cfg_path = std::env::args().nth(1);

    println!("starting");
    client.start(cfg_path, Box::new(msg));
    println!("started.");

    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        let peer_id = client.get_peer_id();
        info!("peer id: {peer_id:?}");
        let peers = client.relay_list();
        println!("{:?}", peers);
    });

    let r = tokio::runtime::Runtime::new().unwrap();
    r.block_on(async move {
        println!(".....");
        loop {
            // if let Some(msg) = rx.recv().await {
            //     println!("msg:{}", msg.len());
            // }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        println!(".....");
    });

    Ok(())
}
