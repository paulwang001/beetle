use anyhow::Result;
use luffa_sdk::{Callback, Client};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::time::Duration;

#[derive(Debug, Clone)]
struct Messager {
    sender: Arc<Sender<Vec<u8>>>,
}

impl Callback for Messager {
    fn on_message(&self, msg: Vec<u8>) {}
}

impl Messager {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        Self { sender:Arc::new(sender) }
    }
}

fn main() -> Result<()> {
    let (tx, mut rx) = channel(1024);
    let msg = Messager::new(tx);
    let client = Client::new();
    let cfg_path = std::env::args().nth(1);

    println!("starting");
    client.start(cfg_path, Box::new(msg));
    println!("started.");
    let r = tokio::runtime::Runtime::new().unwrap();
    r.block_on(async move {
        println!(".....");
        // tokio::spawn(async move {
        //     loop {
        //         tokio::time::sleep(Duration::from_secs(5)).await;
        //         let peers = client.relay_list();
        //         println!("{:?}", peers);
        //     }
        // });
        loop {
            if let Some(msg) = rx.recv().await {
                println!("msg:{}", msg.len());
            }
        }
        println!(".....");
    });

    Ok(())
}
