use luffa_sdk::{Callback, Client};
use std::sync::mpsc::{channel, Receiver, Sender};
struct Messager {
    sender: Sender<Vec<u8>>,
}

impl Callback for Messager {
    fn on_message(&self, msg: Vec<u8>) {}
}

impl Messager {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        Self { sender }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let client = Client::new();
    let cfg_path = std::env::args().nth(1);
    let (tx, rx) = channel();
    let msg = Messager::new(tx);
    tracing::info!("starting");
    client.start(cfg_path, Box::new(msg));
    tracing::info!("started.");

    while let Ok(msg) = rx.recv() {
        tracing::info!("msg:{}", msg.len());
    }

    Ok(())
}
