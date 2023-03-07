use anyhow::Result;
use futures::pending;
use luffa_rpc_types::Message;
use luffa_sdk::{Callback, Client};
use std::future::{Future, IntoFuture};
use std::task::Poll;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc, sync::Mutex};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

#[derive(Debug, Clone)]
struct Messager {
    queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl Callback for Messager {
    fn on_message(&self, crc:u64,from_id:u64,to:u64,msg: Vec<u8>) {
        tracing::warn!("on>>>> {}", msg.len());
        if let Some(msg) = luffa_rpc_types::message_from(msg) {
           match msg {
              Message::RelayNode { did } =>{
                
              }
              Message::ContactsSync { did, contacts }=>{

              }
              Message::ContactsExchange { exchange }=>{

              }
              Message::Chat { content }=>{

              }
              _=>{
                
              }
           }
        }
        // let mut q = self.queue.lock().unwrap();
        // q.push_back(msg);
        
        // self.sender.send(msg);
    }
}

impl Messager {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

// impl Future for Messager {
//     type Output = Vec<u8>;

//     fn poll(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Self::Output> {
//         loop {
//             let mut q = self.queue.lock().unwrap();
//             match q.pop_front() {
//                 Some(msg) => Poll::ready(msg),
//                 None => Poll::Pending,
//             }
//         }
//     }
// }

fn main() -> Result<()> {
    // let (tx, mut rx) = channel(1024);
    let msg = Messager::new();
    let client = Client::new();
    let cfg_path = std::env::args().nth(1);
    // let msg_t = msg.clone();
    println!("starting");
    client.start(cfg_path, Box::new(msg));
    println!("started.");

    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        let peer_id = client.get_peer_id();
        info!("peer id: {peer_id:?}");
        let peers = client.relay_list();
        // client.send_msg(to, msg)
        println!("{:?}", peers);
    });

    let r = tokio::runtime::Runtime::new().unwrap();
    r.block_on(async move {
        println!(".....");
        loop {
            // if let Some(msg) = rx.recv().await {
            //     println!("msg:{}", msg.len());
            // }
            // let m = msg.await;
            // info!("----------------{m:?}--------------");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        println!(".....");
    });
    
    Ok(())
}
