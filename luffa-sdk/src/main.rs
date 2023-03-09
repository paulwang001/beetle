#![feature(poll_ready)]
use anyhow::Result;
use futures::pending;
use luffa_rpc_types::{Message, message_to, ContactsToken};
use luffa_sdk::{Callback, Client};
use tracing::log::warn;
use std::future::{Future, IntoFuture};
use std::sync::RwLock;
use std::task::Poll;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc, sync::Mutex};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::{info, error};

#[derive(Debug, Clone)]
struct Messager {
    queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl Callback for Messager {
    fn on_message(&self, crc:u64,from_id:u64,to:u64,msg: Vec<u8>) {
        tracing::warn!("on>>>> {}", msg.len());
        if let Some(msg) = luffa_rpc_types::message_from(msg.clone()) {
           match msg {
              Message::RelayNode { did } =>{
                
              }
              Message::ContactsSync { did, contacts }=>{

              }
              Message::ContactsExchange { exchange }=>{

              }
              Message::Chat { content }=>{

              }
              Message::Feedback { crc, status }=>{
                error!("Feedback:{crc}  {status:?}");
              }
              _=>{
                
              }
           }
        }
        let mut q = self.queue.lock().unwrap();
        q.push_back(msg);
        
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

impl Future for Messager {
    type Output = Vec<u8>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut q = self.queue.lock().unwrap();
        match q.pop_front() {
            Some(msg) => Poll::Ready(msg),
            None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }
}

fn main() -> Result<()> {
    // let (tx, mut rx) = channel(1024);

    

    let msg = Messager::new();
    let msg = Box::new(msg);
    let to_id = std::env::args().nth(1).unwrap_or_default();
    let to_id:u64 = to_id.parse().unwrap_or_default();
    // let msg_t = Arc::new(msg.clone());
    let client = Client::new();
    let cfg_path = std::env::args().nth(2);
    // let msg_t = msg.clone();
    println!("starting");
    client.start(cfg_path, msg);
    println!("started.");



    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        let peer_id = client.get_local_id();
        info!("peer id: {peer_id:?}");
        let peers = client.relay_list();
        // client.send_msg(to, msg)
        println!("{:?}", peers);
        if to_id > 0 {
            let msg = Message::Feedback { crc: to_id, status: luffa_rpc_types::FeedbackStatus::Read };
            let msg = message_to(msg).unwrap();
            match client.send_msg(to_id, msg) {
                Ok(_)=>{
                    info!("send seccess");
                }
                Err(e)=>{
                    error!("{e:?}");
                }
            }

            let code = client.show_code(Some("hello".to_string()));
            if let Err(e)= client.parse_contacts_code(code.clone()) {
                eprintln!("{e:?}");
            }
            else if let Err(e) = client.answer_contacts_code(code, None) {

                println!("name:{e:?}");
            }
        }
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
            // let my_id = client.get_local_id().unwrap();
            // warn!("myid: {my_id}");
            
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
        println!(".....");
    });
    
    Ok(())
}
