#![feature(poll_ready)]
use anyhow::Result;
use futures::pending;
use luffa_rpc_types::{Message, message_to, ContactsToken, ContactsEvent};
use luffa_sdk::{Callback, Client};
use tracing::log::warn;
use std::future::{Future, IntoFuture};
use std::sync::RwLock;
use std::sync::mpsc::sync_channel;
use std::task::Poll;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc, sync::Mutex};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::{info, error};


#[derive(Debug)]
struct Process {
   tx:std::sync::mpsc::SyncSender<(u64,u64,u64,Vec<u8>)>,
}

impl Process {
    pub fn new(tx:std::sync::mpsc::SyncSender<(u64,u64,u64,Vec<u8>)>,)->Self{
        Self { tx }
    }
}

impl Callback for Process {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, msg: Vec<u8>) {
        self.tx.send((crc,from_id,to,msg)).unwrap();
    }
}

fn main() -> Result<()> {
    let (tx, rx) = sync_channel(1024);

    let process = Process::new(tx);

    let msg = Box::new(process);
    let to_id = std::env::args().nth(1).unwrap_or_default();
    let to_id:u64 = to_id.parse().unwrap_or_default();
    // let msg_t = Arc::new(msg.clone());
    let client = Client::new();
    let cfg_path = std::env::args().nth(2);
    // let msg_t = msg.clone();
    println!("starting");
    client.start(cfg_path, msg);
    println!("started.");
    let client = Arc::new(client);
    let client_t = client.clone();
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        let peer_id = client.get_local_id();
        info!("peer id: {peer_id:?}");
        let peers = client.relay_list();
        // client.send_msg(to, msg)
        println!("{:?}", peers);
        if to_id > 0 {
            let code = client.show_code(Some("Hello".to_owned()));
            if let Ok((_,data)) = multibase::decode(code) {
                let msg:Message  = serde_cbor::from_slice(&data).unwrap();
                let msg = message_to(msg).unwrap();
                match client.send_msg(to_id, msg) {
                    Ok(_)=>{
                        info!("send seccess");
                    }
                    Err(e)=>{
                        error!("{e:?}");
                    }
                }
            }

        }
    });

    while let Ok((crc,from_id,to,data)) = rx.recv() {
        let msg:Message  = serde_cbor::from_slice(&data).unwrap();

        match &msg {
            Message::ContactsExchange { exchange }=>{
                match exchange {
                    ContactsEvent::Offer { token }=>{
                        if to_id == 0 {
                            let code = serde_cbor::to_vec(&msg).unwrap();
                            let code = multibase::encode(multibase::Base::Base64, code);
                            client_t.answer_contacts_code(code, Some("World".to_owned())).unwrap();
                        }
                    }
                    ContactsEvent::Answer { token }=>{
                        if to_id > 0 {
                           let msg = Message::Chat { 
                              content: luffa_rpc_types::ChatContent::Send { 
                                data: luffa_rpc_types::ContentData::Text { 
                                    source: luffa_rpc_types::DataSource::Text { content:"Ok".to_owned() }, reference: None } } } ;
                            let msg = message_to(msg).unwrap();        
                           client_t.send_msg(from_id, msg).unwrap();
                        }
                    }
                }
            }
            _=>{
                let list = client_t.contacts_list(0);
                println!("contacts>> {:?}",list);
                let list = client_t.session_list(10);
                println!(" session>> {:?}",list);
            }
            
        }
    }

    
    Ok(())
}
