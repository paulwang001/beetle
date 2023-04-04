use std::fmt::{Debug, Formatter};
use luffa_sdk::{Callback, Client};

#[test]
fn test_remove_key() {
    let client = Client::new();

    client.init(None).expect("init failed");
    let Some(name) = client.gen_key("1212121", true).expect("gen_key_failed") else {
        panic!("missing name at gen key");
    };

    client.start(Some(name.clone()), None, Box::new(OnMessageImpl)).expect("start failed");

    std::thread::sleep(std::time::Duration::from_secs(5));

    client.remove_key(&name).expect("remove key failed");

    println!("删除成功");
}

#[test]
fn test_remove_sled() {
    let db = sled::open("/tmp/db").expect("open sled db failed");
    let tree = db.open_tree("cc").unwrap();
    tree.insert("a1", "xx").unwrap();
    tree.flush().unwrap();
    db.flush().unwrap();

    std::fs::remove_dir_all("/tmp/db").expect("remove db dir failed");

    tree.insert("a2", "xx").unwrap();

}

#[test]
fn test_remove_sled2() {
    let db = sled::open("/tmp/db").expect("open sled db failed");
    let tree = db.open_tree("cc").unwrap();
    tree.insert("a1", "xx").unwrap();
    tree.flush().unwrap();
    db.flush().unwrap();

    // std::fs::remove_dir_all("/tmp/db").expect("remove db dir failed");

    tree.insert("a2", "xx").unwrap();

}

#[derive(Debug)]
struct OnMessageImpl;

impl Callback for OnMessageImpl {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, event_time: u64, msg: Vec<u8>) {
        println!("{crc} {from_id} {to} {event_time} {}", msg.len());
    }
}