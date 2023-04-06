use chrono::Utc;
use luffa_sdk::{Callback, Client};

#[test]
fn test() -> anyhow::Result<()>{
    let client = Client::new();
    client.init(None)?;
    let name = client.gen_key("", true)?;
    println!("{}", name.clone().unwrap());
    client.start(name.clone(), None, Box::new(OnMessageImpl)).expect("start failed");
    let now = Utc::now().timestamp();
    println!("123");
    let name = client.get_current_user()?;

    println!("current_use{}", name.unwrap());

    // client.remove_key(&name.clone().unwrap())?;
    println!("{}", Utc::now().timestamp() - now);
    Ok(())
}

#[derive(Debug)]
struct OnMessageImpl;

impl Callback for OnMessageImpl {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, event_time: u64, msg: Vec<u8>) {
        println!("{crc} {from_id} {to} {event_time} {}", msg.len());
    }
}