use chrono::Utc;
use luffa_sdk::{Callback, Client};

#[test]
fn test() -> anyhow::Result<()> {
    // step one new client
    let client = Client::new();

    // step two init client
    client.init(None)?;

    // step three gen key return short name
    let name = client.gen_key("", true)?;
    println!("short_name: {name:?}");

    // step four save short name
    client.save_key(name.as_ref().unwrap())?;

    // step five start
    client
        .start(name, None, Box::new(OnMessageImpl))
        .expect("start failed");

    let now = Utc::now().timestamp();
    println!("123");
    let name = client.get_current_user()?;
    println!("name1: {}", name.clone().unwrap());

    let u_id = client.get_local_id()?;
    println!("u_id {}", u_id.unwrap());
    client.remove_key(&name.clone().unwrap())?;
    client.stop()?;
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
