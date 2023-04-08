use luffa_sdk::{Callback, Client};

#[test]
fn test_import_key() -> anyhow::Result<()> {
    for i in 0..10 {
        let client = Client::new();
        client.init(None)?;

        let phrase =
            "pony obtain used century taste ceiling second hour already anger inform cereal";
        let password = "";
        let name = client.import_key(phrase, password)?;
        tracing::error!("{name:?}");
        // step four save short name
        client.save_key(name.as_ref().unwrap())?;

        // step five start
        client
            .start(name.clone(), None, Box::new(OnMessageImpl))
            .expect("start failed");

        client.remove_key(name.as_ref().unwrap())?;
        client.stop()?;
    }
    Ok(())
}
#[derive(Debug)]
struct OnMessageImpl;

impl Callback for OnMessageImpl {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, event_time: u64, msg: Vec<u8>) {
        println!("{crc} {from_id} {to} {event_time} {}", msg.len());
    }
}
