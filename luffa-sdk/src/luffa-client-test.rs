use std::io;
use std::io::BufRead;
use clap::Parser;
use futures::select;
use once_cell::sync::Lazy;
use luffa_sdk::{Callback, Client, ClientResult};

pub static KEYS: Lazy<Vec<User>> = Lazy::new(|| {
    vec![
        User{ mnemonic: "quick visa mad coyote amateur dirt idea wheel dune wash crew error".to_string(), to: 10871006697545602478 },
        User{ mnemonic: "hour upper shock ranch effort interest avocado carry travel soda rival that".to_string(), to: 13473655988076347637 }
    ]
});

pub struct User{
    pub mnemonic: String,
    pub to: u64,
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct TestArgs {
    #[clap(short, long)]
    idx: usize,
}

// 13473655988076347637
// 10871006697545602478
fn main() -> ClientResult<()> {
    let args = TestArgs::parse();
    let idx = args.idx;
    let user = KEYS.get(idx).clone().unwrap();


    let client = Client::new();
    client.init(None)?;
    let name = client.import_key(&user.mnemonic, "")?.unwrap();
    client.save_key(&name)?;
    let my_id = client.start(Some(name), None, Box::new(OnMessageImpl))?;
    // Read full lines from stdin
    println!("my_id: {my_id}");
    let code = client.gen_offer_code(my_id)?;
    println!("{code}");
    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();
    //
    loop {
        let line = stdin.next().unwrap()?;
        let line:Vec<&str> = line.split(" ").collect();
        if line.len() > 1 {
            tracing::error!("command: {:?} error", line);
            continue;
        }
        let command = *line.get(0).unwrap();
        let args;

        let args = *line.get(1).unwrap();
        println!("{:?}", line);

        let msg_id = client.send_msg(user.to, "123".as_bytes().to_vec())?;
        println!("msg_id: {msg_id}");
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