use std::sync::mpsc::sync_channel;
use criterion::{Criterion, criterion_group, criterion_main, black_box};
use once_cell::sync::Lazy;
use luffa_rpc_types::{Message, message_to};
use luffa_sdk::{Callback, Client};

pub static KEYS: Lazy<Vec<User>> = Lazy::new(|| {
    vec![
        User {
            mnemonic: "quick visa mad coyote amateur dirt idea wheel dune wash crew error"
                .to_string(),
            to: 10871006697545602478,
        },
        User {
            mnemonic: "hour upper shock ranch effort interest avocado carry travel soda rival that"
                .to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic:
            "hawk nasty wreck brisk target immune height december vault cliff tower merry"
                .to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "duck culture horror sausage jungle wait dirt elegant hold van learn match"
                .to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "suggest option city crucial maid catch win prevent bind thing disagree boil"
                .to_string(),
            to: 13473655988076347637,
        },
    ]
});

#[derive(Debug)]
pub struct User {
    pub mnemonic: String,
    pub to: u64,
}

fn bench_send_msg(client:Client, msg: Vec<u8>) {
    let crc = client.send_msg(10871006697545602478, msg).unwrap();
    println!("{crc}");
}

#[derive(Debug)]
struct Process {
    tx: std::sync::mpsc::SyncSender<(u64, u64, u64, Vec<u8>)>,
}

impl Process {
    pub fn new(tx: std::sync::mpsc::SyncSender<(u64, u64, u64, Vec<u8>)>) -> Self {
        Self { tx }
    }
}

impl Callback for Process {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, event_time: u64, msg: Vec<u8>) {
        self.tx.send((crc, from_id, to, msg)).unwrap();
    }
}


fn criterion_benchmark(c: &mut Criterion) {
    let idx = 0;
    let user = KEYS.get(idx).clone().unwrap();
    println!("{:?}", user);
    let client = Client::new();
    let (tx, rx) = sync_channel(1024);
    client.init(None).unwrap();
    let name = client.import_key(&user.mnemonic, "").unwrap().unwrap();
    client.save_key(&name).unwrap();
    let process = Process::new(tx.clone());
    let msg = Box::new(process);
    let my_id = client.start(Some(name), None, msg).unwrap();

    let msg = Message::Chat {
        content: luffa_rpc_types::ChatContent::Send {
            data: luffa_rpc_types::ContentData::Text {
                source: luffa_rpc_types::DataSource::Text {
                    content: "123".to_string(),
                },
                reference: None,
            },
        },
    };
    let msg = message_to(msg).unwrap();

    c.bench_function("send_msg", |b| b.iter(|| bench_send_msg(black_box(client.clone()), black_box(msg.clone()))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);