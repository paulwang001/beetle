use luffa_sdk::bs58_decode;
#[test]
fn test_bs58() {
    let data = bs58_decode("g6ZRrKdDMgC").unwrap();
    println!("{}", data);
}