use luffa_sdk::avatar_nickname::avatar::generate_avatar;
use luffa_sdk::avatar_nickname::nickname::generate_nickname;

#[test]
fn test_avatar_nickname() {
    let peer_id = "FzirHMQQmcPff5aAvSdQHu16eBPXXLgM89jcbwkarYUT";
    // let avatar = generate_avatar(peer_id);
    // println!("{avatar}");
    // let data:Vec<&str> = avatar.chars();
    // println!("{}", data.len());
    let nickname = generate_nickname(peer_id);
    println!("{nickname}");
}