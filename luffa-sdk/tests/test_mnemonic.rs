use bip39::{Language, Mnemonic, MnemonicType};

#[test]
fn test() {
    let mnemonic = Mnemonic::new(MnemonicType::Words12, Language::English);
    println!("{}", mnemonic.phrase());
}