fn main() {
    uniffi::generate_scaffolding("./src/luffa_rpc_types.udl").unwrap();
}
