fn main() {
    protobuf_codegen_pure::Codegen::new()
        .out_dir("src/protocol")
        .inputs(&["protos/AdminProtocol.proto", "protos/mini_canal_packet.proto", "protos/mini_canal_entry.proto"])
        .include("protos")
        .run()
        .expect("Codegen failed.");
}