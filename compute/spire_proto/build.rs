fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../spiredb/apps/spiredb_common/priv/proto";
    let protos = &[
        format!("{}/cluster.proto", proto_root),
        format!("{}/data_access.proto", proto_root),
        format!("{}/internal.proto", proto_root),
    ];

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(protos, &[proto_root.to_string()])?;

    Ok(())
}
