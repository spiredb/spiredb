import Config

# Configure RA data directory from Environment
# This is critical for persistence and performance (avoiding /tmp)
# CRITICAL: Create node directory BEFORE Ra application starts (DETS needs it)
ra_base_dir = System.get_env("SPIRE_RA_DATA_DIR", "/tmp/spiredb/ra")
node_name = to_string(node())

# Ra appends node name to data_dir, so pre-create that directory
ra_node_dir = Path.join(ra_base_dir, node_name)
File.mkdir_p!(ra_node_dir)

config :ra, data_dir: String.to_charlist(ra_base_dir)

# Ensure other runtime configs are picked up if needed
# (e.g. Logger level)
if log_level = System.get_env("SPIRE_LOG_LEVEL") do
  level =
    case log_level do
      "debug" -> :debug
      "info" -> :info
      "warn" -> :warning
      "error" -> :error
      _ -> :info
    end

  config :logger, level: level
end

if config_env() == :prod do
  config :libcluster,
    topologies: [
      k8s_dns: [
        strategy: Cluster.Strategy.Kubernetes.DNS,
        config: [
          service: System.get_env("SPIRE_HEADLESS_SERVICE", "spiredb-headless"),
          application_name: "spiredb",
          namespace: System.get_env("POD_NAMESPACE", "spire"),
          polling_interval: 5_000
        ]
      ]
    ]
end
