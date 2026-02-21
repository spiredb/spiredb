import Config

# Test environment configuration
config :spiredb_store,
  resp_port: 6379,
  resp_max_connections: 100,
  resp_connection_timeout: 30_000,
  rocksdb_path: "/tmp/spiredb/test/data",
  raft_data_dir: "/tmp/spiredb/test/raft",
  vector_data_dir: "/tmp/spiredb/test/vectors"

# PD configuration - allow enabling Raft for integration tests
config :spiredb_pd,
  start_raft: System.get_env("SPIRE_PD_START_RAFT") == "true",
  num_regions: 1,
  disable_services: true

config :opentelemetry, traces_exporter: :none

# Configure Ra specifically for test environment
# config :ra,
#   data_dir: "test_data/ra"

# Logger level - can be overridden with SPIRE_LOG_LEVEL env var
config :logger,
  level: String.to_existing_atom(System.get_env("SPIRE_LOG_LEVEL", "warning"))
