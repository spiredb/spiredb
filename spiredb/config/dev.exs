import Config

# Development environment configuration
config :spiredb_store,
  resp_port: 6379,
  resp_max_connections: 1000,
  resp_connection_timeout: 60_000,
  rocksdb_path: "/tmp/spiredb/dev/data",
  raft_data_dir: "/tmp/spiredb/dev/raft",
  vector_data_dir: "/tmp/spiredb/dev/vectors"

config :logger, level: :debug
