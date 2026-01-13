import Config

# Centralized configuration for SpireDB
# Environment-specific configs are in dev.exs, test.exs, prod.exs

# PD (Placement Driver) configuration
config :spiredb_pd,
  num_regions: String.to_integer(System.get_env("SPIRE_NUM_REGIONS", "16")),
  start_raft: System.get_env("SPIRE_PD_START_RAFT", "true") == "true",
  heartbeat_interval: 10_000,
  raft_data_dir: System.get_env("SPIRE_PD_DATA_DIR", "/tmp/spiredb/pd")

# Ra (Raft consensus library) - required for Raft to work
config :ra,
  data_dir: String.to_charlist(System.get_env("SPIRE_RA_DATA_DIR", "/tmp/spiredb/ra"))

# Store configuration
config :spiredb_store,
  resp_port: String.to_integer(System.get_env("SPIRE_RESP_PORT", "6379")),
  resp_max_connections: String.to_integer(System.get_env("SPIRE_RESP_MAX_CONNECTIONS", "10000")),
  resp_connection_timeout:
    String.to_integer(System.get_env("SPIRE_RESP_CONNECTION_TIMEOUT", "60000")),
  rocksdb_path: System.get_env("SPIRE_ROCKSDB_PATH", "/tmp/spiredb/data"),
  rocksdb_max_open_files:
    String.to_integer(System.get_env("SPIRE_ROCKSDB_MAX_OPEN_FILES", "10000")),
  rocksdb_create_if_missing: true,
  raft_data_dir: System.get_env("SPIRE_RAFT_DATA_DIR", "/tmp/spiredb/regions"),
  raft_election_timeout: String.to_integer(System.get_env("SPIRE_RAFT_ELECTION_TIMEOUT", "1000")),
  raft_heartbeat_interval:
    String.to_integer(System.get_env("SPIRE_RAFT_HEARTBEAT_INTERVAL", "150")),
  raft_wal_max_size:
    String.to_integer(System.get_env("SPIRE_RAFT_WAL_MAX_SIZE", "#{64 * 1024 * 1024}"))

# Logger configuration
config :logger,
  level: String.to_existing_atom(System.get_env("SPIRE_LOG_LEVEL", "info")),
  backends: [:console]

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [
    :request_id,
    :region_id,
    :command,
    :duration_ms,
    :region,
    :start_key,
    :end_key,
    :failed,
    :store,
    :peer_id,
    :learner,
    :from,
    :to,
    :source,
    :target,
    :original,
    :new,
    :new_region_id,
    :split_key,
    :task_id,
    :task_epoch,
    :known_epoch,
    :old,
    :error,
    :results_count,
    :rows_count,
    :table,
    :pk,
    :batch_size,
    :reason,
    :columns,
    :key,
    :dead,
    :alive,
    :num_leaders,
    :avg_leaders,
    :transfers,
    :max,
    :min,
    :operations,
    :active,
    :results,
    :stores,
    :total_regions,
    :interval_seconds,
    :address
  ]

# Import environment specific config
import_config "#{config_env()}.exs"
