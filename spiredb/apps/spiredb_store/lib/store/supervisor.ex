defmodule Store.Supervisor do
  @moduledoc """
  Supervisor for Store components.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    db_path = Application.get_env(:spiredb_store, :rocksdb_path, "/var/lib/spiredb/data")

    children = [
      # Registry for region-level processes (ReadTsTracker, etc)
      {Registry, keys: :unique, name: Store.Region.Registry},

      # KV Engine (must start before Server)
      {Store.KV.Engine, [path: db_path, name: Store.KV.Engine]},

      # RocksDB Telemetry (after engine)
      {Store.Telemetry.RocksDB, []},

      # Plugin Registry
      {Store.Plugin.Registry, []},

      # Vector Index (for FT.* commands)
      {Store.VectorIndex, []},

      # gRPC Connection Pool (for internal transaction client)
      {Store.Transaction.ConnectionPool, []},

      # Transaction back-pressure (must start before Manager)
      {Store.Transaction.BackPressure, []},

      # Transaction metrics handler (attaches telemetry handlers)
      {Store.Transaction.MetricsHandler, []},

      # Lock Wait Queue (for distributed transaction deadlock detection)
      {Store.Transaction.LockWaitQueue, []},

      # Transaction Manager (for MULTI/EXEC)
      {Store.Transaction.Manager, []},

      # Transaction Reaper (timeout cleanup, orphaned lock cleanup)
      {Store.Transaction.Reaper, []},

      # CDC Change Stream (for realtime change capture)
      {Store.ChangeStream, []},

      # Stream Watermark tracking
      {Store.Stream.Watermark, []},

      # TTL background cleanup
      {Store.KV.TTLFilter, []},

      # Main store server (manages regions + KV engine)
      {Store.Server, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
