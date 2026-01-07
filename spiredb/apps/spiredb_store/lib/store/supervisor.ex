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
      # KV Engine (must start before Server)
      {Store.KV.Engine, [path: db_path, name: Store.KV.Engine]},

      # RocksDB Telemetry (after engine)
      {Store.Telemetry.RocksDB, []},

      # Plugin Registry
      {Store.Plugin.Registry, []},

      # Vector Index (for FT.* commands)
      {Store.VectorIndex, []},

      # Transaction Manager (for MULTI/EXEC)
      {Store.Transaction.Manager, []},

      # CDC Change Stream (for realtime change capture)
      {Store.ChangeStream, []},

      # Main store server (manages regions + KV engine)
      {Store.Server, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
