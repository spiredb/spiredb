defmodule PD.Supervisor do
  @moduledoc """
  Supervisor for Placement Driver (PD) components.
  """

  use Supervisor
  require Logger

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    _node_name = Node.self()

    children =
      if Application.get_env(:spiredb_pd, :start_raft, true) do
        [
          # Start Cluster Manager which handles PD Server Lifecycle (Bootstrap vs Join)
          PD.ClusterManager,
          # Plugin Manager for cluster-wide plugin coordination
          PD.PluginManager
        ]
      else
        # Standalone mode - still run PluginManager
        [PD.PluginManager]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
