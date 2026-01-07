defmodule Store.Region.Raft do
  @moduledoc """
  Wrapper around :ra library for Raft consensus.
  """

  require Logger

  alias Store.Region.StateMachine

  # Starts a Raft server for a specific region
  def start_server(region_id, nodes, system_name \\ :default) do
    # Server ID is usually {atom_name, node_name}
    server_id = server_id(region_id)
    cluster_name = cluster_name(region_id)

    # Machine configuration
    machine = {:module, StateMachine, %{}}

    # Raft configuration (default for now)
    # We need to ensure we persist to the correct directory
    # ra uses the current directory + /ra_data by default or configured global
    # We can pass data_dir in start options or rely on global config.

    # Start the server
    # :ra.start_server(server_id, cluster_name, machine, nodes)
    # The signature is start_server(ServerId, ClusterId, MachineConf, Members)
    # But checking newer ra versions, it might be start_or_restart_server or distinct.
    # Looking at hex docs (assuming standard usage):
    # start_server(ServerId, ClusterName, MachineConf, Members)

    Logger.info("Starting Raft server for region #{region_id} at #{inspect(server_id)}")

    # Generate a UID (simple random/time based for now, standard chars)
    uid = :erlang.phash2({server_id, System.system_time()}) |> Integer.to_string()

    # Extract server IDs from nodes list for initial_members
    # nodes is [{region_id, node}, ...]
    # We need to convert them to proper Ra server IDs: {name, node}
    member_ids =
      Enum.map(nodes, fn {r_id, n} ->
        {String.to_atom("region_#{r_id}"), n}
      end)

    # Get Raft configuration with production defaults
    election_timeout = Application.get_env(:spiredb_store, :raft_election_timeout, 1000)
    heartbeat_interval = Application.get_env(:spiredb_store, :raft_heartbeat_interval, 150)

    # WAL configuration for durability
    wal_max_size = Application.get_env(:spiredb_store, :raft_wal_max_size, 64 * 1024 * 1024)
    snapshot_interval = Application.get_env(:spiredb_store, :raft_snapshot_interval, 4096)

    config = %{
      id: server_id,
      uid: uid,
      cluster_name: cluster_name,
      machine: machine,
      initial_members: member_ids,
      log_init_args: %{},
      # WAL settings - pre-allocate for better write performance
      wal_max_size_bytes: wal_max_size,
      wal_pre_allocate: false,
      wal_write_strategy: :default,
      # Segment settings for efficient log management
      segment_max_entries: 32768,
      # Snapshot more frequently to reduce recovery time
      snapshot_interval: snapshot_interval,
      # Raft timing
      election_timeout: election_timeout,
      heartbeat_timeout: heartbeat_interval
    }

    Logger.debug(
      "Raft config: election_timeout=#{election_timeout}ms, heartbeat=#{heartbeat_interval}ms"
    )

    Logger.info("Starting :ra.start_server with config: #{inspect(config)}")

    :ra.start_server(system_name, config)
  end

  def process_command(region_id, command, timeout \\ 3000) do
    server = server_id(region_id)
    :ra.process_command(server, command, timeout)
  end

  @doc """
  Execute a batch of operations atomically via Raft.
  """
  def batch_execute(pid, operations, timeout \\ 5000) when is_pid(pid) do
    # Convert pid to server_id by extracting region from registered name
    # For batch, we use the {:batch, ops} command
    try do
      :ra.process_command(pid, {:batch, operations}, timeout)
    catch
      :exit, reason ->
        {:error, reason}
    end
  end

  def server_id(region_id) do
    {String.to_atom("region_#{region_id}"), node()}
  end

  def cluster_name(region_id) do
    String.to_atom("region_#{region_id}_cluster")
  end
end
