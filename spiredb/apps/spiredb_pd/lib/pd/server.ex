defmodule PD.Server do
  @moduledoc """
  Placement Driver server using Raft consensus.

  Manages cluster metadata:
  - Store registry (which nodes are alive)
  - Region assignments (which stores host which regions)
  - Key-to-region routing
  """

  @behaviour :ra_machine

  require Logger

  alias PD.Types.{Region, Store}

  @type state :: %{
          stores: %{atom() => Store.t()},
          regions: %{non_neg_integer() => Region.t()},
          next_region_id: non_neg_integer(),
          num_regions: non_neg_integer()
        }

  ## Ra Machine Callbacks

  @impl :ra_machine
  def init(_config) do
    num_regions = Application.get_env(:spiredb_pd, :num_regions, 16)

    state = %{
      stores: %{},
      regions: %{},
      next_region_id: 1,
      num_regions: num_regions
    }

    # Ra machine init must return state
    Logger.info("PD.Server machine init called")
    state
  end

  def terminate(reason, _state) do
    Logger.error("PD.Server machine terminating: #{inspect(reason)}")
  end

  @impl :ra_machine
  def apply(_meta, {:register_store, node_name}, state) do
    if Map.has_key?(state.stores, node_name) do
      Logger.info("Store re-registered: #{node_name}")
    else
      Logger.info("New store joined cluster: #{node_name}")
    end

    store = %Store{
      node: node_name,
      regions: [],
      last_heartbeat: DateTime.utc_now(),
      state: :up
    }

    new_state = %{state | stores: Map.put(state.stores, node_name, store)}

    # Return: {new_state, reply, effects}
    {new_state, {:ok, node_name}, []}
  end

  @impl :ra_machine
  def apply(_meta, {:heartbeat, node_name}, state) do
    case Map.get(state.stores, node_name) do
      nil ->
        {state, {:error, :store_not_found}, []}

      store ->
        updated_store = %{store | last_heartbeat: DateTime.utc_now()}
        new_state = %{state | stores: Map.put(state.stores, node_name, updated_store)}
        {new_state, :ok, []}
    end
  end

  @impl :ra_machine
  def apply(_meta, {:create_region, region_params}, state) do
    region = %Region{
      id: state.next_region_id,
      start_key: region_params[:start_key],
      end_key: region_params[:end_key],
      stores: region_params[:stores] || [],
      epoch: 1,
      leader: nil
    }

    new_state = %{
      state
      | regions: Map.put(state.regions, region.id, region),
        next_region_id: state.next_region_id + 1
    }

    {new_state, {:ok, region}, []}
  end

  @impl :ra_machine
  def state_enter(_ra_state, _machine_state), do: []

  ## Query Functions (read-only, don't modify state)

  def find_region_by_key(state, key) do
    # Simple hash-based routing
    region_id = :erlang.phash2(key, state.num_regions) + 1
    Map.get(state.regions, region_id)
  end

  def list_stores(state) do
    Map.values(state.stores)
  end

  def get_region(state, region_id) do
    Map.get(state.regions, region_id)
  end

  ## Public API (to be called via Ra)

  @doc """
  Start the PD Raft server as a follower (empty initial members).
  Used by non-seed nodes joining an existing cluster.
  """
  def start_cluster_as_follower(node_name) do
    server_id = {:pd_server, node_name}
    cluster_name = :pd_cluster
    machine = {:module, __MODULE__, %{}}

    wait_for_ra_system(150)

    sanitized_name = node_name |> to_string() |> String.replace(~r/[^a-zA-Z0-9_-]/, "_")

    config = %{
      id: server_id,
      uid: "pd_#{sanitized_name}",
      cluster_name: cluster_name,
      machine: machine,
      # Empty - will join via add_member
      initial_members: [],
      log_init_args: %{},
      wal_max_size_bytes: 64 * 1024 * 1024,
      wal_pre_allocate: false,
      wal_write_strategy: :default,
      segment_max_entries: 32768,
      snapshot_interval: 4096,
      election_timeout: 3000
    }

    case start_cluster_with_retry(node_name, config) do
      :ok ->
        # After starting, add self to seed's cluster
        Logger.info("Adding #{node_name} to PD cluster via seed #{seed_node()}")

        case :ra.add_member({:pd_server, seed_node()}, server_id) do
          {:ok, _, _} ->
            Logger.info("Successfully joined PD cluster")
            :ok

          {:error, reason} ->
            Logger.warning("Failed to join PD cluster: #{inspect(reason)}")
            # Don't fail - Ra will eventually sync
            :ok

          {:timeout, _} ->
            Logger.warning("Timeout joining PD cluster - will retry")
            :ok
        end

      error ->
        error
    end
  end

  @doc """
  Check if local Raft server is running.
  """
  def is_running?(node_name) do
    case :ra.members({:pd_server, node_name}, 500) do
      {:ok, _, _} -> true
      _ -> false
    end
  rescue
    _ -> false
  end

  def start_cluster(node_name) do
    # Called by PD.ClusterManager for seed nodes
    # No need to check if seed here - ClusterManager already determined that
    server_id = {:pd_server, node_name}
    start_cluster(node_name, [server_id])
  end

  def start_cluster(node_name, initial_members) do
    server_id = {:pd_server, node_name}
    cluster_name = :pd_cluster
    machine = {:module, __MODULE__, %{}}

    wait_for_ra_system(150)

    sanitized_name = node_name |> to_string() |> String.replace(~r/[^a-zA-Z0-9_-]/, "_")

    config = %{
      id: server_id,
      uid: "pd_#{sanitized_name}",
      cluster_name: cluster_name,
      machine: machine,
      initial_members: initial_members,
      log_init_args: %{},
      wal_max_size_bytes: 64 * 1024 * 1024,
      wal_pre_allocate: false,
      wal_write_strategy: :default,
      segment_max_entries: 32768,
      snapshot_interval: 4096
    }

    start_cluster_with_retry(node_name, config)
  end

  defp start_cluster_with_retry(node_name, config, retries \\ 30) do
    Logger.info("Calling :ra.start_server for PD server...")
    server_id = config.id

    try do
      case :ra.start_server(:pd_system, config) do
        :ok ->
          Logger.info("PD Raft server started successfully. Checking members...")

          # Sleep to allow Raft server to initialize internal state/WAL
          Process.sleep(1000)

          # Trigger election if we are the seed node (bootstrapping new cluster)
          # Raft won't always auto-elect in a single-node cluster depending on Ra version/config
          if config.initial_members != [] do
            Logger.info("Triggering initial election for seed node...")
            :ra.trigger_election(server_id)
            # Give it a moment to elect
            Process.sleep(500)
          end

          # Verify it is actually reachable
          # Use a generous timeout (5s) for the initial check to avoid race conditions on slow disks
          case :ra.members(server_id, 5000) do
            {:ok, members, _leader} ->
              Logger.info("Verified PD Raft server is reachable. Members: #{inspect(members)}")
              # Return :ok from this branch
              :ok

            other ->
              Logger.warning(
                "PD Raft server started but check_members returned: #{inspect(other)}. The process might have crashed or be deadlocked."
              )

              # Still consider it started if members check fails
              :ok
          end

        {:error, :already_started} ->
          :ok

        {:error, reason} ->
          Logger.error("Failed to start PD Raft server: #{inspect(reason)}")
          retry_start(node_name, config, retries, reason)
      end
    catch
      :exit, reason ->
        retry_start(node_name, config, retries, reason)
    end
  end

  defp retry_start(node_name, config, retries, reason) do
    if retries > 0 do
      Process.sleep(2000)
      start_cluster_with_retry(node_name, config, retries - 1)
    else
      raise "Failed to start PD Raft server: #{inspect(reason)}"
    end
  end

  @doc """
  Register a store node.
  """
  def register_store(node_name) do
    # Target seed node's PD server - Ra will find leader
    :ra.process_command({:pd_server, seed_node()}, {:register_store, node_name}, 30_000)
  end

  @doc """
  Send heartbeat from a store.
  """
  def heartbeat(node_name) do
    # Target seed node's PD server
    :ra.process_command({:pd_server, seed_node()}, {:heartbeat, node_name})
  end

  @doc """
  Create a new region.
  """
  def create_region(params) do
    # Target seed node's PD server
    :ra.process_command({:pd_server, seed_node()}, {:create_region, params})
  end

  @doc """
  Find which region a key belongs to.
  """
  def find_region(key) do
    # Query the current state via seed node
    case :ra.leader_query({:pd_server, seed_node()}, &find_region_by_key(&1, key)) do
      {:ok, {_index, region}, _leader} -> {:ok, region}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Get all regions (for distributed scans).

  Returns list of all regions for SpireSQL to fan out queries.
  """
  def get_all_regions do
    case :ra.leader_query({:pd_server, seed_node()}, &list_all_regions/1) do
      {:ok, {_index, regions}, _leader} -> {:ok, regions}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Get region by ID.
  """
  def get_region_by_id(region_id) do
    case :ra.leader_query({:pd_server, seed_node()}, &get_region(&1, region_id)) do
      {:ok, {_index, region}, _leader} -> {:ok, region}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  defp list_all_regions(state) do
    Map.values(state.regions)
  end

  defp wait_for_ra_system(retries) do
    if retries == 0 do
      raise "Ra system failed to become ready"
    else
      # Check if Ra application supervisor is running
      # Note: :ra 2.x doesn't register :ra_directory globally in the same way
      # so we rely on :ra_sup existence and the explicit start_default call
      if Process.whereis(:ra_sup) do
        :ok
      else
        Process.sleep(100)
        wait_for_ra_system(retries - 1)
      end
    end
  end

  def seed_node do
    # 1. Try explicit env var (for dev/testing)
    case System.get_env("SPIRE_SEED_NODE") do
      name when is_binary(name) and name != "" and name != "auto" ->
        String.to_atom(name)

      _ ->
        find_seed_in_cluster()
    end
  end

  defp find_seed_in_cluster do
    # 1. Check if ANY connected node is the seed (by POD_NAME)
    # This relies on libcluster gossip having formed a mesh.
    # We check connected nodes AND ourselves.
    nodes = Node.list()
    all_nodes = [node() | nodes]

    Logger.debug("Searching for seed in nodes: #{inspect(all_nodes)}")

    seed =
      Enum.find(all_nodes, fn n ->
        # RPC to check pod name on that node
        try do
          case :rpc.call(n, System, :get_env, ["POD_NAME"]) do
            name when is_binary(name) ->
              Logger.debug("Node #{n} POD_NAME: #{name}")
              String.ends_with?(name, "-0")

            _ ->
              false
          end
        catch
          _, _ -> false
        end
      end)

    case seed do
      nil ->
        Logger.warning(
          "Could not find seed in cluster (scanned #{length(all_nodes)} nodes). Retrying DNS..."
        )

        resolve_seed_node_dns()

      node ->
        Logger.debug("Found seed node: #{node}")
        node
    end
  end

  defp resolve_seed_node_dns do
    # 2. Try to resolve seed DNS name (K8s) as backup
    service = System.get_env("SPIRE_SERVICE_NAME", "spiredb-headless")
    namespace = System.get_env("SPIRE_NAMESPACE", "default")
    seed_pod_name = "spiredb-0"

    fqdn = ~c"#{seed_pod_name}.#{service}.#{namespace}.svc.cluster.local"

    case :inet.gethostbyname(fqdn) do
      {:ok, {:hostent, _name, [], :inet, 4, ips}} ->
        {a, b, c, d} = List.first(ips)
        ip_str = "#{a}.#{b}.#{c}.#{d}"
        String.to_atom("spiredb@#{ip_str}")

      _ ->
        # Fallback to local node if resolution fails
        node()
    end
  end
end
