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

    regions =
      for id <- 1..num_regions, into: %{} do
        {id, %Region{id: id, leader: Node.self(), stores: [Node.self()]}}
      end

    state = %{
      stores: %{},
      regions: regions,
      next_region_id: num_regions + 1,
      num_regions: num_regions,
      plugins: %{}
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
  def apply(_meta, {:register_plugin, plugin_info}, state) do
    name = Map.get(plugin_info, :name) || Map.get(plugin_info, "name")
    plugins = Map.get(state, :plugins, %{})

    normalized = %{
      name: name,
      version: Map.get(plugin_info, :version) || Map.get(plugin_info, "version"),
      type: Map.get(plugin_info, :type) || Map.get(plugin_info, "type"),
      description: Map.get(plugin_info, :description) || Map.get(plugin_info, "description"),
      has_nif: Map.get(plugin_info, :has_nif) || Map.get(plugin_info, "has_nif", false),
      registered_at: DateTime.utc_now()
    }

    new_state = %{state | plugins: Map.put(plugins, name, normalized)}
    {new_state, {:ok, name}, []}
  end

  @impl :ra_machine
  def apply(_meta, {:unregister_plugin, plugin_name}, state) do
    plugins = Map.get(state, :plugins, %{})

    if Map.has_key?(plugins, plugin_name) do
      new_state = %{state | plugins: Map.delete(plugins, plugin_name)}
      {new_state, :ok, []}
    else
      {state, {:error, :not_found}, []}
    end
  end

  @impl :ra_machine
  def apply(_meta, {:deregister_store, node_name}, state) do
    case Map.pop(state.stores, node_name) do
      {nil, _} ->
        {state, {:error, :store_not_found}, []}

      {_store, new_stores} ->
        # Remove store from all region assignments
        new_regions =
          state.regions
          |> Enum.map(fn {id, region} ->
            updated_stores = Enum.reject(region.stores, &(&1 == node_name))

            new_leader =
              if region.leader == node_name, do: List.first(updated_stores), else: region.leader

            {id, %{region | stores: updated_stores, leader: new_leader}}
          end)
          |> Map.new()

        new_state = %{state | stores: new_stores, regions: new_regions}
        Logger.info("Store deregistered: #{node_name}")
        {new_state, :ok, []}
    end
  end

  @impl :ra_machine
  def apply(_meta, {:update_region_stores, region_id, stores}, state) do
    case Map.get(state.regions, region_id) do
      nil ->
        {state, {:error, :region_not_found}, []}

      region ->
        updated_region = %{region | stores: stores, epoch: region.epoch + 1}
        new_regions = Map.put(state.regions, region_id, updated_region)
        new_state = %{state | regions: new_regions}
        {new_state, :ok, []}
    end
  end

  @impl :ra_machine
  def state_enter(_ra_state, _machine_state), do: []

  ## Query Functions (read-only, don't modify state)

  def find_region_by_key(state, key) do
    # First try key range-based routing for regions with defined key ranges
    range_match =
      state.regions
      |> Map.values()
      |> Enum.find(fn region ->
        key_in_region?(key, region.start_key, region.end_key)
      end)

    case range_match do
      nil ->
        # Fallback to phash2 for uniform distribution
        region_id = :erlang.phash2(key, state.num_regions) + 1
        Map.get(state.regions, region_id)

      region ->
        region
    end
  end

  # Check if key falls within region's key range
  # start_key is inclusive, end_key is exclusive
  defp key_in_region?(key, start_key, end_key) do
    # If no key ranges defined, don't match (use hash fallback)
    cond do
      is_nil(start_key) and is_nil(end_key) ->
        false

      is_nil(start_key) ->
        # No start means region starts at beginning of keyspace
        key < end_key

      is_nil(end_key) ->
        # No end means region extends to end of keyspace
        key >= start_key

      true ->
        # Both bounds defined
        key >= start_key and key < end_key
    end
  end

  @doc """
  Get stores hosting a region, filtered by health status.
  Returns list of stores sorted by preference (leader first).
  """
  def get_region_stores(state, region_id) do
    case Map.get(state.regions, region_id) do
      nil ->
        []

      region ->
        # Get store health status
        stores_with_status =
          Enum.map(region.stores, fn store_node ->
            store = Map.get(state.stores, store_node)
            status = if store && store.state == :up, do: :up, else: :down
            {store_node, status}
          end)

        # Sort: leader first, then up stores, then down stores
        Enum.sort_by(stores_with_status, fn {node, status} ->
          {if(node == region.leader, do: 0, else: 1), if(status == :up, do: 0, else: 1)}
        end)
    end
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
    uid = "pd_#{sanitized_name}"

    config = %{
      id: server_id,
      uid: uid,
      cluster_name: cluster_name,
      machine: machine,
      # Empty - will join via add_member
      initial_members: [],
      # CRITICAL: log_init_args must contain uid for Ra recovery to work
      log_init_args: %{uid: uid},
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

    uid = "pd_#{sanitized_name}"

    config = %{
      id: server_id,
      uid: uid,
      cluster_name: cluster_name,
      machine: machine,
      initial_members: initial_members,
      # CRITICAL: log_init_args must contain uid for Ra recovery to work
      log_init_args: %{uid: uid},
      wal_max_size_bytes: 64 * 1024 * 1024,
      wal_pre_allocate: false,
      wal_write_strategy: :default,
      segment_max_entries: 32768,
      snapshot_interval: 4096
    }

    start_cluster_with_retry(node_name, config)
  end

  defp start_cluster_with_retry(node_name, config, retries \\ 30) do
    server_id = config.id

    # Check for IP/Node identity mismatch before starting
    # If we find existing Ra data for a different node identity, we must clean it up
    # because Ra will try to recover the old cluster state where this node (with new IP) is not a member.
    :ok = maybe_cleanup_stale_cluster(:pd_system, server_id)

    Logger.info("Calling :ra.start_server for PD server...")

    try do
      case :ra.start_server(:pd_system, config) do
        :ok ->
          Logger.info("PD Raft server started successfully. Checking members...")

          # Short wait for WAL init
          Process.sleep(200)

          # Trigger election if we are the seed node (bootstrapping new cluster)
          if config.initial_members != [] do
            Logger.info("Triggering initial election for seed node...")
            :ra.trigger_election(server_id)

            # Wait for this node to become leader before proceeding
            wait_for_leadership(server_id, 10)
          end

          # Verify it is actually reachable
          case :ra.members(server_id, 5000) do
            {:ok, members, leader} ->
              Logger.info(
                "PD Raft server ready. Members: #{inspect(members)}, Leader: #{inspect(leader)}"
              )

              :ok

            other ->
              Logger.warning(
                "PD Raft check_members returned: #{inspect(other)}. Proceeding anyway."
              )

              :ok
          end

        {:error, :already_started} ->
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          Logger.error("Failed to start PD Raft server: #{inspect(reason)}")
          # Force cleanup stale server and retry
          force_cleanup_and_retry(node_name, config, retries, reason)
      end
    catch
      :exit, reason ->
        # Raft server might be in bad state from previous run
        Logger.warning("PD Raft start caught exception: #{inspect(reason)}")
        force_cleanup_and_retry(node_name, config, retries, reason)
    end
  end

  defp force_cleanup_and_retry(node_name, config, retries, reason) do
    if retries > 0 do
      server_id = config.id

      # Try to force stop/delete any stale Ra server
      Logger.info("Attempting to cleanup stale Ra server before retry...")

      try do
        :ra.force_delete_server(:pd_system, server_id)
        Logger.info("Force deleted stale Ra server")
      catch
        _, err ->
          Logger.debug("Force delete returned: #{inspect(err)}")
      end

      Process.sleep(500)
      start_cluster_with_retry(node_name, config, retries - 1)
    else
      raise "Failed to start PD Raft server after retries: #{inspect(reason)}"
    end
  end

  defp maybe_cleanup_stale_cluster(system, current_server_id) do
    # Check existing registrations for this system
    case :ra_directory.list_registered(system) do
      registered when is_list(registered) ->
        current_node = Node.self()

        Enum.each(registered, fn {server_id, _uid} ->
          # Check if the registered server ID belongs to a different node identity
          # PD server ID format is {:pd_server, node_name}
          should_delete? =
            case server_id do
              {:pd_server, node} when node != current_node ->
                true

              # Also handle case where we might have a mismatch but different format?
              # For PD, we expect singleton, so if found ID != current ID, it's stale?
              # But cluster might have valid peers.
              # However, we are checking LOCAL registrations.
              # Any local registration for a different node name is stale data from previous boot.
              id when id != current_server_id ->
                # Be careful: if we have multiple local servers (unlikely for PD system),
                # we only want to delete if it implies a node identity mismatch.
                # Inspecting the ID to see if it contains a node atom that is not us.
                inspect(id) |> String.contains?(inspect(Node.self())) == false

              _ ->
                false
            end

          if should_delete? do
            Logger.warning(
              "Node identity changed! Found stale PD server: #{inspect(server_id)}. Force deleting."
            )

            :ra.force_delete_server(system, server_id)
          end
        end)

        :ok

      _ ->
        :ok
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
  Deregister a store node (graceful removal).
  Removes the store from cluster metadata and all region assignments.
  """
  def deregister_store(node_name) do
    :ra.process_command({:pd_server, seed_node()}, {:deregister_store, node_name}, 30_000)
  end

  @doc """
  Update the store list for a region.
  Used for replica management (add/remove replicas).
  """
  def update_region_stores(region_id, stores) do
    :ra.process_command({:pd_server, seed_node()}, {:update_region_stores, region_id, stores})
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

  @doc """
  Get all registered stores.
  """
  def get_all_stores do
    case :ra.leader_query({:pd_server, seed_node()}, &list_all_stores/1) do
      {:ok, {_index, stores}, _leader} -> {:ok, stores}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Get store by ID (phash2 of node name).
  """
  def get_store_by_id(store_id) do
    case :ra.leader_query({:pd_server, seed_node()}, &find_store_by_id(&1, store_id)) do
      {:ok, {_index, store}, _leader} -> {:ok, store}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  defp list_all_stores(state) do
    Map.values(state.stores)
  end

  defp find_store_by_id(state, store_id) do
    # store_id is phash2 of node name
    Enum.find(state.stores, fn {node, _store} ->
      :erlang.phash2(node) == store_id
    end)
    |> case do
      {_node, store} -> store
      nil -> nil
    end
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

  defp wait_for_leadership(_server_id, retries) when retries <= 0 do
    Logger.warning("Seed node did not become leader after max retries")
    :timeout
  end

  defp wait_for_leadership(server_id, retries) do
    case :ra.members(server_id, 2000) do
      {:ok, _members, ^server_id} ->
        Logger.info("Seed node became leader")
        :ok

      {:ok, _members, leader} when is_tuple(leader) ->
        # Another node is leader - that's fine in multi-node bootstrap
        Logger.info("Leader is #{inspect(leader)}")
        :ok

      {:ok, _members, :undefined} ->
        # No leader yet, retry
        Process.sleep(500)
        :ra.trigger_election(server_id)
        wait_for_leadership(server_id, retries - 1)

      {:timeout, _} ->
        Process.sleep(500)
        :ra.trigger_election(server_id)
        wait_for_leadership(server_id, retries - 1)

      other ->
        Logger.debug("wait_for_leadership got: #{inspect(other)}")
        Process.sleep(500)
        wait_for_leadership(server_id, retries - 1)
    end
  end

  def seed_node do
    # 1. Try explicit env var (for dev/testing)
    case System.get_env("SPIRE_SEED_NODE") do
      name when is_binary(name) and name != "" and name != "auto" ->
        String.to_atom(name)

      _ ->
        # 2. Check discovery mode to decide strategy
        case System.get_env("SPIRE_DISCOVERY", "epmd") do
          "epmd" ->
            # Single node or static list.
            # If explicit list is provided, take the first one.
            case System.get_env("SPIRE_CLUSTER_NODES") do
              list when is_binary(list) and list != "" ->
                list
                |> String.split(",", trim: true)
                |> List.first()
                |> String.to_atom()

              _ ->
                # Fallback to local node silently (single node mode)
                node()
            end

          _ ->
            # k8sdns, gossip -> search via POD_NAME + DNS fallback
            find_seed_in_cluster()
        end
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
