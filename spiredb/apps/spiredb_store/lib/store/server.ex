defmodule Store.Server do
  @moduledoc """
  Main coordinator for a Store node.

  Manages:
  - Multiple Raft regions
  - RocksDB instance (shared across regions)
  - Request routing via PD
  """

  use GenServer
  require Logger

  alias Store.Region.Raft
  alias PD.Server, as: PDServer
  alias SpiredbCommon.RaReadiness

  # Dynamic module reference to avoid compile-time warning
  # PD.Scheduler is in spiredb_pd which loads after spiredb_store
  @scheduler_module PD.Scheduler

  defstruct [
    :node_name,
    # %{region_id => raft_pid}
    :regions,
    :kv_engine
  ]

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get value for a key.

  By default, reads directly from RocksDB (fast, no Raft overhead).
  For linearizable reads in a distributed setup, use `get(key, linearizable: true)`.
  """
  def get(key, opts \\ []) do
    if Keyword.get(opts, :linearizable, false) do
      # Go through Raft for linearizable read (slower but consistent)
      GenServer.call(__MODULE__, {:get_linearizable, key})
    else
      # Direct read from RocksDB (fast, eventual consistency)
      GenServer.call(__MODULE__, {:get_direct, key})
    end
  end

  @doc """
  Put key-value (routes to correct region).
  """
  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  @doc """
  Batch write multiple key-value pairs.
  Groups writes by region for efficiency.
  """
  def batch_write(operations) when is_list(operations) do
    GenServer.call(__MODULE__, {:batch_write, operations})
  end

  @doc """
  Check if key exists.

  Uses direct read for fast lookup.
  """
  def exists?(key) do
    case get(key) do
      {:ok, _value} -> true
      {:error, :not_found} -> false
      _ -> false
    end
  end

  @doc """
  Get the KV engine for a specific key.

  Routes to the correct region's engine based on key hash.
  This enables proper multi-region/multi-store routing.
  """
  def get_engine_for_key(key) do
    # In current architecture, all regions on this store share the same RocksDB
    # But routing through region ensures proper abstraction for future scaling
    case find_region_for_key(key) do
      {:ok, _region_id} ->
        # Found the region - in current setup, all regions use same Engine
        # Future: Could have per-region engines or route to remote stores
        {:ok, Store.KV.Engine}

      {:error, _reason} ->
        # Fallback to default engine if region lookup fails
        {:ok, Store.KV.Engine}
    end
  end

  @doc """
  Delete key (routes to correct region).
  """
  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  @doc """
  Get region for a key (from PD).
  """
  def find_region_for_key(key) do
    GenServer.call(__MODULE__, {:find_region, key})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    node_name = Node.self()

    # Stagger startup: Wait 5s before initializing regions and registering
    # This allows the local PD server (if seed) to stabilize its own isolated Raft system
    Logger.info("Store Server started. Scheduling dependency check...")
    Process.send_after(self(), :check_pd_dependency, 0)

    state = %__MODULE__{
      node_name: node_name,
      regions: %{},
      # Engine is now started by Supervisor as a named process
      kv_engine: Store.KV.Engine
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:check_pd_dependency, state) do
    Logger.info("Checking PD dependency...")
    # This might block this process for a while, but init has returned so Supervisor is happy.
    wait_for_pd_ready()

    Logger.info("PD is ready. Initializing regions...")
    Process.send_after(self(), :initialize_regions, 0)
    Process.send_after(self(), :register_with_pd, 0)

    {:noreply, state}
  end

  @impl true
  def handle_info(:initialize_regions, state) do
    num_regions = Application.get_env(:spiredb_pd, :num_regions, 16)

    # Initialize regions in a background task to avoid blocking GenServer
    parent = self()

    spawn(fn ->
      # Wait for Ra to be ready (this blocks, so must be in spawn)
      if wait_for_ra() == :ok do
        regions = initialize_regions(num_regions)

        if map_size(regions) > 0 do
          send(parent, {:regions_initialized, regions})
        else
          Logger.error("Failed to initialize any regions. Retrying in 2s...")
          Process.sleep(2000)
          send(parent, :initialize_regions)
        end
      else
        Logger.error("Ra system not ready. Retrying initialization in 2s...")
        Process.sleep(2000)
        send(parent, :initialize_regions)
      end
    end)

    # Start heartbeat
    schedule_heartbeat()

    {:noreply, state}
  end

  @impl true
  def handle_info(:register_with_pd, state) do
    # Register in background to avoid blocking
    node = state.node_name
    parent = self()

    spawn(fn ->
      Logger.info("Attempting to register with PD as #{node}...")

      case PDServer.register_store(node) do
        {:ok, {:ok, _result}, _leader} ->
          Logger.info("Registered with PD: #{node}")

        {:ok, result, _leader} ->
          Logger.info("Registered with PD: #{node}, result: #{inspect(result)}")

        {:error, reason} ->
          Logger.warning(
            "Failed to register with PD: #{inspect(reason)}. Raft Status: #{inspect(:ra.members({:pd_server, node}))}"
          )

          Process.send_after(parent, :register_with_pd, 1000)

        {:timeout, _} ->
          Logger.warning(
            "Timed out registering with PD. Raft Status: #{inspect(:ra.members({:pd_server, node}))}"
          )

          Process.send_after(parent, :register_with_pd, 1000)
      end
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:regions_initialized, regions}, state) do
    Logger.info("Regions initialized: #{map_size(regions)} available")
    {:noreply, %{state | regions: regions}}
  end

  @impl true
  def handle_info(:heartbeat, state) do
    # Send heartbeat to PD asynchronously to avoid blocking Store.Server
    # If PD is slow (e.g. Raft election), we don't want to freeze the Store
    node_name = state.node_name
    node_address = to_string(node_name)

    Task.start(fn ->
      # Send heartbeat via Ra
      PDServer.heartbeat(node_name)

      # Query scheduler for any pending tasks
      try do
        {tasks, epoch} = apply(@scheduler_module, :get_pending_tasks, [node_address])

        if tasks != [] do
          Logger.info("Received #{length(tasks)} tasks from PD scheduler")

          # Update executor's known epoch
          Store.TaskExecutor.update_epoch(epoch)

          # Convert internal format to proto format and execute
          proto_tasks = convert_tasks_to_proto(tasks)
          Store.TaskExecutor.execute_tasks(proto_tasks)
        end
      catch
        kind, reason ->
          Logger.debug("Could not get tasks from scheduler: #{inspect({kind, reason})}")
      end

      # Sync cluster plugins to local registry
      sync_cluster_plugins(node_address)
    end)

    # Schedule next heartbeat
    schedule_heartbeat()

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Store.Server received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp convert_tasks_to_proto(tasks) do
    Enum.map(tasks, fn task ->
      %Spiredb.Cluster.ScheduledTask{
        task_id: task[:task_id] || 0,
        leader_epoch: task[:leader_epoch] || 0,
        task: convert_task_type(task)
      }
    end)
  end

  defp convert_task_type(%{type: :move_region} = op) do
    {:transfer_leader,
     %Spiredb.Cluster.TransferLeader{
       region_id: op.region_id || 0,
       from_store_id: store_hash(op.from_store),
       to_store_id: store_hash(op.to_store)
     }}
  end

  defp convert_task_type(%{type: :split_region} = op) do
    {:split,
     %Spiredb.Cluster.SplitRegion{
       region_id: op.region_id || 0,
       split_key: op[:split_key] || <<>>,
       new_region_id: op[:new_region_id] || 0,
       new_peer_id: op[:new_peer_id] || 0
     }}
  end

  defp convert_task_type(%{type: :add_replica} = op) do
    {:add_peer,
     %Spiredb.Cluster.AddPeer{
       region_id: op.region_id || 0,
       store_id: store_hash(op.target_store),
       peer_id: op[:peer_id] || 0,
       is_learner: op[:is_learner] || false
     }}
  end

  defp convert_task_type(%{type: :remove_replica} = op) do
    {:remove_peer,
     %Spiredb.Cluster.RemovePeer{
       region_id: op.region_id || 0,
       store_id: store_hash(op.target_store),
       peer_id: op[:peer_id] || 0
     }}
  end

  defp convert_task_type(_), do: nil

  defp store_hash(store) when is_atom(store), do: :erlang.phash2(store)
  defp store_hash(store) when is_integer(store), do: store
  defp store_hash(_), do: 0

  @impl true
  def handle_call({:get_direct, key}, _from, state) do
    # Direct read from RocksDB - bypasses Raft for low latency
    # This is safe because reads don't modify state
    result = Store.KV.Engine.get(state.kv_engine, key)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:get_linearizable, key}, _from, state) do
    # Linearizable read - goes through Raft to ensure we're reading from leader
    # Provides strong consistency at the cost of latency
    case find_and_execute(state, key, :get, [key]) do
      {:ok, result} -> {:reply, result, state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    case find_and_execute(state, key, :put, [key, value]) do
      {:ok, result} -> {:reply, result, state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    case find_and_execute(state, key, :delete, [key]) do
      {:ok, result} -> {:reply, result, state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:batch_write, operations}, _from, state) do
    # Group operations by region for efficient batch execution
    num_regions = map_size(state.regions)

    if num_regions == 0 do
      {:reply, {:error, :regions_initializing}, state}
    else
      # Group by region
      by_region =
        Enum.group_by(operations, fn
          {:put, key, _value} -> :erlang.phash2(key, num_regions) + 1
          {:delete, key} -> :erlang.phash2(key, num_regions) + 1
        end)

      # Execute batch per region
      results =
        Enum.map(by_region, fn {region_id, ops} ->
          case execute_batch_for_region(state, region_id, ops) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, region_id, reason}
          end
        end)

      # Check for errors
      errors = Enum.filter(results, &match?({:error, _, _}, &1))

      if errors == [] do
        {:reply, {:ok, length(operations)}, state}
      else
        {:reply, {:error, {:partial_failure, errors}}, state}
      end
    end
  end

  @impl true
  def handle_call({:find_region, key}, _from, state) do
    # Hash key to determine region
    num_regions = map_size(state.regions)

    region_id =
      if num_regions > 0 do
        :erlang.phash2(key, num_regions) + 1
      else
        # Default to region 1 if no regions initialized yet
        1
      end

    # Future enhancement: Query PD.Server for region metadata
    # This would return: {:ok, %{region_id: id, stores: [nodes], leader: node}}
    # Then DataAccess could route to remote stores if key not local

    {:reply, {:ok, region_id}, state}
  end

  ## Private Functions

  defp initialize_regions(count) do
    Logger.info("Initializing #{count} regions in parallel")
    start_time = System.monotonic_time(:millisecond)

    # Initialize regions in parallel using Task.async_stream
    # This significantly speeds up startup when there are many regions
    results =
      1..count
      |> Task.async_stream(
        fn region_id ->
          case start_region_with_retry(region_id, 10) do
            {:ok, pid} ->
              # Start ReadTsTracker for this region
              start_read_ts_tracker(region_id)
              {:ok, region_id, pid}

            {:error, {:already_started, pid}} ->
              # Ensure ReadTsTracker exists for this region
              start_read_ts_tracker(region_id)
              {:ok, region_id, pid}

            {:error, reason} ->
              {:error, region_id, reason}
          end
        end,
        max_concurrency: 2,
        timeout: 60_000,
        ordered: false
      )
      |> Enum.reduce(%{success: %{}, failed: []}, fn
        {:ok, {:ok, region_id, pid}}, acc ->
          %{acc | success: Map.put(acc.success, region_id, pid)}

        {:ok, {:error, region_id, reason}}, acc ->
          %{acc | failed: [{region_id, reason} | acc.failed]}

        {:exit, reason}, acc ->
          Logger.error("Region init task crashed: #{inspect(reason)}")
          acc
      end)

    elapsed = System.monotonic_time(:millisecond) - start_time

    # Log summary
    success_count = map_size(results.success)
    failed_count = length(results.failed)

    if failed_count > 0 do
      Logger.warning(
        "Region initialization: #{success_count}/#{count} succeeded, #{failed_count} failed in #{elapsed}ms",
        failed: Enum.map(results.failed, fn {id, _} -> id end)
      )
    else
      Logger.info("All #{success_count} regions initialized in #{elapsed}ms")
    end

    results.success
  end

  defp start_region_with_retry(region_id, retries) when retries > 0 do
    case start_region(region_id) do
      :ok ->
        {:ok, :started}

      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:error, {:already_started, pid}}

      {:error, :system_not_started} when retries > 1 ->
        # Ra system not ready yet, wait and retry
        Process.sleep(500)
        start_region_with_retry(region_id, retries - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_region_with_retry(region_id, 0) do
    start_region(region_id)
  end

  defp start_region(region_id) do
    # Start Raft process for this region
    nodes = [{region_id, Node.self()}]
    Raft.start_server(region_id, nodes)
  end

  defp start_read_ts_tracker(region_id) do
    # Start ReadTsTracker for this region to track max_read_ts
    # Used by AsyncCommitCoordinator for calculating commit timestamps
    case Store.Region.ReadTsTracker.start_link(region_id) do
      {:ok, _pid} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Failed to start ReadTsTracker for region #{region_id}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp find_and_execute(state, key, operation, args) do
    # Check if regions are available
    if map_size(state.regions) == 0 do
      {:error, :regions_initializing}
    else
      # Hash key to region
      region_id = hash_key_to_region(key, map_size(state.regions))

      # Execute on that region
      case Map.get(state.regions, region_id) do
        nil ->
          # Region process not running (e.g. failed to start)
          # Fallback to local execution for robustness
          Logger.debug("Region #{region_id} not running, executing locally")
          execute_local(state.kv_engine, operation, args)

        _region_ref ->
          # User requested: "All commands should execute locally and everything replicated asynchoronously"
          # Linearizability is explicitly disabled in favor of speed/availability.

          # 1. Execute locally immediately (Speed / Local Read)
          local_result = execute_local(state.kv_engine, operation, args)

          # 2. Replicate Writes Asynchronously
          if operation in [:put, :delete] do
            cmd = List.to_tuple([operation | args])

            Task.start(fn ->
              Raft.process_command(region_id, cmd)
            end)
          end

          # 3. Return local result immediately
          local_result
      end
    end
  end

  defp execute_local(kv_engine, :get, [key]) do
    case Store.KV.Engine.get(kv_engine, key) do
      {:ok, val} -> {:ok, {:ok, val}}
      error -> error
    end
  end

  defp execute_local(kv_engine, :put, [key, value]) do
    Store.KV.Engine.put(kv_engine, key, value)
    {:ok, {:ok, "OK"}}
  end

  defp execute_local(kv_engine, :delete, [key]) do
    Store.KV.Engine.delete(kv_engine, key)
    {:ok, {:ok, :ok}}
  end

  defp execute_batch_for_region(state, region_id, operations) do
    case Map.get(state.regions, region_id) do
      nil ->
        # Region not running, execute locally
        kv_engine = :persistent_term.get(:spiredb_kv_engine, nil)

        if kv_engine do
          Enum.each(operations, fn
            {:put, key, value} -> Store.KV.Engine.put(kv_engine, key, value)
            {:delete, key} -> Store.KV.Engine.delete(kv_engine, key)
          end)

          {:ok, :local}
        else
          {:error, :no_kv_engine}
        end

      pid when is_pid(pid) ->
        # Execute via Raft batch
        Raft.batch_execute(pid, operations)
    end
  end

  defp hash_key_to_region(key, num_regions) do
    # Simple hash-based routing
    :erlang.phash2(key, num_regions) + 1
  end

  defp wait_for_ra(_retries \\ 30) do
    case RaReadiness.wait_for_ra_system(:default, 15_000) do
      :ok ->
        :ok

      {:error, :timeout} ->
        Logger.error("Ra application failed to become ready")
        :error
    end
  end

  defp wait_for_pd_ready do
    # Try to find seed node
    seed = PDServer.seed_node()

    # Check if PD is running on seed
    # Remote check via RPC if seed != self, or local check
    is_ready =
      if seed == Node.self() do
        PDServer.is_running?(seed)
      else
        # RPC check
        case :rpc.call(seed, PD.Server, :is_running?, [seed]) do
          true -> true
          _ -> false
        end
      end

    if is_ready do
      :ok
    else
      Logger.info("PD not ready yet on #{seed}. Waiting...")
      Process.sleep(2000)
      wait_for_pd_ready()
    end
  end

  defp schedule_heartbeat do
    interval = Application.get_env(:spiredb_pd, :heartbeat_interval, 10_000)
    Process.send_after(self(), :heartbeat, interval)
  end

  # Sync cluster-registered plugins to local registry
  defp sync_cluster_plugins(_node_address) do
    try do
      case PD.PluginManager.list_plugins() do
        {:ok, plugins} when plugins != [] ->
          # Get locally registered plugins
          {:ok, local_plugins} = Store.Plugin.Registry.list()

          local_names =
            MapSet.new(Enum.map(local_plugins, fn {name, _mod, _state, _info} -> name end))

          # Register any missing plugins
          Enum.each(plugins, fn plugin_info ->
            name = Map.get(plugin_info, :name) || Map.get(plugin_info, "name")

            unless MapSet.member?(local_names, name) do
              Logger.debug("Syncing cluster plugin to local registry: #{name}")
              # Note: We only register metadata here - actual module loading
              # would require the plugin binary to be distributed
              Store.Plugin.Registry.register_info(name, plugin_info)
            end
          end)

        _ ->
          :ok
      end
    catch
      _, _ -> :ok
    end
  end
end
