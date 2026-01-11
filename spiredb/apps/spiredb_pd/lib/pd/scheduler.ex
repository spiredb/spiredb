defmodule PD.Scheduler do
  @moduledoc """
  Automated cluster rebalancing scheduler.

  Runs every 30 seconds to:
  1. Collect cluster metrics
  2. Compute rebalancing plan
  3. Execute operations (rate-limited)

  The scheduler ensures regions are evenly distributed across stores
  and handles failure recovery.
  """

  use GenServer
  require Logger

  alias PD.Scheduler.{LoadMonitor, BalancePlanner}

  @check_interval :timer.seconds(30)
  @startup_delay :timer.seconds(1)
  @max_startup_retries 30
  @max_concurrent_operations 2

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Get scheduler statistics"
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc "Trigger immediate check (for testing)"
  def check_now do
    send(__MODULE__, :check_cluster)
    :ok
  end

  @doc "Get pending tasks for a store and clear them"
  def get_pending_tasks(store_address) do
    GenServer.call(__MODULE__, {:get_pending_tasks, store_address})
  end

  @doc "Get the current leader epoch (Raft term)"
  def get_leader_epoch do
    GenServer.call(__MODULE__, :get_leader_epoch)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("PD Scheduler started", interval_seconds: @check_interval / 1000)
    # Wait for PD.Server before starting checks
    send(self(), :wait_for_pd)

    {:ok,
     %{
       last_check: nil,
       active_operations: [],
       # Map of node -> :up | :down
       known_states: %{},
       # Map of store_address -> [tasks]
       pending_tasks: %{},
       # Next task ID
       next_task_id: 1,
       # Epoch initialized lazily on first check
       leader_epoch: 0,
       # Startup retry counter
       startup_retries: 0,
       stats: %{
         total_checks: 0,
         total_operations: 0,
         successful_operations: 0,
         failed_operations: 0
       }
     }}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    {:reply, state.stats, state}
  end

  @impl true
  def handle_call({:get_pending_tasks, store_address}, _from, state) do
    tasks = Map.get(state.pending_tasks, store_address, [])
    new_pending = Map.delete(state.pending_tasks, store_address)
    {:reply, {tasks, state.leader_epoch}, %{state | pending_tasks: new_pending}}
  end

  @impl true
  def handle_call(:get_leader_epoch, _from, state) do
    {:reply, state.leader_epoch, state}
  end

  @impl true
  def handle_info(:wait_for_pd, state) do
    if PD.Server.is_running?(Node.self()) do
      Logger.info("PD.Server ready, starting scheduler checks")
      schedule_first_check()
      {:noreply, state}
    else
      retries = state.startup_retries

      if retries < @max_startup_retries do
        Logger.debug("Waiting for PD.Server (attempt #{retries + 1}/#{@max_startup_retries})")
        Process.send_after(self(), :wait_for_pd, @startup_delay)
        {:noreply, %{state | startup_retries: retries + 1}}
      else
        Logger.error("PD.Server not ready after #{@max_startup_retries} retries, starting anyway")
        schedule_first_check()
        {:noreply, state}
      end
    end
  end

  @impl true
  def handle_info(:check_cluster, state) do
    Logger.debug("Scheduler check cycle starting")

    # Step 1: Collect metrics
    metrics = LoadMonitor.collect_metrics()

    # Log state transitions
    new_known_states = log_node_state_changes(metrics.stores, state.known_states || %{})

    Logger.debug("Collected metrics",
      stores: length(metrics.stores),
      total_regions: metrics.total_regions
    )

    # Step 2: Compute rebalancing plan
    plan = BalancePlanner.compute_plan(metrics, state.active_operations)

    # Step 3: Execute operations (if any and not at limit)
    new_state = maybe_execute_plan(plan, state)

    # Schedule next check
    schedule_next_check()

    updated_stats = %{
      new_state.stats
      | total_checks: new_state.stats.total_checks + 1
    }

    # Update epoch lazily (first check or if it changed)
    current_epoch = get_ra_term()

    {:noreply,
     %{
       new_state
       | last_check: DateTime.utc_now(),
         stats: updated_stats,
         known_states: new_known_states,
         leader_epoch: current_epoch
     }}
  end

  @impl true
  def handle_info({:operations_complete, results}, state) do
    Logger.info("Operations completed", results: length(results))

    # Count successes and failures
    {successes, failures} =
      Enum.split_with(results, fn
        {:ok, _} -> true
        _ -> false
      end)

    updated_stats = %{
      state.stats
      | successful_operations: state.stats.successful_operations + length(successes),
        failed_operations: state.stats.failed_operations + length(failures)
    }

    # Clear active operations
    {:noreply, %{state | active_operations: [], stats: updated_stats}}
  end

  defp log_node_state_changes(stores, known_states) do
    Enum.reduce(stores, known_states, fn store, acc ->
      current_status = if store.is_alive, do: :up, else: :down
      previous_status = Map.get(acc, store.node)

      case {previous_status, current_status} do
        {nil, :up} ->
          Logger.info("Store discovered UP: #{store.node}")

        {nil, :down} ->
          Logger.warning("Store discovered DOWN: #{store.node}")

        {:up, :down} ->
          Logger.warning("Store detected DOWN (missing heartbeats): #{store.node}")

        {:down, :up} ->
          Logger.info("Store recovered (UP): #{store.node}")

        _ ->
          # No change
          :ok
      end

      Map.put(acc, store.node, current_status)
    end)
  end

  ## Private Functions

  defp schedule_first_check do
    # Delay first check to give PD.Server time to start
    Process.send_after(self(), :check_cluster, @startup_delay)
  end

  defp schedule_next_check do
    Process.send_after(self(), :check_cluster, @check_interval)
  end

  defp maybe_execute_plan(%{operations: []}, state) do
    # No operations needed
    state
  end

  defp maybe_execute_plan(plan, state) do
    if length(state.active_operations) >= @max_concurrent_operations do
      Logger.debug("Skipping execution - at max concurrent operations",
        active: length(state.active_operations),
        max: @max_concurrent_operations
      )

      state
    else
      Logger.info("Queueing tasks for heartbeat delivery",
        operations: length(plan.operations),
        reason: plan.reason
      )

      # Queue tasks for delivery via heartbeat responses
      # Group operations by target store
      new_pending = queue_operations_by_store(plan.operations, state.pending_tasks, state)

      updated_stats = %{
        state.stats
        | total_operations: state.stats.total_operations + length(plan.operations)
      }

      %{state | pending_tasks: new_pending, active_operations: [], stats: updated_stats}
    end
  end

  defp queue_operations_by_store(operations, pending_tasks, state) do
    Enum.reduce(operations, pending_tasks, fn op, acc ->
      # Determine target store for this operation
      target_store = get_target_store(op)

      # Add task metadata
      task_with_meta =
        Map.merge(op, %{
          task_id: state.next_task_id,
          leader_epoch: state.leader_epoch
        })

      # Append to store's pending list
      store_key = to_string(target_store)
      existing = Map.get(acc, store_key, [])
      Map.put(acc, store_key, existing ++ [task_with_meta])
    end)
  end

  defp get_target_store(%{to_store: store}), do: store
  defp get_target_store(%{target_store: store}), do: store
  defp get_target_store(%{from_store: store}), do: store
  defp get_target_store(_), do: Node.self()

  defp get_ra_term do
    # Get current Raft term from Ra
    # This represents the leader epoch
    case :ra.members({:pd_server, Node.self()}, 1000) do
      {:ok, _members, leader} when is_tuple(leader) ->
        # Ra returns leader as {name, node}
        # For now, use a simple incrementing epoch based on time
        # In production, this should come from :ra.overview or similar
        System.monotonic_time(:second)

      _ ->
        # Fallback if Ra not available
        0
    end
  end
end
