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

  alias PD.Scheduler.{LoadMonitor, BalancePlanner, Executor}

  @check_interval :timer.seconds(30)
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

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("PD Scheduler started", interval_seconds: @check_interval / 1000)
    schedule_next_check()

    {:ok,
     %{
       last_check: nil,
       active_operations: [],
       # Map of node -> :up | :down
       known_states: %{},
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

    {:noreply,
     %{
       new_state
       | last_check: DateTime.utc_now(),
         stats: updated_stats,
         known_states: new_known_states
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
      Logger.info("Executing plan",
        operations: length(plan.operations),
        reason: plan.reason
      )

      # Execute async
      Executor.execute_async(plan.operations, self())

      updated_stats = %{
        state.stats
        | total_operations: state.stats.total_operations + length(plan.operations)
      }

      %{state | active_operations: plan.operations, stats: updated_stats}
    end
  end
end
