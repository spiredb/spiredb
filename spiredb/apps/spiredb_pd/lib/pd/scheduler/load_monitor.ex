defmodule PD.Scheduler.LoadMonitor do
  @moduledoc """
  Monitors cluster load and capacity.

  Collects metrics from all stores:
  - Region count per store
  - Store capacity (disk, memory)
  - Heartbeat timestamps
  - Store state (alive/dead)
  """

  require Logger

  @doc """
  Collect current cluster metrics.

  Returns a map with:
  - stores: list of store metrics
  - timestamp: when metrics were collected
  - total_regions: total number of regions across all stores
  """
  def collect_metrics do
    # Query PD.Server state via Ra to get actual store data
    case query_pd_state() do
      {:ok, state} ->
        stores = Map.values(state.stores)
        store_metrics = Enum.map(stores, &compute_store_metrics/1)

        %{
          stores: store_metrics,
          timestamp: DateTime.utc_now(),
          total_regions: count_total_regions(store_metrics)
        }

      {:error, _reason} ->
        # Fallback to empty if query fails (e.g., Raft not running in tests)
        Logger.debug("PD state query failed, returning empty metrics")

        %{
          stores: [],
          timestamp: DateTime.utc_now(),
          total_regions: 0
        }
    end
  end

  defp query_pd_state do
    # Query Ra state machine for PD.Server state
    # Ra servers are identified as {:pd_server, node_name}
    server_id = {:pd_server, Node.self()}

    # Use ra:consistent_query to read the state
    query_fun = fn state ->
      # Return the full state so we can access stores
      state
    end

    case :ra.consistent_query(server_id, query_fun) do
      {:ok, state, _leader} ->
        {:ok, state}

      {:error, reason} ->
        {:error, reason}

      {:timeout, _} ->
        {:error, :timeout}
    end
  end

  defp compute_store_metrics(store) do
    regions = store.regions || []

    %{
      node: store.node,
      region_count: length(regions),
      regions: regions,
      last_heartbeat: store.last_heartbeat,
      state: store.state,
      # Simple capacity metric: region count
      # Future: weighted by region size, disk usage, etc.
      capacity_used: length(regions),
      is_alive: is_store_alive?(store)
    }
  end

  defp is_store_alive?(store) do
    # Consider store alive if heartbeat within last 60 seconds
    case store.last_heartbeat do
      nil ->
        false

      timestamp ->
        age_seconds = DateTime.diff(DateTime.utc_now(), timestamp, :second)
        age_seconds < 60
    end
  end

  defp count_total_regions(store_metrics) do
    Enum.reduce(store_metrics, 0, fn store, acc ->
      acc + store.region_count
    end)
  end
end
