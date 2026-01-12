defmodule Store.Transaction.MetricsHandler do
  @moduledoc """
  Telemetry handler for transaction metrics.

  Aggregates metrics and exposes via Prometheus-compatible format.
  """

  use GenServer

  require Logger

  @metrics_table :txn_metrics

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Attach telemetry handlers.
  """
  def attach do
    events = [
      [:spiredb, :transaction, :begin],
      [:spiredb, :transaction, :commit],
      [:spiredb, :transaction, :abort],
      [:spiredb, :transaction, :conflict],
      [:spiredb, :transaction, :lock_wait],
      [:spiredb, :transaction, :async_commit],
      [:spiredb, :transaction, :deadlock]
    ]

    :telemetry.attach_many(
      "spiredb-txn-metrics",
      events,
      &handle_event/4,
      nil
    )
  end

  @doc """
  Get metrics in Prometheus format.
  """
  def prometheus_metrics do
    try do
      [
        format_counter(:spiredb_txn_commits_total, :commits),
        format_counter(:spiredb_txn_aborts_total, :aborts),
        format_counter(:spiredb_txn_conflicts_total, :conflicts),
        format_counter(:spiredb_txn_deadlocks_total, :deadlocks),
        format_gauge(:spiredb_txn_lock_wait_ms, :lock_wait)
      ]
      |> List.flatten()
      |> Enum.join("\n")
    rescue
      _ -> ""
    end
  end

  @doc """
  Get a specific metric value.
  """
  def get_metric(name, label \\ :total) do
    try do
      case :ets.lookup(@metrics_table, {name, label}) do
        [{_, value}] -> value
        [] -> 0
      end
    rescue
      _ -> 0
    end
  end

  ## Callbacks

  @impl true
  def init(_opts) do
    try do
      :ets.new(@metrics_table, [:named_table, :public, :set])
    rescue
      ArgumentError -> :ok
    end

    attach()
    Logger.info("Transaction MetricsHandler started")
    {:ok, %{}}
  end

  ## Telemetry Handlers

  def handle_event([:spiredb, :transaction, :commit], measurements, metadata, _config) do
    increment_counter(:commits, metadata[:isolation_level] || :unknown)
    increment_counter(:commits, :total)
    update_gauge(:commit_latency, metadata[:isolation_level], measurements[:duration_us] || 0)
  end

  def handle_event([:spiredb, :transaction, :abort], _measurements, metadata, _config) do
    increment_counter(:aborts, metadata[:reason] || :unknown)
    increment_counter(:aborts, :total)
  end

  def handle_event([:spiredb, :transaction, :conflict], _measurements, metadata, _config) do
    increment_counter(:conflicts, metadata[:conflict_type] || :unknown)
    increment_counter(:conflicts, :total)
  end

  def handle_event([:spiredb, :transaction, :lock_wait], measurements, _metadata, _config) do
    update_gauge(:lock_wait, :latest, measurements[:wait_time_ms] || 0)
  end

  def handle_event([:spiredb, :transaction, :deadlock], _measurements, _metadata, _config) do
    increment_counter(:deadlocks, :total)
  end

  def handle_event([:spiredb, :transaction, :begin], _measurements, _metadata, _config) do
    increment_counter(:begins, :total)
  end

  def handle_event([:spiredb, :transaction, :async_commit], _measurements, metadata, _config) do
    increment_counter(:async_commits, metadata[:phase] || :unknown)
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  @impl true
  def handle_cast({:counter, name, label}, state) do
    key = {name, label}

    try do
      :ets.update_counter(@metrics_table, key, 1, {key, 0})
    rescue
      _ -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:gauge, name, label, value}, state) do
    key = {name, label}

    try do
      :ets.insert(@metrics_table, {key, value})
    rescue
      _ -> :ok
    end

    {:noreply, state}
  end

  ## Private

  defp increment_counter(name, label) do
    key = {name, label}

    try do
      :ets.update_counter(@metrics_table, key, 1, {key, 0})
    rescue
      _ -> :ok
    end
  end

  defp update_gauge(name, label, value) do
    key = {name, label}

    try do
      :ets.insert(@metrics_table, {key, value})
    rescue
      _ -> :ok
    end
  end

  defp format_counter(metric_name, internal_name) do
    try do
      :ets.match(@metrics_table, {{internal_name, :"$1"}, :"$2"})
      |> Enum.map(fn [label, count] ->
        "#{metric_name}{label=\"#{label}\"} #{count}"
      end)
    rescue
      _ -> []
    end
  end

  defp format_gauge(metric_name, internal_name) do
    try do
      :ets.match(@metrics_table, {{internal_name, :"$1"}, :"$2"})
      |> Enum.map(fn [label, value] ->
        "#{metric_name}{label=\"#{label}\"} #{value}"
      end)
    rescue
      _ -> []
    end
  end
end
