defmodule PD.Schema.StatsCollector do
  @moduledoc """
  Periodic collection of table statistics for query optimizer.
  """
  use GenServer
  require Logger
  alias PD.Schema.Registry

  # 5 minutes
  @collection_interval 300_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    Logger.info("StatsCollector started")
    schedule_collection()
    {:ok, %{}}
  end

  def handle_info(:collect_stats, state) do
    Logger.info("Collecting table statistics...")
    collect_all_stats()
    schedule_collection()
    {:noreply, state}
  end

  defp schedule_collection do
    Process.send_after(self(), :collect_stats, @collection_interval)
  end

  defp collect_all_stats do
    case Registry.list_tables() do
      {:ok, tables} ->
        Enum.each(tables, &collect_table_stats/1)

      error ->
        Logger.error("Failed to list tables for stats collection: #{inspect(error)}")
    end
  end

  defp collect_table_stats(table) do
    # In a real implementation, this would query store nodes
    # For now, we'll just log
    Logger.debug("Collecting stats for table #{table.name}")
  end
end
