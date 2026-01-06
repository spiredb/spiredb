defmodule Store.Telemetry.RocksDB do
  @moduledoc """
  Periodically exports RocksDB metrics via Telemetry.

  Emits metrics for:
  - Block cache hits/misses
  - Compaction statistics
  - Memtable usage
  - SST file counts
  - Write amplification
  """

  use GenServer
  require Logger

  @metrics_interval :timer.seconds(30)

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Get current metrics snapshot"
  def get_metrics(pid \\ __MODULE__) do
    GenServer.call(pid, :get_metrics)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    schedule_collection()
    {:ok, %{last_metrics: %{}}}
  end

  @impl true
  def handle_call(:get_metrics, _from, state) do
    metrics = collect_metrics()
    {:reply, {:ok, metrics}, %{state | last_metrics: metrics}}
  end

  @impl true
  def handle_info(:collect_metrics, state) do
    metrics = collect_metrics()
    emit_telemetry(metrics)
    schedule_collection()
    {:noreply, %{state | last_metrics: metrics}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("RocksDB Telemetry received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Private

  defp schedule_collection do
    Process.send_after(self(), :collect_metrics, @metrics_interval)
  end

  defp collect_metrics do
    case get_engine_stats() do
      {:ok, stats} ->
        parse_stats(stats)

      :error ->
        %{}
    end
  end

  defp get_engine_stats do
    try do
      case :persistent_term.get(:spiredb_rocksdb_ref, nil) do
        nil -> :error
        db_ref -> {:ok, :rocksdb.stats(db_ref)}
      end
    rescue
      _ -> :error
    catch
      _, _ -> :error
    end
  end

  defp parse_stats(stats) when is_list(stats) do
    stats
    |> Enum.reduce(%{}, fn
      {:stats_level, _}, acc -> acc
      {key, value}, acc when is_binary(key) and is_number(value) -> Map.put(acc, key, value)
      _, acc -> acc
    end)
    |> extract_key_metrics()
  end

  defp parse_stats(_), do: %{}

  defp extract_key_metrics(raw_stats) do
    %{
      # Block cache
      block_cache_hit: Map.get(raw_stats, "rocksdb.block.cache.hit", 0),
      block_cache_miss: Map.get(raw_stats, "rocksdb.block.cache.miss", 0),
      block_cache_bytes_read: Map.get(raw_stats, "rocksdb.block.cache.bytes.read", 0),

      # Compaction
      compact_read_bytes: Map.get(raw_stats, "rocksdb.compact.read.bytes", 0),
      compact_write_bytes: Map.get(raw_stats, "rocksdb.compact.write.bytes", 0),
      num_running_compactions: Map.get(raw_stats, "rocksdb.num-running-compactions", 0),

      # Memtable
      memtable_bytes: Map.get(raw_stats, "rocksdb.cur-size-all-mem-tables", 0),
      num_immutable_memtables: Map.get(raw_stats, "rocksdb.num-immutable-mem-table", 0),

      # SST Files
      num_files_at_level0: Map.get(raw_stats, "rocksdb.num-files-at-level0", 0),
      total_sst_files_size: Map.get(raw_stats, "rocksdb.total-sst-files-size", 0),

      # Writes
      bytes_written: Map.get(raw_stats, "rocksdb.bytes.written", 0),
      bytes_read: Map.get(raw_stats, "rocksdb.bytes.read", 0),
      num_keys_written: Map.get(raw_stats, "rocksdb.number.keys.written", 0),
      num_keys_read: Map.get(raw_stats, "rocksdb.number.keys.read", 0),

      # Flushes
      flush_write_bytes: Map.get(raw_stats, "rocksdb.flush.write.bytes", 0),
      num_running_flushes: Map.get(raw_stats, "rocksdb.num-running-flushes", 0)
    }
  end

  defp emit_telemetry(metrics) when map_size(metrics) > 0 do
    :telemetry.execute(
      [:spiredb, :rocksdb, :stats],
      metrics,
      %{node: Node.self()}
    )

    # Calculate and emit derived metrics
    hit_rate = calculate_hit_rate(metrics)

    :telemetry.execute(
      [:spiredb, :rocksdb, :cache],
      %{hit_rate: hit_rate},
      %{node: Node.self()}
    )
  end

  defp emit_telemetry(_), do: :ok

  defp calculate_hit_rate(%{block_cache_hit: hits, block_cache_miss: misses})
       when hits + misses > 0 do
    hits / (hits + misses) * 100
  end

  defp calculate_hit_rate(_), do: 0.0
end
