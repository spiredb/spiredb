defmodule Store.KV.Metrics do
  @moduledoc """
  RocksDB metrics exporter for Prometheus.

  Collects statistics from RocksDB Engine and formats them
  for Prometheus scraping.
  """

  require Logger
  alias Store.KV.Engine

  @doc """
  Get RocksDB metrics in Prometheus text format.
  """
  def prometheus_metrics do
    case get_rocksdb_stats() do
      {:ok, stats} when is_binary(stats) ->
        format_prometheus(stats)

      {:ok, stats} when is_list(stats) ->
        # Handle keyword list format from :rocksdb.statistics_info
        format_prometheus_from_list(stats)

      {:error, _} ->
        "# RocksDB statistics not available\n"

      other ->
        Logger.debug("Unexpected stats format: #{inspect(other)}")
        "# RocksDB statistics format unknown\n"
    end
  end

  @doc """
  Get RocksDB metrics as a map.
  """
  def get_metrics do
    case get_rocksdb_stats() do
      {:ok, stats} -> {:ok, parse_stats(stats)}
      error -> error
    end
  end

  ## Private

  defp get_rocksdb_stats do
    try do
      case Process.whereis(Engine) do
        nil -> {:error, :engine_not_running}
        _pid -> Engine.get_stats(Engine)
      end
    catch
      :exit, _ -> {:error, :engine_not_responding}
    end
  end

  defp parse_stats(stats) when is_binary(stats) do
    # RocksDB statistics format is "key : value\n"
    stats
    |> String.split("\n")
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, " : ", parts: 2) do
        [key, value] ->
          key = String.trim(key) |> String.downcase() |> String.replace(" ", "_")
          value = parse_value(String.trim(value))
          Map.put(acc, key, value)

        _ ->
          acc
      end
    end)
  end

  defp parse_stats(_), do: %{}

  defp parse_value(str) do
    case Float.parse(str) do
      {num, ""} ->
        num

      {num, _} ->
        num

      :error ->
        case Integer.parse(str) do
          {num, ""} -> num
          {num, _} -> num
          :error -> str
        end
    end
  end

  defp format_prometheus(stats) when is_binary(stats) do
    metrics = parse_stats(stats)

    # Key RocksDB metrics
    metrics_list = [
      {"rocksdb_block_cache_hit", Map.get(metrics, "rocksdb.block.cache.hit", 0),
       "Block cache hits"},
      {"rocksdb_block_cache_miss", Map.get(metrics, "rocksdb.block.cache.miss", 0),
       "Block cache misses"},
      {"rocksdb_bytes_written", Map.get(metrics, "rocksdb.bytes.written", 0),
       "Total bytes written"},
      {"rocksdb_bytes_read", Map.get(metrics, "rocksdb.bytes.read", 0), "Total bytes read"},
      {"rocksdb_compaction_time_micros", Map.get(metrics, "rocksdb.compaction.times.micros", 0),
       "Compaction time in microseconds"},
      {"rocksdb_flush_write_bytes", Map.get(metrics, "rocksdb.flush.write.bytes", 0),
       "Bytes written during flush"},
      {"rocksdb_stall_micros", Map.get(metrics, "rocksdb.stall.micros", 0),
       "Write stall time in microseconds"},
      {"rocksdb_memtable_hit", Map.get(metrics, "rocksdb.memtable.hit", 0), "Memtable hits"},
      {"rocksdb_memtable_miss", Map.get(metrics, "rocksdb.memtable.miss", 0), "Memtable misses"},
      {"rocksdb_bloom_filter_useful", Map.get(metrics, "rocksdb.bloom.filter.useful", 0),
       "Bloom filter useful checks"},
      {"rocksdb_num_keys_written", Map.get(metrics, "rocksdb.number.keys.written", 0),
       "Number of keys written"},
      {"rocksdb_num_keys_read", Map.get(metrics, "rocksdb.number.keys.read", 0),
       "Number of keys read"}
    ]

    metrics_list
    |> Enum.map(fn {name, value, help} ->
      """
      # HELP #{name} #{help}
      # TYPE #{name} counter
      #{name} #{value}
      """
    end)
    |> Enum.join()
  end

  defp format_prometheus(_), do: "# RocksDB statistics unavailable\n"

  defp format_prometheus_from_list(stats) when is_list(stats) do
    # Convert keyword list to map and format
    metrics_list = [
      {"rocksdb_stats_level", Keyword.get(stats, :stats_level, 0), "Statistics collection level"}
    ]

    metrics_list
    |> Enum.map(fn {name, value, help} ->
      value_str = if is_atom(value), do: "\"#{value}\"", else: value

      """
      # HELP #{name} #{help}
      # TYPE #{name} gauge
      #{name} #{value_str}
      """
    end)
    |> Enum.join()
  end
end
