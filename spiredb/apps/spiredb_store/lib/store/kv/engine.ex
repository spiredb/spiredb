defmodule Store.KV.Engine do
  @moduledoc """
  RocksDB wrapper.

  Provides simple KV interface over RocksDB with production-optimized settings.
  """

  use GenServer

  require Logger

  alias Store.KV.ColumnFamilies

  defstruct [:db_ref, :cf_map, :path, :cache, :rate_limiter, :statistics]

  ## Client API

  @doc """
  Child specification for supervision tree.
  Uses 30s shutdown timeout to allow RocksDB to flush and close cleanly.
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 30_000,
      type: :worker
    }
  end

  def start_link(opts) do
    _path = opts[:path] || raise "Path is required"
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def get(pid, key) do
    case get_ref() do
      nil ->
        GenServer.call(pid, {:get, key})

      db_ref ->
        case :rocksdb.get(db_ref, key, []) do
          {:ok, value} -> {:ok, value}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  def put(pid, key, value) do
    case get_ref() do
      nil -> GenServer.call(pid, {:put, key, value})
      db_ref -> :rocksdb.put(db_ref, key, value, [])
    end
  end

  def delete(pid, key) do
    case get_ref() do
      nil -> GenServer.call(pid, {:delete, key})
      db_ref -> :rocksdb.delete(db_ref, key, [])
    end
  end

  def write_batch(pid, operations) do
    GenServer.call(pid, {:write_batch, operations})
  end

  @doc """
  Scan a range of keys.

  Returns batches of {key, value} pairs for streaming to clients.
  """
  def scan_range(pid, start_key, end_key, opts \\ []) do
    GenServer.call(pid, {:scan_range, start_key, end_key, opts}, :infinity)
  end

  @doc """
  Get direct access to db_ref for advanced operations.
  Internal use only.
  """
  def get_db_ref(pid) do
    GenServer.call(pid, :get_db_ref)
  end

  @doc """
  Get RocksDB statistics for monitoring.
  """
  def get_stats(pid) do
    GenServer.call(pid, :get_stats)
  end

  @doc """
  Get cache information for monitoring.
  """
  def get_cache_info(pid) do
    GenServer.call(pid, :get_cache_info)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    path = opts[:path] || Application.get_env(:spiredb_store, :rocksdb_path, "/tmp/spiredb/data")
    File.mkdir_p!(path)

    # Cleanup stale references from previous crashes/restarts
    # This fixes "Lock held by current process" errors
    try do
      if old_ref = :persistent_term.get(:spiredb_rocksdb_ref, nil) do
        Logger.warning("Found stale RocksDB reference in persistent_term. Closing it.")

        try do
          :rocksdb.close(old_ref)
        catch
          _, _ -> :ok
        end

        :persistent_term.erase(:spiredb_rocksdb_ref)
      end
    catch
      _, _ -> :ok
    end

    _path_charlist = to_charlist(path)

    # Load configuration with production defaults
    config = load_rocksdb_config()

    # Create block cache for reads (critical for point lookups)
    {:ok, cache} = :rocksdb.new_cache(:lru, config.block_cache_size)

    # Create rate limiter to prevent compaction storms
    {:ok, rate_limiter} = :rocksdb.new_rate_limiter(config.rate_limit_bytes_per_sec, true)

    # Create statistics handle for monitoring
    {:ok, statistics} = :rocksdb.new_statistics()
    :rocksdb.set_stats_level(statistics, :stats_except_detailed_timers)

    # Block-based table options with bloom filters
    block_based_opts = [
      block_size: config.block_size,
      block_cache: cache,
      bloom_filter_policy: config.bloom_bits_per_key,
      whole_key_filtering: true,
      cache_index_and_filter_blocks: true,
      format_version: 5
    ]

    # Combined DB and CF options
    db_opts = [
      # Basic options
      create_if_missing: config.create_if_missing,
      max_open_files: config.max_open_files,

      # Compression - configurable, defaults to none for compatibility
      # Production deployments should set to :lz4 or :zstd if available
      compression: config.compression,
      bottommost_compression: config.bottommost_compression,

      # Write buffer (memtable) settings
      write_buffer_size: config.write_buffer_size,
      max_write_buffer_number: config.max_write_buffer_number,
      min_write_buffer_number_to_merge: 2,

      # Block-based table with bloom filters
      block_based_table_options: block_based_opts,

      # Level compaction settings
      level_compaction_dynamic_level_bytes: true,
      max_bytes_for_level_base: config.max_bytes_for_level_base,
      target_file_size_base: config.target_file_size_base,

      # Background job settings
      max_background_jobs: config.max_background_jobs,
      max_background_compactions: config.max_background_compactions,
      max_background_flushes: config.max_background_flushes,

      # WAL settings - optimized for write performance
      max_total_wal_size: config.max_total_wal_size,
      wal_recovery_mode: :point_in_time_recovery,
      recycle_log_file_num: config.recycle_log_file_num,
      wal_bytes_per_sync: config.wal_bytes_per_sync,

      # Rate limiting for compaction I/O
      rate_limiter: rate_limiter,

      # Statistics for monitoring
      statistics: statistics,

      # Performance optimizations
      optimize_filters_for_hits: true,
      allow_concurrent_memtable_write: true,
      enable_write_thread_adaptive_yield: true
    ]

    Logger.info("""
    Opening RocksDB at #{path}
      compression: #{config.compression}
      bottommost_compression: #{config.bottommost_compression}
      block_cache_mb: #{div(config.block_cache_size, 1024 * 1024)}
      write_buffer_mb: #{div(config.write_buffer_size, 1024 * 1024)}
      max_write_buffers: #{config.max_write_buffer_number}
      bloom_bits: #{config.bloom_bits_per_key}
      max_open_files: #{config.max_open_files}
      max_background_jobs: #{config.max_background_jobs}
    """)

    case ColumnFamilies.open_with_cf(path, db_opts) do
      {:ok, db_ref, cf_map} ->
        Logger.info(
          "RocksDB opened successfully at #{path} with #{map_size(cf_map)} column families"
        )

        # Store db_ref and cf_map in persistent_term for direct concurrent access
        # This bypasses the GenServer bottleneck
        :persistent_term.put(:spiredb_rocksdb_ref, db_ref)
        :persistent_term.put(:spiredb_rocksdb_cf_map, cf_map)

        state = %__MODULE__{
          db_ref: db_ref,
          cf_map: cf_map,
          path: path,
          cache: cache,
          rate_limiter: rate_limiter,
          statistics: statistics
        }

        {:ok, state}

      {:error, reason} ->
        # Clean up resources on failure
        :rocksdb.release_cache(cache)
        :rocksdb.release_rate_limiter(rate_limiter)
        :rocksdb.release_statistics(statistics)
        Logger.error("Failed to open RocksDB: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # Helper to get db_ref safely
  defp get_ref do
    try do
      :persistent_term.get(:spiredb_rocksdb_ref)
    rescue
      ArgumentError -> nil
    end
  end

  defp load_rocksdb_config do
    %{
      # Basic settings
      create_if_missing: Application.get_env(:spiredb_store, :rocksdb_create_if_missing, true),
      max_open_files: Application.get_env(:spiredb_store, :rocksdb_max_open_files, 10_000),

      # Compression - LZ4 for speed, Zstd for cold data
      compression: Application.get_env(:spiredb_store, :rocksdb_compression, :lz4),
      bottommost_compression:
        Application.get_env(:spiredb_store, :rocksdb_bottommost_compression, :zstd),

      # Block cache - 512MB default, critical for read performance
      block_cache_size:
        Application.get_env(:spiredb_store, :rocksdb_block_cache_size, 512 * 1024 * 1024),
      block_size: Application.get_env(:spiredb_store, :rocksdb_block_size, 16 * 1024),

      # Bloom filter - 10 bits per key is good balance of space/accuracy
      bloom_bits_per_key: Application.get_env(:spiredb_store, :rocksdb_bloom_bits_per_key, 10),

      # Write buffer settings - 128MB per buffer, 4 buffers
      write_buffer_size:
        Application.get_env(:spiredb_store, :rocksdb_write_buffer_size, 128 * 1024 * 1024),
      max_write_buffer_number:
        Application.get_env(:spiredb_store, :rocksdb_max_write_buffer_number, 4),

      # Compaction settings
      max_bytes_for_level_base:
        Application.get_env(:spiredb_store, :rocksdb_max_bytes_for_level_base, 512 * 1024 * 1024),
      target_file_size_base:
        Application.get_env(:spiredb_store, :rocksdb_target_file_size_base, 64 * 1024 * 1024),

      # Background threads
      max_background_jobs: Application.get_env(:spiredb_store, :rocksdb_max_background_jobs, 4),
      max_background_compactions:
        Application.get_env(:spiredb_store, :rocksdb_max_background_compactions, 3),
      max_background_flushes:
        Application.get_env(:spiredb_store, :rocksdb_max_background_flushes, 2),

      # WAL settings - optimized for write performance
      max_total_wal_size:
        Application.get_env(:spiredb_store, :rocksdb_max_total_wal_size, 512 * 1024 * 1024),
      # Recycle WAL files to avoid allocation overhead (0 = disabled)
      recycle_log_file_num: Application.get_env(:spiredb_store, :rocksdb_recycle_log_file_num, 4),
      # Sync WAL every N bytes (0 = OS handles it)
      wal_bytes_per_sync:
        Application.get_env(:spiredb_store, :rocksdb_wal_bytes_per_sync, 512 * 1024),

      # Rate limiter - 100MB/s default to prevent I/O storms
      rate_limit_bytes_per_sec:
        Application.get_env(:spiredb_store, :rocksdb_rate_limit_bytes_per_sec, 100 * 1024 * 1024)
    }
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    result =
      case :rocksdb.get(state.db_ref, key, []) do
        {:ok, value} -> {:ok, value}
        :not_found -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    result = :rocksdb.put(state.db_ref, key, value, [])
    {:reply, result, state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    result = :rocksdb.delete(state.db_ref, key, [])
    {:reply, result, state}
  end

  @impl true
  def handle_call({:write_batch, operations}, _from, state) do
    # Operations: list of {:put, k, v} or {:delete, k}
    {:ok, batch} = :rocksdb.batch()

    Enum.each(operations, fn
      {:put, k, v} -> :rocksdb.batch_put(batch, k, v)
      {:delete, k} -> :rocksdb.batch_delete(batch, k)
    end)

    result = :rocksdb.write_batch(state.db_ref, batch, [])
    :rocksdb.release_batch(batch)

    {:reply, result, state}
  end

  @impl true
  def handle_call({:scan_range, start_key, end_key, opts}, _from, state) do
    # Use Scanner module for efficient range iteration
    alias Store.KV.Scanner
    result = Scanner.scan_range(state.db_ref, start_key, end_key, opts)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:get_db_ref, _from, state) do
    {:reply, state.db_ref, state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats =
      if state.statistics do
        :rocksdb.statistics_info(state.statistics)
      else
        []
      end

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:get_cache_info, _from, state) do
    info =
      if state.cache do
        :rocksdb.cache_info(state.cache)
      else
        []
      end

    {:reply, info, state}
  end

  @impl true
  def terminate(_reason, state) do
    Logger.info("Shutting down RocksDB engine")

    # Close database first
    if state.db_ref do
      :rocksdb.close(state.db_ref)
    end

    # Release resources
    if state.cache do
      :rocksdb.release_cache(state.cache)
    end

    if state.rate_limiter do
      :rocksdb.release_rate_limiter(state.rate_limiter)
    end

    if state.statistics do
      :rocksdb.release_statistics(state.statistics)
    end

    :persistent_term.erase(:spiredb_rocksdb_ref)

    :ok
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Store.KV.Engine received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end
end
