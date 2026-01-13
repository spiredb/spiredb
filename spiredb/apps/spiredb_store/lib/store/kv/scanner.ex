defmodule Store.KV.Scanner do
  @moduledoc """
  Streaming iterator for RocksDB range scans.

  Provides both streaming and batch-based APIs:
  - `scan_stream/4` - Returns a lazy Stream (memory efficient for large scans)
  - `scan_range/4` - Returns all results at once (backwards compatible)

  Designed to support high concurrency (100+ concurrent scans per node).
  """

  require Logger
  alias Store.KV.IteratorPool

  @doc """
  Stream scan results lazily.

  Returns a Stream of `{key, value}` tuples. Only fetches data as needed,
  preventing OOM for large scans.

  ## Options
  - `:batch_size` - internal buffer size (default: 1000)
  - `:limit` - max total rows (default: unlimited, 0 means no limit)

  ## Example

      db_ref
      |> Scanner.scan_stream("a", "z")
      |> Stream.take(100)
      |> Enum.to_list()

  """
  def scan_stream(db_ref, start_key, end_key, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 1000)
    limit = Keyword.get(opts, :limit, 0)

    Stream.resource(
      # Start function - create iterator
      fn -> {db_ref, init_iterator(db_ref, start_key, end_key)} end,

      # Next function - fetch next batch
      fn {db_ref, state} ->
        case next_batch(state, end_key, batch_size, limit) do
          {:halt, new_state} -> {:halt, {db_ref, new_state}}
          {rows, new_state} -> {rows, {db_ref, new_state}}
        end
      end,

      # Cleanup function - close iterator
      fn {db_ref, state} -> cleanup_iterator(state, db_ref) end
    )
  end

  @doc """
  Scan a range of keys and return batches of rows.

  This is the original batch-based API, kept for backwards compatibility.
  For large scans, prefer `scan_stream/4` to avoid memory issues.

  ## Options
  - `:batch_size` - rows per batch (default: 1000)
  - `:limit` - max total rows (default: unlimited)

  ## Returns
  `{:ok, [{rows, stats}]}` or `{:error, reason}`
  """
  def scan_range(db_ref, start_key, end_key, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 1000)
    limit = Keyword.get(opts, :limit, 0)
    start_time = System.monotonic_time(:millisecond)

    try do
      # Use stream internally but collect with batching
      stream = scan_stream(db_ref, start_key, end_key, batch_size: batch_size, limit: limit)

      batches =
        stream
        |> Stream.chunk_every(batch_size)
        |> Enum.map(fn rows ->
          stats = build_batch_stats(rows, start_time)
          {rows, stats}
        end)

      {:ok, batches}
    rescue
      e ->
        Logger.error(
          "Scan failed with error: #{inspect(e)}\nStacktrace: #{inspect(__STACKTRACE__)}"
        )

        {:error, :scan_failed}
    end
  end

  # Stream.resource callbacks

  defp init_iterator(db_ref, start_key, end_key) do
    case IteratorPool.checkout(db_ref) do
      {:ok, iter} ->
        case :rocksdb.iterator_move(iter, {:seek, start_key}) do
          {:ok, first_key, first_value} ->
            if end_key != "" and first_key > end_key do
              IteratorPool.checkin(iter, db_ref)
              {:done, nil}
            else
              {:continue, iter, first_key, first_value, 0}
            end

          {:error, :invalid_iterator} ->
            IteratorPool.checkin(iter, db_ref)
            {:done, nil}
        end

      {:error, reason} ->
        Logger.error("Failed to create iterator: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp next_batch({:done, _}, _end_key, _batch_size, _limit) do
    {:halt, {:done, nil}}
  end

  defp next_batch({:error, _reason} = error, _end_key, _batch_size, _limit) do
    {:halt, error}
  end

  defp next_batch(
         {:continue, iter, current_key, current_value, count},
         end_key,
         batch_size,
         limit
       ) do
    # Check if we've hit the limit
    if limit > 0 and count >= limit do
      {:halt, {:done, iter}}
    else
      # Collect a batch of rows
      {rows, next_state} =
        collect_batch(iter, current_key, current_value, end_key, batch_size, limit, count)

      if rows == [] do
        {:halt, next_state}
      else
        {rows, next_state}
      end
    end
  end

  defp collect_batch(iter, current_key, current_value, end_key, batch_size, limit, count) do
    collect_batch_loop(iter, current_key, current_value, end_key, batch_size, limit, count, [])
  end

  defp collect_batch_loop(
         iter,
         current_key,
         current_value,
         end_key,
         batch_size,
         limit,
         count,
         acc
       ) do
    new_count = count + 1
    new_acc = [{current_key, current_value} | acc]

    # Check if we should stop this batch
    batch_full = length(new_acc) >= batch_size
    hit_limit = limit > 0 and new_count >= limit

    if batch_full or hit_limit do
      # Batch is complete, prepare for next iteration
      case :rocksdb.iterator_move(iter, :next) do
        {:ok, next_key, next_value} ->
          if end_key != "" and next_key > end_key do
            {Enum.reverse(new_acc), {:done, iter}}
          else
            {Enum.reverse(new_acc), {:continue, iter, next_key, next_value, new_count}}
          end

        {:error, :invalid_iterator} ->
          {Enum.reverse(new_acc), {:done, iter}}
      end
    else
      # Continue collecting in this batch
      case :rocksdb.iterator_move(iter, :next) do
        {:ok, next_key, next_value} ->
          if end_key != "" and next_key > end_key do
            {Enum.reverse(new_acc), {:done, iter}}
          else
            collect_batch_loop(
              iter,
              next_key,
              next_value,
              end_key,
              batch_size,
              limit,
              new_count,
              new_acc
            )
          end

        {:error, :invalid_iterator} ->
          {Enum.reverse(new_acc), {:done, iter}}
      end
    end
  end

  defp cleanup_iterator({:done, nil}, _db_ref), do: :ok
  defp cleanup_iterator({:error, _}, _db_ref), do: :ok

  defp cleanup_iterator({:done, iter}, db_ref) when not is_nil(iter) do
    IteratorPool.checkin(iter, db_ref)
    :ok
  end

  defp cleanup_iterator({:continue, iter, _, _, _}, db_ref) do
    IteratorPool.checkin(iter, db_ref)
    :ok
  end

  defp build_batch_stats(rows, start_time) do
    scan_time = System.monotonic_time(:millisecond) - start_time

    bytes_read =
      Enum.reduce(rows, 0, fn {k, v}, acc ->
        acc + byte_size(k) + byte_size(v)
      end)

    %{
      rows_returned: length(rows),
      bytes_read: bytes_read,
      scan_time_ms: scan_time
    }
  end
end
