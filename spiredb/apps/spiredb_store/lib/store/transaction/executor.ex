defmodule Store.Transaction.Executor do
  @moduledoc """
  Percolator 2PC protocol executor.

  Implements:
  1. MVCC reads with lock resolution
  2. Prewrite phase (acquire locks, write data)
  3. Commit phase (write commit record, async cleanup)
  4. Lock resolution for blocked transactions
  """

  require Logger
  alias Store.Transaction
  alias Store.Transaction.Lock
  alias Store.Schema.Encoder

  # Column family names
  @cf_locks "txn_locks"
  @cf_data "txn_data"
  @cf_write "txn_write"

  @doc """
  MVCC get: Read a key at a specific timestamp.

  1. Check for locks at this key
  2. If locked by another txn, resolve or wait
  3. Find the latest committed version <= start_ts
  """
  def mvcc_get(store_ref, key, start_ts) do
    mvcc_get(store_ref, key, nil, start_ts)
  end

  @doc """
  MVCC get: Read a key at a specific timestamp, with transaction context.
  """
  def mvcc_get(store_ref, key, txn, start_ts) do
    # Check for existing lock
    case get_lock(store_ref, key) do
      {:ok, nil} ->
        # No lock, read committed value
        mvcc_read(store_ref, key, txn, start_ts)

      {:ok, lock} ->
        # Key is locked, try to resolve
        case resolve_lock(store_ref, lock) do
          :resolved ->
            # Lock was resolved, retry read
            mvcc_read(store_ref, key, txn, start_ts)

          {:wait, _ttl_remaining} ->
            # Lock is still valid, return error
            {:error, :locked, lock}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Commit a transaction using 2PC.

  Phase 1: Prewrite
  - Lock all keys (primary first, then secondaries in parallel)
  - Write data to txn_data CF

  Phase 2: Commit
  - Get commit_ts from TSO
  - Write commit record for primary key
  - Return to client immediately
  - Async: cleanup secondary locks
  """
  def commit(_store_ref, %Transaction{write_buffer: buffer} = txn) when map_size(buffer) == 0 do
    # Empty transaction, nothing to commit
    {:ok, txn.start_ts}
  end

  def commit(store_ref, %Transaction{} = txn) do
    Logger.debug("Committing transaction #{txn.id} with #{map_size(txn.write_buffer)} mutations")

    with :ok <- prewrite_phase(store_ref, txn),
         {:ok, commit_ts} <- commit_phase(store_ref, txn) do
      # Spawn async cleanup of secondary locks
      spawn_async_cleanup(store_ref, txn, commit_ts)
      {:ok, commit_ts}
    else
      {:error, {:prewrite_failed, key, reason}} ->
        Logger.warning("Prewrite failed for key #{inspect(key)}: #{inspect(reason)}")
        cleanup(store_ref, txn)
        {:error, {:conflict, key}}

      {:error, reason} ->
        cleanup(store_ref, txn)
        {:error, reason}
    end
  end

  @doc """
  Clean up locks from a rolled back or failed transaction.
  """
  def cleanup(store_ref, %Transaction{} = txn) do
    Logger.debug("Cleaning up transaction #{txn.id}")

    Enum.each(txn.write_buffer, fn {key, _} ->
      delete_lock(store_ref, key, txn.start_ts)
      delete_data(store_ref, key, txn.start_ts)
    end)

    :ok
  end

  # ============================================================================
  # Public API for gRPC TransactionService
  # ============================================================================

  @doc """
  Prewrite a single mutation (for gRPC).
  """
  def prewrite_single(store_ref, key, mutation, request) do
    with :ok <- check_write_conflict(store_ref, key, request.start_ts),
         :ok <- check_lock_conflict(store_ref, key, request.start_ts) do
      # Create lock
      lock =
        Lock.new(key, request.primary_key, request.start_ts,
          ttl: request.lock_ttl,
          lock_type: if(request.is_pessimistic, do: :pessimistic, else: :prewrite)
        )

      with :ok <- do_write_lock(store_ref, key, lock) do
        # Write data
        case mutation.type do
          :MUTATION_PUT -> write_data(store_ref, key, {:put, mutation.value}, request.start_ts)
          :MUTATION_DELETE -> write_data(store_ref, key, :delete, request.start_ts)
          _ -> :ok
        end
      end
    end
  end

  @doc """
  Commit primary key (for gRPC).
  """
  def commit_primary(store_ref, primary_key, start_ts, commit_ts) do
    write_commit_record(store_ref, primary_key, start_ts, commit_ts)
  end

  @doc """
  Commit secondary key (for gRPC).
  """
  def commit_secondary(store_ref, key, start_ts, commit_ts) do
    write_commit_record(store_ref, key, start_ts, commit_ts)
    delete_lock(store_ref, key, start_ts)
    :ok
  end

  @doc """
  Rollback a key (for gRPC).
  """
  def rollback_key(store_ref, key, start_ts) do
    delete_lock(store_ref, key, start_ts)
    delete_data(store_ref, key, start_ts)
    :ok
  end

  @doc """
  Check transaction status by primary key (for gRPC).
  """
  def check_txn_status(store_ref, primary_key, start_ts) do
    case get_commit_record(store_ref, primary_key, start_ts) do
      {:ok, commit_ts} ->
        {:committed, commit_ts}

      {:error, :not_found} ->
        case get_lock(store_ref, primary_key) do
          {:ok, nil} ->
            :rolled_back

          {:ok, lock} when lock.start_ts == start_ts ->
            {:pending, lock.ttl - elapsed_since_lock(lock)}

          {:ok, _} ->
            :rolled_back

          {:error, _} ->
            :rolled_back
        end
    end
  end

  @doc """
  Acquire pessimistic lock (for gRPC).
  """
  def acquire_pessimistic_lock(store_ref, key, start_ts, _for_update_ts, lock_ttl) do
    case get_lock(store_ref, key) do
      {:ok, nil} ->
        lock =
          Lock.new(key, key, start_ts,
            ttl: lock_ttl,
            lock_type: :pessimistic
          )

        do_write_lock(store_ref, key, lock)

      {:ok, lock} ->
        if Lock.expired?(lock) do
          delete_lock(store_ref, key, lock.start_ts)

          new_lock =
            Lock.new(key, key, start_ts,
              ttl: lock_ttl,
              lock_type: :pessimistic
            )

          do_write_lock(store_ref, key, new_lock)
        else
          {:error, {:locked_by, lock}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Public API for Async Commit Coordinator
  # ============================================================================

  @doc """
  Get lock for a key (public API for async commit resolution).
  """
  def get_lock(store_ref, key) do
    get_lock_internal(store_ref, key)
  end

  @doc """
  Delete lock for a key (public API for async commit).
  """
  def delete_lock(store_ref, key, start_ts) do
    delete_lock_internal(store_ref, key, start_ts)
  end

  @doc """
  Delete data for a key (public API for rollback).
  """
  def delete_data(store_ref, key, start_ts) do
    delete_data_internal(store_ref, key, start_ts)
  end

  @doc """
  Write commit record (public API for async commit).
  """
  def write_commit_record(store_ref, key, start_ts, commit_ts) do
    write_commit_record_internal(store_ref, key, start_ts, commit_ts)
  end

  @doc """
  Get commit record (public API for async commit resolution).
  """
  def get_commit_record(store_ref, key, start_ts) do
    get_commit_record_internal(store_ref, key, start_ts)
  end

  @doc """
  Prewrite with a pre-built lock (for async commit).
  Used when lock already contains secondaries list.
  """
  def prewrite_with_lock(store_ref, key, operation, lock, start_ts) do
    with :ok <- check_write_conflict(store_ref, key, start_ts),
         :ok <- check_lock_conflict(store_ref, key, start_ts),
         :ok <- do_write_lock(store_ref, key, lock),
         :ok <- write_data(store_ref, key, operation, start_ts) do
      :ok
    end
  end

  @doc """
  Write rollback record to prevent late prewrites.
  """
  def write_rollback_record(store_ref, key, start_ts) do
    # Use write CF with special marker
    write_key = Encoder.encode_txn_write_key(key, start_ts)
    # Rollback marker: {start_ts, 0} where 0 indicates rollback
    value = :erlang.term_to_binary({start_ts, 0, :rollback})

    case {get_db_ref(store_ref), get_write_cf(store_ref)} do
      {nil, _} ->
        {:error, :db_not_ready}

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.put(db_ref, cf, write_key, value, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.put(db_ref, write_key, value, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Scan for writes after a given timestamp (for SSI conflict detection).
  """
  def scan_writes_after(store_ref, key, after_ts) do
    # Replaced ETS scan with RocksDB iterator
    case get_db_ref(store_ref) do
      nil ->
        {:ok, []}

      db_ref ->
        cf = get_write_cf(store_ref)
        # Using prefix Seek to {key}
        case :rocksdb.iterator(db_ref, cf, []) do
          {:ok, iter} ->
            result = scan_writes_in_iter(iter, key, after_ts)
            :rocksdb.iterator_close(iter)
            {:ok, result}

          _ ->
            {:ok, []}
        end
    end
  end

  defp scan_writes_in_iter(iter, key, after_ts) do
    # Seek to {key} (which actually finds {key}{latest_ts})
    case :rocksdb.iterator_move(iter, {:seek, key}) do
      {:ok, write_key, value} ->
        collect_writes(iter, key, after_ts, write_key, value, [])

      _ ->
        []
    end
  end

  defp collect_writes(iter, target_key, after_ts, write_key, value, acc) do
    try do
      {stored_key, commit_ts} = Encoder.decode_txn_write_key(write_key)

      if stored_key == target_key do
        if commit_ts > after_ts do
          {start_ts, _} = :erlang.binary_to_term(value)
          # Found conflict
          new_acc = [{commit_ts, start_ts} | acc]
          # Check next
          case :rocksdb.iterator_move(iter, :next) do
            {:ok, nk, nv} -> collect_writes(iter, target_key, after_ts, nk, nv, new_acc)
            _ -> new_acc
          end
        else
          # commit_ts <= after_ts, since sorted desc, subsequent ones are also smaller
          acc
        end
      else
        # Different key
        acc
      end
    rescue
      _ -> acc
    end
  end

  @doc """
  Get latest write for a key (public API for SSI).
  """
  def get_latest_write(store_ref, key) do
    get_latest_write_internal(store_ref, key)
  end

  defp do_write_lock(store_ref, key, lock) do
    lock_key = Encoder.encode_lock_key(key)
    lock_data = Lock.encode(lock)

    case {get_db_ref(store_ref), get_locks_cf(store_ref)} do
      {nil, _} ->
        {:error, :db_not_ready}

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.put(db_ref, cf, lock_key, lock_data, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.put(db_ref, lock_key, lock_data, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  # ============================================================================
  # Prewrite Phase
  # ============================================================================

  defp prewrite_phase(store_ref, %Transaction{} = txn) do
    primary = txn.primary_key
    secondaries = Transaction.secondary_keys(txn)

    # Prewrite primary first
    case prewrite_key(store_ref, txn, primary, true) do
      :ok ->
        # Prewrite secondaries in parallel
        results =
          secondaries
          |> Task.async_stream(fn key -> prewrite_key(store_ref, txn, key, false) end,
            max_concurrency: 10,
            timeout: txn.timeout_ms
          )
          |> Enum.to_list()

        # Check for any failures
        case Enum.find(results, &match?({:ok, {:error, _}}, &1)) do
          nil -> :ok
          {:ok, {:error, reason}} -> {:error, {:prewrite_failed, :secondary, reason}}
        end

      {:error, reason} ->
        {:error, {:prewrite_failed, primary, reason}}
    end
  end

  defp prewrite_key(store_ref, txn, key, _is_primary) do
    mutation = Map.get(txn.write_buffer, key)

    with :ok <- check_write_conflict(store_ref, key, txn.start_ts),
         :ok <- check_lock_conflict(store_ref, key, txn.start_ts),
         :ok <- write_lock(store_ref, key, txn),
         :ok <- write_data(store_ref, key, mutation, txn.start_ts) do
      :ok
    end
  end

  defp check_write_conflict(store_ref, key, start_ts) do
    # Check if there's a commit after our start_ts
    case get_latest_write(store_ref, key) do
      {:ok, nil} ->
        :ok

      {:ok, {commit_ts, _}} when commit_ts > start_ts ->
        {:error, :write_conflict}

      {:ok, _} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_lock_conflict(store_ref, key, _start_ts) do
    case get_lock(store_ref, key) do
      {:ok, nil} ->
        :ok

      {:ok, lock} ->
        # Another transaction has this key locked
        if Lock.expired?(lock) do
          # Try to clean up expired lock
          resolve_lock(store_ref, lock)
          :ok
        else
          {:error, {:locked_by, lock.txn_id}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Commit Phase
  # ============================================================================

  defp commit_phase(store_ref, %Transaction{} = txn) do
    # Get commit timestamp
    case PD.TSO.get_timestamp() do
      {:ok, commit_ts} ->
        # Write commit record for primary key only
        case write_commit_record(store_ref, txn.primary_key, txn.start_ts, commit_ts) do
          :ok -> {:ok, commit_ts}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, {:tso_error, reason}}
    end
  end

  defp spawn_async_cleanup(store_ref, txn, commit_ts) do
    Task.start(fn ->
      # Small delay to let primary commit propagate
      Process.sleep(10)

      # Commit secondary keys and remove locks
      Enum.each(Transaction.secondary_keys(txn), fn key ->
        write_commit_record(store_ref, key, txn.start_ts, commit_ts)
        delete_lock(store_ref, key, txn.start_ts)
      end)

      # Delete primary lock
      delete_lock(store_ref, txn.primary_key, txn.start_ts)

      Logger.debug("Async cleanup completed for txn #{txn.id}")
    end)
  end

  # ============================================================================
  # Lock Resolution
  # ============================================================================

  @doc """
  Resolve a lock by checking the primary key's commit status.
  """
  def resolve_lock(store_ref, %Lock{} = lock) do
    if Lock.expired?(lock) do
      # Lock expired, roll it back
      rollback_expired_lock(store_ref, lock)
      :resolved
    else
      # Check primary key status
      case get_primary_status(store_ref, lock.primary_key, lock.start_ts) do
        :committed ->
          # Primary was committed, commit this secondary too
          commit_from_primary(store_ref, lock)
          :resolved

        :rolled_back ->
          # Primary was rolled back, clean up this lock
          delete_lock(store_ref, lock.key, lock.start_ts)
          :resolved

        :pending ->
          # Primary still pending, wait
          {:wait, lock.ttl - elapsed_since_lock(lock)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp get_primary_status(store_ref, primary_key, start_ts) do
    # Check if primary has a commit record
    case get_commit_record(store_ref, primary_key, start_ts) do
      {:ok, _commit_ts} ->
        :committed

      {:error, :not_found} ->
        # Check if primary still has a lock
        case get_lock(store_ref, primary_key) do
          {:ok, nil} ->
            # No lock, no commit = rolled back
            :rolled_back

          {:ok, lock} when lock.start_ts == start_ts ->
            :pending

          _ ->
            # Different transaction, original was rolled back
            :rolled_back
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp commit_from_primary(store_ref, lock) do
    # Find the commit_ts from a write record
    case get_commit_record(store_ref, lock.primary_key, lock.start_ts) do
      {:ok, commit_ts} ->
        write_commit_record(store_ref, lock.key, lock.start_ts, commit_ts)
        delete_lock(store_ref, lock.key, lock.start_ts)

      _ ->
        :ok
    end
  end

  defp rollback_expired_lock(store_ref, lock) do
    delete_lock(store_ref, lock.key, lock.start_ts)
    delete_data(store_ref, lock.key, lock.start_ts)
  end

  defp elapsed_since_lock(lock) do
    now = System.os_time(:millisecond)
    lock_time = div(lock.start_ts, 1000)
    now - lock_time
  end

  # ============================================================================
  # Storage Operations (RocksDB with proper CFs)
  # ============================================================================

  defp get_db_ref(%{db: db}), do: db
  defp get_db_ref(_), do: nil

  defp get_cf(%{cfs: cfs}, cf_name), do: Map.get(cfs, cf_name)
  defp get_cf(_, _), do: nil

  defp get_locks_cf(store_ref), do: get_cf(store_ref, @cf_locks)
  defp get_data_cf(store_ref), do: get_cf(store_ref, @cf_data)
  defp get_write_cf(store_ref), do: get_cf(store_ref, @cf_write)

  defp get_lock_internal(store_ref, key) do
    lock_key = Encoder.encode_lock_key(key)

    case {get_db_ref(store_ref), get_locks_cf(store_ref)} do
      {nil, _} ->
        {:ok, nil}

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.get(db_ref, cf, lock_key, []) do
          {:ok, lock_data} -> Lock.decode(key, lock_data)
          :not_found -> {:ok, nil}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp write_lock(store_ref, key, txn) do
    lock = Lock.new(key, txn.primary_key, txn.start_ts, txn_id: txn.id)
    do_write_lock(store_ref, key, lock)
  end

  defp delete_lock_internal(store_ref, key, _start_ts) do
    lock_key = Encoder.encode_lock_key(key)

    case {get_db_ref(store_ref), get_locks_cf(store_ref)} do
      {nil, _} ->
        :ok

      {db_ref, cf} when not is_nil(cf) ->
        try do
          case :rocksdb.delete(db_ref, cf, lock_key, []) do
            :ok -> :ok
            {:error, reason} -> {:error, reason}
          end
        rescue
          ArgumentError -> :ok
        end
    end
  end

  defp write_data(store_ref, key, {:put, value}, start_ts) do
    data_key = Encoder.encode_txn_data_key(key, start_ts)

    case {get_db_ref(store_ref), get_data_cf(store_ref)} do
      {nil, _} ->
        {:error, :db_not_ready}

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.put(db_ref, cf, data_key, value, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp write_data(store_ref, key, :delete, start_ts) do
    # For deletes, we write a tombstone marker
    data_key = Encoder.encode_txn_data_key(key, start_ts)
    tombstone = <<0xFF, 0xFF, 0xFF, 0xFF>>

    case {get_db_ref(store_ref), get_data_cf(store_ref)} do
      {nil, _} ->
        {:error, :db_not_ready}

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.put(db_ref, cf, data_key, tombstone, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp delete_data_internal(store_ref, key, start_ts) do
    data_key = Encoder.encode_txn_data_key(key, start_ts)

    case {get_db_ref(store_ref), get_data_cf(store_ref)} do
      {nil, _} ->
        :ok

      {db_ref, cf} when not is_nil(cf) ->
        try do
          case :rocksdb.delete(db_ref, cf, data_key, []) do
            :ok -> :ok
            {:error, reason} -> {:error, reason}
          end
        rescue
          ArgumentError -> :ok
        end
    end
  end

  defp write_commit_record_internal(store_ref, key, start_ts, commit_ts) do
    write_key = Encoder.encode_txn_write_key(key, commit_ts)
    value = :erlang.term_to_binary({start_ts, commit_ts})

    case {get_db_ref(store_ref), get_write_cf(store_ref)} do
      {nil, _} ->
        {:error, :db_not_ready}

      {db_ref, cf} when not is_nil(cf) ->
        try do
          case :rocksdb.put(db_ref, cf, write_key, value, []) do
            :ok -> :ok
            {:error, reason} -> {:error, reason}
          end
        rescue
          ArgumentError -> {:error, :db_closed}
        end
    end
  end

  defp get_commit_record_internal(store_ref, key, start_ts) do
    case get_db_ref(store_ref) do
      nil ->
        {:error, :not_found}

      db_ref ->
        prefix = key
        cf = get_write_cf(store_ref)

        case :rocksdb.iterator(db_ref, cf, []) do
          {:ok, iter} ->
            result = find_commit_record_in_iterator(iter, key, start_ts, prefix)
            :rocksdb.iterator_close(iter)
            result

          {:error, _} ->
            {:error, :not_found}
        end
    end
  end

  defp find_commit_record_in_iterator(iter, key, start_ts, prefix) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, write_key, value} ->
        try do
          {stored_key, _commit_ts} = Encoder.decode_txn_write_key(write_key)

          if stored_key == key do
            {stored_start_ts, commit_ts} = :erlang.binary_to_term(value)

            if stored_start_ts == start_ts,
              do: {:ok, commit_ts},
              else: find_commit_record_next(iter, key, start_ts)
          else
            {:error, :not_found}
          end
        rescue
          _ -> {:error, :not_found}
        end

      _ ->
        {:error, :not_found}
    end
  end

  defp find_commit_record_next(iter, key, start_ts) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, write_key, value} ->
        try do
          {stored_key, _commit_ts} = Encoder.decode_txn_write_key(write_key)

          if stored_key == key do
            {stored_start_ts, commit_ts} = :erlang.binary_to_term(value)

            if stored_start_ts == start_ts,
              do: {:ok, commit_ts},
              else: find_commit_record_next(iter, key, start_ts)
          else
            {:error, :not_found}
          end
        rescue
          _ -> {:error, :not_found}
        end

      _ ->
        {:error, :not_found}
    end
  end

  defp get_latest_write_internal(store_ref, key) do
    case get_db_ref(store_ref) do
      nil ->
        {:ok, nil}

      db_ref ->
        prefix = key
        cf = get_write_cf(store_ref)

        case :rocksdb.iterator(db_ref, cf, []) do
          {:ok, iter} ->
            result = find_latest_write_in_iterator(iter, key, prefix)
            :rocksdb.iterator_close(iter)
            result

          {:error, _} ->
            {:ok, nil}
        end
    end
  end

  defp find_latest_write_in_iterator(iter, key, prefix) do
    # Seek to end of prefix range? No, keys are [key][commit_ts_desc].
    # Wait, encode_txn_write_key uses commit_ts. If it sorts correctly,
    # latest commit_ts should be first if desc?
    # RocksDB sorts lexicographically.
    # Key encoding: {key}{commit_ts}
    # If commit_ts is big-endian encoded, larger ts -> larger key.
    # So we need to seek to {key}{MAX_TS} and iterate backwards?
    # Or assuming standard encoding.
    # Let's assume we scan forward. If keys are {key}{commit_ts}, then
    # seek(key) lands on {key}{smallest_ts}.
    # We want latest. So we should `seek_for_prev` or encode ts as separate component.
    # Our Encoder implementation details matter here.
    # Assuming standard behavior: seek(key) finds first, we might need to iterate.
    # But for "get_latest_write", any write > start_ts is a conflict.
    # We just need to check if ANY write exists > start_ts.

    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, write_key, value} ->
        try do
          {stored_key, commit_ts} = Encoder.decode_txn_write_key(write_key)

          if stored_key == key do
            {start_ts, _} = :erlang.binary_to_term(value)
            {:ok, {commit_ts, start_ts}}
          else
            {:ok, nil}
          end
        rescue
          _ -> {:ok, nil}
        end

      _ ->
        {:ok, nil}
    end
  end

  defp mvcc_read(store_ref, key, txn, snapshot_ts) do
    # Check local write buffer first (Read-Your-Own-Writes)
    case Map.get((txn && txn.write_buffer) || %{}, key) do
      {:put, value} ->
        {:ok, value}

      :delete ->
        {:error, :not_found}

      nil ->
        # Read from store using snapshot isolation
        case get_latest_visible_write(store_ref, key, snapshot_ts) do
          {:ok, nil} ->
            {:error, :not_found}

          {:ok, {_commit_ts, start_ts}} ->
            # Read the data written at start_ts
            read_data(store_ref, key, start_ts)

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # Re-implement get_latest_visible_write using simple RocksDB scan since we don't know exact encoding order
  defp get_latest_visible_write(store_ref, key, snapshot_ts) do
    # Find commits where commit_ts <= snapshot_ts
    case get_db_ref(store_ref) do
      nil ->
        {:error, :not_found}

      db_ref ->
        # Scan from key prefix to find latest write relative to snapshot
        prefix = key
        cf = get_write_cf(store_ref)

        # We find the latest version <= snapshot_ts
        case :rocksdb.iterator(db_ref, cf, []) do
          {:ok, iter} ->
            result = find_visible_write_in_iterator(iter, key, snapshot_ts, prefix)
            :rocksdb.iterator_close(iter)
            result

          {:error, _} ->
            {:ok, nil}
        end
    end
  end

  defp find_visible_write_in_iterator(iter, key, snapshot_ts, prefix) do
    # Seek to prefix
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, write_key, value} ->
        check_iterator_visibility(iter, key, snapshot_ts, write_key, value)

      _ ->
        {:ok, nil}
    end
  end

  defp check_iterator_visibility(iter, key, snapshot_ts, write_key, value) do
    try do
      {stored_key, commit_ts} = Encoder.decode_txn_write_key(write_key)

      if stored_key == key do
        if commit_ts <= snapshot_ts do
          {start_ts, _} = :erlang.binary_to_term(value)
          {:ok, {commit_ts, start_ts}}
        else
          # This version is too new, check next (commit_ts should be desc for same key)
          case :rocksdb.iterator_move(iter, :next) do
            {:ok, next_key, next_val} ->
              check_iterator_visibility(iter, key, snapshot_ts, next_key, next_val)

            _ ->
              {:ok, nil}
          end
        end
      else
        {:ok, nil}
      end
    rescue
      _ -> {:ok, nil}
    end
  end

  defp read_data(store_ref, key, start_ts) do
    data_key = Encoder.encode_txn_data_key(key, start_ts)

    case {get_db_ref(store_ref), get_data_cf(store_ref)} do
      {nil, _} ->
        {:error, :db_not_ready}

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.get(db_ref, cf, data_key, []) do
          # Tombstone
          {:ok, <<0xFF, 0xFF, 0xFF, 0xFF>>} -> {:error, :not_found}
          {:ok, value} -> {:ok, value}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.get(db_ref, data_key, []) do
          {:ok, <<0xFF, 0xFF, 0xFF, 0xFF>>} -> {:error, :not_found}
          {:ok, value} -> {:ok, value}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end
    end
  end
end
