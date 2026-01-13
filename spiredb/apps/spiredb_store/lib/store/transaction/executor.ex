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
  def mvcc_get(key, start_ts) do
    # Check for existing lock
    case get_lock(key) do
      {:ok, nil} ->
        # No lock, read committed value
        read_committed_value(key, start_ts)

      {:ok, lock} ->
        # Key is locked, try to resolve
        case resolve_lock(lock) do
          :resolved ->
            # Lock was resolved, retry read
            read_committed_value(key, start_ts)

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
  def commit(%Transaction{write_buffer: buffer} = txn) when map_size(buffer) == 0 do
    # Empty transaction, nothing to commit
    {:ok, txn.start_ts}
  end

  def commit(%Transaction{} = txn) do
    Logger.debug("Committing transaction #{txn.id} with #{map_size(txn.write_buffer)} mutations")

    with :ok <- prewrite_phase(txn),
         {:ok, commit_ts} <- commit_phase(txn) do
      # Spawn async cleanup of secondary locks
      spawn_async_cleanup(txn, commit_ts)
      {:ok, commit_ts}
    else
      {:error, {:prewrite_failed, key, reason}} ->
        Logger.warning("Prewrite failed for key #{inspect(key)}: #{inspect(reason)}")
        cleanup(txn)
        {:error, {:conflict, key}}

      {:error, reason} ->
        cleanup(txn)
        {:error, reason}
    end
  end

  @doc """
  Clean up locks from a rolled back or failed transaction.
  """
  def cleanup(%Transaction{} = txn) do
    Logger.debug("Cleaning up transaction #{txn.id}")

    Enum.each(txn.write_buffer, fn {key, _} ->
      delete_lock(key, txn.start_ts)
      delete_data(key, txn.start_ts)
    end)

    :ok
  end

  # ============================================================================
  # Public API for gRPC TransactionService
  # ============================================================================

  @doc """
  Prewrite a single mutation (for gRPC).
  """
  def prewrite_single(key, mutation, request) do
    with :ok <- check_write_conflict(key, request.start_ts),
         :ok <- check_lock_conflict(key, request.start_ts) do
      # Create lock
      lock =
        Lock.new(key, request.primary_key, request.start_ts,
          ttl: request.lock_ttl,
          lock_type: if(request.is_pessimistic, do: :pessimistic, else: :prewrite)
        )

      with :ok <- do_write_lock(key, lock) do
        # Write data
        case mutation.type do
          :MUTATION_PUT -> write_data(key, {:put, mutation.value}, request.start_ts)
          :MUTATION_DELETE -> write_data(key, :delete, request.start_ts)
          _ -> :ok
        end
      end
    end
  end

  @doc """
  Commit primary key (for gRPC).
  """
  def commit_primary(primary_key, start_ts, commit_ts) do
    write_commit_record(primary_key, start_ts, commit_ts)
  end

  @doc """
  Commit secondary key (for gRPC).
  """
  def commit_secondary(key, start_ts, commit_ts) do
    write_commit_record(key, start_ts, commit_ts)
    delete_lock(key, start_ts)
    :ok
  end

  @doc """
  Rollback a key (for gRPC).
  """
  def rollback_key(key, start_ts) do
    delete_lock(key, start_ts)
    delete_data(key, start_ts)
    :ok
  end

  @doc """
  Check transaction status by primary key (for gRPC).
  """
  def check_txn_status(primary_key, start_ts) do
    case get_commit_record(primary_key, start_ts) do
      {:ok, commit_ts} ->
        {:committed, commit_ts}

      {:error, :not_found} ->
        case get_lock(primary_key) do
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
  def acquire_pessimistic_lock(key, start_ts, _for_update_ts, lock_ttl) do
    case get_lock(key) do
      {:ok, nil} ->
        lock =
          Lock.new(key, key, start_ts,
            ttl: lock_ttl,
            lock_type: :pessimistic
          )

        do_write_lock(key, lock)

      {:ok, lock} ->
        if Lock.expired?(lock) do
          delete_lock(key, lock.start_ts)

          new_lock =
            Lock.new(key, key, start_ts,
              ttl: lock_ttl,
              lock_type: :pessimistic
            )

          do_write_lock(key, new_lock)
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
  def get_lock(key) do
    get_lock_internal(key)
  end

  @doc """
  Delete lock for a key (public API for async commit).
  """
  def delete_lock(key, start_ts) do
    delete_lock_internal(key, start_ts)
  end

  @doc """
  Delete data for a key (public API for rollback).
  """
  def delete_data(key, start_ts) do
    delete_data_internal(key, start_ts)
  end

  @doc """
  Write commit record (public API for async commit).
  """
  def write_commit_record(key, start_ts, commit_ts) do
    write_commit_record_internal(key, start_ts, commit_ts)
  end

  @doc """
  Get commit record (public API for async commit resolution).
  """
  def get_commit_record(key, start_ts) do
    get_commit_record_internal(key, start_ts)
  end

  @doc """
  Prewrite with a pre-built lock (for async commit).
  Used when lock already contains secondaries list.
  """
  def prewrite_with_lock(key, operation, lock, start_ts) do
    with :ok <- check_write_conflict(key, start_ts),
         :ok <- check_lock_conflict(key, start_ts),
         :ok <- do_write_lock(key, lock),
         :ok <- write_data(key, operation, start_ts) do
      :ok
    end
  end

  @doc """
  Write rollback record to prevent late prewrites.
  """
  def write_rollback_record(key, start_ts) do
    # Use write CF with special marker
    write_key = Encoder.encode_txn_write_key(key, start_ts)
    # Rollback marker: {start_ts, 0} where 0 indicates rollback
    value = :erlang.term_to_binary({start_ts, 0, :rollback})

    case {get_db_ref(), get_write_cf()} do
      {nil, _} ->
        try do
          :ets.insert(:txn_write, {write_key, value})
          :ok
        rescue
          ArgumentError ->
            :ets.new(:txn_write, [:named_table, :public, :ordered_set])
            :ets.insert(:txn_write, {write_key, value})
            :ok
        end

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
  def scan_writes_after(key, after_ts) do
    try do
      records = :ets.tab2list(:txn_write)

      commits =
        records
        |> Enum.filter(fn {write_key, _value} ->
          {stored_key, commit_ts} = Encoder.decode_txn_write_key(write_key)
          stored_key == key and commit_ts > after_ts
        end)
        |> Enum.map(fn {_write_key, value} ->
          case :erlang.binary_to_term(value) do
            {start_ts, commit_ts} -> {commit_ts, start_ts}
            {start_ts, commit_ts, _} -> {commit_ts, start_ts}
          end
        end)
        |> Enum.sort_by(fn {commit_ts, _} -> commit_ts end, :desc)

      {:ok, commits}
    rescue
      _ -> {:ok, []}
    end
  end

  @doc """
  Get latest write for a key (public API for SSI).
  """
  def get_latest_write(key) do
    get_latest_write_internal(key)
  end

  defp do_write_lock(key, lock) do
    try do
      :ets.insert(:txn_locks, {key, Lock.encode(lock)})
      :ok
    rescue
      ArgumentError ->
        :ets.new(:txn_locks, [:named_table, :public, :set])
        :ets.insert(:txn_locks, {key, Lock.encode(lock)})
        :ok
    end
  end

  # ============================================================================
  # Prewrite Phase
  # ============================================================================

  defp prewrite_phase(%Transaction{} = txn) do
    primary = txn.primary_key
    secondaries = Transaction.secondary_keys(txn)

    # Prewrite primary first
    case prewrite_key(txn, primary, true) do
      :ok ->
        # Prewrite secondaries in parallel
        results =
          secondaries
          |> Task.async_stream(fn key -> prewrite_key(txn, key, false) end,
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

  defp prewrite_key(txn, key, _is_primary) do
    mutation = Map.get(txn.write_buffer, key)

    with :ok <- check_write_conflict(key, txn.start_ts),
         :ok <- check_lock_conflict(key, txn.start_ts),
         :ok <- write_lock(key, txn),
         :ok <- write_data(key, mutation, txn.start_ts) do
      :ok
    end
  end

  defp check_write_conflict(key, start_ts) do
    # Check if there's a commit after our start_ts
    case get_latest_write(key) do
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

  defp check_lock_conflict(key, _start_ts) do
    case get_lock(key) do
      {:ok, nil} ->
        :ok

      {:ok, lock} ->
        # Another transaction has this key locked
        if Lock.expired?(lock) do
          # Try to clean up expired lock
          resolve_lock(lock)
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

  defp commit_phase(%Transaction{} = txn) do
    # Get commit timestamp
    case PD.TSO.get_timestamp() do
      {:ok, commit_ts} ->
        # Write commit record for primary key only
        case write_commit_record(txn.primary_key, txn.start_ts, commit_ts) do
          :ok -> {:ok, commit_ts}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, {:tso_error, reason}}
    end
  end

  defp spawn_async_cleanup(txn, commit_ts) do
    Task.start(fn ->
      # Small delay to let primary commit propagate
      Process.sleep(10)

      # Commit secondary keys and remove locks
      Enum.each(Transaction.secondary_keys(txn), fn key ->
        write_commit_record(key, txn.start_ts, commit_ts)
        delete_lock(key, txn.start_ts)
      end)

      # Delete primary lock
      delete_lock(txn.primary_key, txn.start_ts)

      Logger.debug("Async cleanup completed for txn #{txn.id}")
    end)
  end

  # ============================================================================
  # Lock Resolution
  # ============================================================================

  @doc """
  Resolve a lock by checking the primary key's commit status.
  """
  def resolve_lock(%Lock{} = lock) do
    if Lock.expired?(lock) do
      # Lock expired, roll it back
      rollback_expired_lock(lock)
      :resolved
    else
      # Check primary key status
      case get_primary_status(lock.primary_key, lock.start_ts) do
        :committed ->
          # Primary was committed, commit this secondary too
          commit_from_primary(lock)
          :resolved

        :rolled_back ->
          # Primary was rolled back, clean up this lock
          delete_lock(lock.key, lock.start_ts)
          :resolved

        :pending ->
          # Primary still pending, wait
          {:wait, lock.ttl - elapsed_since_lock(lock)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp get_primary_status(primary_key, start_ts) do
    # Check if primary has a commit record
    case get_commit_record(primary_key, start_ts) do
      {:ok, _commit_ts} ->
        :committed

      {:error, :not_found} ->
        # Check if primary still has a lock
        case get_lock(primary_key) do
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

  defp commit_from_primary(lock) do
    # Find the commit_ts from a write record
    case get_commit_record(lock.primary_key, lock.start_ts) do
      {:ok, commit_ts} ->
        write_commit_record(lock.key, lock.start_ts, commit_ts)
        delete_lock(lock.key, lock.start_ts)

      _ ->
        :ok
    end
  end

  defp rollback_expired_lock(lock) do
    delete_lock(lock.key, lock.start_ts)
    delete_data(lock.key, lock.start_ts)
  end

  defp elapsed_since_lock(lock) do
    now = System.os_time(:millisecond)
    lock_time = div(lock.start_ts, 1000)
    now - lock_time
  end

  # ============================================================================
  # Storage Operations (RocksDB with ETS fallback for tests)
  # ============================================================================

  defp get_db_ref do
    # Try to get RocksDB from persistent_term, fallback to ETS for tests
    :persistent_term.get(:spiredb_rocksdb_ref, nil)
  end

  defp get_cf(cf_name) do
    case :persistent_term.get(:spiredb_rocksdb_cf_map, nil) do
      nil -> nil
      cf_map -> Map.get(cf_map, cf_name)
    end
  end

  defp get_locks_cf, do: get_cf(@cf_locks)
  defp get_data_cf, do: get_cf(@cf_data)
  defp get_write_cf, do: get_cf(@cf_write)

  defp get_lock_internal(key) do
    # Use RocksDB if available, otherwise ETS
    lock_key = Encoder.encode_lock_key(key)

    case {get_db_ref(), get_locks_cf()} do
      {nil, _} ->
        # Fallback to ETS for tests
        case :ets.lookup(:txn_locks, key) do
          [{^key, lock_data}] -> Lock.decode(key, lock_data)
          [] -> {:ok, nil}
        end

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.get(db_ref, cf, lock_key, []) do
          {:ok, lock_data} -> Lock.decode(key, lock_data)
          :not_found -> {:ok, nil}
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.get(db_ref, lock_key, []) do
          {:ok, lock_data} -> Lock.decode(key, lock_data)
          :not_found -> {:ok, nil}
          {:error, reason} -> {:error, reason}
        end
    end
  rescue
    # ETS table doesn't exist
    ArgumentError -> {:ok, nil}
  end

  defp write_lock(key, txn) do
    lock = Lock.new(key, txn.primary_key, txn.start_ts, txn_id: txn.id)
    lock_key = Encoder.encode_lock_key(key)
    lock_data = Lock.encode(lock)

    case {get_db_ref(), get_locks_cf()} do
      {nil, _} ->
        # Fallback to ETS for tests
        try do
          :ets.insert(:txn_locks, {key, lock_data})
          :ok
        rescue
          ArgumentError ->
            :ets.new(:txn_locks, [:named_table, :public, :set])
            :ets.insert(:txn_locks, {key, lock_data})
            :ok
        end

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

  defp delete_lock_internal(key, _start_ts) do
    lock_key = Encoder.encode_lock_key(key)

    case {get_db_ref(), get_locks_cf()} do
      {nil, _} ->
        # Fallback to ETS for tests
        try do
          :ets.delete(:txn_locks, key)
          :ok
        rescue
          ArgumentError -> :ok
        end

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.delete(db_ref, cf, lock_key, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.delete(db_ref, lock_key, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp write_data(key, {:put, value}, start_ts) do
    data_key = Encoder.encode_txn_data_key(key, start_ts)

    case {get_db_ref(), get_data_cf()} do
      {nil, _} ->
        # Fallback to ETS for tests
        try do
          :ets.insert(:txn_data, {data_key, value})
          :ok
        rescue
          ArgumentError ->
            :ets.new(:txn_data, [:named_table, :public, :ordered_set])
            :ets.insert(:txn_data, {data_key, value})
            :ok
        end

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.put(db_ref, cf, data_key, value, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.put(db_ref, data_key, value, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp write_data(key, :delete, start_ts) do
    # For deletes, we write a tombstone marker
    data_key = Encoder.encode_txn_data_key(key, start_ts)
    # Tombstone marker
    tombstone = <<0xFF, 0xFF, 0xFF, 0xFF>>

    case {get_db_ref(), get_data_cf()} do
      {nil, _} ->
        # Fallback to ETS for tests
        try do
          :ets.insert(:txn_data, {data_key, :tombstone})
          :ok
        rescue
          ArgumentError ->
            :ets.new(:txn_data, [:named_table, :public, :ordered_set])
            :ets.insert(:txn_data, {data_key, :tombstone})
            :ok
        end

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.put(db_ref, cf, data_key, tombstone, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.put(db_ref, data_key, tombstone, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp delete_data_internal(key, start_ts) do
    data_key = Encoder.encode_txn_data_key(key, start_ts)

    case {get_db_ref(), get_data_cf()} do
      {nil, _} ->
        try do
          :ets.delete(:txn_data, data_key)
          :ok
        rescue
          ArgumentError -> :ok
        end

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.delete(db_ref, cf, data_key, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {db_ref, nil} ->
        case :rocksdb.delete(db_ref, data_key, []) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp write_commit_record_internal(key, start_ts, commit_ts) do
    write_key = Encoder.encode_txn_write_key(key, commit_ts)
    value = :erlang.term_to_binary({start_ts, commit_ts})

    case {get_db_ref(), get_write_cf()} do
      {nil, _} ->
        try do
          :ets.insert(:txn_write, {write_key, value})
          :ok
        rescue
          ArgumentError ->
            :ets.new(:txn_write, [:named_table, :public, :ordered_set])
            :ets.insert(:txn_write, {write_key, value})
            :ok
        end

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

  defp get_commit_record_internal(key, start_ts) do
    # Find commit record for this key/start_ts combination
    case get_db_ref() do
      nil ->
        # Fallback to ETS
        try do
          records = :ets.tab2list(:txn_write)

          matching =
            Enum.find(records, fn {write_key, value} ->
              {stored_key, _commit_ts} = Encoder.decode_txn_write_key(write_key)

              if stored_key == key do
                {stored_start_ts, _} = :erlang.binary_to_term(value)
                stored_start_ts == start_ts
              else
                false
              end
            end)

          case matching do
            {_write_key, value} ->
              {_stored_start_ts, commit_ts} = :erlang.binary_to_term(value)
              {:ok, commit_ts}

            nil ->
              {:error, :not_found}
          end
        rescue
          _ -> {:error, :not_found}
        end

      db_ref ->
        # Scan RocksDB with prefix for this key
        # Use iterator to find matching record
        prefix = key

        case :rocksdb.iterator(db_ref, [{:prefix_same_as_start, true}]) do
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

            if stored_start_ts == start_ts do
              {:ok, commit_ts}
            else
              # Continue scanning
              find_commit_record_next(iter, key, start_ts)
            end
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

            if stored_start_ts == start_ts do
              {:ok, commit_ts}
            else
              find_commit_record_next(iter, key, start_ts)
            end
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

  defp get_latest_write_internal(key) do
    case get_db_ref() do
      nil ->
        # Fallback to ETS
        try do
          case :ets.match(:txn_write, {{key, :"$1"}, :"$2"}) do
            [[commit_ts, data] | _] ->
              {start_ts, _} = :erlang.binary_to_term(data)
              {:ok, {commit_ts, start_ts}}

            [] ->
              {:ok, nil}
          end
        rescue
          ArgumentError -> {:ok, nil}
        end

      db_ref ->
        # Scan from key prefix to find latest write
        prefix = key

        case :rocksdb.iterator(db_ref, [{:prefix_same_as_start, true}]) do
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

  defp read_committed_value(key, snapshot_ts) do
    # Find the latest committed version <= snapshot_ts
    case get_latest_visible_write(key, snapshot_ts) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, {_commit_ts, start_ts}} ->
        # Read the data written at start_ts
        read_data(key, start_ts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_latest_visible_write(key, snapshot_ts) do
    # Find commits where commit_ts <= snapshot_ts
    try do
      results = :ets.match(:txn_write, {{key, :"$1"}, :"$2"})

      visible =
        results
        |> Enum.map(fn [commit_ts, data] ->
          {start_ts, _} = :erlang.binary_to_term(data)
          {commit_ts, start_ts}
        end)
        |> Enum.filter(fn {commit_ts, _} -> commit_ts <= snapshot_ts end)
        |> Enum.sort_by(fn {commit_ts, _} -> commit_ts end, :desc)

      case visible do
        [latest | _] -> {:ok, latest}
        [] -> {:ok, nil}
      end
    rescue
      ArgumentError -> {:ok, nil}
    end
  end

  defp read_data(key, start_ts) do
    data_key = Encoder.encode_txn_data_key(key, start_ts)

    try do
      case :ets.lookup(:txn_data, data_key) do
        [{^data_key, :tombstone}] -> {:error, :not_found}
        [{^data_key, value}] -> {:ok, value}
        [] -> {:error, :not_found}
      end
    rescue
      ArgumentError -> {:error, :not_found}
    end
  end
end
