defmodule Store.Transaction.CrashRecovery do
  @moduledoc """
  Crash recovery for pending transactions.

  On startup, scans the `txn_pending` column family for transactions
  that were in-progress when the node crashed and completes them.
  """

  require Logger
  alias Store.Transaction.Executor

  @pending_cf "txn_pending"

  @doc """
  Run crash recovery on startup.
  Scans txn_pending CF and completes or rolls back pending transactions.
  """
  def recover_pending_commits(store_ref) do
    Logger.info("CrashRecovery: scanning for pending commits...")

    case get_pending_commits(store_ref) do
      {:ok, pending} when map_size(pending) == 0 ->
        Logger.info("CrashRecovery: no pending commits found")
        :ok

      {:ok, pending} ->
        Logger.info("CrashRecovery: found #{map_size(pending)} pending commits")

        Enum.each(pending, fn {txn_id, entry} ->
          recover_transaction(store_ref, txn_id, entry)
        end)

        Logger.info("CrashRecovery: completed")
        :ok

      {:error, reason} ->
        Logger.warning("CrashRecovery: failed to scan pending commits: #{inspect(reason)}")
        :ok
    end
  end

  @doc """
  Write a pending commit record before committing.
  Used to track in-flight commits for crash recovery.
  """
  def write_pending_commit(store_ref, txn_id, start_ts, commit_ts, keys) do
    case {get_db_ref(store_ref), get_pending_cf(store_ref)} do
      {nil, _} ->
        :ok

      {_, nil} ->
        :ok

      {db_ref, cf} ->
        key = encode_pending_key(txn_id)
        value = encode_pending_value(start_ts, commit_ts, keys)
        :rocksdb.put(db_ref, cf, key, value, [])
    end
  end

  @doc """
  Remove pending commit record after successful commit.
  """
  def clear_pending_commit(store_ref, txn_id) do
    case {get_db_ref(store_ref), get_pending_cf(store_ref)} do
      {nil, _} ->
        :ok

      {_, nil} ->
        :ok

      {db_ref, cf} ->
        key = encode_pending_key(txn_id)
        :rocksdb.delete(db_ref, cf, key, [])
    end
  end

  ## Private

  defp get_pending_commits(store_ref) do
    case {get_db_ref(store_ref), get_pending_cf(store_ref)} do
      {nil, _} -> {:ok, %{}}
      {_, nil} -> {:ok, %{}}
      {db_ref, cf} -> scan_pending_cf(db_ref, cf)
    end
  end

  defp scan_pending_cf(db_ref, cf) do
    case :rocksdb.iterator(db_ref, cf, []) do
      {:ok, iter} ->
        result = scan_pending_iter(iter, %{})
        :rocksdb.iterator_close(iter)
        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    e -> {:error, e}
  end

  defp scan_pending_iter(iter, acc) do
    case :rocksdb.iterator_move(iter, :first) do
      {:ok, key, value} ->
        acc = process_pending_entry(key, value, acc)
        scan_pending_next(iter, acc)

      {:error, :invalid_iterator} ->
        acc
    end
  end

  defp scan_pending_next(iter, acc) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} ->
        acc = process_pending_entry(key, value, acc)
        scan_pending_next(iter, acc)

      {:error, :invalid_iterator} ->
        acc
    end
  end

  defp process_pending_entry(key, value, acc) do
    case decode_pending_key(key) do
      {:ok, txn_id} ->
        case decode_pending_value(value) do
          {:ok, entry} -> Map.put(acc, txn_id, entry)
          {:error, _} -> acc
        end

      {:error, _} ->
        acc
    end
  end

  defp recover_transaction(store_ref, txn_id, entry) do
    Logger.info("CrashRecovery: recovering transaction #{txn_id}")

    # Check if all keys are committed
    all_committed =
      Enum.all?(entry.keys, fn key ->
        case Executor.get_commit_record(store_ref, key, entry.start_ts) do
          {:ok, _} -> true
          {:error, :not_found} -> false
        end
      end)

    if all_committed do
      # Already fully committed - just cleanup locks and pending record
      Logger.info("CrashRecovery: txn #{txn_id} already committed, cleaning up")
      cleanup_locks(store_ref, entry)
      clear_pending_commit(store_ref, txn_id)
    else
      # Need to complete the commit
      Logger.info("CrashRecovery: completing commit for txn #{txn_id}")
      complete_commit(store_ref, txn_id, entry)
    end
  end

  defp complete_commit(store_ref, txn_id, entry) do
    # Write commit records for all keys
    Enum.each(entry.keys, fn key ->
      case Executor.get_commit_record(store_ref, key, entry.start_ts) do
        {:ok, _} ->
          # Already committed
          :ok

        {:error, :not_found} ->
          # Write commit record
          Executor.write_commit_record(store_ref, key, entry.start_ts, entry.commit_ts)
      end
    end)

    # Cleanup locks
    cleanup_locks(store_ref, entry)

    # Clear pending record
    clear_pending_commit(store_ref, txn_id)
  end

  defp cleanup_locks(store_ref, entry) do
    Enum.each(entry.keys, fn key ->
      Executor.delete_lock(store_ref, key, entry.start_ts)
    end)
  end

  # Encoding helpers

  defp encode_pending_key(txn_id) when is_binary(txn_id) do
    "pending:" <> txn_id
  end

  defp decode_pending_key(<<"pending:", txn_id::binary>>) do
    {:ok, txn_id}
  end

  defp decode_pending_key(_), do: {:error, :invalid_key}

  defp encode_pending_value(start_ts, commit_ts, keys) do
    keys_count = length(keys)

    keys_bin =
      Enum.map(keys, fn k -> <<byte_size(k)::16, k::binary>> end)
      |> IO.iodata_to_binary()

    <<start_ts::64, commit_ts::64, keys_count::32, keys_bin::binary>>
  end

  defp decode_pending_value(<<start_ts::64, commit_ts::64, keys_count::32, keys_bin::binary>>) do
    case decode_keys(keys_bin, keys_count, []) do
      {:ok, keys} ->
        {:ok, %{start_ts: start_ts, commit_ts: commit_ts, keys: keys}}

      {:error, _} = err ->
        err
    end
  end

  defp decode_pending_value(_), do: {:error, :invalid_value}

  defp decode_keys(_bin, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_keys(<<len::16, key::binary-size(len), rest::binary>>, count, acc) do
    decode_keys(rest, count - 1, [key | acc])
  end

  defp decode_keys(_, _, _), do: {:error, :invalid_keys}

  # RocksDB helpers

  defp get_db_ref(%{db: db}), do: db
  defp get_db_ref(_), do: nil

  defp get_pending_cf(%{cfs: cfs}), do: Map.get(cfs, @pending_cf)
  defp get_pending_cf(_), do: nil
end
