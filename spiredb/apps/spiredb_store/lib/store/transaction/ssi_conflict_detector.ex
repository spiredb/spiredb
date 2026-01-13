defmodule Store.Transaction.SSIConflictDetector do
  @moduledoc """
  Serializable Snapshot Isolation conflict detection.

  Implements PostgreSQL-style SSI with:
  - Read-write conflict tracking (SIREAD locks)
  - Write-read conflict detection
  - Dangerous structure (rw-antidependency cycle) detection

  Based on "Serializable Snapshot Isolation in PostgreSQL" (Ports & Grittner, VLDB 2012)
  """

  require Logger
  alias Store.Transaction
  alias Store.Transaction.Executor

  # ETS table for tracking SIREAD locks (read predicates)
  @siread_table :ssi_siread_locks
  # ETS table for tracking in-flight SERIALIZABLE transactions
  @active_txns_table :ssi_active_txns
  # ETS table for rw-conflict edges
  @conflict_table :ssi_conflicts

  @doc """
  Initialize SSI tracking tables.
  """
  def init do
    ensure_table(@siread_table, [:named_table, :public, :bag])
    ensure_table(@active_txns_table, [:named_table, :public, :set])
    ensure_table(@conflict_table, [:named_table, :public, :bag])
    :ok
  end

  defp ensure_table(name, opts) do
    case :ets.whereis(name) do
      :undefined -> :ets.new(name, opts)
      _ -> name
    end
  end

  @doc """
  Register a SERIALIZABLE transaction.
  """
  def register_transaction(%Transaction{isolation: :serializable} = txn) do
    init()
    :ets.insert(@active_txns_table, {txn.id, txn.start_ts, :active})
    :ok
  end

  def register_transaction(_txn), do: :ok

  @doc """
  Record a read operation for SSI tracking.
  Creates SIREAD lock on the key.
  """
  def record_read(store_ref, %Transaction{isolation: :serializable} = txn, key, read_ts) do
    init()
    # Insert SIREAD lock: {key, txn_id, start_ts, read_ts}
    :ets.insert(@siread_table, {key, txn.id, txn.start_ts, read_ts})

    # Check for rw-conflict: Did a concurrent txn write to this key?
    check_write_before_our_read(store_ref, txn, key)
  end

  def record_read(_store_ref, _txn, _key, _read_ts), do: :ok

  @doc """
  Record a write operation for SSI tracking.
  Checks for wr-conflict with concurrent readers.
  """
  def record_write(%Transaction{isolation: :serializable} = txn, key) do
    init()
    # Check for wr-conflict: Did a concurrent txn read this key?
    check_read_before_our_write(txn, key)
  end

  def record_write(_txn, _key), do: :ok

  @doc """
  Validate transaction can commit without causing serialization anomaly.

  Returns :ok or {:error, {:serialization_failure, reason}}
  """
  def validate_commit(store_ref, %Transaction{isolation: :serializable} = txn) do
    Logger.debug("SSI validating commit for txn #{txn.id}")

    with :ok <- check_read_set_modified(store_ref, txn),
         :ok <- check_dangerous_structure(txn) do
      :ok
    end
  end

  def validate_commit(_store_ref, _txn), do: :ok

  @doc """
  Clean up SSI tracking for committed/aborted transaction.
  """
  def cleanup_transaction(txn_id) do
    init()
    :ets.delete(@active_txns_table, txn_id)

    spawn(fn ->
      Process.sleep(5000)

      try do
        :ets.match_delete(@siread_table, {:_, txn_id, :_, :_})
        :ets.match_delete(@conflict_table, {:_, txn_id, :_, :_})
        :ets.match_delete(@conflict_table, {txn_id, :_, :_, :_})
      rescue
        ArgumentError -> :ok
      end
    end)

    :ok
  end

  ## Private Functions

  defp check_write_before_our_read(store_ref, txn, key) do
    case Executor.scan_writes_after(store_ref, key, txn.start_ts) do
      {:ok, commits} ->
        writers =
          Enum.filter(commits, fn {_commit_ts, start_ts} ->
            start_ts < txn.start_ts
          end)

        Enum.each(writers, fn {_commit_ts, start_ts} ->
          writer_id = get_txn_id_for_start_ts(start_ts)
          :ets.insert(@conflict_table, {txn.id, writer_id, :rw, key})
        end)

        :ok

      {:error, _} ->
        :ok
    end
  end

  defp check_read_before_our_write(txn, key) do
    readers = :ets.match(@siread_table, {key, :"$1", :"$2", :_})

    concurrent_readers =
      Enum.filter(readers, fn [reader_id, reader_start_ts] ->
        reader_id != txn.id and reader_start_ts < txn.start_ts
      end)

    Enum.each(concurrent_readers, fn [reader_id, _] ->
      :ets.insert(@conflict_table, {reader_id, txn.id, :rw, key})
    end)

    :ok
  end

  defp check_read_set_modified(store_ref, txn) do
    # read_set is a MapSet of keys
    Enum.reduce_while(txn.read_set, :ok, fn key, :ok ->
      case key_modified_after?(store_ref, key, txn.start_ts) do
        false -> {:cont, :ok}
        true -> {:halt, {:error, {:serialization_failure, {:read_conflict, key}}}}
      end
    end)
  end

  defp check_dangerous_structure(txn) do
    # Get incoming rw-antidependency edges (other txns that read data we wrote)
    incoming =
      :ets.match(@conflict_table, {:"$1", txn.id, :rw, :_})
      |> List.flatten()
      |> Enum.uniq()

    Enum.reduce_while(incoming, :ok, fn in_txn, :ok ->
      in_txn_outgoing =
        :ets.match(@conflict_table, {in_txn, :"$1", :rw, :_})
        |> List.flatten()
        |> Enum.uniq()

      cycle_candidates =
        MapSet.intersection(MapSet.new(in_txn_outgoing), MapSet.new(incoming))

      if MapSet.size(cycle_candidates) > 0 do
        dangerous =
          Enum.any?(cycle_candidates, fn candidate ->
            is_committed_concurrent?(candidate, txn.start_ts)
          end)

        if dangerous do
          Logger.warning("SSI: dangerous structure detected for #{txn.id}")
          {:halt, {:error, {:serialization_failure, :write_skew}}}
        else
          {:cont, :ok}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp key_modified_after?(store_ref, key, ts) do
    case Executor.get_latest_write(store_ref, key) do
      {:ok, nil} -> false
      {:ok, {commit_ts, _start_ts}} -> commit_ts > ts
      {:error, _} -> false
    end
  end

  defp is_committed_concurrent?(txn_id, our_start_ts) do
    case :ets.lookup(@active_txns_table, txn_id) do
      [{_, start_ts, :committed}] when start_ts < our_start_ts -> true
      _ -> false
    end
  end

  defp get_txn_id_for_start_ts(start_ts) do
    case :ets.match(@active_txns_table, {:"$1", start_ts, :_}) do
      [[txn_id]] -> txn_id
      _ -> "unknown_#{start_ts}"
    end
  end
end
