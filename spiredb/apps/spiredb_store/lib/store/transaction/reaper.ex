defmodule Store.Transaction.Reaper do
  @moduledoc """
  Background process to clean up timed-out transactions.
  Sow the pain, kill the sorrow.

  - Scans active transactions periodically
  - Rolls back transactions exceeding timeout
  - Cleans up orphaned locks
  """

  use GenServer

  require Logger

  # Check every 5 seconds
  @reap_interval 5_000
  # Scan for orphaned locks every minute
  @lock_scan_interval 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Force rollback a transaction (called by Manager).
  """
  def force_rollback(txn_id, reason) do
    GenServer.cast(__MODULE__, {:force_rollback, txn_id, reason})
  end

  ## Callbacks

  @impl true
  def init(_opts) do
    Logger.info("Transaction Reaper started")
    schedule_reap()
    schedule_lock_scan()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:reap, state) do
    reap_timed_out_transactions()
    schedule_reap()
    {:noreply, state}
  end

  @impl true
  def handle_info(:scan_locks, state) do
    scan_orphaned_locks()
    schedule_lock_scan()
    {:noreply, state}
  end

  @impl true
  def handle_cast({:force_rollback, txn_id, reason}, state) do
    Logger.warning("Force rollback transaction #{txn_id}: #{inspect(reason)}")
    do_force_rollback(txn_id)
    {:noreply, state}
  end

  ## Private

  defp schedule_reap do
    Process.send_after(self(), :reap, @reap_interval)
  end

  defp schedule_lock_scan do
    Process.send_after(self(), :scan_locks, @lock_scan_interval)
  end

  defp reap_timed_out_transactions do
    case list_active_transactions() do
      {:ok, transactions} ->
        now = System.monotonic_time(:millisecond)

        Enum.each(transactions, fn {txn_id, txn} ->
          age = now - txn.started_at

          if age > txn.timeout_ms do
            Logger.warning("Reaping timed-out transaction #{txn_id} (age: #{age}ms)")
            do_force_rollback(txn_id)
          end
        end)

      {:error, _} ->
        :ok
    end
  end

  defp scan_orphaned_locks do
    Logger.debug("Scanning for orphaned locks")

    db_ref = get_db_ref()
    cf_map = get_cf_map()

    if db_ref && Map.has_key?(cf_map, "txn_locks") do
      locks_cf = Map.get(cf_map, "txn_locks")

      case :rocksdb.iterator(db_ref, locks_cf, []) do
        {:ok, iter} ->
          scan_locks_iter(iter, db_ref, locks_cf)
          :rocksdb.iterator_close(iter)

        {:error, reason} ->
          Logger.error("Failed to scan locks: #{inspect(reason)}")
      end
    end
  rescue
    _ -> :ok
  end

  defp scan_locks_iter(iter, db_ref, locks_cf) do
    case :rocksdb.iterator_move(iter, :first) do
      {:ok, key, value} ->
        check_and_cleanup_lock(key, value, db_ref, locks_cf)
        scan_locks_next(iter, db_ref, locks_cf)

      {:error, :invalid_iterator} ->
        :ok
    end
  end

  defp scan_locks_next(iter, db_ref, locks_cf) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} ->
        check_and_cleanup_lock(key, value, db_ref, locks_cf)
        scan_locks_next(iter, db_ref, locks_cf)

      {:error, :invalid_iterator} ->
        :ok
    end
  end

  defp check_and_cleanup_lock(key, value, db_ref, locks_cf) do
    case Store.Transaction.Lock.decode(key, value) do
      {:ok, lock} ->
        if Store.Transaction.Lock.expired?(lock) do
          Logger.warning("Cleaning up orphaned lock for key: #{inspect(key)}")
          :rocksdb.delete(db_ref, locks_cf, key, [])
        end

      {:error, _} ->
        :ok
    end
  end

  defp do_force_rollback(txn_id) do
    try do
      Store.Transaction.Manager.rollback(Store.Transaction.Manager, txn_id)
    catch
      _, _ -> :ok
    end
  end

  defp list_active_transactions do
    try do
      # Access Manager state directly via sys
      {:ok, state} = :sys.get_state(Store.Transaction.Manager)
      {:ok, state.transactions}
    catch
      _, _ -> {:error, :not_available}
    end
  end

  defp get_db_ref do
    try do
      :persistent_term.get(:spiredb_rocksdb_ref)
    rescue
      _ -> nil
    end
  end

  defp get_cf_map do
    try do
      :persistent_term.get(:spiredb_rocksdb_cf_map, %{})
    rescue
      _ -> %{}
    end
  end
end
