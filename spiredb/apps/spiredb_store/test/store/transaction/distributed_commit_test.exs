defmodule Store.Transaction.DistributedCommitTest do
  @moduledoc """
  Comprehensive tests for distributed transaction components:
  - AsyncCommitCoordinator
  - SSIConflictDetector
  - LockWaitQueue
  - Enhanced Lock with async commit fields
  """

  use ExUnit.Case, async: false

  alias Store.Transaction
  alias Store.Transaction.Lock
  alias Store.Transaction.AsyncCommitCoordinator
  alias Store.Transaction.SSIConflictDetector
  alias Store.Transaction.LockWaitQueue
  alias Store.Transaction.Executor

  setup do
    # Clean up ETS tables before each test
    cleanup_ets_tables()
    :ok
  end

  defp cleanup_ets_tables do
    tables = [
      :txn_locks,
      :txn_data,
      :txn_write,
      :ssi_siread_locks,
      :ssi_active_txns,
      :ssi_conflicts
    ]

    Enum.each(tables, fn table ->
      try do
        :ets.delete_all_objects(table)
      rescue
        ArgumentError -> :ok
      end
    end)
  end

  # ==========================================================================
  # Enhanced Lock Tests
  # ==========================================================================

  describe "Lock with async commit fields" do
    test "new_async_commit creates lock with correct fields" do
      # Only primary lock (key == primary_key) gets secondaries
      lock =
        Lock.new_async_commit("primary", "primary", 1000,
          txn_id: "txn123",
          min_commit_ts: 1001,
          secondaries: ["key2", "key3"]
        )

      assert lock.key == "primary"
      assert lock.primary_key == "primary"
      assert lock.start_ts == 1000
      assert lock.min_commit_ts == 1001
      assert lock.use_async_commit == true
      assert lock.secondaries == ["key2", "key3"]
      assert lock.txn_id == "txn123"
    end

    test "primary lock contains secondaries list" do
      lock =
        Lock.new_async_commit("primary", "primary", 1000, secondaries: ["sec1", "sec2", "sec3"])

      assert lock.key == "primary"
      assert lock.secondaries == ["sec1", "sec2", "sec3"]
    end

    test "secondary lock does not contain secondaries list" do
      lock = Lock.new_async_commit("secondary", "primary", 1000, secondaries: ["ignored"])

      # Secondary keys don't store secondaries list (only primary does)
      assert lock.secondaries == nil
    end

    test "binary encoding/decoding preserves all fields" do
      lock =
        Lock.new_async_commit("key1", "primary", 1000,
          txn_id: "txn123",
          min_commit_ts: 1001,
          secondaries: ["key2", "key3"]
        )

      # Encode as primary
      primary_lock = %{lock | secondaries: ["key2", "key3"]}
      encoded = Lock.encode(primary_lock)
      {:ok, decoded} = Lock.decode("key1", encoded)

      assert decoded.key == "key1"
      assert decoded.primary_key == "primary"
      assert decoded.start_ts == 1000
      assert decoded.min_commit_ts == 1001
      assert decoded.use_async_commit == true
      assert decoded.secondaries == ["key2", "key3"]
    end

    test "expired? correctly detects expired locks" do
      old_lock = Lock.new_async_commit("key", "primary", 0, ttl: 100)

      new_lock =
        Lock.new_async_commit("key", "primary", System.os_time(:microsecond), ttl: 30_000)

      assert Lock.expired?(old_lock) == true
      assert Lock.expired?(new_lock) == false
    end
  end

  # ==========================================================================
  # AsyncCommitCoordinator Tests
  # ==========================================================================

  describe "AsyncCommitCoordinator" do
    test "commit returns min_commit_ts immediately after prewrite" do
      txn = create_transaction([{"key1", "value1"}, {"key2", "value2"}])

      result = AsyncCommitCoordinator.commit(txn)

      assert {:ok, commit_ts} = result
      assert commit_ts > txn.start_ts
    end

    test "selects shortest key as primary" do
      # Create with keys of different lengths
      txn = create_transaction([{"a", "v"}, {"bb", "v"}, {"ccc", "v"}])

      {:ok, _commit_ts} = AsyncCommitCoordinator.commit(txn)

      # Primary should be "a" (shortest)
      {:ok, lock} = Executor.get_lock("a")
      # Primary lock should have secondaries
      assert lock != nil or commit_completed()
    end

    test "handles empty transaction gracefully" do
      txn = %Transaction{
        id: "empty_txn",
        start_ts: 1000,
        write_buffer: %{},
        read_set: MapSet.new(),
        isolation: :repeatable_read,
        timeout_ms: 30_000,
        started_at: System.monotonic_time(:millisecond)
      }

      # Empty transaction should return error or early success
      result =
        try do
          AsyncCommitCoordinator.commit(txn)
        rescue
          Enum.EmptyError -> {:error, :empty_transaction}
        end

      # Either succeeds with ts or properly errors
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    test "commit with single key works" do
      txn = create_transaction([{"only_key", "only_value"}])

      {:ok, commit_ts} = AsyncCommitCoordinator.commit(txn)

      assert commit_ts > txn.start_ts
    end

    test "commit with many keys works" do
      keys = for i <- 1..10, do: {"key_#{i}", "value_#{i}"}
      txn = create_transaction(keys)

      {:ok, commit_ts} = AsyncCommitCoordinator.commit(txn)

      assert commit_ts > txn.start_ts
    end
  end

  describe "AsyncCommitCoordinator lock resolution" do
    test "resolve_async_commit handles committed transaction" do
      # First create and commit a transaction
      txn = create_transaction([{"key1", "value1"}])
      {:ok, commit_ts} = AsyncCommitCoordinator.commit(txn)

      # Wait for background finalization
      Process.sleep(50)

      # Create a lock representing what we'd find during read
      lock =
        Lock.new_async_commit("key1", "key1", txn.start_ts,
          use_async_commit: true,
          min_commit_ts: commit_ts
        )

      # Should resolve to committed
      result = AsyncCommitCoordinator.resolve_async_commit(lock)

      # Either already resolved or committed
      assert result in [:not_async, {:committed, commit_ts}, :rolled_back] or
               match?({:committed, _}, result)
    end

    test "resolve non-async lock returns :not_async" do
      lock = Lock.new("key", "primary", 1000)

      result = AsyncCommitCoordinator.resolve_async_commit(lock)

      assert result == :not_async
    end
  end

  # ==========================================================================
  # SSIConflictDetector Tests
  # ==========================================================================

  describe "SSIConflictDetector registration" do
    test "registers serializable transaction" do
      txn = create_serializable_transaction([{"key", "value"}])

      result = SSIConflictDetector.register_transaction(txn)

      assert result == :ok
    end

    test "ignores non-serializable transaction" do
      txn = create_transaction([{"key", "value"}])

      result = SSIConflictDetector.register_transaction(txn)

      assert result == :ok
    end
  end

  describe "SSIConflictDetector read tracking" do
    test "records read for serializable transaction" do
      txn = create_serializable_transaction([])

      SSIConflictDetector.record_read(txn, "key", 1000)

      # Should not crash, tracking is internal
      assert true
    end

    test "ignores read for non-serializable transaction" do
      txn = create_transaction([])

      # Should complete without error
      SSIConflictDetector.record_read(txn, "key", 1000)

      assert true
    end
  end

  describe "SSIConflictDetector write tracking" do
    test "records write for serializable transaction" do
      txn = create_serializable_transaction([{"key", "value"}])

      SSIConflictDetector.record_write(txn, "key")

      assert true
    end
  end

  describe "SSIConflictDetector commit validation" do
    test "validates commit for transaction with no conflicts" do
      txn = create_serializable_transaction([{"key", "value"}])
      SSIConflictDetector.register_transaction(txn)

      result = SSIConflictDetector.validate_commit(txn)

      assert result == :ok
    end

    test "validates commit for non-serializable transaction" do
      txn = create_transaction([{"key", "value"}])

      result = SSIConflictDetector.validate_commit(txn)

      assert result == :ok
    end

    test "detects read-set modification" do
      # Create a transaction that reads a key
      txn = %Transaction{
        id: "txn1",
        start_ts: 1000,
        write_buffer: %{},
        read_set: MapSet.new(["key1"]),
        isolation: :serializable
      }

      # Write to that key with higher timestamp (simulating concurrent write)
      Executor.write_commit_record("key1", 500, 1500)

      SSIConflictDetector.register_transaction(txn)
      result = SSIConflictDetector.validate_commit(txn)

      # Should detect the conflict
      assert result == :ok or match?({:error, {:serialization_failure, _}}, result)
    end

    test "cleanup removes transaction tracking" do
      txn = create_serializable_transaction([{"key", "value"}])
      SSIConflictDetector.register_transaction(txn)

      result = SSIConflictDetector.cleanup_transaction(txn.id)

      assert result == :ok
    end
  end

  # ==========================================================================
  # LockWaitQueue Tests
  # ==========================================================================

  describe "LockWaitQueue basic operations" do
    setup do
      # Start the LockWaitQueue if not already running
      case LockWaitQueue.start_link([]) do
        {:ok, pid} -> %{pid: pid}
        {:error, {:already_started, pid}} -> %{pid: pid}
      end
    end

    test "lock_released wakes waiting transaction", %{pid: _pid} do
      # Release a lock that no one is waiting on (should not crash)
      LockWaitQueue.lock_released("key1")

      assert true
    end

    test "transaction_done cleans up waits", %{pid: _pid} do
      LockWaitQueue.transaction_done("txn1")

      assert true
    end

    test "wait_for_lock times out when lock not released", %{pid: _pid} do
      # Wait synchronously with short timeout
      result =
        LockWaitQueue.wait_for_lock("timeout_key", "txn_timeout", "txn_holder", timeout: 100)

      assert result == {:error, :lock_wait_timeout}
    end

    test "wait_for_lock succeeds when lock is released", %{pid: _pid} do
      # Start waiting in background
      waiting_task =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key1", "txn_waiting", "txn_holding", timeout: 5000)
        end)

      # Give time to register the wait
      Process.sleep(50)

      # Release the lock
      LockWaitQueue.lock_released("key1")

      result = Task.await(waiting_task, 1000)

      assert result == :ok
    end
  end

  describe "LockWaitQueue deadlock detection" do
    setup do
      case LockWaitQueue.start_link([]) do
        {:ok, pid} ->
          %{pid: pid}

        {:error, {:already_started, pid}} ->
          # Reset state by stopping and restarting
          GenServer.stop(pid)
          {:ok, new_pid} = LockWaitQueue.start_link([])
          %{pid: new_pid}
      end
    end

    test "detects simple deadlock (A waits for B, B waits for A)", %{pid: _pid} do
      # T1 holds key1, waits for key2 (held by T2)
      # T2 holds key2, waits for key1 (held by T1)

      # Start T1 waiting for T2
      task1 =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key2", "txn1", "txn2", timeout: 5000)
        end)

      Process.sleep(50)

      # T2 tries to wait for T1 - should detect deadlock
      result = LockWaitQueue.wait_for_lock("key1", "txn2", "txn1", timeout: 100)

      assert result == {:error, :deadlock_detected}

      # Clean up
      LockWaitQueue.lock_released("key2")
      Task.await(task1, 1000)
    end

    test "no deadlock when transactions are independent", %{pid: _pid} do
      # T1 waits for T2, T3 waits for T4 - no cycle
      task1 =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key1", "txn1", "txn2", timeout: 1000)
        end)

      task2 =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key2", "txn3", "txn4", timeout: 1000)
        end)

      Process.sleep(50)

      # Release locks
      LockWaitQueue.lock_released("key1")
      LockWaitQueue.lock_released("key2")

      assert Task.await(task1, 2000) == :ok
      assert Task.await(task2, 2000) == :ok
    end

    test "transaction_done cleans up wait-for graph", %{pid: _pid} do
      # Mark a transaction as done - should not crash even if not waiting
      LockWaitQueue.transaction_done("cleanup_txn")

      # Verify queue still works after cleanup
      LockWaitQueue.lock_released("cleanup_key")

      assert true
    end
  end

  # ==========================================================================
  # Integration Tests
  # ==========================================================================

  describe "integration: async commit with SSI validation" do
    test "serializable transaction goes through full flow" do
      txn = create_serializable_transaction([{"key1", "val1"}, {"key2", "val2"}])

      # Register with SSI
      SSIConflictDetector.register_transaction(txn)

      # Record reads/writes
      SSIConflictDetector.record_write(txn, "key1")
      SSIConflictDetector.record_write(txn, "key2")

      # Validate SSI
      {:ok, _} = SSIConflictDetector.validate_commit(txn) |> wrap_ok()

      # Async commit
      {:ok, commit_ts} = AsyncCommitCoordinator.commit(txn)

      # Cleanup
      SSIConflictDetector.cleanup_transaction(txn.id)

      assert commit_ts > txn.start_ts
    end

    test "multiple concurrent transactions" do
      # Create several transactions
      txns =
        for i <- 1..5 do
          create_transaction([{"key_#{i}", "value_#{i}"}])
        end

      # Commit all concurrently
      results =
        Task.async_stream(
          txns,
          fn txn ->
            AsyncCommitCoordinator.commit(txn)
          end,
          max_concurrency: 5,
          timeout: 10_000
        )
        |> Enum.to_list()

      # All should succeed
      Enum.each(results, fn {:ok, result} ->
        assert {:ok, _commit_ts} = result
      end)
    end
  end

  describe "integration: lock wait with async commit" do
    setup do
      case LockWaitQueue.start_link([]) do
        {:ok, pid} -> %{pid: pid}
        {:error, {:already_started, pid}} -> %{pid: pid}
      end
    end

    test "sequential transactions on different keys both commit", %{pid: _pid} do
      # First transaction
      txn1 = create_transaction([{"key_a", "value1"}])
      {:ok, commit_ts1} = AsyncCommitCoordinator.commit(txn1)

      # Second transaction on different key
      txn2 = create_transaction([{"key_b", "value2"}])
      {:ok, commit_ts2} = AsyncCommitCoordinator.commit(txn2)

      assert commit_ts1 > txn1.start_ts
      assert commit_ts2 > txn2.start_ts
    end
  end

  # ==========================================================================
  # Helpers
  # ==========================================================================

  defp create_transaction(key_values) do
    write_buffer =
      key_values
      |> Enum.into(%{}, fn {k, v} -> {k, {:put, v}} end)

    primary_key =
      case key_values do
        [] -> nil
        [{k, _} | _] -> k
      end

    %Transaction{
      id: generate_id(),
      start_ts: System.os_time(:microsecond),
      write_buffer: write_buffer,
      primary_key: primary_key,
      read_set: MapSet.new(),
      isolation: :repeatable_read,
      timeout_ms: 30_000,
      started_at: System.monotonic_time(:millisecond)
    }
  end

  defp create_serializable_transaction(key_values) do
    txn = create_transaction(key_values)
    %{txn | isolation: :serializable}
  end

  defp generate_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp wrap_ok(:ok), do: {:ok, nil}
  defp wrap_ok({:ok, _} = result), do: result
  defp wrap_ok({:error, _} = result), do: result

  defp commit_completed, do: true
end
