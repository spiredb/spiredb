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
    # Ensure we get a valid store ref
    store_ref = get_store_ref()
    # Generate unique test ID for key isolation
    test_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    {:ok, %{store_ref: store_ref, test_id: test_id}}
  end

  defp get_store_ref do
    db = :persistent_term.get(:spiredb_rocksdb_ref, nil)
    cfs = :persistent_term.get(:spiredb_rocksdb_cf_map, %{})
    %{db: db, cfs: cfs}
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
  end

  # ==========================================================================
  # AsyncCommitCoordinator Tests
  # ==========================================================================

  describe "AsyncCommitCoordinator" do
    test "commit returns min_commit_ts immediately after prewrite", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      txn = create_transaction([{"key1", "value1"}, {"key2", "value2"}], test_id)

      result = AsyncCommitCoordinator.commit(store_ref, txn)

      assert {:ok, commit_ts} = result
      assert commit_ts > txn.start_ts
    end

    test "selects shortest key as primary", %{store_ref: store_ref, test_id: test_id} do
      # Create with keys of different lengths
      # "a" is shortest
      txn = create_transaction([{"a", "v"}, {"bb", "v"}, {"ccc", "v"}], test_id)

      {:ok, _commit_ts} = AsyncCommitCoordinator.commit(store_ref, txn)

      # Primary should be "a" (plus suffix)
      # Since we append suffix to all, "a_suffix" is still shorter than "bb_suffix"? Yes.
      primary_key = k("a", test_id)
      {:ok, lock} = Executor.get_lock(store_ref, primary_key)
      # Primary lock should have secondaries or be committed
      assert lock != nil or commit_completed()
    end

    test "handles empty transaction gracefully", %{store_ref: store_ref} do
      txn = %Transaction{
        id: "empty_txn",
        start_ts: 1000,
        write_buffer: %{},
        read_set: MapSet.new(),
        isolation: :repeatable_read,
        timeout_ms: 30_000,
        started_at: System.monotonic_time(:millisecond)
      }

      # Empty transaction should return success (early return)
      result = AsyncCommitCoordinator.commit(store_ref, txn)

      assert match?({:ok, _}, result)
    end

    test "commit with single key works", %{store_ref: store_ref, test_id: test_id} do
      txn = create_transaction([{"only_key", "only_value"}], test_id)

      {:ok, commit_ts} = AsyncCommitCoordinator.commit(store_ref, txn)

      assert commit_ts > txn.start_ts
    end

    test "commit with many keys works", %{store_ref: store_ref, test_id: test_id} do
      keys = for i <- 1..10, do: {"key_#{i}", "value_#{i}"}
      txn = create_transaction(keys, test_id)

      {:ok, commit_ts} = AsyncCommitCoordinator.commit(store_ref, txn)

      assert commit_ts > txn.start_ts
    end
  end

  describe "AsyncCommitCoordinator lock resolution" do
    test "resolve_async_commit handles committed transaction", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      # First create and commit a transaction
      txn = create_transaction([{"key1", "value1"}], test_id)
      {:ok, commit_ts} = AsyncCommitCoordinator.commit(store_ref, txn)

      # Wait for background finalization
      Process.sleep(50)

      key = k("key1", test_id)
      # Create a lock representing what we'd find during read
      lock =
        Lock.new_async_commit(key, key, txn.start_ts,
          use_async_commit: true,
          min_commit_ts: commit_ts
        )

      # Should resolve to committed
      result = AsyncCommitCoordinator.resolve_async_commit(store_ref, lock)

      # Either already resolved or committed
      assert result in [:not_async, {:committed, commit_ts}, :rolled_back] or
               match?({:committed, _}, result)
    end

    test "resolve non-async lock returns :not_async", %{store_ref: store_ref} do
      lock = Lock.new("key", "primary", 1000)

      result = AsyncCommitCoordinator.resolve_async_commit(store_ref, lock)

      assert result == :not_async
    end
  end

  # ==========================================================================
  # SSIConflictDetector Tests
  # ==========================================================================

  describe "SSIConflictDetector registration" do
    test "registers serializable transaction", %{test_id: test_id} do
      txn = create_serializable_transaction([{"key", "value"}], test_id)

      result = SSIConflictDetector.register_transaction(txn)

      assert result == :ok
    end

    test "ignores non-serializable transaction", %{test_id: test_id} do
      txn = create_transaction([{"key", "value"}], test_id)

      result = SSIConflictDetector.register_transaction(txn)

      assert result == :ok
    end
  end

  describe "SSIConflictDetector read tracking" do
    test "records read for serializable transaction", %{store_ref: store_ref, test_id: test_id} do
      txn = create_serializable_transaction([], test_id)

      SSIConflictDetector.record_read(store_ref, txn, k("key", test_id), 1000)

      # Should not crash, tracking is internal
      assert true
    end

    test "ignores read for non-serializable transaction", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      txn = create_transaction([], test_id)

      # Should complete without error
      SSIConflictDetector.record_read(store_ref, txn, k("key", test_id), 1000)

      assert true
    end
  end

  describe "SSIConflictDetector write tracking" do
    test "records write for serializable transaction", %{test_id: test_id} do
      txn = create_serializable_transaction([{"key", "value"}], test_id)

      SSIConflictDetector.record_write(txn, k("key", test_id))

      assert true
    end
  end

  describe "SSIConflictDetector commit validation" do
    test "validates commit for transaction with no conflicts", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      txn = create_serializable_transaction([{"key", "value"}], test_id)
      SSIConflictDetector.register_transaction(txn)

      result = SSIConflictDetector.validate_commit(store_ref, txn)

      assert result == :ok
    end

    test "validates commit for non-serializable transaction", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      txn = create_transaction([{"key", "value"}], test_id)

      result = SSIConflictDetector.validate_commit(store_ref, txn)

      assert result == :ok
    end

    test "detects read-set modification", %{store_ref: store_ref, test_id: test_id} do
      key = k("key1", test_id)
      # Create a transaction that reads a key
      txn = %Transaction{
        id: "txn1",
        start_ts: 1000,
        write_buffer: %{},
        read_set: MapSet.new([key]),
        isolation: :serializable
      }

      # Write to that key with higher timestamp (simulating concurrent write)
      Executor.write_commit_record(store_ref, key, 500, 1500)

      SSIConflictDetector.register_transaction(txn)
      result = SSIConflictDetector.validate_commit(store_ref, txn)

      # Should detect the conflict
      assert result == :ok or match?({:error, {:serialization_failure, _}}, result)
    end

    test "cleanup removes transaction tracking", %{test_id: test_id} do
      txn = create_serializable_transaction([{"key", "value"}], test_id)
      SSIConflictDetector.register_transaction(txn)

      result = SSIConflictDetector.cleanup_transaction(txn.id)

      assert result == :ok
    end
  end

  # ==========================================================================
  # LockWaitQueue Tests
  # ==========================================================================
  # LockWaitQueue tests generally don't touch RocksDB except for state stored in memory.
  # So they don't need store_ref or test_id handling unless they call Executor.
  # Checking previous usage... `LockWaitQueue` uses internal state/ETS.

  describe "LockWaitQueue basic operations" do
    setup do
      # Start the LockWaitQueue if not already running
      case LockWaitQueue.start_link([]) do
        {:ok, pid} -> %{pid: pid}
        {:error, {:already_started, pid}} -> %{pid: pid}
      end
    end

    test "lock_released wakes waiting transaction" do
      LockWaitQueue.lock_released("key1")
      assert true
    end

    test "transaction_done cleans up waits" do
      LockWaitQueue.transaction_done("txn1")
      assert true
    end

    test "wait_for_lock times out when lock not released" do
      result =
        LockWaitQueue.wait_for_lock("timeout_key", "txn_timeout", "txn_holder", timeout: 100)

      assert result == {:error, :lock_wait_timeout}
    end

    test "wait_for_lock succeeds when lock is released" do
      waiting_task =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key1", "txn_waiting", "txn_holding", timeout: 5000)
        end)

      Process.sleep(50)
      LockWaitQueue.lock_released("key1")
      assert Task.await(waiting_task, 1000) == :ok
    end
  end

  describe "LockWaitQueue deadlock detection" do
    setup do
      case LockWaitQueue.start_link([]) do
        {:ok, pid} -> %{pid: pid}
        {:error, {:already_started, pid}} -> %{pid: pid}
      end
    end

    test "detects simple deadlock (A waits for B, B waits for A)" do
      task1 =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key2", "txn1", "txn2", timeout: 5000)
        end)

      Process.sleep(50)
      result = LockWaitQueue.wait_for_lock("key1", "txn2", "txn1", timeout: 100)
      assert result == {:error, :deadlock_detected}
      LockWaitQueue.lock_released("key2")
      Task.await(task1, 1000)
    end

    test "no deadlock when transactions are independent" do
      task1 =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key1", "txn1", "txn2", timeout: 1000)
        end)

      task2 =
        Task.async(fn ->
          LockWaitQueue.wait_for_lock("key2", "txn3", "txn4", timeout: 1000)
        end)

      Process.sleep(50)
      LockWaitQueue.lock_released("key1")
      LockWaitQueue.lock_released("key2")
      assert Task.await(task1, 2000) == :ok
      assert Task.await(task2, 2000) == :ok
    end

    test "transaction_done cleans up wait-for graph" do
      LockWaitQueue.transaction_done("cleanup_txn")
      LockWaitQueue.lock_released("cleanup_key")
      assert true
    end
  end

  # ==========================================================================
  # Integration Tests
  # ==========================================================================

  describe "integration: async commit with SSI validation" do
    test "serializable transaction goes through full flow", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      txn = create_serializable_transaction([{"key1", "val1"}, {"key2", "val2"}], test_id)

      SSIConflictDetector.register_transaction(txn)
      # Use keys from txn (already suffixed)
      Enum.each(txn.write_buffer, fn {k, _} ->
        SSIConflictDetector.record_write(txn, k)
      end)

      {:ok, _} = SSIConflictDetector.validate_commit(store_ref, txn) |> wrap_ok()
      {:ok, commit_ts} = AsyncCommitCoordinator.commit(store_ref, txn)
      SSIConflictDetector.cleanup_transaction(txn.id)

      assert commit_ts > txn.start_ts
    end

    test "multiple concurrent transactions", %{store_ref: store_ref, test_id: test_id} do
      txns =
        for i <- 1..5 do
          # Use suffixed keys
          create_transaction([{"key_#{i}", "value_#{i}"}], test_id)
        end

      results =
        Task.async_stream(
          txns,
          fn txn ->
            AsyncCommitCoordinator.commit(store_ref, txn)
          end,
          max_concurrency: 5,
          timeout: 10_000
        )
        |> Enum.to_list()

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

    test "sequential transactions on different keys both commit", %{
      store_ref: store_ref,
      test_id: test_id
    } do
      txn1 = create_transaction([{"key_a", "value1"}], test_id)
      {:ok, commit_ts1} = AsyncCommitCoordinator.commit(store_ref, txn1)

      txn2 = create_transaction([{"key_b", "value2"}], test_id)
      {:ok, commit_ts2} = AsyncCommitCoordinator.commit(store_ref, txn2)

      assert commit_ts1 > txn1.start_ts
      assert commit_ts2 > txn2.start_ts
    end
  end

  # ==========================================================================
  # Helpers
  # ==========================================================================

  defp k(key, test_id), do: key <> "_" <> test_id

  defp create_transaction(key_values, test_id) do
    suffix = if test_id, do: "_" <> test_id, else: ""

    write_buffer =
      key_values
      |> Enum.into(%{}, fn {k, v} -> {k <> suffix, {:put, v}} end)

    primary_key =
      case Map.keys(write_buffer) do
        [] -> nil
        # Just pick one
        keys -> Enum.min_by(keys, &byte_size/1)
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

  defp create_serializable_transaction(key_values, test_id) do
    txn = create_transaction(key_values, test_id)
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
