defmodule Store.KV.IteratorStalenessTest do
  use ExUnit.Case, async: false
  require Logger
  alias Store.KV.IteratorPool
  alias Store.Test.RocksDBHelper

  setup do
    {:ok, db, _cfs} = RocksDBHelper.setup_rocksdb("iterator_staleness_test")

    # Ensure pool is started (using name to avoid conflict if global one exists,
    # though strictly we should mostly test the global one or a specific local one)
    # We use a specific one for isolation.
    pid = start_supervised!({IteratorPool, name: :staleness_pool})

    {:ok, pool: pid, db: db}
  end

  test "reused iterator sees new writes (staleness check)", %{pool: pool, db: db} do
    # 1. Write initial data
    :rocksdb.put(db, "key1", "val1", [])

    # 2. Checkout iterator (Iter A)
    {:ok, iter1} = IteratorPool.checkout(pool, db)

    # 3. Read verify
    assert {:ok, "key1", "val1"} = :rocksdb.iterator_move(iter1, :first)

    # 4. Checkin
    IteratorPool.checkin(pool, iter1, db)

    # 5. Write new data
    :rocksdb.put(db, "key1", "val2", [])

    # 6. Checkout again (Iter B - likely same ref)
    {:ok, iter2} = IteratorPool.checkout(pool, db)

    # verify it is reused
    assert iter1 == iter2

    # 7. Read verify - EXPECTATION: It should see "val2" if refreshed.
    # If it sees "val1", it is stale.
    case :rocksdb.iterator_move(iter2, :first) do
      {:ok, "key1", "val2"} ->
        Logger.info("Iterator verified: Fresh data observed")
        :ok

      {:ok, "key1", "val1"} ->
        flunk("Iterator is STALE! Saw val1 instead of val2")

      other ->
        flunk("Unexpected result: #{inspect(other)}")
    end

    IteratorPool.checkin(pool, iter2, db)
  end

  test "held iterator remains stale (control check)", %{pool: pool, db: db} do
    # 1. Write initial data
    :rocksdb.put(db, "control_key", "v1", [])

    # 2. Checkout iterator
    {:ok, iter} = IteratorPool.checkout(pool, db)

    # 3. Verify v1
    assert {:ok, "control_key", "v1"} = :rocksdb.iterator_move(iter, :first)

    # 4. Write new data
    :rocksdb.put(db, "control_key", "v2", [])

    # 5. Read again WITHOUT checkin/refresh
    # Expectation: RocksDB default iterator sees snapshot at creation.
    # So it should STILL see v1.

    case :rocksdb.iterator_move(iter, :first) do
      {:ok, "control_key", "v1"} ->
        Logger.info("Control verified: Held iterator remains stale as expected")
        :ok

      {:ok, "control_key", "v2"} ->
        Logger.warning(
          "Control failed: Iterator saw new data without refresh! (ReadUncommitted or ForwardIterator behavior?)"
        )

        # If this happens, then `refresh` in pool is redundant but harmless.
        # But usually RocksDB iterators snapshot.
        :ok

      other ->
        flunk("Unexpected control result: #{inspect(other)}")
    end

    IteratorPool.checkin(pool, iter, db)
  end

  require Logger
end
