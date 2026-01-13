defmodule Store.KV.IteratorPoolTest do
  # Not async because we testing pool capacity/state
  use ExUnit.Case, async: false
  alias Store.KV.IteratorPool
  alias Store.Test.RocksDBHelper

  setup do
    {:ok, db, _cfs} = RocksDBHelper.setup_rocksdb("iterator_pool_test")

    # Start pool purely for this test
    pid = start_supervised!({IteratorPool, name: :test_pool})

    {:ok, pool: pid, db: db}
  end

  test "checkout creates new iterator", %{pool: pool, db: db} do
    {:ok, iter} = IteratorPool.checkout(pool, db)
    assert is_reference(iter)

    # Checkin it back
    IteratorPool.checkin(pool, iter, db)
  end

  test "checkout reuses iterator", %{pool: pool, db: db} do
    {:ok, iter1} = IteratorPool.checkout(pool, db)
    IteratorPool.checkin(pool, iter1, db)

    {:ok, iter2} = IteratorPool.checkout(pool, db)

    # It should be the same reference if implementation reuses list head
    assert iter1 == iter2
  end

  test "pool limit behavior", %{pool: pool, db: db} do
    # Only checks we don't crash
    iters =
      for _ <- 1..105 do
        {:ok, iter} = IteratorPool.checkout(pool, db)
        iter
      end

    # Checkin all
    Enum.each(iters, fn iter -> IteratorPool.checkin(pool, iter, db) end)

    # Checkout again shouldn't crash
    {:ok, _} = IteratorPool.checkout(pool, db)
  end
end
