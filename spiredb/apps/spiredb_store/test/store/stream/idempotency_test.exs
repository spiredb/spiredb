defmodule Store.Stream.IdempotencyTest do
  use ExUnit.Case, async: false
  alias Store.Stream.Idempotency
  alias Store.Test.RocksDBHelper

  setup do
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("idempotency_test")

    db = :persistent_term.get(:spiredb_rocksdb_ref)
    cfs = :persistent_term.get(:spiredb_rocksdb_cf_map)
    store_ref = %{db: db, cfs: cfs}

    {:ok, store_ref: store_ref}
  end

  test "stores and checks idempotency key", %{store_ref: store_ref} do
    key = "req_123"
    result = "success"

    # First time
    assert :ok = Idempotency.check_and_store(store_ref, key, result)

    # Second time
    assert {:error, :already_processed, ^result} =
             Idempotency.check_and_store(store_ref, key, "other_result")

    # Check simple check
    assert Idempotency.is_processed?(store_ref, key)
  end

  test "TTL expires keys (simulated)", %{store_ref: store_ref} do
    key = "req_ttl"
    result = "val"

    # Store with 0 TTL (expires immediately/next second)
    # Actually logic uses system time.
    # expiration = now + ttl.
    # if now > exp, it overwrites.
    # If I verify immediately, now <= now + 0 is True?
    # now > now + 0 is False.

    # check_and_store logic:
    # expiration = now + ttl
    # check: if now > exp ...

    # If ttl = -1. expiration = now - 1.
    # now > now - 1 is True.

    assert :ok = Idempotency.check_and_store(store_ref, key, result, ttl: -1)

    # Immediate check should fail or succeed?
    # is_processed? checks: now <= exp.
    # now <= now - 1 is False.
    refute Idempotency.is_processed?(store_ref, key)

    # check_and_store again should succeed (overwrite)
    assert :ok = Idempotency.check_and_store(store_ref, key, "new_val")
  end
end
