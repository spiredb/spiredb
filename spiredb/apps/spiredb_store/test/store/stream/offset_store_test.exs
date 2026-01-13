defmodule Store.Stream.OffsetStoreTest do
  use ExUnit.Case, async: false
  alias Store.Stream.OffsetStore
  alias Store.Test.RocksDBHelper

  setup do
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("offset_store_test")

    db = :persistent_term.get(:spiredb_rocksdb_ref)
    cfs = :persistent_term.get(:spiredb_rocksdb_cf_map)
    store_ref = %{db: db, cfs: cfs}

    {:ok, store_ref: store_ref}
  end

  test "stores and retrieves offset", %{store_ref: store_ref} do
    assert OffsetStore.get_offset(store_ref, "c1") == {:error, :not_found}

    assert :ok = OffsetStore.store_offset(store_ref, "c1", 100)
    assert {:ok, 100} = OffsetStore.get_offset(store_ref, "c1")

    assert :ok = OffsetStore.store_offset(store_ref, "c1", 200)
    assert {:ok, 200} = OffsetStore.get_offset(store_ref, "c1")
  end

  test "lists consumers", %{store_ref: store_ref} do
    OffsetStore.store_offset(store_ref, "c1", 100)
    OffsetStore.store_offset(store_ref, "c2", 200)

    consumers = OffsetStore.list_consumers(store_ref) |> Map.new()
    assert consumers["c1"] == 100
    assert consumers["c2"] == 200
  end

  test "deletes consumer", %{store_ref: store_ref} do
    OffsetStore.store_offset(store_ref, "c1", 100)
    assert :ok = OffsetStore.delete_consumer(store_ref, "c1")
    assert OffsetStore.get_offset(store_ref, "c1") == {:error, :not_found}
  end
end
