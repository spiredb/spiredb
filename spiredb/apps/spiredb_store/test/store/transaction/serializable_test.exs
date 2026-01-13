defmodule Store.Transaction.SerializableTest do
  @moduledoc """
  Tests specifically for Serializable isolation level and read/write set tracking.
  """
  use ExUnit.Case, async: false
  alias Store.Transaction.Manager
  alias Store.Test.RocksDBHelper

  setup do
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("serializable_test")
    store_ref = get_store_ref()

    manager = start_supervised!({Manager, [name: nil, store_ref: store_ref]})

    unless Process.whereis(PD.TSO) do
      start_supervised!({PD.TSO, name: PD.TSO})
    end

    {:ok, %{manager: manager, store_ref: store_ref}}
  end

  defp get_store_ref do
    db = :persistent_term.get(:spiredb_rocksdb_ref, nil)
    cfs = :persistent_term.get(:spiredb_rocksdb_cf_map, %{})
    %{db: db, cfs: cfs}
  end

  test "serializable transaction tracks read set in Manager", %{manager: manager} do
    # Pre-populate data so read succeeds (Manager currently only tracks successful reads)
    {:ok, txn1} = Manager.begin_transaction(manager)
    Manager.put(manager, txn1, "s_key1", "val1")
    Manager.commit(manager, txn1)

    # Ensure commit completed and data is visible (RocksDB write might be async if not flushed?)
    # Actually commit writes to RocksDB.
    # Let's verify data is there.
    Process.sleep(50)

    {:ok, txn_id} = Manager.begin_transaction(manager, isolation: :serializable)

    # Perform a retrieval (should be tracked)
    {:ok, val} = Manager.get(manager, txn_id, "s_key1")
    assert val == "val1"

    # Access state directly to verify (white-box testing)
    state = :sys.get_state(manager)
    txn = state.transactions[txn_id]

    assert MapSet.member?(txn.read_set, "s_key1")
    assert txn.isolation == :serializable
  end

  test "serializable transaction tracks write set in Manager", %{manager: manager} do
    {:ok, txn_id} = Manager.begin_transaction(manager, isolation: :serializable)

    Manager.put(manager, txn_id, "s_key2", "val")
    Manager.delete(manager, txn_id, "s_key3")

    state = :sys.get_state(manager)
    txn = state.transactions[txn_id]

    assert MapSet.member?(txn.write_set, "s_key2")
    assert MapSet.member?(txn.write_set, "s_key3")
  end

  test "transaction separates read and write sets", %{manager: manager} do
    # Pre-populate read key
    {:ok, txn1} = Manager.begin_transaction(manager)
    Manager.put(manager, txn1, "r_key", "val")
    Manager.commit(manager, txn1)
    Process.sleep(50)

    {:ok, txn_id} = Manager.begin_transaction(manager)

    {:ok, val} = Manager.get(manager, txn_id, "r_key")
    assert val == "val"

    Manager.put(manager, txn_id, "w_key", "val")

    state = :sys.get_state(manager)
    txn = state.transactions[txn_id]

    assert MapSet.member?(txn.read_set, "r_key")
    refute MapSet.member?(txn.read_set, "w_key")

    assert MapSet.member?(txn.write_set, "w_key")
    refute MapSet.member?(txn.write_set, "r_key")
  end
end
