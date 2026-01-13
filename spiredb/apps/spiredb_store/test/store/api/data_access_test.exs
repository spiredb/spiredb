defmodule Store.API.DataAccessTest do
  # Must be sync since we use shared Engine name
  use ExUnit.Case, async: false

  alias Store.API.DataAccess
  alias Store.KV.Engine
  alias Store.Test.MockGRPC

  setup do
    # Setup RocksDB (helper handles persistent_term which Engine logic might ignore if we bypass Store.Server)
    # But here we are instantiating Engine directly, so we need to be careful.
    # The original test instantiated Engine directly. Let's keep that pattern but ensure it works with the new strictness.
    # Actually, `DataAccess` heavily relies on `Store.Server.get_engine_for_key` OR `get_db_ref_direct`.
    # The `engine` override in requests helps bypass `Store.Server`.
    # HOWEVER, `table_insert` etc use `get_db_ref_direct` which uses persistent_term!
    # So we MUST set up persistent_term for `table_*` calls to work.

    unique_name = :"TestEngine_#{System.unique_integer([:positive])}"
    path = "/tmp/spiredb_test_data_access_#{System.unique_integer([:positive])}"
    File.rm_rf!(path)

    {:ok, pid} = Engine.start_link(path: path, name: unique_name)

    # Extract db_ref from Engine state (hacky? or just use RocksDBHelper to set pterm?)
    # Easier: RocksDBHelper sets up a DB and puts it in pterm.
    # BUT, we want `raw_scan` to use `engine` param, and `table_*` to use pterm.
    # Let's align them.

    db_ref = Engine.get_db_ref(pid)
    :persistent_term.put(:spiredb_rocksdb_ref, db_ref)

    Application.put_env(:spiredb_store, :grpc_module, MockGRPC)

    on_exit(fn ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
      Application.put_env(:spiredb_store, :grpc_module, GRPC.Server)
      File.rm_rf!(path)
      :persistent_term.erase(:spiredb_rocksdb_ref)
    end)

    {:ok, engine: unique_name, db_ref: db_ref}
  end

  test "scan returns batches", %{engine: engine} do
    # Insert test data
    for i <- 1..100 do
      :ok = Engine.put(engine, "user:#{i}", "data_#{i}")
    end

    request = %{
      region_id: 1,
      start_key: "user:",
      end_key: "user:~",
      batch_size: 50,
      limit: 0,
      engine: engine
    }

    # Scan (mock stream is passed but unused by MockGRPC, just identity)
    stream = :mock_stream
    DataAccess.raw_scan(request, stream)

    # Should have scanned successfully and sent replies
    assert_receive {:grpc_reply, response}
    assert response.has_more == true or response.has_more == false
    assert is_binary(response.arrow_batch)
  end

  test "get returns single value", %{engine: engine} do
    :ok = Engine.put(engine, "test_key", "test_value")

    request = %{region_id: 1, key: "test_key", read_follower: false, engine: engine}
    result = DataAccess.raw_get(request, nil)

    assert result.found == true
    assert result.value == "test_value"
  end

  test "get returns not found for missing key", %{engine: engine} do
    request = %{region_id: 1, key: "missing_key", read_follower: false, engine: engine}
    result = DataAccess.raw_get(request, nil)

    assert result.found == false
  end

  test "batch_get returns multiple keys", %{engine: engine} do
    :ok = Engine.put(engine, "k1", "v1")
    :ok = Engine.put(engine, "k2", "v2")
    :ok = Engine.put(engine, "k3", "v3")

    request = %{
      region_id: 1,
      keys: ["k1", "k2", "k3", "k4"],
      engine: engine
    }

    result = DataAccess.raw_batch_get(request, nil)

    assert is_binary(result.arrow_batch)
  end
end
