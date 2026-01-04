defmodule Store.API.DataAccessTest do
  # Must be sync since we use shared Engine name
  use ExUnit.Case, async: false

  alias Store.API.DataAccess
  alias Store.KV.Engine
  alias Store.Test.MockGRPC

  setup do
    # Tests pass engine directly in requests, no need to start Store.Server

    # Start a unique Engine for this test process
    unique_name = :"TestEngine_#{System.unique_integer([:positive])}"
    path = "/tmp/spiredb_test_data_access_#{System.unique_integer([:positive])}"

    File.rm_rf!(path)
    {:ok, pid} = Engine.start_link(path: path, name: unique_name)

    # Configure MockGRPC
    Application.put_env(:spiredb_store, :grpc_module, MockGRPC)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :kill)
      end

      Application.put_env(:spiredb_store, :grpc_module, GRPC.Server)
      File.rm_rf!(path)
    end)

    {:ok, engine: unique_name}
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
