defmodule Store.API.RESP.TTLCommandsTest do
  use ExUnit.Case, async: true

  alias Store.KV.TTL

  # Use a simple in-memory store instead of Store.Server
  # This allows tests to run without the full application

  setup do
    # Each test gets a fresh in-memory store
    # ETS tables are automatically cleaned up when the process ends
    store = :ets.new(:ttl_test_store, [:set, :public])
    {:ok, store: store}
  end

  # Mock get/put/delete functions using ETS
  defp mock_get(store, key) do
    case :ets.lookup(store, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  defp mock_put(store, key, value) do
    :ets.insert(store, {key, value})
    {:ok, "OK"}
  end

  describe "SET with EX (seconds TTL) encoding" do
    test "encodes value with TTL when EX option provided", %{store: store} do
      # Simulate SET key value EX 300
      value = "myvalue"
      ttl_seconds = 300

      encoded = TTL.encode_with_ttl(value, ttl_seconds)
      mock_put(store, "mykey", encoded)

      # GET should decode properly
      {:ok, stored} = mock_get(store, "mykey")
      {:ok, decoded} = TTL.decode(stored)

      assert decoded == "myvalue"
    end

    test "TTL remaining returns positive value for active TTL", %{store: store} do
      encoded = TTL.encode_with_ttl("myvalue", 300)
      mock_put(store, "mykey", encoded)

      {:ok, stored} = mock_get(store, "mykey")
      remaining = TTL.ttl_remaining(stored)

      assert remaining > 0
      assert remaining <= 300
    end

    test "expired values are detected", %{store: _store} do
      # Create an already-expired value
      expiry_ts = System.system_time(:second) - 10
      encoded = <<0x01, expiry_ts::64-big, "myvalue">>

      result = TTL.decode(encoded)

      assert {:expired, "myvalue"} = result
    end
  end

  describe "SET with PX (milliseconds TTL) encoding" do
    test "encodes with millisecond TTL converted to seconds", %{store: store} do
      # PX 60000 = 60 seconds
      ttl_seconds = div(60000, 1000)
      encoded = TTL.encode_with_ttl("myvalue", ttl_seconds)
      mock_put(store, "mykey", encoded)

      {:ok, stored} = mock_get(store, "mykey")
      remaining = TTL.ttl_remaining(stored)

      assert remaining > 0
      assert remaining <= 60
    end
  end

  describe "NX/XX option simulation" do
    test "NX succeeds when key doesn't exist", %{store: store} do
      # Simulate NX: only set if not exists
      result =
        case mock_get(store, "nx_key") do
          {:error, :not_found} ->
            mock_put(store, "nx_key", TTL.encode_no_ttl("value"))
            :ok

          {:ok, _} ->
            :skip
        end

      assert result == :ok
      {:ok, stored} = mock_get(store, "nx_key")
      {:ok, "value"} = TTL.decode(stored)
    end

    test "NX fails when key exists", %{store: store} do
      mock_put(store, "nx_key", TTL.encode_no_ttl("first"))

      result =
        case mock_get(store, "nx_key") do
          {:error, :not_found} -> :ok
          {:ok, _} -> :skip
        end

      assert result == :skip
    end

    test "XX succeeds when key exists", %{store: store} do
      mock_put(store, "xx_key", TTL.encode_no_ttl("first"))

      result =
        case mock_get(store, "xx_key") do
          {:ok, _} ->
            mock_put(store, "xx_key", TTL.encode_no_ttl("second"))
            :ok

          _ ->
            :skip
        end

      assert result == :ok
      {:ok, stored} = mock_get(store, "xx_key")
      {:ok, "second"} = TTL.decode(stored)
    end

    test "XX fails when key doesn't exist", %{store: store} do
      result =
        case mock_get(store, "xx_key_missing") do
          {:ok, _} -> :ok
          _ -> :skip
        end

      assert result == :skip
    end
  end

  describe "EXPIRE command simulation" do
    test "adds TTL to existing key", %{store: store} do
      # Set without TTL
      mock_put(store, "expire_key", TTL.encode_no_ttl("myvalue"))

      # Check no TTL
      {:ok, stored1} = mock_get(store, "expire_key")
      assert TTL.ttl_remaining(stored1) == -1

      # EXPIRE - update TTL
      {:ok, old_value} = mock_get(store, "expire_key")
      new_value = TTL.update_ttl(old_value, 300)
      mock_put(store, "expire_key", new_value)

      # Check TTL set
      {:ok, stored2} = mock_get(store, "expire_key")
      remaining = TTL.ttl_remaining(stored2)

      assert remaining > 0
      assert remaining <= 300
    end
  end

  describe "PERSIST command simulation" do
    test "removes TTL from key", %{store: store} do
      # Set with TTL
      mock_put(store, "persist_key", TTL.encode_with_ttl("myvalue", 300))

      # Verify TTL exists
      {:ok, stored1} = mock_get(store, "persist_key")
      assert TTL.ttl_remaining(stored1) > 0

      # PERSIST
      {:ok, old_value} = mock_get(store, "persist_key")
      persisted = TTL.persist(old_value)
      mock_put(store, "persist_key", persisted)

      # TTL should be -1
      {:ok, stored2} = mock_get(store, "persist_key")
      assert TTL.ttl_remaining(stored2) == -1

      # Value should be preserved
      {:ok, decoded} = TTL.decode(stored2)
      assert decoded == "myvalue"
    end
  end

  describe "TTL command simulation" do
    test "returns -1 for key without TTL", %{store: store} do
      mock_put(store, "no_ttl_key", TTL.encode_no_ttl("myvalue"))

      {:ok, stored} = mock_get(store, "no_ttl_key")
      assert TTL.ttl_remaining(stored) == -1
    end

    test "returns -2 for expired key" do
      expiry_ts = System.system_time(:second) - 10
      encoded = <<0x01, expiry_ts::64-big, "myvalue">>

      assert TTL.ttl_remaining(encoded) == -2
    end

    test "returns positive for key with active TTL", %{store: store} do
      mock_put(store, "ttl_key", TTL.encode_with_ttl("myvalue", 300))

      {:ok, stored} = mock_get(store, "ttl_key")
      remaining = TTL.ttl_remaining(stored)

      assert remaining > 0
      assert remaining <= 300
    end
  end

  describe "combined EX + NX/XX" do
    test "EX + NX sets TTL when key doesn't exist", %{store: store} do
      # NX check
      case mock_get(store, "exnx_key") do
        {:error, :not_found} ->
          # Set with TTL
          mock_put(store, "exnx_key", TTL.encode_with_ttl("value", 300))

        _ ->
          :skip
      end

      {:ok, stored} = mock_get(store, "exnx_key")
      {:ok, "value"} = TTL.decode(stored)
      assert TTL.ttl_remaining(stored) > 0
    end

    test "EX + XX updates TTL on existing key", %{store: store} do
      # Create existing key
      mock_put(store, "exxx_key", TTL.encode_no_ttl("first"))

      # XX check
      case mock_get(store, "exxx_key") do
        {:ok, _} ->
          mock_put(store, "exxx_key", TTL.encode_with_ttl("second", 300))

        _ ->
          :skip
      end

      {:ok, stored} = mock_get(store, "exxx_key")
      {:ok, "second"} = TTL.decode(stored)
      assert TTL.ttl_remaining(stored) > 0
    end
  end
end
