defmodule Store.Plugin.StorageTest do
  use ExUnit.Case, async: false

  alias Store.Plugin.Storage

  @plugin_name "test_plugin"

  alias Store.Test.RocksDBHelper

  setup do
    # Setup isolated RocksDB for this test
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("plugin_storage_test")

    # Clean up any previous test data (logically empty due to new DB, but keeping intent)
    Storage.clear_all(@plugin_name)
    :ok
  end

  describe "get/put" do
    test "put and get a binary value" do
      assert :ok = Storage.put(@plugin_name, "key1", "value1")
      assert {:ok, "value1"} = Storage.get(@plugin_name, "key1")
    end

    test "get returns not_found for missing key" do
      assert {:error, :not_found} = Storage.get(@plugin_name, "nonexistent")
    end

    test "put overwrites existing value" do
      Storage.put(@plugin_name, "key1", "value1")
      Storage.put(@plugin_name, "key1", "value2")
      assert {:ok, "value2"} = Storage.get(@plugin_name, "key1")
    end
  end

  describe "put_term/get_term" do
    test "stores and retrieves Elixir terms" do
      term = %{foo: "bar", count: 42, list: [1, 2, 3]}
      assert :ok = Storage.put_term(@plugin_name, "complex", term)
      assert {:ok, ^term} = Storage.get_term(@plugin_name, "complex")
    end

    test "handles atoms" do
      assert :ok = Storage.put_term(@plugin_name, "atom", :my_atom)
      assert {:ok, :my_atom} = Storage.get_term(@plugin_name, "atom")
    end

    test "handles tuples" do
      tuple = {:ok, 123, "data"}
      assert :ok = Storage.put_term(@plugin_name, "tuple", tuple)
      assert {:ok, ^tuple} = Storage.get_term(@plugin_name, "tuple")
    end
  end

  describe "delete" do
    test "deletes an existing key" do
      Storage.put(@plugin_name, "to_delete", "value")
      assert {:ok, "value"} = Storage.get(@plugin_name, "to_delete")

      assert :ok = Storage.delete(@plugin_name, "to_delete")
      assert {:error, :not_found} = Storage.get(@plugin_name, "to_delete")
    end

    test "delete on nonexistent key is ok" do
      assert :ok = Storage.delete(@plugin_name, "nonexistent")
    end
  end

  describe "list_keys" do
    test "lists all keys for a plugin" do
      Storage.put(@plugin_name, "a", "1")
      Storage.put(@plugin_name, "b", "2")
      Storage.put(@plugin_name, "c", "3")

      {:ok, keys} = Storage.list_keys(@plugin_name)
      assert Enum.sort(keys) == ["a", "b", "c"]
    end

    test "returns empty list for plugin with no keys" do
      {:ok, keys} = Storage.list_keys("empty_plugin")
      assert keys == []
    end

    test "does not return keys from other plugins" do
      Storage.put(@plugin_name, "mine", "1")
      Storage.put("other_plugin", "theirs", "2")

      {:ok, my_keys} = Storage.list_keys(@plugin_name)
      {:ok, their_keys} = Storage.list_keys("other_plugin")

      assert my_keys == ["mine"]
      assert their_keys == ["theirs"]

      # Cleanup
      Storage.clear_all("other_plugin")
    end
  end

  describe "clear_all" do
    test "removes all keys for a plugin" do
      Storage.put(@plugin_name, "a", "1")
      Storage.put(@plugin_name, "b", "2")
      Storage.put(@plugin_name, "c", "3")

      assert :ok = Storage.clear_all(@plugin_name)

      {:ok, keys} = Storage.list_keys(@plugin_name)
      assert keys == []
    end

    test "does not affect other plugins" do
      Storage.put(@plugin_name, "mine", "1")
      Storage.put("other_plugin", "theirs", "2")

      Storage.clear_all(@plugin_name)

      assert {:error, :not_found} = Storage.get(@plugin_name, "mine")
      assert {:ok, "2"} = Storage.get("other_plugin", "theirs")

      # Cleanup
      Storage.clear_all("other_plugin")
    end
  end

  describe "namespace isolation" do
    test "same key in different plugins are independent" do
      Storage.put("plugin_a", "shared_key", "value_a")
      Storage.put("plugin_b", "shared_key", "value_b")

      assert {:ok, "value_a"} = Storage.get("plugin_a", "shared_key")
      assert {:ok, "value_b"} = Storage.get("plugin_b", "shared_key")

      # Cleanup
      Storage.clear_all("plugin_a")
      Storage.clear_all("plugin_b")
    end
  end
end
