defmodule Store.Plugin.RegistryTest do
  @moduledoc """
  Tests for plugin registry functionality.
  """

  use ExUnit.Case, async: false

  alias Store.Plugin.Registry

  # Dummy plugin implementations for testing

  defmodule DummyIndexPlugin do
    @behaviour Store.Plugin
    @behaviour Store.Plugin.Index

    @impl Store.Plugin
    def info do
      %{
        name: "dummy_index",
        version: "1.0.0",
        type: :index,
        description: "A dummy index plugin for testing",
        has_nif: false
      }
    end

    @impl Store.Plugin
    def init(_opts), do: {:ok, %{indexes: %{}}}

    @impl Store.Plugin
    def shutdown(_state), do: :ok

    @impl Store.Plugin.Index
    def create(name, _opts), do: {:ok, name}

    @impl Store.Plugin.Index
    def drop(_name), do: :ok

    @impl Store.Plugin.Index
    def insert(_name, _id, _data), do: :ok

    @impl Store.Plugin.Index
    def delete(_name, _id), do: :ok

    @impl Store.Plugin.Index
    def search(_name, _query, _opts), do: {:ok, []}
  end

  defmodule DummyFunctionPlugin do
    @behaviour Store.Plugin
    @behaviour Store.Plugin.Function

    @impl Store.Plugin
    def info do
      %{
        name: "dummy_functions",
        version: "0.5.0",
        type: :function,
        description: "Custom SQL functions",
        has_nif: false
      }
    end

    @impl Store.Plugin
    def init(_opts), do: {:ok, %{}}

    @impl Store.Plugin
    def shutdown(_state), do: :ok

    @impl Store.Plugin.Function
    def functions do
      [
        {"MY_UPPER", 1, :string},
        {"MY_ADD", 2, :int64}
      ]
    end

    @impl Store.Plugin.Function
    def execute("MY_UPPER", [str]), do: {:ok, String.upcase(str)}
    def execute("MY_ADD", [a, b]), do: {:ok, a + b}
    def execute(_, _), do: {:error, :unknown_function}
  end

  defmodule DummyStoragePlugin do
    @behaviour Store.Plugin

    @impl Store.Plugin
    def info do
      %{
        name: "dummy_storage",
        version: "2.0.0",
        type: :storage,
        description: "Custom storage backend",
        has_nif: true
      }
    end

    @impl Store.Plugin
    def init(opts) do
      if Keyword.get(opts, :fail_init) do
        {:error, :init_failed}
      else
        {:ok, %{path: "/tmp/dummy"}}
      end
    end

    @impl Store.Plugin
    def shutdown(_state), do: :ok
  end

  setup do
    {:ok, pid} = Registry.start_link(name: nil)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    {:ok, registry: pid}
  end

  describe "plugin registration" do
    test "registers a plugin successfully", %{registry: pid} do
      assert :ok = Registry.register(pid, DummyIndexPlugin)
    end

    test "prevents duplicate registration", %{registry: pid} do
      :ok = Registry.register(pid, DummyIndexPlugin)
      assert {:error, :already_registered} = Registry.register(pid, DummyIndexPlugin)
    end

    test "handles init failure", %{registry: pid} do
      assert {:error, {:init_failed, :init_failed}} =
               Registry.register(pid, DummyStoragePlugin, fail_init: true)
    end

    test "registers multiple plugin types", %{registry: pid} do
      :ok = Registry.register(pid, DummyIndexPlugin)
      :ok = Registry.register(pid, DummyFunctionPlugin)
      :ok = Registry.register(pid, DummyStoragePlugin)

      {:ok, plugins} = Registry.list(pid)
      assert length(plugins) == 3
    end
  end

  describe "plugin lookup" do
    setup %{registry: pid} do
      :ok = Registry.register(pid, DummyIndexPlugin)
      :ok = Registry.register(pid, DummyFunctionPlugin)
      :ok
    end

    test "gets plugin by name", %{registry: pid} do
      {:ok, module, _state, info} = Registry.get(pid, "dummy_index")
      assert module == DummyIndexPlugin
      assert info.type == :index
    end

    test "returns error for unknown plugin", %{registry: pid} do
      assert {:error, :not_found} = Registry.get(pid, "nonexistent")
    end

    test "lists all plugins", %{registry: pid} do
      {:ok, plugins} = Registry.list(pid)
      names = Enum.map(plugins, & &1.name)
      assert "dummy_index" in names
      assert "dummy_functions" in names
    end

    test "lists by type", %{registry: pid} do
      {:ok, index_plugins} = Registry.list_by_type(pid, :index)
      assert length(index_plugins) == 1
      assert hd(index_plugins).name == "dummy_index"

      {:ok, function_plugins} = Registry.list_by_type(pid, :function)
      assert length(function_plugins) == 1
      assert hd(function_plugins).name == "dummy_functions"
    end
  end

  describe "plugin lifecycle" do
    test "unregisters plugin", %{registry: pid} do
      :ok = Registry.register(pid, DummyIndexPlugin)
      assert :ok = Registry.unregister(pid, "dummy_index")
      assert {:error, :not_found} = Registry.get(pid, "dummy_index")
    end

    test "unregister returns error for unknown plugin", %{registry: pid} do
      assert {:error, :not_found} = Registry.unregister(pid, "nonexistent")
    end

    test "reloads plugin", %{registry: pid} do
      :ok = Registry.register(pid, DummyIndexPlugin)
      assert :ok = Registry.reload(pid, "dummy_index")

      # Plugin should still be accessible
      {:ok, _module, _state, _info} = Registry.get(pid, "dummy_index")
    end

    test "reload returns error for unknown plugin", %{registry: pid} do
      assert {:error, :not_found} = Registry.reload(pid, "nonexistent")
    end
  end

  describe "plugin functionality" do
    test "function plugin executes correctly", %{registry: pid} do
      :ok = Registry.register(pid, DummyFunctionPlugin)

      # Get the plugin and execute functions
      {:ok, module, _state, _info} = Registry.get(pid, "dummy_functions")

      assert {:ok, "HELLO"} = module.execute("MY_UPPER", ["hello"])
      assert {:ok, 15} = module.execute("MY_ADD", [10, 5])
    end

    test "function plugin lists available functions", %{registry: pid} do
      :ok = Registry.register(pid, DummyFunctionPlugin)
      {:ok, module, _state, _info} = Registry.get(pid, "dummy_functions")

      functions = module.functions()
      assert {"MY_UPPER", 1, :string} in functions
      assert {"MY_ADD", 2, :int64} in functions
    end

    test "index plugin operations", %{registry: pid} do
      :ok = Registry.register(pid, DummyIndexPlugin)
      {:ok, module, _state, _info} = Registry.get(pid, "dummy_index")

      assert {:ok, "test_idx"} = module.create("test_idx", [])
      assert :ok = module.insert("test_idx", "doc:1", %{data: "test"})
      assert {:ok, []} = module.search("test_idx", %{}, [])
      assert :ok = module.delete("test_idx", "doc:1")
      assert :ok = module.drop("test_idx")
    end
  end
end
