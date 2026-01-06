defmodule Store.Plugin.LoaderTest do
  @moduledoc """
  Tests for plugin loader functionality.
  """

  use ExUnit.Case, async: false

  alias Store.Plugin.Loader
  alias Store.Plugin.Registry

  @test_plugin_dir "/tmp/spiredb_test_plugins"

  setup do
    # Create test plugin directory
    File.rm_rf!(@test_plugin_dir)
    File.mkdir_p!(@test_plugin_dir)

    # Only start registry if not already running (may be started by application supervisor)
    registry_pid =
      case Process.whereis(Store.Plugin.Registry) do
        nil ->
          {:ok, pid} = Registry.start_link(name: Store.Plugin.Registry)
          pid

        pid ->
          pid
      end

    on_exit(fn ->
      File.rm_rf!(@test_plugin_dir)
      # Only stop if we started it (if it was nil before)
    end)

    {:ok, plugin_dir: @test_plugin_dir, registry_pid: registry_pid}
  end

  describe "load_all/1" do
    test "returns empty list when no plugins", %{plugin_dir: dir} do
      assert [] = Loader.load_all(plugin_dir: dir)
    end

    test "returns empty list when directory doesn't exist" do
      assert [] = Loader.load_all(plugin_dir: "/nonexistent/path")
    end

    test "skips plugins without metadata", %{plugin_dir: dir} do
      # Create plugin dir without plugin.json
      plugin_path = Path.join(dir, "bad_plugin")
      File.mkdir_p!(plugin_path)
      File.mkdir_p!(Path.join(plugin_path, "lib"))

      assert [] = Loader.load_all(plugin_dir: dir)
    end
  end

  describe "load_plugin/1" do
    test "fails without plugin.json", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "no_metadata")
      File.mkdir_p!(plugin_path)

      assert {:error, :metadata_not_found} = Loader.load_plugin(plugin_path)
    end

    test "fails with invalid plugin.json", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "invalid_json")
      File.mkdir_p!(plugin_path)
      File.write!(Path.join(plugin_path, "plugin.json"), "not valid json")

      assert {:error, :invalid_metadata} = Loader.load_plugin(plugin_path)
    end

    test "fails with missing module name", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "no_module")
      File.mkdir_p!(plugin_path)

      metadata = %{
        "name" => "test_plugin",
        "version" => "1.0.0"
        # Missing "module" key
      }

      File.write!(Path.join(plugin_path, "plugin.json"), Jason.encode!(metadata))

      assert {:error, :missing_module_name} = Loader.load_plugin(plugin_path)
    end

    test "fails when module cannot be loaded", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "missing_module")
      File.mkdir_p!(plugin_path)

      metadata = %{
        "name" => "test_plugin",
        "version" => "1.0.0",
        "module" => "NonexistentModule.That.Does.Not.Exist"
      }

      File.write!(Path.join(plugin_path, "plugin.json"), Jason.encode!(metadata))

      assert {:error, :module_not_found} = Loader.load_plugin(plugin_path)
    end
  end

  describe "unload/1" do
    test "unloads registered plugin" do
      # First register a plugin directly
      defmodule TestUnloadPlugin do
        @behaviour Store.Plugin

        def info,
          do: %{
            name: "test_unload",
            version: "1.0.0",
            type: :function,
            description: "Test",
            has_nif: false
          }

        def init(_), do: {:ok, %{}}
        def shutdown(_), do: :ok
      end

      :ok = Registry.register(TestUnloadPlugin)

      # Verify it's registered
      {:ok, _, _, _} = Registry.get("test_unload")

      # Unload
      :ok = Loader.unload("test_unload")

      # Verify it's gone
      {:error, :not_found} = Registry.get("test_unload")
    end
  end

  describe "load_from_binary/3" do
    test "loads a valid beam binary" do
      # Create a simple module dynamically with explicit atom name
      {:module, mod, binary, _} =
        defmodule :"Elixir.TestBinaryLoaderPlugin" do
          @behaviour Store.Plugin

          def info do
            %{
              name: "test_binary_loader",
              version: "1.0.0",
              type: :function,
              description: "Test binary plugin",
              has_nif: false
            }
          end

          def init(_opts), do: {:ok, %{}}
          def shutdown(_state), do: :ok
        end

      # Unload it first
      :code.purge(mod)
      :code.delete(mod)

      # Now load from binary
      result =
        Loader.load_from_binary("TestBinaryLoaderPlugin", binary, %{
          "name" => "test_binary_loader"
        })

      assert {:ok, "test_binary_loader"} = result

      # Verify it's registered
      assert {:ok, _, _, _} = Registry.get("test_binary_loader")

      # Cleanup
      Registry.unregister("test_binary_loader")
    end
  end

  describe "full plugin lifecycle integration" do
    test "create -> compile -> load -> use -> unload", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "integration_test_plugin")
      lib_path = Path.join(plugin_path, "lib")
      File.mkdir_p!(lib_path)

      # Step 1: Create plugin source code
      plugin_source = """
      defmodule IntegrationTestPlugin do
        @behaviour Store.Plugin

        def info do
          %{
            name: "integration_test",
            version: "1.0.0",
            type: :function,
            description: "Integration test plugin",
            has_nif: false
          }
        end

        def init(_opts), do: {:ok, %{counter: 0}}
        def shutdown(_state), do: :ok

        # Custom function for testing
        def increment(n), do: n + 1
      end
      """

      source_path = Path.join(lib_path, "integration_test_plugin.ex")
      File.write!(source_path, plugin_source)

      # Step 2: Create plugin.json metadata
      metadata = %{
        "name" => "integration_test",
        "version" => "1.0.0",
        "module" => "IntegrationTestPlugin",
        "type" => "function",
        "description" => "Integration test plugin"
      }

      File.write!(Path.join(plugin_path, "plugin.json"), Jason.encode!(metadata))

      # Step 3: Load the plugin (should compile .ex and register)
      assert {:ok, "integration_test"} = Loader.load_plugin(plugin_path)

      # Step 4: Verify plugin is registered
      assert {:ok, module, _state, _info} = Registry.get("integration_test")
      assert module == IntegrationTestPlugin

      # Step 5: Use the plugin
      info = module.info()
      assert info.name == "integration_test"
      assert info.version == "1.0.0"
      assert module.increment(5) == 6

      # Step 6: Unload the plugin
      assert :ok = Loader.unload("integration_test")

      # Step 7: Verify plugin is unregistered
      assert {:error, :not_found} = Registry.get("integration_test")
    end
  end

  describe "plugin directory structure" do
    test "adds lib path to code path", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "with_lib")
      lib_path = Path.join(plugin_path, "lib")
      File.mkdir_p!(lib_path)

      # Create a dummy beam file
      File.write!(Path.join(lib_path, "dummy.beam"), "")

      # Load should fail at module loading, but code path should be added
      metadata = %{
        "name" => "test",
        "version" => "1.0.0",
        "module" => "Dummy"
      }

      File.write!(Path.join(plugin_path, "plugin.json"), Jason.encode!(metadata))

      # This will fail because module doesn't exist, but lib should be in path
      Loader.load_plugin(plugin_path)

      # Verify lib path was added
      paths = :code.get_path() |> Enum.map(&to_string/1)
      assert Enum.any?(paths, &String.contains?(&1, "with_lib/lib"))
    end

    test "adds ebin path to code path", %{plugin_dir: dir} do
      plugin_path = Path.join(dir, "with_ebin")
      ebin_path = Path.join(plugin_path, "ebin")
      File.mkdir_p!(ebin_path)

      metadata = %{
        "name" => "test",
        "version" => "1.0.0",
        "module" => "EbinDummy"
      }

      File.write!(Path.join(plugin_path, "plugin.json"), Jason.encode!(metadata))

      Loader.load_plugin(plugin_path)

      paths = :code.get_path() |> Enum.map(&to_string/1)
      assert Enum.any?(paths, &String.contains?(&1, "with_ebin/ebin"))
    end
  end
end
