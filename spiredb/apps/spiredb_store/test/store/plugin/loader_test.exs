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

    # Start registry
    {:ok, registry_pid} = Registry.start_link(name: Store.Plugin.Registry)

    on_exit(fn ->
      File.rm_rf!(@test_plugin_dir)
      if Process.alive?(registry_pid), do: GenServer.stop(registry_pid)
    end)

    {:ok, plugin_dir: @test_plugin_dir}
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
