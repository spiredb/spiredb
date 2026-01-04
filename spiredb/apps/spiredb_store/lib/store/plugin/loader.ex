defmodule Store.Plugin.Loader do
  @moduledoc """
  Loads plugins from the filesystem.

  Supports:
  - Pure Elixir plugins (.ex, .beam)
  - Rustler NIF plugins (priv/native/*.so)

  Plugin directory structure:
  /var/lib/spiredb/plugins/
    my_plugin/
      lib/
        my_plugin.beam or my_plugin.ex
      priv/
        native/
          libmy_plugin.so (optional NIF)
      plugin.json (metadata)
  """

  require Logger

  @default_plugin_dir "/var/lib/spiredb/plugins"

  @doc """
  Discover and load all plugins from the plugin directory.
  """
  def load_all(opts \\ []) do
    plugin_dir = Keyword.get(opts, :plugin_dir, @default_plugin_dir)

    if File.dir?(plugin_dir) do
      plugin_dir
      |> File.ls!()
      |> Enum.filter(&File.dir?(Path.join(plugin_dir, &1)))
      |> Enum.map(&load_plugin(Path.join(plugin_dir, &1)))
      |> Enum.filter(&match?({:ok, _}, &1))
    else
      Logger.debug("Plugin directory not found: #{plugin_dir}")
      []
    end
  end

  @doc """
  Load a single plugin from a directory.
  """
  def load_plugin(plugin_path) do
    Logger.info("Loading plugin from #{plugin_path}")

    with {:ok, metadata} <- load_metadata(plugin_path),
         :ok <- add_to_code_path(plugin_path),
         {:ok, module} <- load_module(metadata) do
      # Register with plugin registry
      case Store.Plugin.Registry.register(module) do
        :ok ->
          Logger.info("Loaded plugin: #{metadata["name"]}")
          {:ok, metadata["name"]}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} ->
        Logger.warning("Failed to load plugin from #{plugin_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Unload a plugin by name.
  """
  def unload(name) do
    Store.Plugin.Registry.unregister(name)
  end

  # Private helpers

  defp load_metadata(plugin_path) do
    metadata_file = Path.join(plugin_path, "plugin.json")

    if File.exists?(metadata_file) do
      case File.read(metadata_file) do
        {:ok, content} ->
          case Jason.decode(content) do
            {:ok, metadata} -> {:ok, metadata}
            {:error, _} -> {:error, :invalid_metadata}
          end

        {:error, _} ->
          {:error, :metadata_not_readable}
      end
    else
      {:error, :metadata_not_found}
    end
  end

  defp add_to_code_path(plugin_path) do
    lib_path = Path.join(plugin_path, "lib")
    ebin_path = Path.join(plugin_path, "ebin")

    if File.dir?(ebin_path) do
      Code.prepend_path(ebin_path)
    end

    if File.dir?(lib_path) do
      Code.prepend_path(lib_path)
    end

    :ok
  end

  defp load_module(%{"module" => module_name}) when is_binary(module_name) do
    module = String.to_atom("Elixir.#{module_name}")

    if Code.ensure_loaded?(module) do
      {:ok, module}
    else
      {:error, :module_not_found}
    end
  end

  defp load_module(_), do: {:error, :missing_module_name}
end
