defmodule Store.Plugin.Loader do
  @moduledoc """
  Loads plugins from the filesystem.

  Supports:
  - Pure Elixir plugins (.ex source, .beam compiled)
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
         :ok <- compile_sources(plugin_path),
         :ok <- load_nif(plugin_path, metadata),
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
  Load a plugin directly from a binary (.beam file content).
  """
  def load_from_binary(module_name, binary, metadata \\ %{}) when is_binary(binary) do
    module = String.to_atom("Elixir.#{module_name}")

    case :code.load_binary(module, ~c"#{module_name}.beam", binary) do
      {:module, ^module} ->
        name = Map.get(metadata, "name", module_name)

        case Store.Plugin.Registry.register(module) do
          :ok ->
            Logger.info("Loaded binary plugin: #{name}")
            {:ok, name}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        Logger.warning("Failed to load binary: #{inspect(reason)}")
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

  defp compile_sources(plugin_path) do
    lib_path = Path.join(plugin_path, "lib")

    if File.dir?(lib_path) do
      lib_path
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.each(fn source_file ->
        Logger.debug("Compiling plugin source: #{source_file}")

        try do
          Code.compile_file(source_file)
        rescue
          e ->
            Logger.warning("Failed to compile #{source_file}: #{inspect(e)}")
        end
      end)
    end

    :ok
  end

  defp load_nif(plugin_path, metadata) do
    if Map.get(metadata, "has_nif", false) do
      nif_name = Map.get(metadata, "nif_name", metadata["module"])
      nif_path = Path.join([plugin_path, "priv", "native", "lib#{nif_name}"])

      if File.exists?(nif_path <> ".so") or File.exists?(nif_path <> ".dylib") do
        Logger.debug("Loading NIF from #{nif_path}")

        case :erlang.load_nif(to_charlist(nif_path), 0) do
          :ok ->
            Logger.info("Loaded NIF: #{nif_name}")
            :ok

          {:error, {:reload, _}} ->
            # Already loaded
            :ok

          {:error, reason} ->
            Logger.warning("Failed to load NIF #{nif_name}: #{inspect(reason)}")
            # Don't fail plugin load for NIF issues
            :ok
        end
      else
        Logger.debug("NIF not found at #{nif_path}, skipping")
        :ok
      end
    else
      :ok
    end
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
