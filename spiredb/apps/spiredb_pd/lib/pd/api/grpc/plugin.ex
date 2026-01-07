defmodule PD.API.GRPC.Plugin do
  @moduledoc """
  gRPC PluginService implementation.
  """

  use GRPC.Server, service: Spiredb.Cluster.PluginService.Service

  require Logger

  # Dynamic module references to avoid compile-time warnings
  # These modules are in spiredb_store which may not be loaded
  @loader_module Store.Plugin.Loader
  @registry_module Store.Plugin.Registry

  alias Spiredb.Cluster.{
    InstallPluginResponse,
    PluginInfo,
    PluginList,
    Empty
  }

  def install_plugin(request, _stream) do
    source =
      case request.source do
        {:hex_package, pkg} -> {:hex, pkg}
        {:github_repo, repo} -> {:github, repo}
        {:tarball, data} -> {:tarball, data}
      end

    case apply(@loader_module, :load_plugin, [source]) do
      {:ok, name} ->
        %InstallPluginResponse{name: name, version: "0.0.0"}

      {:error, reason} ->
        Logger.error("Plugin install failed: #{inspect(reason)}")

        raise GRPC.RPCError,
          status: :internal,
          message: "Failed to install plugin: #{inspect(reason)}"
    end
  end

  def uninstall_plugin(request, _stream) do
    case apply(@loader_module, :unload, [request.name]) do
      :ok ->
        %Empty{}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Plugin not found"

      {:error, reason} ->
        Logger.error("Plugin uninstall failed: #{inspect(reason)}")
        raise GRPC.RPCError, status: :internal, message: "Failed to uninstall plugin"
    end
  end

  def list_plugins(_request, _stream) do
    {:ok, plugins} = apply(@registry_module, :list, [])

    plugin_infos =
      Enum.map(plugins, fn plugin ->
        %PluginInfo{
          name: plugin.name,
          version: plugin[:version] || "0.0.0",
          type: plugin_type_to_proto(plugin.type),
          has_nif: plugin[:has_nif] || false,
          state: :PLUGIN_LOADED
        }
      end)

    %PluginList{plugins: plugin_infos}
  end

  def reload_plugin(request, _stream) do
    case apply(@registry_module, :reload, [request.name]) do
      :ok ->
        %Empty{}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Plugin not found"

      {:error, reason} ->
        Logger.error("Plugin reload failed: #{inspect(reason)}")
        raise GRPC.RPCError, status: :internal, message: "Failed to reload plugin"
    end
  end

  # Helpers

  defp plugin_type_to_proto(:data_type), do: :PLUGIN_DATA_TYPE
  defp plugin_type_to_proto(:function), do: :PLUGIN_FUNCTION
  defp plugin_type_to_proto(:storage), do: :PLUGIN_STORAGE
  defp plugin_type_to_proto(:index), do: :PLUGIN_INDEX
  defp plugin_type_to_proto(_), do: :PLUGIN_DATA_TYPE
end
