defmodule PD.API.GRPC.Plugin do
  @moduledoc """
  gRPC PluginService implementation.
  """

  use GRPC.Server, service: Spiredb.Cluster.PluginService.Service

  require Logger
  alias Store.Plugin.Registry
  alias Store.Plugin.Loader

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

    case Loader.load_plugin(source) do
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
    case Loader.unload(request.name) do
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
    {:ok, plugins} = Registry.list()

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
    case Registry.reload(request.name) do
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
