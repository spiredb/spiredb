defmodule Store.API.RESP.PluginCommands do
  @moduledoc """
  RESP handlers for plugin management commands.

  Commands:
  - SPIRE.PLUGIN.LIST - List loaded plugins
  - SPIRE.PLUGIN.INFO <name> - Get plugin info
  - SPIRE.PLUGIN.RELOAD <name> - Reload a plugin
  """

  require Logger
  alias Store.Plugin.Registry

  def execute(["SPIRE.PLUGIN.LIST"]) do
    case Registry.list() do
      {:ok, plugins} ->
        Enum.map(plugins, fn p ->
          [p.name, Atom.to_string(p.type), p.version]
        end)
    end
  end

  def execute(["SPIRE.PLUGIN.INFO", name]) do
    case Registry.get(name) do
      {:ok, _module, _state, info} ->
        [
          "name",
          info.name,
          "type",
          Atom.to_string(info.type),
          "version",
          info.version,
          "description",
          info.description,
          "has_nif",
          if(info.has_nif, do: "true", else: "false")
        ]

      {:error, :not_found} ->
        {:error, "ERR plugin '#{name}' not found"}
    end
  end

  def execute(["SPIRE.PLUGIN.RELOAD", name]) do
    case Registry.reload(name) do
      :ok -> "OK"
      {:error, :not_found} -> {:error, "ERR plugin '#{name}' not found"}
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(_) do
    {:error, "ERR unknown SPIRE.PLUGIN command"}
  end
end
