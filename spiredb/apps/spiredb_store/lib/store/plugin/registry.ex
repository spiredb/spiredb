defmodule Store.Plugin.Registry do
  @moduledoc """
  Registry for loaded plugins.

  Tracks active plugins and provides lookup by name or type.
  """

  use GenServer

  require Logger

  defstruct [
    # name -> {module, state, info}
    plugins: %{},
    # type -> [name, ...]
    by_type: %{}
  ]

  ## Client API

  def start_link(opts \\ []) do
    # Use Keyword.has_key? to distinguish explicit nil from missing key
    name = if Keyword.has_key?(opts, :name), do: opts[:name], else: __MODULE__

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      # Anonymous process when name: nil explicitly passed
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc """
  Register a plugin.
  """
  def register(pid \\ __MODULE__, module, opts \\ []) do
    GenServer.call(pid, {:register, module, opts})
  end

  @doc """
  Unregister a plugin by name.
  """
  def unregister(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:unregister, name})
  end

  @doc """
  Get a plugin by name.
  """
  def get(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:get, name})
  end

  @doc """
  List all plugins.
  """
  def list(pid \\ __MODULE__) do
    GenServer.call(pid, :list)
  end

  @doc """
  List plugins by type.
  """
  def list_by_type(pid \\ __MODULE__, type) do
    GenServer.call(pid, {:list_by_type, type})
  end

  @doc """
  Reload a plugin.
  """
  def reload(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:reload, name})
  end

  @doc """
  Register plugin info only (metadata without module).
  Used for cluster sync when module isn't locally available.
  """
  def register_info(pid \\ __MODULE__, name, info) when is_binary(name) and is_map(info) do
    GenServer.call(pid, {:register_info, name, info})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("Plugin Registry started")
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:register, module, opts}, _from, state) do
    info = module.info()
    name = info.name

    if Map.has_key?(state.plugins, name) do
      {:reply, {:error, :already_registered}, state}
    else
      case module.init(opts) do
        {:ok, plugin_state} ->
          Logger.info("Registered plugin: #{name} (#{info.type})")

          new_plugins = Map.put(state.plugins, name, {module, plugin_state, info})
          new_by_type = Map.update(state.by_type, info.type, [name], &[name | &1])

          new_state = %{state | plugins: new_plugins, by_type: new_by_type}
          {:reply, :ok, new_state}

        {:error, reason} ->
          {:reply, {:error, {:init_failed, reason}}, state}
      end
    end
  end

  @impl true
  def handle_call({:unregister, name}, _from, state) do
    case Map.pop(state.plugins, name) do
      {nil, _} ->
        {:reply, {:error, :not_found}, state}

      {{module, plugin_state, info}, new_plugins} ->
        # Shutdown plugin
        try do
          module.shutdown(plugin_state)
        catch
          _, _ -> :ok
        end

        Logger.info("Unregistered plugin: #{name}")

        new_by_type = Map.update(state.by_type, info.type, [], &List.delete(&1, name))
        new_state = %{state | plugins: new_plugins, by_type: new_by_type}
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:register_info, name, info}, _from, state) do
    # Register plugin metadata only (no module, for cluster sync)
    if Map.has_key?(state.plugins, name) do
      {:reply, {:error, :already_registered}, state}
    else
      plugin_type = Map.get(info, :type) || Map.get(info, "type") || :unknown
      normalized_info = normalize_info(info)

      new_plugins = Map.put(state.plugins, name, {nil, nil, normalized_info})
      new_by_type = Map.update(state.by_type, plugin_type, [name], &[name | &1])

      Logger.debug("Registered plugin info (no module): #{name}")
      {:reply, :ok, %{state | plugins: new_plugins, by_type: new_by_type}}
    end
  end

  @impl true
  def handle_call({:get, name}, _from, state) do
    case Map.get(state.plugins, name) do
      nil -> {:reply, {:error, :not_found}, state}
      {module, plugin_state, info} -> {:reply, {:ok, module, plugin_state, info}, state}
    end
  end

  @impl true
  def handle_call(:list, _from, state) do
    plugins =
      state.plugins
      |> Enum.map(fn {name, {module, _state, info}} ->
        info
        |> Map.put(:name, name)
        |> Map.put(:has_module, module != nil)
      end)

    {:reply, {:ok, plugins}, state}
  end

  @impl true
  def handle_call({:list_by_type, type}, _from, state) do
    names = Map.get(state.by_type, type, [])

    plugins =
      names
      |> Enum.map(fn name ->
        {_module, _state, info} = Map.get(state.plugins, name)
        Map.put(info, :name, name)
      end)

    {:reply, {:ok, plugins}, state}
  end

  @impl true
  def handle_call({:reload, name}, _from, state) do
    case Map.get(state.plugins, name) do
      nil ->
        {:reply, {:error, :not_found}, state}

      {module, plugin_state, info} ->
        # Shutdown old instance
        try do
          module.shutdown(plugin_state)
        catch
          _, _ -> :ok
        end

        # Reinitialize
        case module.init([]) do
          {:ok, new_plugin_state} ->
            Logger.info("Reloaded plugin: #{name}")
            new_plugins = Map.put(state.plugins, name, {module, new_plugin_state, info})
            {:reply, :ok, %{state | plugins: new_plugins}}

          {:error, reason} ->
            # Remove failed plugin
            new_plugins = Map.delete(state.plugins, name)
            {:reply, {:error, {:reload_failed, reason}}, %{state | plugins: new_plugins}}
        end
    end
  end

  @impl true
  def terminate(_reason, state) do
    # Shutdown all plugins
    Enum.each(state.plugins, fn {name, {module, plugin_state, _}} ->
      try do
        module.shutdown(plugin_state)
        Logger.debug("Shutdown plugin: #{name}")
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Plugin.Registry received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Private helpers

  defp normalize_info(info) when is_map(info) do
    %{
      name: Map.get(info, :name) || Map.get(info, "name"),
      version: Map.get(info, :version) || Map.get(info, "version"),
      type: Map.get(info, :type) || Map.get(info, "type"),
      description: Map.get(info, :description) || Map.get(info, "description"),
      has_nif: Map.get(info, :has_nif) || Map.get(info, "has_nif", false)
    }
  end
end
