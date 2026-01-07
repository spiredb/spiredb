defmodule PD.PluginManager do
  @moduledoc """
  Cluster-wide plugin management via Raft consensus.

  Handles:
  - Plugin registration/deregistration across the cluster
  - Plugin state synchronization
  - Plugin version coordination
  """

  use GenServer
  require Logger

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Register a plugin with the cluster via Raft.
  """
  @spec register_plugin(map()) :: :ok | {:error, term()}
  def register_plugin(plugin_info) when is_map(plugin_info) do
    GenServer.call(__MODULE__, {:register_plugin, plugin_info})
  end

  @doc """
  Unregister a plugin from the cluster via Raft.
  """
  @spec unregister_plugin(String.t()) :: :ok | {:error, term()}
  def unregister_plugin(plugin_name) when is_binary(plugin_name) do
    GenServer.call(__MODULE__, {:unregister_plugin, plugin_name})
  end

  @doc """
  Get all registered cluster plugins from Raft state.
  """
  @spec list_plugins() :: {:ok, [map()]} | {:error, term()}
  def list_plugins do
    GenServer.call(__MODULE__, :list_plugins)
  end

  @doc """
  Get a specific plugin by name.
  """
  @spec get_plugin(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_plugin(plugin_name) when is_binary(plugin_name) do
    GenServer.call(__MODULE__, {:get_plugin, plugin_name})
  end

  @doc """
  Get plugins that should be synced to a store node.
  Called during heartbeat processing.
  """
  @spec get_plugins_for_node(String.t()) :: {:ok, [map()]}
  def get_plugins_for_node(_node_address) do
    list_plugins()
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    # Load from Raft state asynchronously
    send(self(), :load_from_raft)
    {:ok, %{plugins: %{}, loaded: false}}
  end

  @impl true
  def handle_call({:register_plugin, plugin_info}, _from, state) do
    name = Map.get(plugin_info, :name) || Map.get(plugin_info, "name")

    if is_nil(name) do
      {:reply, {:error, :missing_name}, state}
    else
      case ra_command({:register_plugin, plugin_info}) do
        {:ok, {:ok, ^name}, _leader} ->
          # Update local cache
          normalized = normalize_plugin_info(plugin_info)
          plugins = Map.put(state.plugins, name, normalized)
          Logger.info("Registered cluster plugin via Raft: #{name}")
          {:reply, :ok, %{state | plugins: plugins}}

        {:ok, result, _leader} ->
          {:reply, result, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}

        {:timeout, _} ->
          {:reply, {:error, :timeout}, state}
      end
    end
  end

  @impl true
  def handle_call({:unregister_plugin, plugin_name}, _from, state) do
    case ra_command({:unregister_plugin, plugin_name}) do
      {:ok, :ok, _leader} ->
        plugins = Map.delete(state.plugins, plugin_name)
        Logger.info("Unregistered cluster plugin via Raft: #{plugin_name}")
        {:reply, :ok, %{state | plugins: plugins}}

      {:ok, {:error, :not_found}, _leader} ->
        {:reply, {:error, :not_found}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:timeout, _} ->
        {:reply, {:error, :timeout}, state}
    end
  end

  @impl true
  def handle_call(:list_plugins, _from, state) do
    {:reply, {:ok, Map.values(state.plugins)}, state}
  end

  @impl true
  def handle_call({:get_plugin, plugin_name}, _from, state) do
    case Map.get(state.plugins, plugin_name) do
      nil -> {:reply, {:error, :not_found}, state}
      info -> {:reply, {:ok, info}, state}
    end
  end

  @impl true
  def handle_info(:load_from_raft, state) do
    plugins = load_plugins_from_raft()
    {:noreply, %{state | plugins: plugins, loaded: true}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("PD.PluginManager received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Private

  defp normalize_plugin_info(info) when is_map(info) do
    %{
      name: Map.get(info, :name) || Map.get(info, "name"),
      version: Map.get(info, :version) || Map.get(info, "version"),
      type: Map.get(info, :type) || Map.get(info, "type"),
      description: Map.get(info, :description) || Map.get(info, "description"),
      has_nif: Map.get(info, :has_nif) || Map.get(info, "has_nif", false),
      registered_at: DateTime.utc_now()
    }
  end

  defp ra_command(command) do
    try do
      server_id = {:pd_server, PD.Server.seed_node()}
      :ra.process_command(server_id, command, 5000)
    catch
      :exit, reason ->
        Logger.debug("Raft command failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp load_plugins_from_raft do
    try do
      server_id = {:pd_server, PD.Server.seed_node()}

      case :ra.leader_query(server_id, &get_plugins/1, 5000) do
        {:ok, {_idx, plugins}, _leader} when is_map(plugins) ->
          plugins

        _ ->
          %{}
      end
    catch
      _, _ -> %{}
    end
  end

  defp get_plugins(state) do
    Map.get(state, :plugins, %{})
  end
end
