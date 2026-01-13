defmodule Store.Region.StateMachine do
  @moduledoc false
  @behaviour :ra_machine

  alias Store.KV.Engine
  require Logger

  @type state :: map()

  @impl true
  def init(_config) do
    Logger.info("Initializing Region StateMachine")
    %{}
  end

  @impl true
  def apply(_meta, {:put, key, value}, state) do
    # Assuming Store.KV.Engine is registered with that name
    case Engine.put(Store.KV.Engine, key, value) do
      :ok -> {state, :ok, []}
      error -> {state, error, []}
    end
  end

  def apply(_meta, {:delete, key}, state) do
    case Engine.delete(Store.KV.Engine, key) do
      :ok -> {state, :ok, []}
      error -> {state, error, []}
    end
  end

  def apply(_meta, {:get, key}, state) do
    case Engine.get(Store.KV.Engine, key) do
      {:ok, value} -> {state, {:ok, value}, []}
      {:error, :not_found} -> {state, {:error, :not_found}, []}
      error -> {state, error, []}
    end
  end

  def apply(_meta, {:batch, operations}, state) when is_list(operations) do
    results =
      Enum.map(operations, fn
        {:put, key, value} ->
          case Engine.put(Store.KV.Engine, key, value) do
            :ok -> :ok
            error -> error
          end

        {:delete, key} ->
          case Engine.delete(Store.KV.Engine, key) do
            :ok -> :ok
            error -> error
          end
      end)

    # Return success count
    success_count = Enum.count(results, &(&1 == :ok))
    {state, {:ok, success_count}, []}
  end

  def apply(_meta, command, state) do
    Logger.warning("Unknown command received: #{inspect(command)}")
    {state, {:error, :unknown_command}, []}
  end

  @impl true
  def state_enter(_ra_state, _machine_state), do: []
end
