defmodule Store.Test.MockServer do
  @moduledoc false
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    {:ok, %{}}
  end

  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  def handle_call({:get, key}, _from, state) do
    case Map.get(state, key) do
      nil -> {:reply, {:error, :not_found}, state}
      val -> {:reply, {:ok, val}, state}
    end
  end

  def handle_call({:put, key, value}, _from, state) do
    {:reply, {:ok, :ok}, Map.put(state, key, value)}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, {:ok, :ok}, Map.delete(state, key)}
  end
end
