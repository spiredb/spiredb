defmodule Store.Stream.Watermark do
  @moduledoc """
  Track event-time vs processing-time watermarks.

  High watermark: max event_time seen
  Low watermark: min uncommitted event_time (for late arrival handling)
  """
  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Update the high watermark (max event timestamp seen).
  """
  def update_high_watermark(pid \\ __MODULE__, event_time) do
    GenServer.cast(pid, {:update_high, event_time})
  end

  @doc """
  Get current watermarks.
  Returns %{high: timestamp, low: timestamp}
  """
  def get_watermarks(pid \\ __MODULE__) do
    GenServer.call(pid, :get_watermarks)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    {:ok, %{high: 0, low: 0}}
  end

  @impl true
  def handle_cast({:update_high, ts}, state) do
    new_high = max(state.high, ts)
    # For now, simplistic low watermark tracking (equal to high or some lag?)
    # Real low watermark requires tracking outstanding uncommitted txns
    {:noreply, %{state | high: new_high}}
  end

  @impl true
  def handle_call(:get_watermarks, _from, state) do
    {:reply, state, state}
  end
end
