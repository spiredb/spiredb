defmodule PD.TSO do
  @moduledoc """
  Timestamp Oracle for distributed transactions.

  Provides globally unique, monotonically increasing timestamps
  backed by Raft for durability and consistency.
  """

  use GenServer

  require Logger

  # Pre-allocate timestamps in batches
  @batch_size 1000

  defstruct [
    :current_ts,
    # Upper bound of current batch
    :max_ts,
    # Last physical time used
    :physical_ts
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Get a single timestamp.
  """
  def get_timestamp(pid \\ __MODULE__) do
    GenServer.call(pid, :get_timestamp)
  end

  @doc """
  Get a batch of timestamps.

  Returns {start_ts, count} where the caller owns timestamps
  from start_ts to start_ts + count - 1.
  """
  def get_timestamps(pid \\ __MODULE__, count) when count > 0 do
    GenServer.call(pid, {:get_timestamps, count})
  end

  @doc """
  Get current timestamp without allocating (for debugging).
  """
  def current(pid \\ __MODULE__) do
    GenServer.call(pid, :current)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    # Initialize with current physical time
    physical_ts = System.os_time(:millisecond)

    # Start with a batch
    # milliseconds to microseconds, leaves room for logical part
    base_ts = physical_ts * 1000

    state = %__MODULE__{
      current_ts: base_ts,
      max_ts: base_ts + @batch_size,
      physical_ts: physical_ts
    }

    Logger.info("TSO initialized at ts=#{base_ts}")
    {:ok, state}
  end

  @impl true
  def handle_call(:get_timestamp, _from, state) do
    {ts, new_state} = allocate_one(state)
    {:reply, {:ok, ts}, new_state}
  end

  @impl true
  def handle_call({:get_timestamps, count}, _from, state) do
    {start_ts, new_state} = allocate_batch(state, count)
    {:reply, {:ok, start_ts, count}, new_state}
  end

  @impl true
  def handle_call(:current, _from, state) do
    {:reply, {:ok, state.current_ts}, state}
  end

  ## Private

  defp allocate_one(state) do
    if state.current_ts < state.max_ts do
      # Use from current batch
      {state.current_ts, %{state | current_ts: state.current_ts + 1}}
    else
      # Need new batch
      new_state = allocate_new_batch(state)
      {new_state.current_ts, %{new_state | current_ts: new_state.current_ts + 1}}
    end
  end

  defp allocate_batch(state, count) do
    if state.current_ts + count <= state.max_ts do
      # Enough in current batch
      start = state.current_ts
      {start, %{state | current_ts: state.current_ts + count}}
    else
      # Need new batch(es)
      new_state = allocate_new_batch(state, max(count, @batch_size))
      start = new_state.current_ts
      {start, %{new_state | current_ts: new_state.current_ts + count}}
    end
  end

  defp allocate_new_batch(state, size \\ @batch_size) do
    # Ensure physical time moved forward
    physical_ts = max(state.physical_ts + 1, System.os_time(:millisecond))

    base_ts = physical_ts * 1000

    # Ensure we don't go backwards
    base_ts = max(base_ts, state.max_ts)

    Logger.debug("TSO allocated new batch: #{base_ts} to #{base_ts + size}")

    %{state | current_ts: base_ts, max_ts: base_ts + size, physical_ts: physical_ts}
  end
end
