defmodule Store.Region.ReadTsTracker do
  @moduledoc """
  Tracks maximum read timestamp per region for async commit.

  Used to calculate valid commit_ts without TSO round-trip.
  For async commit: commit_ts = max(min_commit_ts, max_read_ts + 1)
  """

  use GenServer

  require Logger

  @type state :: %{
          region_id: non_neg_integer(),
          max_read_ts: non_neg_integer()
        }

  def start_link(opts) do
    region_id = Keyword.fetch!(opts, :region_id)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(region_id))
  end

  @doc """
  Update max_read_ts when a read occurs.
  Non-blocking cast for performance.
  """
  def record_read(region_id, read_ts) when is_integer(region_id) do
    GenServer.cast(via_tuple(region_id), {:record_read, read_ts})
  catch
    :exit, _ -> :ok
  end

  def record_read(nil, _read_ts), do: :ok

  @doc """
  Get current max_read_ts for commit_ts calculation.
  Returns 0 if tracker doesn't exist (single-node mode).
  """
  def get_max_read_ts(region_id) when is_integer(region_id) do
    GenServer.call(via_tuple(region_id), :get_max_read_ts, 1000)
  catch
    :exit, _ -> 0
  end

  def get_max_read_ts(nil), do: 0

  @doc """
  Check if commit_ts is safe (no reads after commit_ts).
  Used during async commit resolution.
  """
  def is_commit_ts_safe?(region_id, commit_ts) when is_integer(region_id) do
    GenServer.call(via_tuple(region_id), {:is_safe, commit_ts}, 1000)
  catch
    :exit, _ -> true
  end

  def is_commit_ts_safe?(nil, _commit_ts), do: true

  ## Callbacks

  @impl true
  def init(opts) do
    region_id = Keyword.fetch!(opts, :region_id)

    {:ok,
     %{
       region_id: region_id,
       max_read_ts: 0
     }}
  end

  @impl true
  def handle_cast({:record_read, read_ts}, state) do
    new_max = max(state.max_read_ts, read_ts)
    {:noreply, %{state | max_read_ts: new_max}}
  end

  @impl true
  def handle_call(:get_max_read_ts, _from, state) do
    {:reply, state.max_read_ts, state}
  end

  @impl true
  def handle_call({:is_safe, commit_ts}, _from, state) do
    # Safe if no reads happened after this commit_ts
    safe = state.max_read_ts < commit_ts
    {:reply, safe, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("ReadTsTracker received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp via_tuple(region_id) do
    {:via, Registry, {Store.Region.Registry, {:read_ts_tracker, region_id}}}
  end
end
