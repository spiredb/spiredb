defmodule Store.Transaction.BackPressure do
  @moduledoc """
  Back-pressure mechanism for transaction overload protection.

  Prevents system overload by:
  - Tracking active transaction count
  - Rejecting new transactions when limits exceeded
  - Applying adaptive delays based on load
  """

  use GenServer

  require Logger

  @default_max_active 10_000
  @default_max_pending_writes 100_000
  @high_watermark_ratio 0.8
  @low_watermark_ratio 0.5

  defstruct [
    :max_active,
    :max_pending_writes,
    active_count: 0,
    pending_writes: 0,
    in_back_pressure: false
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Try to acquire a transaction slot.
  Returns :ok or {:error, :overloaded}
  """
  def try_acquire do
    GenServer.call(__MODULE__, :try_acquire, 5000)
  catch
    # If not running, allow
    :exit, _ -> :ok
  end

  @doc """
  Release a transaction slot.
  """
  def release do
    GenServer.cast(__MODULE__, :release)
  catch
    :exit, _ -> :ok
  end

  @doc """
  Track pending writes for a transaction.
  """
  def add_pending_writes(count) do
    GenServer.cast(__MODULE__, {:add_writes, count})
  catch
    :exit, _ -> :ok
  end

  @doc """
  Remove pending writes when committed/rolled back.
  """
  def remove_pending_writes(count) do
    GenServer.cast(__MODULE__, {:remove_writes, count})
  catch
    :exit, _ -> :ok
  end

  @doc """
  Check if system is in back-pressure mode.
  """
  def in_back_pressure? do
    GenServer.call(__MODULE__, :in_back_pressure?, 1000)
  catch
    :exit, _ -> false
  end

  @doc """
  Get current stats.
  """
  def get_stats do
    GenServer.call(__MODULE__, :get_stats, 1000)
  catch
    :exit, _ -> %{active: 0, pending_writes: 0, in_back_pressure: false}
  end

  ## Callbacks

  @impl true
  def init(opts) do
    max_active = Keyword.get(opts, :max_active, @default_max_active)
    max_pending = Keyword.get(opts, :max_pending_writes, @default_max_pending_writes)

    Logger.info("Transaction BackPressure started (max_active: #{max_active})")

    {:ok,
     %__MODULE__{
       max_active: max_active,
       max_pending_writes: max_pending
     }}
  end

  @impl true
  def handle_call(:try_acquire, _from, state) do
    cond do
      state.active_count >= state.max_active ->
        Logger.warning("Transaction rejected: max active (#{state.max_active}) reached")
        {:reply, {:error, :overloaded}, state}

      state.pending_writes >= state.max_pending_writes ->
        Logger.warning(
          "Transaction rejected: max pending writes (#{state.max_pending_writes}) reached"
        )

        {:reply, {:error, :overloaded}, state}

      true ->
        new_state = %{state | active_count: state.active_count + 1}
        new_state = check_back_pressure(new_state)
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call(:in_back_pressure?, _from, state) do
    {:reply, state.in_back_pressure, state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = %{
      active: state.active_count,
      pending_writes: state.pending_writes,
      in_back_pressure: state.in_back_pressure,
      max_active: state.max_active,
      max_pending_writes: state.max_pending_writes
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_cast(:release, state) do
    new_count = max(0, state.active_count - 1)
    new_state = %{state | active_count: new_count}
    new_state = check_back_pressure(new_state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:add_writes, count}, state) do
    new_state = %{state | pending_writes: state.pending_writes + count}
    new_state = check_back_pressure(new_state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:remove_writes, count}, state) do
    new_pending = max(0, state.pending_writes - count)
    new_state = %{state | pending_writes: new_pending}
    new_state = check_back_pressure(new_state)
    {:noreply, new_state}
  end

  ## Private

  defp check_back_pressure(state) do
    active_ratio = state.active_count / state.max_active
    writes_ratio = state.pending_writes / state.max_pending_writes

    max_ratio = max(active_ratio, writes_ratio)

    cond do
      not state.in_back_pressure and max_ratio >= @high_watermark_ratio ->
        Logger.warning("Entering back-pressure mode (ratio: #{Float.round(max_ratio, 2)})")
        %{state | in_back_pressure: true}

      state.in_back_pressure and max_ratio <= @low_watermark_ratio ->
        Logger.info("Exiting back-pressure mode (ratio: #{Float.round(max_ratio, 2)})")
        %{state | in_back_pressure: false}

      true ->
        state
    end
  end
end
