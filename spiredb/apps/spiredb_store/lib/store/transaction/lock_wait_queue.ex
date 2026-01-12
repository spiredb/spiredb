defmodule Store.Transaction.LockWaitQueue do
  @moduledoc """
  Lock wait queue for blocked transactions.

  Instead of immediately failing on lock conflict:
  1. Add to wait queue
  2. Wake when lock released
  3. Timeout after configurable duration

  Includes deadlock detection via wait-for graph.
  """

  use GenServer

  require Logger

  @default_lock_wait_timeout 10_000

  defstruct [
    # key => [{waiting_txn_id, waiting_pid, timestamp}, ...]
    wait_queues: %{},
    # txn_id => [keys waiting on]
    txn_waits: %{},
    # For deadlock detection: txn_id => holding_txn_id
    wait_for_graph: %{}
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Wait for lock on key. Returns when lock available or timeout.
  """
  def wait_for_lock(key, txn_id, holding_txn_id, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_lock_wait_timeout)

    case GenServer.call(__MODULE__, {:wait, key, txn_id, holding_txn_id}, timeout + 1000) do
      :ok -> :ok
      {:error, :deadlock} -> {:error, :deadlock_detected}
      {:error, :timeout} -> {:error, :lock_wait_timeout}
    end
  catch
    :exit, {:timeout, _} -> {:error, :lock_wait_timeout}
    :exit, {:noproc, _} -> {:error, :lock_wait_queue_not_started}
  end

  @doc """
  Notify that lock on key is released.
  """
  def lock_released(key) do
    GenServer.cast(__MODULE__, {:lock_released, key})
  catch
    :exit, _ -> :ok
  end

  @doc """
  Transaction completed - clean up any waits.
  """
  def transaction_done(txn_id) do
    GenServer.cast(__MODULE__, {:txn_done, txn_id})
  catch
    :exit, _ -> :ok
  end

  ## Callbacks

  @impl true
  def init(_opts) do
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:wait, key, txn_id, holding_txn_id}, from, state) do
    # Check for deadlock before waiting
    case detect_deadlock(state.wait_for_graph, txn_id, holding_txn_id) do
      :no_deadlock ->
        # Add to wait queue
        waiter = {txn_id, from, System.monotonic_time(:millisecond)}
        wait_queue = Map.get(state.wait_queues, key, [])
        new_wait_queues = Map.put(state.wait_queues, key, wait_queue ++ [waiter])

        # Update wait-for graph
        new_wait_for_graph = Map.put(state.wait_for_graph, txn_id, holding_txn_id)

        # Track which keys this txn is waiting on
        txn_keys = Map.get(state.txn_waits, txn_id, [])
        new_txn_waits = Map.put(state.txn_waits, txn_id, [key | txn_keys])

        # Schedule timeout check
        Process.send_after(self(), {:check_timeout, key, txn_id}, @default_lock_wait_timeout)

        {:noreply,
         %{
           state
           | wait_queues: new_wait_queues,
             wait_for_graph: new_wait_for_graph,
             txn_waits: new_txn_waits
         }}

      :deadlock ->
        {:reply, {:error, :deadlock}, state}
    end
  end

  @impl true
  def handle_cast({:lock_released, key}, state) do
    case Map.get(state.wait_queues, key, []) do
      [] ->
        {:noreply, state}

      [{txn_id, from, _timestamp} | rest] ->
        # Wake the first waiter
        GenServer.reply(from, :ok)

        # Remove from wait-for graph
        new_wait_for_graph = Map.delete(state.wait_for_graph, txn_id)

        # Update wait queues
        new_wait_queues =
          if rest == [] do
            Map.delete(state.wait_queues, key)
          else
            Map.put(state.wait_queues, key, rest)
          end

        {:noreply, %{state | wait_queues: new_wait_queues, wait_for_graph: new_wait_for_graph}}
    end
  end

  @impl true
  def handle_cast({:txn_done, txn_id}, state) do
    # Remove all waits for this transaction
    keys_waiting = Map.get(state.txn_waits, txn_id, [])

    new_wait_queues =
      Enum.reduce(keys_waiting, state.wait_queues, fn key, queues ->
        queue = Map.get(queues, key, [])
        filtered = Enum.reject(queue, fn {tid, _, _} -> tid == txn_id end)

        if filtered == [] do
          Map.delete(queues, key)
        else
          Map.put(queues, key, filtered)
        end
      end)

    new_wait_for_graph = Map.delete(state.wait_for_graph, txn_id)
    new_txn_waits = Map.delete(state.txn_waits, txn_id)

    {:noreply,
     %{
       state
       | wait_queues: new_wait_queues,
         wait_for_graph: new_wait_for_graph,
         txn_waits: new_txn_waits
     }}
  end

  @impl true
  def handle_info({:check_timeout, key, txn_id}, state) do
    case Map.get(state.wait_queues, key, []) do
      [] ->
        {:noreply, state}

      waiters ->
        # Find and timeout the matching waiter
        case Enum.split_with(waiters, fn {tid, _, _} -> tid == txn_id end) do
          {[{^txn_id, from, _}], rest} ->
            GenServer.reply(from, {:error, :timeout})

            new_wait_queues =
              if rest == [] do
                Map.delete(state.wait_queues, key)
              else
                Map.put(state.wait_queues, key, rest)
              end

            new_wait_for_graph = Map.delete(state.wait_for_graph, txn_id)

            {:noreply,
             %{state | wait_queues: new_wait_queues, wait_for_graph: new_wait_for_graph}}

          _ ->
            {:noreply, state}
        end
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("LockWaitQueue received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Deadlock Detection

  defp detect_deadlock(wait_for_graph, txn_id, holding_txn_id) do
    # DFS to find cycle starting from holding_txn_id
    visited = MapSet.new([txn_id])
    detect_cycle(wait_for_graph, holding_txn_id, txn_id, visited)
  end

  defp detect_cycle(_graph, current, target, _visited) when current == target do
    :deadlock
  end

  defp detect_cycle(graph, current, target, visited) do
    if MapSet.member?(visited, current) do
      :no_deadlock
    else
      case Map.get(graph, current) do
        nil ->
          :no_deadlock

        next ->
          detect_cycle(graph, next, target, MapSet.put(visited, current))
      end
    end
  end
end
