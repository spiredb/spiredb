defmodule Store.Transaction.Manager do
  @moduledoc """
  Transaction manager for active transactions.

  Tracks active transactions per connection and provides
  the coordination for Percolator-style 2PC.
  """

  use GenServer

  require Logger
  alias Store.Transaction
  alias Store.Transaction.Lock
  alias Store.Transaction.Executor

  defstruct [
    # txn_id -> Transaction
    transactions: %{}
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Begin a new transaction.
  """
  def begin_transaction(pid \\ __MODULE__, opts \\ []) do
    GenServer.call(pid, {:begin, opts})
  end

  @doc """
  Get a value within a transaction (MVCC read).
  """
  def get(pid \\ __MODULE__, txn_id, key) do
    GenServer.call(pid, {:get, txn_id, key})
  end

  @doc """
  Put a value within a transaction (buffered).
  """
  def put(pid \\ __MODULE__, txn_id, key, value) do
    GenServer.call(pid, {:put, txn_id, key, value})
  end

  @doc """
  Delete a key within a transaction (buffered).
  """
  def delete(pid \\ __MODULE__, txn_id, key) do
    GenServer.call(pid, {:delete, txn_id, key})
  end

  @doc """
  Create a savepoint.
  """
  def savepoint(pid \\ __MODULE__, txn_id, name) do
    GenServer.call(pid, {:savepoint, txn_id, name})
  end

  @doc """
  Rollback to a savepoint.
  """
  def rollback_to(pid \\ __MODULE__, txn_id, name) do
    GenServer.call(pid, {:rollback_to, txn_id, name})
  end

  @doc """
  Commit a transaction (2PC).
  """
  def commit(pid \\ __MODULE__, txn_id) do
    GenServer.call(pid, {:commit, txn_id}, 60_000)
  end

  @doc """
  Rollback a transaction.
  """
  def rollback(pid \\ __MODULE__, txn_id) do
    GenServer.call(pid, {:rollback, txn_id})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("Transaction Manager started")
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:begin, opts}, _from, state) do
    case PD.TSO.get_timestamp() do
      {:ok, start_ts} ->
        txn = Transaction.new(start_ts, opts)
        new_state = %{state | transactions: Map.put(state.transactions, txn.id, txn)}
        Logger.debug("Transaction #{txn.id} started at ts=#{start_ts}")
        {:reply, {:ok, txn.id}, new_state}

      {:error, reason} ->
        {:reply, {:error, {:tso_error, reason}}, state}
    end
  end

  @impl true
  def handle_call({:get, txn_id, key}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id),
         {:ok, value} <- Executor.mvcc_get(key, txn.start_ts) do
      # Record in read set for conflict detection
      updated_txn = Transaction.record_read(txn, key)
      new_state = update_txn(state, updated_txn)
      {:reply, {:ok, value}, new_state}
    else
      {:error, :locked, lock} ->
        {:reply, {:error, {:locked, lock}}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:put, txn_id, key, value}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id) do
      updated_txn =
        txn
        |> Transaction.put(key, value)
        |> Transaction.set_primary(key)

      new_state = update_txn(state, updated_txn)
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, txn_id, key}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id) do
      updated_txn =
        txn
        |> Transaction.delete(key)
        |> Transaction.set_primary(key)

      new_state = update_txn(state, updated_txn)
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:savepoint, txn_id, name}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id) do
      updated_txn = Transaction.savepoint(txn, name)
      new_state = update_txn(state, updated_txn)
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:rollback_to, txn_id, name}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id),
         {:ok, updated_txn} <- Transaction.rollback_to(txn, name) do
      new_state = update_txn(state, updated_txn)
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:commit, txn_id}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id) do
      case Executor.commit(txn) do
        {:ok, commit_ts} ->
          # Remove transaction from active set
          new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
          Logger.debug("Transaction #{txn_id} committed at ts=#{commit_ts}")
          {:reply, {:ok, commit_ts}, new_state}

        {:error, reason} ->
          # Transaction failed, clean up
          Task.start(fn -> Executor.cleanup(txn) end)
          new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
          {:reply, {:error, reason}, new_state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:rollback, txn_id}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id) do
      Task.start(fn -> Executor.cleanup(txn) end)
      new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
      Logger.debug("Transaction #{txn_id} rolled back")
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Private helpers

  defp get_txn(state, txn_id) do
    case Map.get(state.transactions, txn_id) do
      nil ->
        {:error, :transaction_not_found}

      txn ->
        if Transaction.timed_out?(txn) do
          {:error, :transaction_timeout}
        else
          {:ok, txn}
        end
    end
  end

  defp update_txn(state, txn) do
    %{state | transactions: Map.put(state.transactions, txn.id, txn)}
  end
end
