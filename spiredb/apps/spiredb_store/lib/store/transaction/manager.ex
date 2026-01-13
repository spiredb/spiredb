defmodule Store.Transaction.Manager do
  @moduledoc """
  Transaction manager for active transactions.

  Tracks active transactions per connection and provides
  the coordination for Percolator-style 2PC.

  Integrates:
  - SSIConflictDetector for serializable isolation
  - AsyncCommitCoordinator for 1-RTT cross-store commits
  - LockWaitQueue for lock conflict handling
  """

  use GenServer

  require Logger
  alias Store.Transaction
  alias Store.Transaction.Executor
  alias Store.Transaction.SSIConflictDetector
  alias Store.Transaction.AsyncCommitCoordinator

  defstruct [
    # txn_id -> Transaction
    transactions: %{},
    # RocksDB reference %{db: ref, cfs: %{name -> cf}}
    store_ref: nil
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
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
  def init(opts) do
    Logger.info("Transaction Manager started")
    store_ref = resolve_store_ref(opts)

    # Run crash recovery for any pending commits from previous run
    # Skip if running in test environment (detected via process name registry presence check or env)
    unless Application.get_env(:spiredb_common, :environment) == :test do
      spawn(fn ->
        # Wait for RocksDB to be ready
        Process.sleep(500)
        # Crash recovery needs the global store ref for now, or update it too
        Store.Transaction.CrashRecovery.recover_pending_commits(store_ref)
      end)
    end

    {:ok, %__MODULE__{store_ref: store_ref}}
  end

  @impl true
  def handle_call({:begin, opts}, _from, state) do
    case PD.TSO.get_timestamp() do
      {:ok, start_ts} ->
        txn = Transaction.new(start_ts, opts)
        new_state = %{state | transactions: Map.put(state.transactions, txn.id, txn)}

        # Register with SSI for serializable transactions
        SSIConflictDetector.register_transaction(txn)

        Logger.debug("Transaction #{txn.id} started at ts=#{start_ts}")
        {:reply, {:ok, txn.id}, new_state}

      {:error, reason} ->
        {:reply, {:error, {:tso_error, reason}}, state}
    end
  end

  @impl true
  def handle_call({:get, txn_id, key}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id),
         {:ok, value} <- Executor.mvcc_get(state.store_ref, key, txn.start_ts) do
      # Record in read set for conflict detection
      updated_txn = Transaction.record_read(txn, key)
      new_state = update_txn(state, updated_txn)

      # Track read for SSI
      SSIConflictDetector.record_read(state.store_ref, txn, key, txn.start_ts)

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

      # Track write for SSI
      SSIConflictDetector.record_write(txn, key)

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

      # Track write for SSI
      SSIConflictDetector.record_write(txn, key)

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
    with {:ok, txn} <- get_txn(state, txn_id),
         :ok <- SSIConflictDetector.validate_commit(state.store_ref, txn) do
      # Choose commit strategy based on transaction characteristics
      result = commit_transaction(state.store_ref, txn)

      case result do
        {:ok, commit_ts} ->
          # Cleanup SSI tracking
          SSIConflictDetector.cleanup_transaction(txn_id)
          new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
          Logger.debug("Transaction #{txn_id} committed at ts=#{commit_ts}")
          {:reply, {:ok, commit_ts}, new_state}

        {:error, reason} ->
          # Transaction failed, clean up
          SSIConflictDetector.cleanup_transaction(txn_id)
          Task.start(fn -> Executor.cleanup(state.store_ref, txn) end)
          new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
          {:reply, {:error, reason}, new_state}
      end
    else
      {:error, {:serialization_failure, _} = reason} ->
        # SSI validation failed
        with {:ok, txn} <- get_txn(state, txn_id) do
          SSIConflictDetector.cleanup_transaction(txn_id)
          Task.start(fn -> Executor.cleanup(state.store_ref, txn) end)
          new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
          {:reply, {:error, reason}, new_state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:rollback, txn_id}, _from, state) do
    with {:ok, txn} <- get_txn(state, txn_id) do
      SSIConflictDetector.cleanup_transaction(txn_id)
      Task.start(fn -> Executor.cleanup(state.store_ref, txn) end)
      new_state = %{state | transactions: Map.delete(state.transactions, txn_id)}
      Logger.debug("Transaction #{txn_id} rolled back")
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Private helpers

  defp resolve_store_ref(opts) do
    case opts[:store_ref] do
      ref when is_map(ref) ->
        ref

      _ ->
        db = :persistent_term.get(:spiredb_rocksdb_ref, nil)
        cfs = :persistent_term.get(:spiredb_rocksdb_cf_map, %{})
        %{db: db, cfs: cfs}
    end
  end

  defp commit_transaction(store_ref, txn) do
    key_count = map_size(txn.write_buffer)

    cond do
      key_count == 0 ->
        # Empty transaction
        {:ok, txn.start_ts}

      key_count > 1 and use_async_commit?() ->
        # Multi-key transaction: use async commit for lower latency
        AsyncCommitCoordinator.commit(store_ref, txn)

      true ->
        # Standard 2PC
        Executor.commit(store_ref, txn)
    end
  end

  defp use_async_commit? do
    # Can be configured via application env
    Application.get_env(:spiredb_store, :use_async_commit, true)
  end

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

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Transaction.Manager received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end
end
