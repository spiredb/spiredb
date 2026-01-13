defmodule Store.Transaction do
  @moduledoc """
  Percolator-style transaction state.

  A transaction consists of:
  - start_ts: Timestamp when transaction began (from PD.TSO)
  - commit_ts: Timestamp when transaction committed (from PD.TSO)
  - primary_key: The designated coordinator key
  - write_buffer: Local writes buffered until commit
  - locks: Keys that have been locked
  - isolation: Isolation level
  - savepoints: Named savepoints for partial rollback
  """

  @type isolation_level :: :read_committed | :repeatable_read | :serializable
  @type lock_type :: :optimistic | :pessimistic

  @type t :: %__MODULE__{
          id: binary(),
          start_ts: non_neg_integer(),
          commit_ts: non_neg_integer() | nil,
          primary_key: binary() | nil,
          write_buffer: %{binary() => {:put, binary()} | :delete},
          read_set: MapSet.t(binary()),
          locks: MapSet.t(binary()),
          isolation: isolation_level(),
          lock_type: lock_type(),
          savepoints: %{String.t() => %{binary() => any()}},
          timeout_ms: pos_integer(),
          started_at: integer()
        }

  defstruct [
    :id,
    :start_ts,
    :commit_ts,
    :primary_key,
    write_buffer: %{},
    read_set: MapSet.new(),
    write_set: MapSet.new(),
    locks: MapSet.new(),
    isolation: :repeatable_read,
    lock_type: :optimistic,
    savepoints: %{},
    timeout_ms: 30_000,
    started_at: nil
  ]

  @doc """
  Create a new transaction with a start timestamp.
  """
  def new(start_ts, opts \\ []) do
    %__MODULE__{
      id: generate_txn_id(),
      start_ts: start_ts,
      isolation: Keyword.get(opts, :isolation, :repeatable_read),
      lock_type: Keyword.get(opts, :lock_type, :optimistic),
      timeout_ms: Keyword.get(opts, :timeout_ms, 30_000),
      started_at: System.monotonic_time(:millisecond)
    }
  end

  @doc """
  Buffer a write operation.
  """
  def put(txn, key, value) do
    txn
    |> add_to_write_set(key)
    |> Map.update!(:write_buffer, &Map.put(&1, key, {:put, value}))
  end

  @doc """
  Buffer a delete operation.
  """
  def delete(txn, key) do
    txn
    |> add_to_write_set(key)
    |> Map.update!(:write_buffer, &Map.put(&1, key, :delete))
  end

  @doc """
  Record a key in the read set (for conflict detection).
  """
  def record_read(txn, key) do
    %{txn | read_set: MapSet.put(txn.read_set, key)}
  end

  @doc """
  Create a savepoint.
  """
  def savepoint(txn, name) do
    %{txn | savepoints: Map.put(txn.savepoints, name, txn.write_buffer)}
  end

  @doc """
  Rollback to a savepoint.
  """
  def rollback_to(txn, name) do
    case Map.get(txn.savepoints, name) do
      nil -> {:error, :savepoint_not_found}
      buffer -> {:ok, %{txn | write_buffer: buffer}}
    end
  end

  @doc """
  Set the primary key for this transaction.
  The primary key is the first key written.
  """
  def set_primary(txn, key) do
    if txn.primary_key do
      txn
    else
      %{txn | primary_key: key}
    end
  end

  @doc """
  Check if transaction has timed out.
  """
  def timed_out?(txn) do
    elapsed = System.monotonic_time(:millisecond) - txn.started_at
    elapsed > txn.timeout_ms
  end

  @doc """
  Get all keys in the write buffer.
  """
  def write_keys(txn) do
    Map.keys(txn.write_buffer)
  end

  @doc """
  Get secondary keys (all keys except primary).
  """
  def secondary_keys(txn) do
    txn.write_buffer
    |> Map.keys()
    |> Enum.reject(&(&1 == txn.primary_key))
  end

  @doc """
  Add a key to the transaction's read set (for Serializable isolation).
  """
  def add_to_read_set(txn, key) do
    %__MODULE__{txn | read_set: MapSet.put(txn.read_set, key)}
  end

  @doc """
  Add a key to the transaction's write set.
  """
  def add_to_write_set(txn, key) do
    %__MODULE__{txn | write_set: MapSet.put(txn.write_set, key)}
  end

  defp generate_txn_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end
end
