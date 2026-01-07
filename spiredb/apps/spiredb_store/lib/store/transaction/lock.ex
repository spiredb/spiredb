defmodule Store.Transaction.Lock do
  @moduledoc """
  Percolator lock structure stored in txn_locks column family.
  """

  @type lock_type :: :prewrite | :pessimistic

  @type t :: %__MODULE__{
          key: binary(),
          primary_key: binary(),
          start_ts: non_neg_integer(),
          ttl: non_neg_integer(),
          lock_type: lock_type(),
          txn_id: binary()
        }

  defstruct [:key, :primary_key, :start_ts, :ttl, :lock_type, :txn_id]

  # 30 seconds
  @default_ttl 30_000

  @doc """
  Create a new lock.
  """
  def new(key, primary_key, start_ts, opts \\ []) do
    %__MODULE__{
      key: key,
      primary_key: primary_key,
      start_ts: start_ts,
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      lock_type: Keyword.get(opts, :lock_type, :prewrite),
      txn_id: Keyword.get(opts, :txn_id)
    }
  end

  @doc """
  Check if lock has expired.
  """
  def expired?(lock, current_time \\ nil) do
    now = current_time || System.os_time(:millisecond)
    # start_ts is in microseconds
    lock_time = div(lock.start_ts, 1000)
    now - lock_time > lock.ttl
  end

  @doc """
  Encode lock for storage.
  """
  def encode(lock) do
    data = %{
      "primary" => Base.encode64(lock.primary_key),
      "start_ts" => lock.start_ts,
      "ttl" => lock.ttl,
      "type" => Atom.to_string(lock.lock_type),
      "txn" => lock.txn_id
    }

    Jason.encode!(data)
  end

  @doc """
  Decode lock from storage.
  """
  def decode(key, data) do
    case Jason.decode(data) do
      {:ok, map} ->
        {:ok,
         %__MODULE__{
           key: key,
           primary_key: Base.decode64!(map["primary"]),
           start_ts: map["start_ts"],
           ttl: map["ttl"],
           lock_type: String.to_atom(map["type"]),
           txn_id: map["txn"]
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
