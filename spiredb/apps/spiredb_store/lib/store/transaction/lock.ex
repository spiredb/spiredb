defmodule Store.Transaction.Lock do
  @moduledoc """
  Enhanced Percolator lock structure for async commit support.

  Primary lock stores secondary key list for distributed commit truth.
  """

  @type lock_type :: :prewrite | :pessimistic

  @type t :: %__MODULE__{
          key: binary(),
          primary_key: binary(),
          start_ts: non_neg_integer(),
          ttl: non_neg_integer(),
          lock_type: lock_type(),
          txn_id: binary(),
          # Async commit fields
          min_commit_ts: non_neg_integer() | nil,
          use_async_commit: boolean(),
          secondaries: [binary()] | nil,
          rollback_ts: non_neg_integer() | nil
        }

  defstruct [
    :key,
    :primary_key,
    :start_ts,
    :ttl,
    :lock_type,
    :txn_id,
    # Async commit fields
    min_commit_ts: nil,
    use_async_commit: false,
    secondaries: nil,
    rollback_ts: nil
  ]

  # 30 seconds
  @default_ttl 30_000

  @doc """
  Create a new lock (standard mode).
  """
  def new(key, primary_key, start_ts, opts \\ []) do
    %__MODULE__{
      key: key,
      primary_key: primary_key,
      start_ts: start_ts,
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      lock_type: Keyword.get(opts, :lock_type, :prewrite),
      txn_id: Keyword.get(opts, :txn_id),
      use_async_commit: false
    }
  end

  @doc """
  Create lock for async commit transaction.
  Primary lock includes list of all secondary keys.
  """
  def new_async_commit(key, primary_key, start_ts, opts \\ []) do
    is_primary = key == primary_key

    %__MODULE__{
      key: key,
      primary_key: primary_key,
      start_ts: start_ts,
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      txn_id: Keyword.get(opts, :txn_id),
      lock_type: :prewrite,
      min_commit_ts: Keyword.get(opts, :min_commit_ts, start_ts + 1),
      use_async_commit: true,
      secondaries: if(is_primary, do: Keyword.get(opts, :secondaries, []), else: nil),
      rollback_ts: nil
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
  Encode lock for storage using efficient binary format.
  """
  def encode(%__MODULE__{} = lock) do
    secondaries_bin =
      if lock.secondaries do
        count = length(lock.secondaries)

        keys_bin =
          Enum.map(lock.secondaries, fn key ->
            <<byte_size(key)::16, key::binary>>
          end)
          |> IO.iodata_to_binary()

        <<count::16, keys_bin::binary>>
      else
        <<0::16>>
      end

    <<
      byte_size(lock.primary_key)::16,
      lock.primary_key::binary,
      lock.start_ts::64,
      lock.ttl::32,
      byte_size(lock.txn_id || "")::16,
      lock.txn_id || ""::binary,
      encode_lock_type(lock.lock_type)::8,
      lock.min_commit_ts || 0::64,
      if(lock.use_async_commit, do: 1, else: 0)::8,
      secondaries_bin::binary
    >>
  end

  @doc """
  Decode lock from binary.
  """
  def decode(
        key,
        <<
          primary_len::16,
          primary_key::binary-size(primary_len),
          start_ts::64,
          ttl::32,
          txn_id_len::16,
          txn_id::binary-size(txn_id_len),
          lock_type_byte::8,
          min_commit_ts::64,
          use_async_commit_byte::8,
          rest::binary
        >>
      ) do
    secondaries = decode_secondaries(rest)

    {:ok,
     %__MODULE__{
       key: key,
       primary_key: primary_key,
       start_ts: start_ts,
       ttl: ttl,
       txn_id: if(txn_id == "", do: nil, else: txn_id),
       lock_type: decode_lock_type(lock_type_byte),
       min_commit_ts: if(min_commit_ts == 0, do: nil, else: min_commit_ts),
       use_async_commit: use_async_commit_byte == 1,
       secondaries: secondaries,
       rollback_ts: nil
     }}
  end

  # Fallback for JSON-encoded locks (backward compatibility)
  def decode(key, data) when is_binary(data) do
    case Jason.decode(data) do
      {:ok, map} ->
        {:ok,
         %__MODULE__{
           key: key,
           primary_key: Base.decode64!(map["primary"]),
           start_ts: map["start_ts"],
           ttl: map["ttl"],
           lock_type: String.to_atom(map["type"]),
           txn_id: map["txn"],
           use_async_commit: false
         }}

      {:error, _} ->
        {:error, :invalid_format}
    end
  end

  defp decode_secondaries(<<0::16, _rest::binary>>), do: nil
  defp decode_secondaries(<<0::16>>), do: nil

  defp decode_secondaries(<<count::16, rest::binary>>) do
    decode_secondary_keys(rest, count, [])
  end

  defp decode_secondaries(_), do: nil

  defp decode_secondary_keys(_bin, 0, acc), do: Enum.reverse(acc)

  defp decode_secondary_keys(<<len::16, key::binary-size(len), rest::binary>>, count, acc) do
    decode_secondary_keys(rest, count - 1, [key | acc])
  end

  defp decode_secondary_keys(_, _, acc), do: Enum.reverse(acc)

  defp encode_lock_type(:prewrite), do: 1
  defp encode_lock_type(:pessimistic), do: 2

  defp decode_lock_type(1), do: :prewrite
  defp decode_lock_type(2), do: :pessimistic
  defp decode_lock_type(_), do: :prewrite
end
