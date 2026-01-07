defmodule Store.Stream.Event do
  @moduledoc """
  Event encoding/decoding for Redis-compatible streams.

  Stream entry IDs follow the Redis format: `{timestamp_ms}-{sequence}`
  - timestamp_ms: Unix epoch milliseconds
  - sequence: Auto-incrementing counter for same-millisecond entries

  Events are stored as:
  - Key: `{stream_name}:{id}` (for RocksDB ordered iteration)
  - Value: Encoded field-value pairs
  """

  @type stream_id :: {non_neg_integer(), non_neg_integer()}
  @type field_value :: {String.t(), String.t()}

  @doc """
  Generate a new stream ID.

  ## Options
  - `:last_id` - The previous entry's ID (for sequence incrementing)
  - `:explicit_id` - Use this exact ID (for XADD with explicit ID)
  """
  @spec generate_id(keyword()) :: stream_id()
  def generate_id(opts \\ []) do
    now_ms = System.system_time(:millisecond)

    case Keyword.get(opts, :explicit_id) do
      nil ->
        # Auto-generate
        case Keyword.get(opts, :last_id) do
          {last_ts, last_seq} when last_ts == now_ms ->
            # Same millisecond, increment sequence
            {now_ms, last_seq + 1}

          {last_ts, _last_seq} when last_ts > now_ms ->
            # Clock drift - use last timestamp + 1 seq
            {last_ts, 1}

          _ ->
            # New millisecond or no previous
            {now_ms, 0}
        end

      explicit when is_binary(explicit) ->
        parse_id(explicit)

      {_ts, _seq} = explicit ->
        explicit
    end
  end

  @doc """
  Parse a string ID like "1234567890123-5" into a tuple.
  """
  @spec parse_id(String.t()) :: stream_id() | :error
  def parse_id(id_str) when is_binary(id_str) do
    case String.split(id_str, "-") do
      [ts_str, seq_str] ->
        case {Integer.parse(ts_str), Integer.parse(seq_str)} do
          {{ts, ""}, {seq, ""}} -> {ts, seq}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  @doc """
  Format a stream ID tuple as a string.
  """
  @spec format_id(stream_id()) :: String.t()
  def format_id({timestamp_ms, sequence}) do
    "#{timestamp_ms}-#{sequence}"
  end

  @doc """
  Build the storage key for a stream entry.
  Key format: `stream_name:timestamp_ms:sequence` (padded for ordering)
  """
  @spec build_key(String.t(), stream_id()) :: binary()
  def build_key(stream_name, {timestamp_ms, sequence}) do
    # Pad timestamp to 16 digits, sequence to 10 digits for proper ordering
    ts_padded = String.pad_leading(Integer.to_string(timestamp_ms), 16, "0")
    seq_padded = String.pad_leading(Integer.to_string(sequence), 10, "0")
    "#{stream_name}:#{ts_padded}:#{seq_padded}"
  end

  @doc """
  Parse a storage key back into stream name and ID.
  """
  @spec parse_key(binary()) :: {:ok, String.t(), stream_id()} | :error
  def parse_key(key) when is_binary(key) do
    case :binary.split(key, ":", [:global]) do
      parts when length(parts) >= 3 ->
        # Last two parts are timestamp and sequence
        [seq_str, ts_str | rest] = Enum.reverse(parts)
        stream_name = rest |> Enum.reverse() |> Enum.join(":")

        case {Integer.parse(ts_str), Integer.parse(seq_str)} do
          {{ts, ""}, {seq, ""}} -> {:ok, stream_name, {ts, seq}}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  @doc """
  Encode field-value pairs for storage.
  Format: Length-prefixed strings (4-byte big-endian length + data)
  """
  @spec encode_fields([field_value()]) :: binary()
  def encode_fields(fields) when is_list(fields) do
    # First encode count
    count = length(fields)
    count_bin = <<count::32-big>>

    # Then encode each field-value pair
    data =
      Enum.reduce(fields, <<>>, fn {field, value}, acc ->
        field_bin = encode_string(to_string(field))
        value_bin = encode_string(to_string(value))
        <<acc::binary, field_bin::binary, value_bin::binary>>
      end)

    <<count_bin::binary, data::binary>>
  end

  @doc """
  Decode stored field-value pairs.
  """
  @spec decode_fields(binary()) :: {:ok, [field_value()]} | :error
  def decode_fields(<<count::32-big, rest::binary>>) do
    decode_fields_loop(rest, count, [])
  end

  def decode_fields(_), do: :error

  defp decode_fields_loop(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_fields_loop(_, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_fields_loop(data, remaining, acc) when remaining > 0 do
    with {:ok, field, rest1} <- decode_string(data),
         {:ok, value, rest2} <- decode_string(rest1) do
      decode_fields_loop(rest2, remaining - 1, [{field, value} | acc])
    else
      _ -> :error
    end
  end

  defp decode_fields_loop(_, _, _), do: :error

  defp encode_string(str) when is_binary(str) do
    len = byte_size(str)
    <<len::32-big, str::binary>>
  end

  defp decode_string(<<len::32-big, rest::binary>>) when byte_size(rest) >= len do
    <<str::binary-size(len), remaining::binary>> = rest
    {:ok, str, remaining}
  end

  defp decode_string(_), do: :error

  @doc """
  Encode a complete stream entry (ID + fields).
  """
  @spec encode_entry(stream_id(), [field_value()]) :: binary()
  def encode_entry({ts, seq}, fields) do
    id_bin = <<ts::64-big, seq::64-big>>
    fields_bin = encode_fields(fields)
    <<id_bin::binary, fields_bin::binary>>
  end

  @doc """
  Decode a complete stream entry.
  """
  @spec decode_entry(binary()) :: {:ok, stream_id(), [field_value()]} | :error
  def decode_entry(<<ts::64-big, seq::64-big, fields_bin::binary>>) do
    case decode_fields(fields_bin) do
      {:ok, fields} -> {:ok, {ts, seq}, fields}
      :error -> :error
    end
  end

  def decode_entry(_), do: :error

  @doc """
  Compare two stream IDs.
  Returns :lt, :eq, or :gt.
  """
  @spec compare_ids(stream_id(), stream_id()) :: :lt | :eq | :gt
  def compare_ids({ts1, seq1}, {ts2, seq2}) do
    cond do
      ts1 < ts2 -> :lt
      ts1 > ts2 -> :gt
      seq1 < seq2 -> :lt
      seq1 > seq2 -> :gt
      true -> :eq
    end
  end

  @doc """
  Check if id1 > id2.
  """
  @spec id_gt?(stream_id(), stream_id()) :: boolean()
  def id_gt?(id1, id2), do: compare_ids(id1, id2) == :gt

  @doc """
  Check if id1 >= id2.
  """
  @spec id_gte?(stream_id(), stream_id()) :: boolean()
  def id_gte?(id1, id2), do: compare_ids(id1, id2) in [:gt, :eq]

  @doc """
  The minimum possible stream ID.
  """
  @spec min_id() :: stream_id()
  def min_id, do: {0, 0}

  @doc """
  The maximum possible stream ID.
  """
  @spec max_id() :: stream_id()
  def max_id, do: {9_999_999_999_999_999, 9_999_999_999}
end
