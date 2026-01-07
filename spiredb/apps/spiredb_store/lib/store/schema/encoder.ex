defmodule Store.Schema.Encoder do
  @moduledoc """
  Binary-safe key encoding for RocksDB column families.

  Key formats:
  - tables CF: {table_id:4B}{pk_bytes}
  - indexes CF: {index_id:4B}{indexed_val}{pk_bytes}
  - vectors CF: {index_id:4B}{doc_id_bytes}
  """

  import Bitwise

  @doc """
  Encode a table row key.

  ## Format
  {table_id:4B big-endian}{pk_bytes}
  """
  def encode_table_key(table_id, pk) when is_integer(table_id) do
    <<table_id::unsigned-big-32>> <> ensure_binary(pk)
  end

  @doc """
  Decode a table row key.

  Returns {table_id, pk_bytes}.
  """
  def decode_table_key(<<table_id::unsigned-big-32, pk_bytes::binary>>) do
    {table_id, pk_bytes}
  end

  @doc """
  Encode a secondary index key.

  ## Format
  {index_id:4B}{indexed_value}{0x00}{pk_bytes}

  The 0x00 separator allows prefix scanning on indexed_value.
  """
  def encode_index_key(index_id, indexed_value, pk) when is_integer(index_id) do
    indexed_bytes = encode_sortable_value(indexed_value)
    pk_bytes = ensure_binary(pk)
    <<index_id::unsigned-big-32>> <> indexed_bytes <> <<0x00>> <> pk_bytes
  end

  @doc """
  Decode a secondary index key.

  Returns {index_id, indexed_value_bytes, pk_bytes}.
  """
  def decode_index_key(<<index_id::unsigned-big-32, rest::binary>>) do
    # Find the separator
    case :binary.split(rest, <<0x00>>) do
      [indexed_bytes, pk_bytes] ->
        {index_id, indexed_bytes, pk_bytes}

      _ ->
        {:error, :invalid_index_key}
    end
  end

  @doc """
  Encode index prefix for range scanning.

  Used to scan all keys for a given index.
  """
  def encode_index_prefix(index_id) when is_integer(index_id) do
    <<index_id::unsigned-big-32>>
  end

  @doc """
  Encode index value prefix for exact match scanning.

  Used to find all PKs with a specific indexed value.
  """
  def encode_index_value_prefix(index_id, indexed_value) when is_integer(index_id) do
    indexed_bytes = encode_sortable_value(indexed_value)
    <<index_id::unsigned-big-32>> <> indexed_bytes <> <<0x00>>
  end

  @doc """
  Encode a vector index key.

  ## Format
  {index_id:4B}{doc_id_bytes}
  """
  def encode_vector_key(index_id, doc_id) when is_integer(index_id) do
    <<index_id::unsigned-big-32>> <> ensure_binary(doc_id)
  end

  @doc """
  Decode a vector index key.
  """
  def decode_vector_key(<<index_id::unsigned-big-32, doc_id::binary>>) do
    {index_id, doc_id}
  end

  @doc """
  Encode vector value with optional payload.

  ## Format
  {vector_bytes}{payload_len:4B}{payload_json}

  If no payload, payload_len = 0.
  """
  def encode_vector_value(vector_bytes, payload \\ nil) when is_binary(vector_bytes) do
    case payload do
      nil ->
        vector_bytes <> <<0::32>>

      payload when is_binary(payload) ->
        payload_len = byte_size(payload)
        vector_bytes <> <<payload_len::unsigned-big-32>> <> payload

      payload when is_map(payload) ->
        json = Jason.encode!(payload)
        payload_len = byte_size(json)
        vector_bytes <> <<payload_len::unsigned-big-32>> <> json
    end
  end

  @doc """
  Decode vector value.

  Returns {vector_bytes, payload_json | nil}.
  """
  def decode_vector_value(data) do
    # Payload length is at the end, 4 bytes before end
    data_len = byte_size(data)

    if data_len < 4 do
      {:error, :invalid_vector_value}
    else
      # Read payload length from end
      payload_len_offset = data_len - 4
      <<vector_bytes::binary-size(payload_len_offset), payload_len::unsigned-big-32>> = data

      if payload_len == 0 do
        {vector_bytes, nil}
      else
        # Payload is before the length
        vector_len = payload_len_offset - payload_len

        <<actual_vector::binary-size(vector_len), payload::binary-size(payload_len), _::binary>> =
          data

        {actual_vector, payload}
      end
    end
  end

  @doc """
  Encode a value to be sortable as binary (for index ordering).

  Numbers are encoded in big-endian for correct byte ordering.
  Strings are used as-is.
  """
  def encode_sortable_value(value) do
    case value do
      v when is_integer(v) and v >= 0 ->
        # Unsigned: prefix with 0x01, then big-endian 8 bytes
        <<0x01, v::unsigned-big-64>>

      v when is_integer(v) ->
        # Signed negative: prefix with 0x00, flip bits for ordering
        <<0x00, bxor(v + 0x8000_0000_0000_0000, 0xFFFF_FFFF_FFFF_FFFF)::64>>

      v when is_float(v) ->
        # Float: IEEE 754 with sign bit handling for ordering
        <<bits::64>> = <<v::float-64>>

        if v >= 0 do
          <<0x02, bxor(bits, 0x8000_0000_0000_0000)::64>>
        else
          <<0x02, bxor(bits, 0xFFFF_FFFF_FFFF_FFFF)::64>>
        end

      v when is_binary(v) ->
        # String: prefix with 0x03, then raw bytes
        <<0x03>> <> v

      true ->
        <<0x04, 0x01>>

      false ->
        <<0x04, 0x00>>

      nil ->
        <<0x00>>
    end
  end

  @doc """
  Encode transaction lock key.

  ## Format in txn_locks CF
  {key_bytes}
  """
  def encode_lock_key(key), do: ensure_binary(key)

  @doc """
  Encode transaction data key (MVCC).

  ## Format in txn_data CF
  {key_bytes}{start_ts:8B big-endian inverted}

  Inverted so newer versions come first in iteration.
  """
  def encode_txn_data_key(key, start_ts) when is_integer(start_ts) do
    inverted_ts = 0xFFFF_FFFF_FFFF_FFFF - start_ts
    ensure_binary(key) <> <<inverted_ts::unsigned-big-64>>
  end

  @doc """
  Decode transaction data key.
  """
  def decode_txn_data_key(data) do
    key_len = byte_size(data) - 8
    <<key::binary-size(key_len), inverted_ts::unsigned-big-64>> = data
    start_ts = 0xFFFF_FFFF_FFFF_FFFF - inverted_ts
    {key, start_ts}
  end

  @doc """
  Encode transaction write key.

  ## Format in txn_write CF
  {key_bytes}{commit_ts:8B big-endian inverted}
  """
  def encode_txn_write_key(key, commit_ts) when is_integer(commit_ts) do
    inverted_ts = 0xFFFF_FFFF_FFFF_FFFF - commit_ts
    ensure_binary(key) <> <<inverted_ts::unsigned-big-64>>
  end

  @doc """
  Decode transaction write key.
  """
  def decode_txn_write_key(data) do
    key_len = byte_size(data) - 8
    <<key::binary-size(key_len), inverted_ts::unsigned-big-64>> = data
    commit_ts = 0xFFFF_FFFF_FFFF_FFFF - inverted_ts
    {key, commit_ts}
  end

  @doc """
  Encode a batch get result as binary.
  """
  def encode_batch_get_result(results) when is_list(results) do
    # Encode as simple format: count:4B then [key_len:4][key][val_len:4][val]...
    # Handle both {key, value} and {key, value, found} tuple formats
    count = length(results)

    data =
      Enum.reduce(results, <<count::32>>, fn
        {key, value, _found}, acc ->
          key_bin = ensure_binary(key)
          val_bin = ensure_binary(value)

          acc <>
            <<byte_size(key_bin)::32, key_bin::binary, byte_size(val_bin)::32, val_bin::binary>>

        {key, value}, acc ->
          key_bin = ensure_binary(key)
          val_bin = ensure_binary(value)

          acc <>
            <<byte_size(key_bin)::32, key_bin::binary, byte_size(val_bin)::32, val_bin::binary>>
      end)

    data
  end

  @doc """
  Encode a scan batch as binary.
  """
  def encode_scan_batch(batch) when is_list(batch) do
    # Encode as simple format: count:4B then [key_len:4][key][val_len:4][val]...
    count = length(batch)

    data =
      Enum.reduce(batch, <<count::32>>, fn {key, value}, acc ->
        key_bin = ensure_binary(key)
        val_bin = ensure_binary(value)

        acc <>
          <<byte_size(key_bin)::32, key_bin::binary, byte_size(val_bin)::32, val_bin::binary>>
      end)

    data
  end

  # Private helpers

  defp ensure_binary(nil), do: <<>>
  defp ensure_binary(v) when is_binary(v), do: v
  defp ensure_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp ensure_binary(v) when is_atom(v), do: Atom.to_string(v)
  defp ensure_binary(v), do: :erlang.term_to_binary(v)
end
