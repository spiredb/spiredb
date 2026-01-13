defmodule Store.Stream do
  @moduledoc """
  Redis-compatible stream implementation backed by RocksDB.

  Supports:
  - XADD: Append entries to a stream
  - XREAD: Read entries from streams
  - XRANGE: Read entries in a range
  - XLEN: Get stream length
  - XINFO: Get stream metadata

  ## Storage Layout

  Stream entries are stored in the `streams` column family:
  - Key: `{stream_name}:{padded_timestamp}:{padded_sequence}`
  - Value: Encoded field-value pairs

  Stream metadata is stored in the `stream_meta` column family:
  - Key: `{stream_name}`
  - Value: Encoded metadata (last_id, length, first_id, etc.)
  """

  require Logger
  alias Store.Stream.Event

  @streams_cf "streams"
  @stream_meta_cf "stream_meta"

  @type stream_entry :: {Event.stream_id(), [Event.field_value()]}

  ## Public API

  @doc """
  Append an entry to a stream (XADD).

  ## Options
  - `:id` - Explicit ID (default: auto-generate)
  - `:maxlen` - Trim stream to max length after adding
  - `:approximate` - Allow approximate trimming (faster)

  ## Examples

      iex> Store.Stream.xadd("mystream", [{"field1", "value1"}, {"field2", "value2"}])
      {:ok, "1234567890123-0"}

      iex> Store.Stream.xadd("mystream", [{"data", "test"}], id: "1234567890123-5")
      {:ok, "1234567890123-5"}
  """
  @spec xadd(String.t(), [Event.field_value()], keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def xadd(stream_name, fields, opts \\ []) when is_binary(stream_name) and is_list(fields) do
    with {:ok, db_ref, cf_map} <- get_db_refs(),
         {:ok, meta} <- get_or_init_meta(db_ref, cf_map, stream_name),
         {:ok, new_id} <- generate_valid_id(meta, opts),
         :ok <- write_entry(db_ref, cf_map, stream_name, new_id, fields),
         :ok <- update_meta(db_ref, cf_map, stream_name, meta, new_id) do
      # Optional maxlen trimming
      case Keyword.get(opts, :maxlen) do
        nil -> :ok
        maxlen -> trim_stream(db_ref, cf_map, stream_name, maxlen, opts)
      end

      {:ok, Event.format_id(new_id)}
    end
  end

  @doc """
  Read entries from one or more streams (XREAD).

  ## Options
  - `:count` - Maximum entries per stream
  - `:block` - Block for N milliseconds if no entries (0 = forever)

  ## Examples

      iex> Store.Stream.xread([{"mystream", "0-0"}])
      {:ok, [{"mystream", [{"1234567890123-0", [{"field", "value"}]}]}]}

      iex> Store.Stream.xread([{"mystream", "$"}], count: 10)
      {:ok, []}
  """
  @spec xread([{String.t(), String.t()}], keyword()) ::
          {:ok, [{String.t(), [stream_entry()]}]} | {:error, term()}
  def xread(streams, opts \\ []) when is_list(streams) do
    count = Keyword.get(opts, :count, 100)

    with {:ok, db_ref, cf_map} <- get_db_refs() do
      results =
        Enum.map(streams, fn {stream_name, from_id_str} ->
          entries = read_after(db_ref, cf_map, stream_name, from_id_str, count)
          {stream_name, entries}
        end)

      {:ok, results}
    end
  end

  @doc """
  Read entries in a range (XRANGE).

  ## Options
  - `:count` - Maximum entries to return

  ## Examples

      iex> Store.Stream.xrange("mystream", "-", "+")
      {:ok, [{"1234567890123-0", [{"field", "value"}]}]}

      iex> Store.Stream.xrange("mystream", "1234567890000-0", "1234567899999-0", count: 10)
      {:ok, [...]}
  """
  @spec xrange(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, [stream_entry()]} | {:error, term()}
  def xrange(stream_name, start_id, end_id, opts \\ [])
      when is_binary(stream_name) and is_binary(start_id) and is_binary(end_id) do
    count = Keyword.get(opts, :count, 100)

    with {:ok, db_ref, cf_map} <- get_db_refs() do
      start_tuple = parse_range_id(start_id, :start)
      end_tuple = parse_range_id(end_id, :end)

      entries = read_range(db_ref, cf_map, stream_name, start_tuple, end_tuple, count)
      {:ok, entries}
    end
  end

  @doc """
  Reverse range read (XREVRANGE).
  """
  @spec xrevrange(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, [stream_entry()]} | {:error, term()}
  def xrevrange(stream_name, end_id, start_id, opts \\ [])
      when is_binary(stream_name) do
    count = Keyword.get(opts, :count, 100)

    with {:ok, db_ref, cf_map} <- get_db_refs() do
      start_tuple = parse_range_id(start_id, :start)
      end_tuple = parse_range_id(end_id, :end)

      entries =
        read_range(db_ref, cf_map, stream_name, start_tuple, end_tuple, count)
        |> Enum.reverse()

      {:ok, entries}
    end
  end

  @doc """
  Get stream length (XLEN).
  """
  @spec xlen(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def xlen(stream_name) when is_binary(stream_name) do
    with {:ok, db_ref, cf_map} <- get_db_refs() do
      case get_meta(db_ref, cf_map, stream_name) do
        {:ok, meta} -> {:ok, Map.get(meta, :length, 0)}
        {:error, :not_found} -> {:ok, 0}
        error -> error
      end
    end
  end

  @doc """
  Get stream info (XINFO STREAM).
  """
  @spec xinfo(String.t()) :: {:ok, map()} | {:error, term()}
  def xinfo(stream_name) when is_binary(stream_name) do
    with {:ok, db_ref, cf_map} <- get_db_refs() do
      case get_meta(db_ref, cf_map, stream_name) do
        {:ok, meta} ->
          info = %{
            length: Map.get(meta, :length, 0),
            first_entry: format_id_or_nil(Map.get(meta, :first_id)),
            last_entry: format_id_or_nil(Map.get(meta, :last_id)),
            radix_tree_keys: 0,
            radix_tree_nodes: 0
          }

          {:ok, info}

        {:error, :not_found} ->
          {:error, :stream_not_found}

        error ->
          error
      end
    end
  end

  @doc """
  Delete a stream and all its entries (XDEL stream).
  """
  @spec delete_stream(String.t()) :: :ok | {:error, term()}
  def delete_stream(stream_name) when is_binary(stream_name) do
    with {:ok, db_ref, cf_map} <- get_db_refs() do
      streams_cf = Map.get(cf_map, @streams_cf)
      meta_cf = Map.get(cf_map, @stream_meta_cf)

      # Delete all entries in the stream
      prefix = "#{stream_name}:"

      case :rocksdb.iterator(db_ref, streams_cf, []) do
        {:ok, iter} ->
          delete_with_prefix(db_ref, streams_cf, iter, prefix)
          :rocksdb.iterator_close(iter)

        _ ->
          :ok
      end

      # Delete metadata
      :rocksdb.delete(db_ref, meta_cf, stream_name, [])
    end
  end

  @doc """
  Delete specific entries from a stream by ID (XDEL).

  Returns the number of entries actually deleted.

  ## Examples

      iex> Store.Stream.xdel("mystream", ["1234567890123-0", "1234567890123-1"])
      {:ok, 2}
  """
  @spec xdel(String.t(), [String.t()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def xdel(stream_name, ids) when is_binary(stream_name) and is_list(ids) do
    with {:ok, db_ref, cf_map} <- get_db_refs() do
      streams_cf = Map.get(cf_map, @streams_cf)

      # Parse all IDs first
      parsed_ids =
        Enum.map(ids, fn id_str ->
          case Event.parse_id(id_str) do
            {_ts, _seq} = id -> {:ok, id, id_str}
            :error -> {:error, id_str}
          end
        end)

      # Filter out valid IDs
      valid_ids = Enum.filter(parsed_ids, &match?({:ok, _, _}, &1))

      # Delete each entry
      deleted_count =
        Enum.reduce(valid_ids, 0, fn {:ok, id, _id_str}, count ->
          key = Event.build_key(stream_name, id)

          case :rocksdb.delete(db_ref, streams_cf, key, []) do
            :ok -> count + 1
            {:error, _} -> count
          end
        end)

      # Update metadata if we deleted any entries
      if deleted_count > 0 do
        update_meta_after_delete(db_ref, cf_map, stream_name, deleted_count)
      end

      {:ok, deleted_count}
    end
  end

  defp update_meta_after_delete(db_ref, cf_map, stream_name, deleted_count) do
    case get_meta(db_ref, cf_map, stream_name) do
      {:ok, meta} ->
        new_length = max(0, Map.get(meta, :length, 0) - deleted_count)

        # If stream is now empty, clear first/last
        {new_first, new_last} =
          if new_length == 0 do
            {nil, nil}
          else
            # Re-find first and last entries
            first = find_first_entry_id(db_ref, cf_map, stream_name)
            last = find_last_entry_id(db_ref, cf_map, stream_name)
            {first, last}
          end

        new_meta = %{
          meta
          | length: new_length,
            first_id: new_first,
            last_id: new_last
        }

        meta_cf = Map.get(cf_map, @stream_meta_cf)
        :rocksdb.put(db_ref, meta_cf, stream_name, encode_meta(new_meta), [])

      _ ->
        :ok
    end
  end

  defp find_last_entry_id(db_ref, cf_map, stream_name) do
    streams_cf = Map.get(cf_map, @streams_cf)
    # Seek to end of stream prefix range
    prefix = "#{stream_name}:"
    # ';' is after ':' in ASCII
    end_prefix = "#{stream_name};"

    case :rocksdb.iterator(db_ref, streams_cf, []) do
      {:ok, iter} ->
        result =
          case :rocksdb.iterator_move(iter, {:seek_for_prev, end_prefix}) do
            {:ok, key, _value} when is_binary(key) ->
              if String.starts_with?(key, prefix) do
                case Event.parse_key(key) do
                  {:ok, _, id} -> id
                  :error -> nil
                end
              else
                nil
              end

            _ ->
              nil
          end

        :rocksdb.iterator_close(iter)
        result

      _ ->
        nil
    end
  end

  ## Private Implementation

  defp get_db_refs do
    case {:persistent_term.get(:spiredb_rocksdb_ref, nil),
          :persistent_term.get(:spiredb_rocksdb_cf_map, nil)} do
      {nil, _} -> {:error, :db_not_available}
      {_, nil} -> {:error, :cf_map_not_available}
      {db_ref, cf_map} -> {:ok, db_ref, cf_map}
    end
  end

  defp get_meta(db_ref, cf_map, stream_name) do
    meta_cf = Map.get(cf_map, @stream_meta_cf)

    case :rocksdb.get(db_ref, meta_cf, stream_name, []) do
      {:ok, data} ->
        {:ok, decode_meta(data)}

      :not_found ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_or_init_meta(db_ref, cf_map, stream_name) do
    case get_meta(db_ref, cf_map, stream_name) do
      {:ok, meta} ->
        {:ok, meta}

      {:error, :not_found} ->
        # New stream
        {:ok, %{length: 0, first_id: nil, last_id: nil}}

      error ->
        error
    end
  end

  defp generate_valid_id(meta, opts) do
    last_id = Map.get(meta, :last_id)

    new_id =
      case Keyword.get(opts, :id) do
        nil ->
          Event.generate_id(last_id: last_id)

        explicit when is_binary(explicit) ->
          case Event.parse_id(explicit) do
            {_ts, _seq} = id -> id
            :error -> {:error, :invalid_id}
          end
      end

    case new_id do
      {:error, _} = err ->
        err

      id when is_tuple(id) ->
        # Validate ID is greater than last
        if last_id == nil or Event.id_gt?(id, last_id) do
          {:ok, id}
        else
          {:error, :id_not_greater_than_last}
        end
    end
  end

  defp write_entry(db_ref, cf_map, stream_name, id, fields) do
    streams_cf = Map.get(cf_map, @streams_cf)
    key = Event.build_key(stream_name, id)
    value = Event.encode_fields(fields)

    case :rocksdb.put(db_ref, streams_cf, key, value, []) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp update_meta(db_ref, cf_map, stream_name, old_meta, new_id) do
    meta_cf = Map.get(cf_map, @stream_meta_cf)

    new_meta = %{
      length: Map.get(old_meta, :length, 0) + 1,
      first_id: Map.get(old_meta, :first_id) || new_id,
      last_id: new_id
    }

    data = encode_meta(new_meta)

    case :rocksdb.put(db_ref, meta_cf, stream_name, data, []) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp encode_meta(meta) do
    :erlang.term_to_binary(meta)
  end

  defp decode_meta(data) do
    :erlang.binary_to_term(data)
  end

  defp read_after(db_ref, cf_map, stream_name, from_id_str, count) do
    start_id =
      case from_id_str do
        "$" -> Event.max_id()
        "0" -> Event.min_id()
        "0-0" -> Event.min_id()
        id -> Event.parse_id(id) || Event.min_id()
      end

    # For XREAD, we want entries AFTER the given ID (exclusive)
    read_range_exclusive(db_ref, cf_map, stream_name, start_id, Event.max_id(), count)
  end

  defp read_range(db_ref, cf_map, stream_name, start_id, end_id, count) do
    streams_cf = Map.get(cf_map, @streams_cf)
    start_key = Event.build_key(stream_name, start_id)
    end_key = Event.build_key(stream_name, end_id)

    case :rocksdb.iterator(db_ref, streams_cf, []) do
      {:ok, iter} ->
        entries = collect_range(iter, start_key, end_key, stream_name, count, [])
        :rocksdb.iterator_close(iter)
        entries

      {:error, _} ->
        []
    end
  end

  defp read_range_exclusive(db_ref, cf_map, stream_name, start_id, end_id, count) do
    streams_cf = Map.get(cf_map, @streams_cf)
    start_key = Event.build_key(stream_name, start_id)
    end_key = Event.build_key(stream_name, end_id)
    prefix = "#{stream_name}:"

    case :rocksdb.iterator(db_ref, streams_cf, []) do
      {:ok, iter} ->
        # Seek to start and skip the start entry
        entries =
          case :rocksdb.iterator_move(iter, {:seek, start_key}) do
            {:ok, ^start_key, _value} ->
              # Skip this entry, read from next
              collect_after(iter, end_key, prefix, count, [])

            {:ok, key, value} when key <= end_key ->
              # Key is different, include it if in range
              if String.starts_with?(key, prefix) do
                entry = decode_entry_from_kv(key, value)
                collect_after(iter, end_key, prefix, count - 1, [entry])
              else
                collect_after(iter, end_key, prefix, count, [])
              end

            _ ->
              []
          end

        :rocksdb.iterator_close(iter)
        entries

      {:error, _} ->
        []
    end
  end

  defp collect_range(iter, start_key, end_key, stream_name, remaining, acc)
       when remaining > 0 do
    prefix = "#{stream_name}:"

    case :rocksdb.iterator_move(iter, {:seek, start_key}) do
      {:ok, key, value} when key <= end_key ->
        if String.starts_with?(key, prefix) do
          entry = decode_entry_from_kv(key, value)
          collect_range_next(iter, end_key, prefix, remaining - 1, [entry | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_range(_iter, _start_key, _end_key, _stream_name, _remaining, acc) do
    Enum.reverse(acc)
  end

  defp collect_range_next(iter, end_key, prefix, remaining, acc) when remaining > 0 do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} when key <= end_key ->
        if String.starts_with?(key, prefix) do
          entry = decode_entry_from_kv(key, value)
          collect_range_next(iter, end_key, prefix, remaining - 1, [entry | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_range_next(_iter, _end_key, _prefix, _remaining, acc) do
    Enum.reverse(acc)
  end

  defp collect_after(iter, end_key, prefix, remaining, acc) when remaining > 0 do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} when key <= end_key ->
        if String.starts_with?(key, prefix) do
          entry = decode_entry_from_kv(key, value)
          collect_after(iter, end_key, prefix, remaining - 1, [entry | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_after(_iter, _end_key, _prefix, _remaining, acc) do
    Enum.reverse(acc)
  end

  defp decode_entry_from_kv(key, value) do
    case Event.parse_key(key) do
      {:ok, _stream, id} ->
        case Event.decode_fields(value) do
          {:ok, fields} -> {Event.format_id(id), fields}
          :error -> {Event.format_id(id), []}
        end

      :error ->
        {"0-0", []}
    end
  end

  defp parse_range_id("-", :start), do: Event.min_id()
  defp parse_range_id("+", :end), do: Event.max_id()

  defp parse_range_id(id_str, _direction) when is_binary(id_str) do
    case Event.parse_id(id_str) do
      {_ts, _seq} = id -> id
      :error -> Event.min_id()
    end
  end

  defp format_id_or_nil(nil), do: nil
  defp format_id_or_nil(id), do: Event.format_id(id)

  defp trim_stream(db_ref, cf_map, stream_name, maxlen, _opts) do
    case xlen(stream_name) do
      {:ok, len} when len > maxlen ->
        to_delete = len - maxlen
        delete_oldest_entries(db_ref, cf_map, stream_name, to_delete)

      _ ->
        :ok
    end
  end

  defp delete_oldest_entries(db_ref, cf_map, stream_name, count) do
    streams_cf = Map.get(cf_map, @streams_cf)
    prefix = "#{stream_name}:"

    case :rocksdb.iterator(db_ref, streams_cf, []) do
      {:ok, iter} ->
        keys_to_delete =
          case :rocksdb.iterator_move(iter, {:seek, prefix}) do
            {:ok, key, _value} when is_binary(key) ->
              if String.starts_with?(key, prefix) do
                collect_keys_to_delete(iter, prefix, count - 1, [key])
              else
                []
              end

            _ ->
              []
          end

        :rocksdb.iterator_close(iter)

        # Delete the keys
        Enum.each(keys_to_delete, fn key ->
          :rocksdb.delete(db_ref, streams_cf, key, [])
        end)

        # Update metadata
        update_meta_after_trim(db_ref, cf_map, stream_name, length(keys_to_delete))

      _ ->
        :ok
    end
  end

  defp collect_keys_to_delete(iter, prefix, remaining, acc) when remaining > 0 do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, _value} when is_binary(key) ->
        if String.starts_with?(key, prefix) do
          collect_keys_to_delete(iter, prefix, remaining - 1, [key | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_keys_to_delete(_iter, _prefix, _remaining, acc) do
    Enum.reverse(acc)
  end

  defp update_meta_after_trim(db_ref, cf_map, stream_name, deleted_count) do
    case get_meta(db_ref, cf_map, stream_name) do
      {:ok, meta} ->
        # Find the new first entry
        new_first_id = find_first_entry_id(db_ref, cf_map, stream_name)

        new_meta = %{
          meta
          | length: max(0, Map.get(meta, :length, 0) - deleted_count),
            first_id: new_first_id
        }

        meta_cf = Map.get(cf_map, @stream_meta_cf)
        :rocksdb.put(db_ref, meta_cf, stream_name, encode_meta(new_meta), [])

      _ ->
        :ok
    end
  end

  defp find_first_entry_id(db_ref, cf_map, stream_name) do
    streams_cf = Map.get(cf_map, @streams_cf)
    prefix = "#{stream_name}:"

    case :rocksdb.iterator(db_ref, streams_cf, []) do
      {:ok, iter} ->
        result =
          case :rocksdb.iterator_move(iter, {:seek, prefix}) do
            {:ok, key, _value} when is_binary(key) ->
              if String.starts_with?(key, prefix) do
                case Event.parse_key(key) do
                  {:ok, _, id} -> id
                  :error -> nil
                end
              else
                nil
              end

            _ ->
              nil
          end

        :rocksdb.iterator_close(iter)
        result

      _ ->
        nil
    end
  end

  defp delete_with_prefix(db_ref, cf, iter, prefix) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, _value} when is_binary(key) ->
        if String.starts_with?(key, prefix) do
          :rocksdb.delete(db_ref, cf, key, [])
          delete_next_with_prefix(db_ref, cf, iter, prefix)
        end

      _ ->
        :ok
    end
  end

  defp delete_next_with_prefix(db_ref, cf, iter, prefix) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, _value} when is_binary(key) ->
        if String.starts_with?(key, prefix) do
          :rocksdb.delete(db_ref, cf, key, [])
          delete_next_with_prefix(db_ref, cf, iter, prefix)
        end

      _ ->
        :ok
    end
  end
end
