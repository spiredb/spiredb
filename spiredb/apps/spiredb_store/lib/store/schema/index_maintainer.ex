defmodule Store.Schema.IndexMaintainer do
  @moduledoc """
  Automatic maintenance of secondary indexes on writes.

  When a row is inserted, updated, or deleted, this module
  ensures all corresponding secondary indexes are kept in sync.
  """

  require Logger

  alias Store.Schema.Encoder
  alias PD.Schema.Registry

  @indexes_cf "indexes"

  @doc """
  Update secondary indexes after a write operation.

  - For inserts: adds index entries for all indexed columns
  - For updates: removes old entries, adds new ones
  - For deletes: removes all index entries
  """
  @spec maintain_indexes(String.t(), binary(), map() | nil, map() | nil) :: :ok | {:error, term()}
  def maintain_indexes(table_name, pk, old_row, new_row) do
    with {:ok, indexes} <- get_table_indexes(table_name),
         {:ok, db_ref, cf_map} <- get_db_refs() do
      indexes_cf = Map.get(cf_map, @indexes_cf)

      Enum.each(indexes, fn index ->
        maintain_single_index(db_ref, indexes_cf, index, pk, old_row, new_row)
      end)

      :ok
    else
      # No schema = no indexes
      {:error, :table_not_found} -> :ok
      {:error, :registry_unavailable} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Lookup primary keys by indexed value.
  """
  @spec lookup_by_index(String.t(), String.t(), term()) :: {:ok, [binary()]} | {:error, term()}
  def lookup_by_index(table_name, column_name, value) do
    with {:ok, index} <- find_index_for_column(table_name, column_name),
         {:ok, db_ref, cf_map} <- get_db_refs() do
      indexes_cf = Map.get(cf_map, @indexes_cf)
      prefix = Encoder.encode_index_value_prefix(index.id, value)

      case scan_index_prefix(db_ref, indexes_cf, prefix) do
        pks when is_list(pks) -> {:ok, pks}
        error -> error
      end
    end
  end

  @doc """
  Rebuild all indexes for a table.
  Scans all rows and recreates index entries.
  """
  @spec rebuild_indexes(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rebuild_indexes(table_name) do
    with {:ok, table} <- Registry.get_table(table_name),
         {:ok, indexes} <- Registry.list_indexes(table_name),
         {:ok, db_ref, cf_map} <- get_db_refs() do
      # Clear existing index entries for this table's indexes
      indexes_cf = Map.get(cf_map, @indexes_cf)
      tables_cf = Map.get(cf_map, "tables")

      Enum.each(indexes, fn index ->
        clear_index(db_ref, indexes_cf, index.id)
      end)

      # Scan all rows and rebuild
      table_prefix = <<table.id::unsigned-big-32>>
      count = rebuild_from_scan(db_ref, tables_cf, indexes_cf, table_prefix, indexes, 0)

      Logger.info("Rebuilt #{length(indexes)} indexes for #{table_name}: #{count} entries")
      {:ok, count}
    end
  end

  ## Private

  defp maintain_single_index(db_ref, indexes_cf, index, pk, old_row, new_row) do
    old_values = extract_indexed_values(old_row, index.columns)
    new_values = extract_indexed_values(new_row, index.columns)

    # Remove old index entry if value changed or row deleted
    if old_values != nil and old_values != new_values do
      old_key = Encoder.encode_index_key(index.id, old_values, pk)
      :rocksdb.delete(db_ref, indexes_cf, old_key, [])
    end

    # Add new index entry if row exists
    if new_values != nil do
      new_key = Encoder.encode_index_key(index.id, new_values, pk)
      # Value is empty - we only need the key for lookup
      :rocksdb.put(db_ref, indexes_cf, new_key, <<>>, [])
    end
  end

  defp extract_indexed_values(nil, _columns), do: nil

  defp extract_indexed_values(row, columns) when is_map(row) do
    values =
      Enum.map(columns, fn col ->
        Map.get(row, col) || Map.get(row, String.to_atom(col))
      end)

    # If all values nil, treat as nil
    if Enum.all?(values, &is_nil/1) do
      nil
    else
      # For single column, return value; for composite, return tuple
      case values do
        [single] -> single
        multiple -> List.to_tuple(multiple)
      end
    end
  end

  defp get_table_indexes(table_name) do
    try do
      Registry.list_indexes(table_name)
    catch
      :exit, _ -> {:error, :registry_unavailable}
      _, _ -> {:error, :registry_unavailable}
    end
  end

  defp find_index_for_column(table_name, column_name) do
    case get_table_indexes(table_name) do
      {:ok, indexes} ->
        case Enum.find(indexes, fn idx -> column_name in idx.columns end) do
          nil -> {:error, :no_index_for_column}
          index -> {:ok, index}
        end

      error ->
        error
    end
  end

  defp scan_index_prefix(db_ref, indexes_cf, prefix) do
    case :rocksdb.iterator(db_ref, indexes_cf, []) do
      {:ok, iter} ->
        pks = collect_pks_with_prefix(iter, prefix, [])
        :rocksdb.iterator_close(iter)
        pks

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp collect_pks_with_prefix(iter, prefix, acc) do
    seek_key = if acc == [], do: {:seek, prefix}, else: :next

    case :rocksdb.iterator_move(iter, seek_key) do
      {:ok, key, _value} ->
        if String.starts_with?(key, prefix) do
          case Encoder.decode_index_key(key) do
            {_index_id, _indexed_val, pk} ->
              collect_pks_with_prefix(iter, prefix, [pk | acc])

            _ ->
              Enum.reverse(acc)
          end
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp clear_index(db_ref, indexes_cf, index_id) do
    prefix = Encoder.encode_index_prefix(index_id)

    case :rocksdb.iterator(db_ref, indexes_cf, []) do
      {:ok, iter} ->
        delete_keys_with_prefix(db_ref, indexes_cf, iter, prefix)
        :rocksdb.iterator_close(iter)

      _ ->
        :ok
    end
  end

  defp delete_keys_with_prefix(db_ref, cf, iter, prefix) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, _} when is_binary(key) ->
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
      {:ok, key, _} when is_binary(key) ->
        if String.starts_with?(key, prefix) do
          :rocksdb.delete(db_ref, cf, key, [])
          delete_next_with_prefix(db_ref, cf, iter, prefix)
        end

      _ ->
        :ok
    end
  end

  defp rebuild_from_scan(db_ref, tables_cf, indexes_cf, table_prefix, indexes, count) do
    case :rocksdb.iterator(db_ref, tables_cf, []) do
      {:ok, iter} ->
        final_count = scan_and_index(iter, indexes_cf, table_prefix, indexes, count, db_ref)
        :rocksdb.iterator_close(iter)
        final_count

      _ ->
        count
    end
  end

  defp scan_and_index(iter, indexes_cf, table_prefix, indexes, count, db_ref) do
    case :rocksdb.iterator_move(iter, {:seek, table_prefix}) do
      {:ok, key, value} when is_binary(key) ->
        if String.starts_with?(key, table_prefix) do
          new_count = index_row(db_ref, indexes_cf, key, value, indexes, count)
          scan_next(iter, indexes_cf, table_prefix, indexes, new_count, db_ref)
        else
          count
        end

      _ ->
        count
    end
  end

  defp scan_next(iter, indexes_cf, table_prefix, indexes, count, db_ref) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} when is_binary(key) ->
        if String.starts_with?(key, table_prefix) do
          new_count = index_row(db_ref, indexes_cf, key, value, indexes, count)
          scan_next(iter, indexes_cf, table_prefix, indexes, new_count, db_ref)
        else
          count
        end

      _ ->
        count
    end
  end

  defp index_row(db_ref, indexes_cf, key, value, indexes, count) do
    # Decode pk from table key
    {_table_id, pk} = Encoder.decode_table_key(key)

    # Decode row value
    row =
      try do
        :erlang.binary_to_term(value)
      rescue
        _ -> %{}
      end

    # Add index entries
    Enum.each(indexes, fn index ->
      values = extract_indexed_values(row, index.columns)

      if values != nil do
        index_key = Encoder.encode_index_key(index.id, values, pk)
        :rocksdb.put(db_ref, indexes_cf, index_key, <<>>, [])
      end
    end)

    count + 1
  end

  defp get_db_refs do
    case {:persistent_term.get(:spiredb_rocksdb_ref, nil),
          :persistent_term.get(:spiredb_rocksdb_cf_map, nil)} do
      {nil, _} -> {:error, :db_not_available}
      {_, nil} -> {:error, :cf_map_not_available}
      {db_ref, cf_map} -> {:ok, db_ref, cf_map}
    end
  end
end
