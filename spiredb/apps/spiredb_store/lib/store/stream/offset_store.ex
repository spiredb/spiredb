defmodule Store.Stream.OffsetStore do
  @moduledoc """
  Persistent consumer offset storage in RocksDB meta CF.

  Key format: "consumer:{consumer_id}"
  Value: term_to_binary(%{offset: n, acked_at: timestamp})
  """

  @meta_cf "meta"
  @prefix "consumer:"

  def store_offset(store_ref, consumer_id, offset) when is_integer(offset) do
    key = encode_key(consumer_id)

    value =
      :erlang.term_to_binary(%{
        offset: offset,
        acked_at: System.system_time(:millisecond)
      })

    put_meta(store_ref, key, value)
  end

  def get_offset(store_ref, consumer_id) do
    key = encode_key(consumer_id)

    case get_meta(store_ref, key) do
      {:ok, binary} ->
        data = :erlang.binary_to_term(binary)
        {:ok, data.offset}

      :not_found ->
        {:error, :not_found}

      error ->
        error
    end
  end

  def list_consumers(store_ref) do
    # Scan meta CF for keys starting with "consumer:"
    # This is a bit expensive if there are many keys, but valid for admin/debug
    case get_meta_cf(store_ref) do
      nil -> []
      {db, cf} -> scan_consumers(db, cf)
    end
  end

  def delete_consumer(store_ref, consumer_id) do
    key = encode_key(consumer_id)
    delete_meta(store_ref, key)
  end

  ## Private

  defp encode_key(consumer_id), do: @prefix <> consumer_id

  defp get_meta_cf(%{db: db, cfs: cfs}) do
    case Map.get(cfs, @meta_cf) do
      nil -> nil
      cf -> {db, cf}
    end
  end

  defp get_meta_cf(_), do: nil

  defp put_meta(store_ref, key, value) do
    case get_meta_cf(store_ref) do
      {db, cf} ->
        :rocksdb.put(db, cf, key, value, [])

      nil ->
        {:error, :meta_cf_not_found}
    end
  end

  defp get_meta(store_ref, key) do
    case get_meta_cf(store_ref) do
      {db, cf} ->
        case :rocksdb.get(db, cf, key, []) do
          {:ok, val} -> {:ok, val}
          :not_found -> :not_found
          {:error, reason} -> {:error, reason}
        end

      nil ->
        {:error, :meta_cf_not_found}
    end
  end

  defp delete_meta(store_ref, key) do
    case get_meta_cf(store_ref) do
      {db, cf} ->
        :rocksdb.delete(db, cf, key, [])

      nil ->
        {:error, :meta_cf_not_found}
    end
  end

  defp scan_consumers(db, cf) do
    {:ok, iter} = :rocksdb.iterator(db, cf, [])
    # Seek to prefix
    result = collect_consumers(iter, :rocksdb.iterator_move(iter, {:seek, @prefix}))
    :rocksdb.iterator_close(iter)
    result
  end

  defp collect_consumers(_iter, {:error, _}), do: []

  defp collect_consumers(iter, {:ok, key, value}) do
    if String.starts_with?(key, @prefix) do
      consumer_id = String.replace_prefix(key, @prefix, "")
      data = :erlang.binary_to_term(value)
      [{consumer_id, data.offset} | collect_consumers(iter, :rocksdb.iterator_move(iter, :next))]
    else
      []
    end
  end
end
