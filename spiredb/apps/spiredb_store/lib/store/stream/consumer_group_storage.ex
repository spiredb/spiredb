defmodule Store.Stream.ConsumerGroupStorage do
  @moduledoc """
  RocksDB storage for consumer group state.

  Key formats in 'stream_groups' CF:
  - Group info: "group:{stream}:{group}" -> term_to_binary(group_info)
  - PEL entry: "pel:{stream}:{group}:{padded_id}" -> term_to_binary(pending_entry)
  - Consumer: "consumer:{stream}:{group}:{consumer}" -> term_to_binary(consumer_info)
  """

  require Logger

  @groups_cf "stream_groups"

  ## Group Operations

  @doc """
  Store group info.
  """
  def store_group(store_ref, group_info) do
    %{stream: stream, name: group} = group_info
    key = group_key(stream, group)
    value = :erlang.term_to_binary(group_info)

    case get_cf_handle(store_ref) do
      nil -> {:error, :cf_not_available}
      cf -> :rocksdb.put(store_ref.db, cf, key, value, [])
    end
  end

  @doc """
  Get group info.
  """
  def get_group(store_ref, stream, group) do
    key = group_key(stream, group)

    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        case :rocksdb.get(store_ref.db, cf, key, []) do
          {:ok, data} -> {:ok, :erlang.binary_to_term(data)}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Delete group and all associated data.
  """
  def delete_group(store_ref, stream, group) do
    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        # Delete group info
        :rocksdb.delete(store_ref.db, cf, group_key(stream, group), [])

        # Delete all PEL entries
        delete_with_prefix(store_ref.db, cf, pel_prefix(stream, group))

        # Delete all consumers
        delete_with_prefix(store_ref.db, cf, consumer_prefix(stream, group))

        :ok
    end
  end

  @doc """
  List all groups for a stream.
  """
  def list_groups(store_ref, stream) do
    prefix = "group:#{stream}:"

    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        case :rocksdb.iterator(store_ref.db, cf, []) do
          {:ok, iter} ->
            groups = collect_with_prefix(iter, prefix, [])
            :rocksdb.iterator_close(iter)
            {:ok, groups}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  ## Pending Entry Operations

  @doc """
  Add a pending entry.
  """
  def add_pending(store_ref, stream, group, entry) do
    key = pel_key(stream, group, entry.id)
    value = :erlang.term_to_binary(entry)

    case get_cf_handle(store_ref) do
      nil -> {:error, :cf_not_available}
      cf -> :rocksdb.put(store_ref.db, cf, key, value, [])
    end
  end

  @doc """
  Remove a pending entry.
  """
  def remove_pending(store_ref, stream, group, id) do
    key = pel_key(stream, group, id)

    case get_cf_handle(store_ref) do
      nil -> {:error, :cf_not_available}
      cf -> :rocksdb.delete(store_ref.db, cf, key, [])
    end
  end

  @doc """
  Get pending entries.

  ## Options
  - `:start` - Start ID (inclusive)
  - `:end` - End ID (inclusive)
  - `:count` - Maximum entries to return
  - `:consumer` - Filter by consumer name
  - `:min_idle` - Minimum idle time in ms
  """
  def get_pending(store_ref, stream, group, opts \\ []) do
    prefix = pel_prefix(stream, group)
    count = Keyword.get(opts, :count, 100)
    consumer_filter = Keyword.get(opts, :consumer)
    min_idle = Keyword.get(opts, :min_idle)
    now = System.system_time(:millisecond)

    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        case :rocksdb.iterator(store_ref.db, cf, []) do
          {:ok, iter} ->
            entries =
              collect_pending_entries(iter, prefix, count, consumer_filter, min_idle, now, [])

            :rocksdb.iterator_close(iter)
            {:ok, entries}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Update pending entry consumer (for XCLAIM).
  """
  def update_pending_consumer(store_ref, stream, group, id, new_consumer, new_delivery_time) do
    key = pel_key(stream, group, id)

    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        case :rocksdb.get(store_ref.db, cf, key, []) do
          {:ok, data} ->
            entry = :erlang.binary_to_term(data)

            updated_entry = %{
              entry
              | consumer: new_consumer,
                delivered_at: new_delivery_time,
                delivery_count: entry.delivery_count + 1
            }

            :rocksdb.put(store_ref.db, cf, key, :erlang.term_to_binary(updated_entry), [])

          :not_found ->
            {:error, :not_found}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  ## Consumer Operations

  @doc """
  Store consumer info.
  """
  def store_consumer(store_ref, stream, group, consumer_info) do
    key = consumer_key(stream, group, consumer_info.name)
    value = :erlang.term_to_binary(consumer_info)

    case get_cf_handle(store_ref) do
      nil -> {:error, :cf_not_available}
      cf -> :rocksdb.put(store_ref.db, cf, key, value, [])
    end
  end

  @doc """
  Get consumer info.
  """
  def get_consumer(store_ref, stream, group, consumer) do
    key = consumer_key(stream, group, consumer)

    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        case :rocksdb.get(store_ref.db, cf, key, []) do
          {:ok, data} -> {:ok, :erlang.binary_to_term(data)}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Delete consumer.
  """
  def delete_consumer(store_ref, stream, group, consumer) do
    key = consumer_key(stream, group, consumer)

    case get_cf_handle(store_ref) do
      nil -> {:error, :cf_not_available}
      cf -> :rocksdb.delete(store_ref.db, cf, key, [])
    end
  end

  @doc """
  List all consumers for a group.
  """
  def list_consumers(store_ref, stream, group) do
    prefix = consumer_prefix(stream, group)

    case get_cf_handle(store_ref) do
      nil ->
        {:error, :cf_not_available}

      cf ->
        case :rocksdb.iterator(store_ref.db, cf, []) do
          {:ok, iter} ->
            consumers = collect_consumers_with_prefix(iter, prefix, [])
            :rocksdb.iterator_close(iter)
            {:ok, consumers}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  ## Private Helpers

  defp group_key(stream, group), do: "group:#{stream}:#{group}"
  defp pel_prefix(stream, group), do: "pel:#{stream}:#{group}:"

  defp pel_key(stream, group, id) when is_binary(id),
    do: "pel:#{stream}:#{group}:#{id}"

  defp pel_key(stream, group, {ts, seq}),
    do: "pel:#{stream}:#{group}:#{pad_ts(ts)}-#{pad_seq(seq)}"

  defp consumer_prefix(stream, group), do: "consumer:#{stream}:#{group}:"
  defp consumer_key(stream, group, name), do: "consumer:#{stream}:#{group}:#{name}"

  defp pad_ts(ts), do: String.pad_leading(Integer.to_string(ts), 16, "0")
  defp pad_seq(seq), do: String.pad_leading(Integer.to_string(seq), 10, "0")

  defp get_cf_handle(store_ref) do
    Map.get(store_ref.cfs, @groups_cf)
  end

  defp delete_with_prefix(db, cf, prefix) do
    case :rocksdb.iterator(db, cf, []) do
      {:ok, iter} ->
        delete_matching_prefix(db, cf, iter, prefix)
        :rocksdb.iterator_close(iter)

      _ ->
        :ok
    end
  end

  defp delete_matching_prefix(db, cf, iter, prefix) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, _value} ->
        if String.starts_with?(key, prefix) do
          :rocksdb.delete(db, cf, key, [])
          delete_next_matching(db, cf, iter, prefix)
        end

      _ ->
        :ok
    end
  end

  defp delete_next_matching(db, cf, iter, prefix) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, _value} ->
        if String.starts_with?(key, prefix) do
          :rocksdb.delete(db, cf, key, [])
          delete_next_matching(db, cf, iter, prefix)
        end

      _ ->
        :ok
    end
  end

  defp collect_with_prefix(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, value} ->
        if String.starts_with?(key, prefix) do
          group_info = :erlang.binary_to_term(value)
          collect_next_with_prefix(iter, prefix, [group_info | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_next_with_prefix(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} ->
        if String.starts_with?(key, prefix) do
          group_info = :erlang.binary_to_term(value)
          collect_next_with_prefix(iter, prefix, [group_info | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_consumers_with_prefix(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, value} ->
        if String.starts_with?(key, prefix) do
          consumer_info = :erlang.binary_to_term(value)
          collect_next_consumers(iter, prefix, [consumer_info | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_next_consumers(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} ->
        if String.starts_with?(key, prefix) do
          consumer_info = :erlang.binary_to_term(value)
          collect_next_consumers(iter, prefix, [consumer_info | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_pending_entries(iter, prefix, count, consumer_filter, min_idle, now, acc)
       when count > 0 do
    move_result =
      if acc == [] do
        :rocksdb.iterator_move(iter, {:seek, prefix})
      else
        :rocksdb.iterator_move(iter, :next)
      end

    case move_result do
      {:ok, key, value} ->
        if String.starts_with?(key, prefix) do
          entry = :erlang.binary_to_term(value)

          matches_consumer = is_nil(consumer_filter) or entry.consumer == consumer_filter

          idle_ms = now - entry.delivered_at
          matches_idle = is_nil(min_idle) or idle_ms >= min_idle

          if matches_consumer and matches_idle do
            entry_with_idle = Map.put(entry, :idle_ms, idle_ms)

            collect_pending_entries(
              iter,
              prefix,
              count - 1,
              consumer_filter,
              min_idle,
              now,
              [entry_with_idle | acc]
            )
          else
            collect_pending_entries(
              iter,
              prefix,
              count,
              consumer_filter,
              min_idle,
              now,
              acc
            )
          end
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_pending_entries(_iter, _prefix, 0, _consumer, _min_idle, _now, acc) do
    Enum.reverse(acc)
  end
end
