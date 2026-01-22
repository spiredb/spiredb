defmodule Store.Stream.ConsumerGroup do
  @moduledoc """
  Consumer group management for Redis-compatible streams.

  Implements:
  - XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER
  - Pending Entry List (PEL) tracking
  - Consumer last-seen tracking for idle detection

  ## Storage

  All state is persisted to RocksDB via `ConsumerGroupStorage`.
  """

  require Logger
  alias Store.Stream.ConsumerGroupStorage, as: Storage
  alias Store.Stream.Event

  @type stream_id :: {non_neg_integer(), non_neg_integer()}

  @type group_info :: %{
          name: String.t(),
          stream: String.t(),
          last_delivered_id: stream_id() | nil,
          entries_read: non_neg_integer(),
          created_at: non_neg_integer()
        }

  @type consumer_info :: %{
          name: String.t(),
          pending_count: non_neg_integer(),
          last_seen: non_neg_integer()
        }

  @type pending_entry :: %{
          id: stream_id(),
          consumer: String.t(),
          delivered_at: non_neg_integer(),
          delivery_count: non_neg_integer()
        }

  ## XGROUP CREATE

  @doc """
  Create a consumer group.

  ## Options
  - `:mkstream` - Create the stream if it doesn't exist
  - `:entries_read` - Number of entries read so far (for lag tracking)

  ## Start IDs
  - `"$"` - Start from the last entry (new entries only)
  - `"0"` or `"0-0"` - Start from the beginning
  - `"<timestamp>-<seq>"` - Start from specific ID
  """
  @spec create(String.t(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def create(stream, group, start_id, opts \\ []) do
    with {:ok, store_ref} <- get_store_ref() do
      # Check if group already exists
      case Storage.get_group(store_ref, stream, group) do
        {:ok, _existing} ->
          {:error, :busygroup}

        {:error, :not_found} ->
          # Resolve start_id to actual ID
          last_delivered_id = resolve_start_id(stream, start_id)
          entries_read = Keyword.get(opts, :entries_read, 0)

          group_info = %{
            name: group,
            stream: stream,
            last_delivered_id: last_delivered_id,
            entries_read: entries_read,
            created_at: System.system_time(:millisecond)
          }

          case Storage.store_group(store_ref, group_info) do
            :ok -> :ok
            error -> error
          end

        error ->
          error
      end
    end
  end

  ## XGROUP DESTROY

  @doc """
  Destroy a consumer group.
  """
  @spec destroy(String.t(), String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def destroy(stream, group) do
    with {:ok, store_ref} <- get_store_ref() do
      case Storage.get_group(store_ref, stream, group) do
        {:ok, _} ->
          Storage.delete_group(store_ref, stream, group)
          {:ok, 1}

        {:error, :not_found} ->
          {:ok, 0}

        error ->
          error
      end
    end
  end

  ## XGROUP SETID

  @doc """
  Set the last delivered ID for a consumer group.
  """
  @spec set_id(String.t(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def set_id(stream, group, id, opts \\ []) do
    with {:ok, store_ref} <- get_store_ref() do
      case Storage.get_group(store_ref, stream, group) do
        {:ok, group_info} ->
          new_id = resolve_start_id(stream, id)
          entries_read = Keyword.get(opts, :entries_read, group_info.entries_read)

          updated = %{
            group_info
            | last_delivered_id: new_id,
              entries_read: entries_read
          }

          Storage.store_group(store_ref, updated)

        {:error, :not_found} ->
          {:error, :nogroup}

        error ->
          error
      end
    end
  end

  ## XGROUP CREATECONSUMER

  @doc """
  Create a consumer in a group.
  """
  @spec create_consumer(String.t(), String.t(), String.t()) ::
          {:ok, 0 | 1} | {:error, term()}
  def create_consumer(stream, group, consumer) do
    with {:ok, store_ref} <- get_store_ref() do
      # Verify group exists
      case Storage.get_group(store_ref, stream, group) do
        {:ok, _} ->
          # Check if consumer already exists
          case Storage.get_consumer(store_ref, stream, group, consumer) do
            {:ok, _} ->
              {:ok, 0}

            {:error, :not_found} ->
              consumer_info = %{
                name: consumer,
                pending_count: 0,
                last_seen: System.system_time(:millisecond)
              }

              case Storage.store_consumer(store_ref, stream, group, consumer_info) do
                :ok -> {:ok, 1}
                error -> error
              end

            error ->
              error
          end

        {:error, :not_found} ->
          {:error, :nogroup}

        error ->
          error
      end
    end
  end

  ## XGROUP DELCONSUMER

  @doc """
  Delete a consumer from a group.

  Returns the number of pending messages that were released.
  """
  @spec delete_consumer(String.t(), String.t(), String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete_consumer(stream, group, consumer) do
    with {:ok, store_ref} <- get_store_ref() do
      # Get consumer to verify it exists and get pending count
      case Storage.get_consumer(store_ref, stream, group, consumer) do
        {:ok, consumer_info} ->
          pending_count = consumer_info.pending_count
          Storage.delete_consumer(store_ref, stream, group, consumer)
          {:ok, pending_count}

        {:error, :not_found} ->
          {:ok, 0}

        error ->
          error
      end
    end
  end

  ## Record Delivery (internal, called by XREADGROUP)

  @doc """
  Record that entries were delivered to a consumer.
  Updates the group's last_delivered_id and adds entries to PEL.
  """
  @spec record_delivery(String.t(), String.t(), String.t(), [stream_id()]) ::
          :ok | {:error, term()}
  def record_delivery(stream, group, consumer, entry_ids) when is_list(entry_ids) do
    with {:ok, store_ref} <- get_store_ref() do
      now = System.system_time(:millisecond)

      # Update group last_delivered_id
      case Storage.get_group(store_ref, stream, group) do
        {:ok, group_info} ->
          # Find max ID
          max_id = Enum.max_by(entry_ids, fn {ts, seq} -> {ts, seq} end, fn -> nil end)

          updated_group =
            if max_id do
              new_last =
                case group_info.last_delivered_id do
                  nil ->
                    max_id

                  current ->
                    if Event.id_gt?(max_id, current), do: max_id, else: current
                end

              %{
                group_info
                | last_delivered_id: new_last,
                  entries_read: group_info.entries_read + length(entry_ids)
              }
            else
              group_info
            end

          Storage.store_group(store_ref, updated_group)

          # Add entries to PEL
          Enum.each(entry_ids, fn id ->
            pending = %{
              id: id,
              consumer: consumer,
              delivered_at: now,
              delivery_count: 1
            }

            Storage.add_pending(store_ref, stream, group, pending)
          end)

          # Update consumer's pending count and last_seen
          ensure_consumer_exists(store_ref, stream, group, consumer)
          update_consumer_pending(store_ref, stream, group, consumer, length(entry_ids))

          :ok

        {:error, :not_found} ->
          {:error, :nogroup}

        error ->
          error
      end
    end
  end

  ## Acknowledge (XACK)

  @doc """
  Acknowledge messages (remove from PEL).
  """
  @spec acknowledge(String.t(), String.t(), [String.t() | stream_id()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def acknowledge(stream, group, ids) when is_list(ids) do
    with {:ok, store_ref} <- get_store_ref() do
      ack_count =
        Enum.reduce(ids, 0, fn id, count ->
          parsed_id =
            case id do
              {_ts, _seq} = tuple -> tuple
              str when is_binary(str) -> Event.parse_id(str) || id
            end

          case Storage.remove_pending(store_ref, stream, group, format_id(parsed_id)) do
            :ok -> count + 1
            _ -> count
          end
        end)

      {:ok, ack_count}
    end
  end

  ## XCLAIM

  @doc """
  Claim messages from another consumer.

  ## Options
  - `:idle` - Set idle time instead of current time
  - `:time` - Set last delivery time (ms since epoch)
  - `:retry_count` - Set retry count
  - `:force` - Create entry in PEL if it doesn't exist
  - `:justid` - Return only IDs, not full messages
  """
  @spec claim(String.t(), String.t(), String.t(), non_neg_integer(), [String.t()], keyword()) ::
          {:ok, [stream_id()]} | {:error, term()}
  def claim(stream, group, consumer, min_idle_ms, ids, opts \\ []) do
    with {:ok, store_ref} <- get_store_ref() do
      now = System.system_time(:millisecond)
      new_delivery_time = Keyword.get(opts, :time, now)

      claimed_ids =
        Enum.reduce(ids, [], fn id_str, acc ->
          parsed_id = Event.parse_id(id_str) || id_str

          case Storage.get_pending(store_ref, stream, group, count: 1) do
            {:ok, entries} ->
              # Find matching entry
              entry = Enum.find(entries, fn e -> e.id == parsed_id end)

              cond do
                entry && now - entry.delivered_at >= min_idle_ms ->
                  # Claim it
                  Storage.update_pending_consumer(
                    store_ref,
                    stream,
                    group,
                    format_id(parsed_id),
                    consumer,
                    new_delivery_time
                  )

                  [parsed_id | acc]

                Keyword.get(opts, :force, false) ->
                  # Force create
                  pending = %{
                    id: parsed_id,
                    consumer: consumer,
                    delivered_at: new_delivery_time,
                    delivery_count: 1
                  }

                  Storage.add_pending(store_ref, stream, group, pending)
                  [parsed_id | acc]

                true ->
                  acc
              end

            _ ->
              acc
          end
        end)

      {:ok, Enum.reverse(claimed_ids)}
    end
  end

  ## XAUTOCLAIM

  @doc """
  Auto-claim idle messages.
  """
  @spec auto_claim(String.t(), String.t(), String.t(), non_neg_integer(), String.t(), keyword()) ::
          {:ok, String.t(), [stream_id()], [String.t()]} | {:error, term()}
  def auto_claim(stream, group, consumer, min_idle_ms, _start_id, opts \\ []) do
    count = Keyword.get(opts, :count, 100)

    with {:ok, store_ref} <- get_store_ref() do
      now = System.system_time(:millisecond)

      case Storage.get_pending(store_ref, stream, group, min_idle: min_idle_ms, count: count) do
        {:ok, entries} ->
          claimed_ids =
            Enum.map(entries, fn entry ->
              Storage.update_pending_consumer(
                store_ref,
                stream,
                group,
                format_id(entry.id),
                consumer,
                now
              )

              entry.id
            end)

          # Calculate next start ID
          next_start =
            case List.last(entries) do
              nil -> "0-0"
              last -> format_id(last.id)
            end

          {:ok, next_start, claimed_ids, []}

        error ->
          error
      end
    end
  end

  ## XPENDING

  @doc """
  Get pending entries info.

  ## Options
  - `:start` - Start ID
  - `:end` - End ID
  - `:count` - Maximum entries
  - `:consumer` - Filter by consumer
  - `:min_idle` - Filter by minimum idle time
  """
  @spec pending(String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def pending(stream, group, opts \\ []) do
    with {:ok, store_ref} <- get_store_ref() do
      case Storage.get_pending(store_ref, stream, group, opts) do
        {:ok, entries} ->
          # Build summary
          summary = %{
            count: length(entries),
            min_id: if(entries != [], do: format_id(List.first(entries).id)),
            max_id: if(entries != [], do: format_id(List.last(entries).id)),
            consumers:
              entries
              |> Enum.group_by(& &1.consumer)
              |> Enum.map(fn {consumer, list} -> {consumer, length(list)} end),
            entries:
              Enum.map(entries, fn e ->
                %{
                  id: format_id(e.id),
                  consumer: e.consumer,
                  idle_ms: e.idle_ms,
                  delivery_count: e.delivery_count
                }
              end)
          }

          {:ok, summary}

        error ->
          error
      end
    end
  end

  ## XINFO GROUPS

  @doc """
  Get info about all groups for a stream.
  """
  @spec info_groups(String.t()) :: {:ok, [map()]} | {:error, term()}
  def info_groups(stream) do
    with {:ok, store_ref} <- get_store_ref() do
      case Storage.list_groups(store_ref, stream) do
        {:ok, groups} ->
          infos =
            Enum.map(groups, fn group ->
              {:ok, consumers} = Storage.list_consumers(store_ref, stream, group.name)
              {:ok, pending} = Storage.get_pending(store_ref, stream, group.name, count: 10000)

              %{
                name: group.name,
                consumers: length(consumers),
                pending: length(pending),
                last_delivered_id: format_id(group.last_delivered_id),
                entries_read: group.entries_read
              }
            end)

          {:ok, infos}

        error ->
          error
      end
    end
  end

  ## XINFO CONSUMERS

  @doc """
  Get info about all consumers in a group.
  """
  @spec info_consumers(String.t(), String.t()) :: {:ok, [map()]} | {:error, term()}
  def info_consumers(stream, group) do
    with {:ok, store_ref} <- get_store_ref() do
      case Storage.list_consumers(store_ref, stream, group) do
        {:ok, consumers} ->
          now = System.system_time(:millisecond)

          infos =
            Enum.map(consumers, fn c ->
              %{
                name: c.name,
                pending: c.pending_count,
                idle: now - c.last_seen
              }
            end)

          {:ok, infos}

        error ->
          error
      end
    end
  end

  ## Private Helpers

  defp get_store_ref do
    case {
      :persistent_term.get(:spiredb_rocksdb_ref, nil),
      :persistent_term.get(:spiredb_rocksdb_cf_map, nil)
    } do
      {nil, _} -> {:error, :db_not_available}
      {_, nil} -> {:error, :cf_map_not_available}
      {db, cfs} -> {:ok, %{db: db, cfs: cfs}}
    end
  end

  defp resolve_start_id(_stream, "$"), do: nil
  defp resolve_start_id(_stream, "0"), do: {0, 0}
  defp resolve_start_id(_stream, "0-0"), do: {0, 0}

  defp resolve_start_id(_stream, id) when is_binary(id) do
    Event.parse_id(id) || {0, 0}
  end

  defp resolve_start_id(_stream, id), do: id

  defp format_id(nil), do: nil
  defp format_id({ts, seq}), do: "#{ts}-#{seq}"
  defp format_id(id) when is_binary(id), do: id

  defp ensure_consumer_exists(store_ref, stream, group, consumer) do
    case Storage.get_consumer(store_ref, stream, group, consumer) do
      {:ok, _} ->
        :ok

      {:error, :not_found} ->
        consumer_info = %{
          name: consumer,
          pending_count: 0,
          last_seen: System.system_time(:millisecond)
        }

        Storage.store_consumer(store_ref, stream, group, consumer_info)

      _ ->
        :ok
    end
  end

  defp update_consumer_pending(store_ref, stream, group, consumer, delta) do
    case Storage.get_consumer(store_ref, stream, group, consumer) do
      {:ok, info} ->
        updated = %{
          info
          | pending_count: max(0, info.pending_count + delta),
            last_seen: System.system_time(:millisecond)
        }

        Storage.store_consumer(store_ref, stream, group, updated)

      _ ->
        :ok
    end
  end
end
