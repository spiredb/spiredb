defmodule Store.ChangeStream do
  @moduledoc """
  Change Data Capture (CDC) for SpireDB.

  Tracks mutations (PUT, DELETE) and allows consumers to subscribe
  to changes from a sequence point. Useful for:
  - Real-time replication
  - Event sourcing
  - Cache invalidation
  - Audit logging

  ## Architecture

  Changes are captured in two ways:
  1. **Local RocksDB WAL** - via WriteBatch hooks (fast, local)
  2. **Raft Log** - via Ra log entries (distributed, replicated)

  ## Usage

      # Get changes since sequence 1000
      {:ok, changes, new_seq} = Store.ChangeStream.get_changes(1000, limit: 100)

      # Subscribe to live changes
      {:ok, pid} = Store.ChangeStream.subscribe(from: :latest)
  """

  use GenServer
  require Logger
  alias Store.Stream.OffsetStore
  alias Store.Stream.Watermark

  @change_log_cf "change_log"
  @change_meta_key "__change_stream_meta__"

  @type change_type :: :put | :delete | :batch
  @type change :: %{
          seq: non_neg_integer(),
          type: change_type(),
          key: binary(),
          value: binary() | nil,
          timestamp: non_neg_integer(),
          cf: String.t()
        }

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Record a change (called internally after KV mutations).
  """
  @spec record_change(change_type(), String.t(), binary(), binary() | nil) :: :ok
  def record_change(type, cf, key, value \\ nil) do
    GenServer.cast(__MODULE__, {:record, type, cf, key, value})
  end

  @doc """
  Get changes since a sequence number.

  ## Options
  - `:limit` - Maximum changes to return (default: 1000)
  - `:cf` - Filter by column family

  Returns `{:ok, changes, last_seq}` or `{:error, reason}`.
  """
  @spec get_changes(non_neg_integer(), keyword()) ::
          {:ok, [change()], non_neg_integer()} | {:error, term()}
  def get_changes(from_seq, opts \\ []) do
    GenServer.call(__MODULE__, {:get_changes, from_seq, opts})
  end

  @doc """
  Get the current sequence number.
  """
  @spec current_seq() :: {:ok, non_neg_integer()}
  def current_seq do
    GenServer.call(__MODULE__, :current_seq)
  end

  @doc """
  Subscribe to live changes.

  ## Options
  - `:from` - Starting sequence (:latest, :earliest, or seq number)

  The subscriber will receive `{:change, change}` messages.
  """
  @spec subscribe(keyword()) :: :ok | {:error, term()}
  def subscribe(opts \\ []) do
    GenServer.call(__MODULE__, {:subscribe, self(), opts})
  end

  @doc """
  Unsubscribe from live changes.
  """
  @spec unsubscribe() :: :ok
  def unsubscribe do
    GenServer.call(__MODULE__, {:unsubscribe, self()})
  end

  @doc """
  Subscribe as a named consumer from a specific offset.
  """
  def subscribe_from_offset(consumer_id, offset, opts \\ []) do
    GenServer.call(__MODULE__, {:subscribe_consumer, self(), consumer_id, offset, opts})
  end

  @doc """
  Acknowledge processing up to an offset for a named consumer.
  """
  def acknowledge(consumer_id, offset) do
    GenServer.cast(__MODULE__, {:acknowledge, consumer_id, offset})
  end

  @doc """
  Get the last acknowledged offset for a consumer.
  """
  def get_consumer_offset(consumer_id) do
    GenServer.call(__MODULE__, {:get_consumer_offset, consumer_id})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    # Load current sequence from storage
    seq = load_current_seq()

    state = %{
      current_seq: seq,
      subscribers: %{},
      buffer: [],
      buffer_size: 0,
      max_buffer_size: 1000,
      flush_interval: 100
    }

    # Schedule periodic flush
    schedule_flush()

    {:ok, state}
  end

  @impl true
  def handle_cast({:record, type, cf, key, value}, state) do
    change = %{
      seq: state.current_seq + 1,
      type: type,
      cf: cf,
      key: key,
      value: value,
      timestamp: System.system_time(:millisecond)
    }

    # Update Watermark
    Watermark.update_high_watermark(change.timestamp)

    # Add to buffer
    new_buffer = [change | state.buffer]
    new_buffer_size = state.buffer_size + 1
    new_seq = state.current_seq + 1

    # Notify subscribers
    notify_subscribers(state.subscribers, change)

    new_state = %{
      state
      | current_seq: new_seq,
        buffer: new_buffer,
        buffer_size: new_buffer_size
    }

    # Flush if buffer is full
    if new_buffer_size >= state.max_buffer_size do
      {:noreply, flush_buffer(new_state)}
    else
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:acknowledge, consumer_id, offset}, state) do
    case get_store_ref() do
      nil -> :ok
      store_ref -> OffsetStore.store_offset(store_ref, consumer_id, offset)
    end

    {:noreply, state}
  end

  @impl true
  def handle_call({:get_changes, from_seq, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 1000)
    cf_filter = Keyword.get(opts, :cf)

    # First check buffer for recent changes
    buffer_changes =
      state.buffer
      |> Enum.filter(fn c -> c.seq > from_seq end)
      |> Enum.reverse()

    # Then read from storage for older changes
    stored_changes = read_stored_changes(from_seq, limit, cf_filter)

    # Combine and limit
    all_changes =
      (stored_changes ++ buffer_changes)
      |> Enum.filter(fn c ->
        c.seq > from_seq and (cf_filter == nil or c.cf == cf_filter)
      end)
      |> Enum.take(limit)

    last_seq =
      case all_changes do
        [] -> from_seq
        changes -> List.last(changes).seq
      end

    {:reply, {:ok, all_changes, last_seq}, state}
  end

  @impl true
  def handle_call(:current_seq, _from, state) do
    {:reply, {:ok, state.current_seq}, state}
  end

  @impl true
  def handle_call({:subscribe, pid, opts}, _from, state) do
    ref = Process.monitor(pid)
    from = Keyword.get(opts, :from, :latest)

    start_seq =
      case from do
        :latest -> state.current_seq
        :earliest -> 0
        seq when is_integer(seq) -> seq
      end

    subscriber = %{pid: pid, ref: ref, from_seq: start_seq}
    new_subscribers = Map.put(state.subscribers, pid, subscriber)

    # Replay history
    changes = read_recent_history(state, start_seq)
    Enum.each(changes, fn change -> send(pid, {:change, change}) end)

    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  @impl true
  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.get(state.subscribers, pid) do
      nil ->
        {:reply, :ok, state}

      %{ref: ref} ->
        Process.demonitor(ref, [:flush])
        new_subscribers = Map.delete(state.subscribers, pid)
        {:reply, :ok, %{state | subscribers: new_subscribers}}
    end
  end

  @impl true
  def handle_call({:subscribe_consumer, pid, consumer_id, offset, _opts}, _from, state) do
    ref = Process.monitor(pid)

    # Store initial offset if provided (or overwrite?)
    # Usually we want to resume, so if offset is nil, we might look it up.
    # But the API is subscribe_from_offset, implying explicit start.
    # We should probably persist this start if it's new/higher?
    # For now, just register the subscriber in memory.

    subscriber = %{pid: pid, ref: ref, from_seq: offset, consumer_id: consumer_id}
    new_subscribers = Map.put(state.subscribers, pid, subscriber)

    # Replay history
    changes = read_recent_history(state, offset)
    Enum.each(changes, fn change -> send(pid, {:change, change}) end)

    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  @impl true
  def handle_call({:get_consumer_offset, consumer_id}, _from, state) do
    offset =
      case get_store_ref() do
        nil ->
          {:error, :db_not_ready}

        store_ref ->
          case OffsetStore.get_offset(store_ref, consumer_id) do
            {:ok, off} -> {:ok, off}
            # Default to 0 if not found
            _ -> {:ok, 0}
          end
      end

    {:reply, offset, state}
  end

  @impl true
  def handle_info(:flush, state) do
    schedule_flush()
    {:noreply, flush_buffer(state)}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_subscribers = Map.delete(state.subscribers, pid)
    {:noreply, %{state | subscribers: new_subscribers}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("ChangeStream received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Private

  defp schedule_flush do
    Process.send_after(self(), :flush, 100)
  end

  defp flush_buffer(%{buffer: []} = state), do: state

  defp flush_buffer(%{buffer: buffer, current_seq: seq} = state) do
    # Store changes to RocksDB
    case get_db_refs() do
      {:ok, db_ref, cf_map} ->
        change_cf = Map.get(cf_map, @change_log_cf)

        if change_cf do
          Enum.each(buffer, fn change ->
            key = encode_seq_key(change.seq)
            value = :erlang.term_to_binary(change)
            :rocksdb.put(db_ref, change_cf, key, value, [])
          end)
        end

        # Update meta
        persist_current_seq(db_ref, cf_map, seq)

      _ ->
        :ok
    end

    %{state | buffer: [], buffer_size: 0}
  end

  defp load_current_seq do
    case get_db_refs() do
      {:ok, db_ref, cf_map} ->
        meta_cf = Map.get(cf_map, "meta")

        case :rocksdb.get(db_ref, meta_cf, @change_meta_key, []) do
          {:ok, data} -> :erlang.binary_to_term(data)
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp persist_current_seq(db_ref, cf_map, seq) do
    meta_cf = Map.get(cf_map, "meta")

    if meta_cf do
      :rocksdb.put(db_ref, meta_cf, @change_meta_key, :erlang.term_to_binary(seq), [])
    end
  end

  defp read_stored_changes(from_seq, limit, cf_filter) do
    case get_db_refs() do
      {:ok, db_ref, cf_map} ->
        change_cf = Map.get(cf_map, @change_log_cf)

        if change_cf do
          start_key = encode_seq_key(from_seq + 1)
          scan_changes(db_ref, change_cf, start_key, limit, cf_filter)
        else
          []
        end

      _ ->
        []
    end
  end

  defp scan_changes(db_ref, cf, start_key, limit, cf_filter) do
    case :rocksdb.iterator(db_ref, cf, []) do
      {:ok, iter} ->
        changes = collect_changes(iter, start_key, limit, cf_filter, [])
        :rocksdb.iterator_close(iter)
        changes

      _ ->
        []
    end
  end

  defp collect_changes(iter, start_key, limit, cf_filter, acc) when limit > 0 do
    case :rocksdb.iterator_move(iter, {:seek, start_key}) do
      {:ok, _key, value} ->
        change = :erlang.binary_to_term(value)

        if cf_filter == nil or change.cf == cf_filter do
          collect_next_changes(iter, limit - 1, cf_filter, [change | acc])
        else
          collect_next_changes(iter, limit, cf_filter, acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_changes(_iter, _start_key, _limit, _cf_filter, acc) do
    Enum.reverse(acc)
  end

  defp collect_next_changes(iter, limit, cf_filter, acc) when limit > 0 do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, _key, value} ->
        change = :erlang.binary_to_term(value)

        if cf_filter == nil or change.cf == cf_filter do
          collect_next_changes(iter, limit - 1, cf_filter, [change | acc])
        else
          collect_next_changes(iter, limit, cf_filter, acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp collect_next_changes(_iter, _limit, _cf_filter, acc) do
    Enum.reverse(acc)
  end

  defp encode_seq_key(seq) do
    # Zero-padded for proper ordering
    String.pad_leading(Integer.to_string(seq), 20, "0")
  end

  defp notify_subscribers(subscribers, change) do
    Enum.each(subscribers, fn {pid, %{from_seq: from_seq}} ->
      if change.seq > from_seq do
        send(pid, {:change, change})
      end
    end)
  end

  defp read_recent_history(state, from_seq) do
    # 1. Read from storage
    # Use a default limit for replay (e.g., 1000) or assume unlimited?
    # For now 1000 is safe.
    stored = read_stored_changes(from_seq, 1000, nil)

    # 2. Read from buffer
    buffered =
      state.buffer
      |> Enum.filter(fn c -> c.seq > from_seq end)
      |> Enum.reverse()

    # If any overlap, we prefer buffer? No, stored are definitely older or same.
    # But read_stored_changes does scan.
    # We should deduplicate if necessary, but sequential logic holds (flush clears buffer).

    # However, read_stored_changes looks at *persisted* data.
    # Buffer contains *unpersisted* data.
    # So they are disjoint sets.

    stored ++ buffered
  end

  defp get_db_refs do
    case {:persistent_term.get(:spiredb_rocksdb_ref, nil),
          :persistent_term.get(:spiredb_rocksdb_cf_map, nil)} do
      {nil, _} -> {:error, :db_not_available}
      {_, nil} -> {:error, :cf_map_not_available}
      {db_ref, cf_map} -> {:ok, db_ref, cf_map}
    end
  end

  defp get_store_ref do
    case get_db_refs() do
      {:ok, db, cfs} -> %{db: db, cfs: cfs}
      _ -> nil
    end
  end
end
