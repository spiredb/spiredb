defmodule Store.KV.TTLFilter do
  @moduledoc """
  Background TTL cleanup process.

  Periodically scans keys with TTL and removes expired ones.
  Uses a cursor-based scan to avoid blocking the database.

  ## Configuration

      config :spiredb_store, Store.KV.TTLFilter,
        scan_interval_ms: 60_000,    # How often to run cleanup
        batch_size: 1000,            # Keys to scan per batch
        enabled: true                # Enable/disable cleanup
  """

  use GenServer
  require Logger

  alias Store.KV.TTL

  @default_scan_interval 60_000
  @default_batch_size 1000

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Force an immediate cleanup scan.
  """
  def force_cleanup do
    GenServer.cast(__MODULE__, :force_cleanup)
  end

  @doc """
  Get cleanup statistics.
  """
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    config = Application.get_env(:spiredb_store, __MODULE__, [])

    state = %{
      scan_interval: Keyword.get(config, :scan_interval_ms, @default_scan_interval),
      batch_size: Keyword.get(config, :batch_size, @default_batch_size),
      enabled: Keyword.get(config, :enabled, true),
      last_scan_at: nil,
      total_expired: 0,
      total_scans: 0,
      cursor: nil
    }

    # Schedule first scan
    if state.enabled do
      schedule_scan(state.scan_interval)
    end

    {:ok, state}
  end

  @impl true
  def handle_cast(:force_cleanup, state) do
    new_state = run_cleanup_scan(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    stats = %{
      enabled: state.enabled,
      last_scan_at: state.last_scan_at,
      total_expired: state.total_expired,
      total_scans: state.total_scans,
      scan_interval_ms: state.scan_interval
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info(:scan, state) do
    new_state =
      if state.enabled do
        run_cleanup_scan(state)
      else
        state
      end

    # Schedule next scan
    schedule_scan(new_state.scan_interval)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("TTLFilter received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Private

  defp schedule_scan(interval) do
    Process.send_after(self(), :scan, interval)
  end

  defp run_cleanup_scan(state) do
    start_time = System.monotonic_time(:millisecond)

    expired_count = scan_and_delete_expired(state.batch_size)

    elapsed = System.monotonic_time(:millisecond) - start_time

    if expired_count > 0 do
      Logger.debug("TTL cleanup: deleted #{expired_count} expired keys in #{elapsed}ms")
    end

    %{
      state
      | last_scan_at: DateTime.utc_now(),
        total_expired: state.total_expired + expired_count,
        total_scans: state.total_scans + 1
    }
  end

  defp scan_and_delete_expired(batch_size) do
    case get_db_refs() do
      {:ok, db_ref, cf_map} ->
        # Scan default CF for expired keys
        default_cf = Map.get(cf_map, "default")

        if default_cf do
          scan_cf_for_expired(db_ref, default_cf, batch_size)
        else
          0
        end

      _ ->
        0
    end
  end

  defp scan_cf_for_expired(db_ref, cf, batch_size) do
    case :rocksdb.iterator(db_ref, cf, []) do
      {:ok, iter} ->
        expired_keys = collect_expired_keys(iter, batch_size, [])
        :rocksdb.iterator_close(iter)

        # Delete expired keys
        Enum.each(expired_keys, fn key ->
          :rocksdb.delete(db_ref, cf, key, [])
        end)

        length(expired_keys)

      _ ->
        0
    end
  end

  defp collect_expired_keys(_iter, 0, acc), do: acc

  defp collect_expired_keys(iter, remaining, acc) do
    try do
      move_result =
        if acc == [] do
          :rocksdb.iterator_move(iter, :first)
        else
          :rocksdb.iterator_move(iter, :next)
        end

      case move_result do
        {:ok, key, value} ->
          if TTL.expired?(value) do
            collect_expired_keys(iter, remaining - 1, [key | acc])
          else
            collect_expired_keys(iter, remaining, acc)
          end

        _ ->
          acc
      end
    rescue
      ArgumentError -> acc
    end
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
