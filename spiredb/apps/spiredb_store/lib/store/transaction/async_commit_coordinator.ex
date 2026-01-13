defmodule Store.Transaction.AsyncCommitCoordinator do
  @moduledoc """
  Coordinator for async commit protocol.

  Key insight: After all prewrites succeed, transaction WILL commit.
  Return success immediately, complete commit asynchronously.

  Commit truth is distributed:
  - Primary lock has secondaries list
  - All locks present = committed
  - Missing lock = check if resolved
  """

  require Logger
  alias Store.Transaction
  alias Store.Transaction.Lock
  alias Store.Transaction.Executor
  alias Store.Region.ReadTsTracker

  @doc """
  Execute async commit for distributed transaction.

  Returns {:ok, min_commit_ts} immediately after prewrite.
  Actual commit happens asynchronously.
  """
  def commit(_store_ref, %Transaction{write_buffer: buffer} = txn) when map_size(buffer) == 0 do
    {:ok, txn.start_ts}
  end

  def commit(store_ref, %Transaction{} = txn) do
    Logger.debug("AsyncCommitCoordinator: starting async commit for #{txn.id}")

    # Determine primary key and secondary keys
    all_keys = Map.keys(txn.write_buffer)
    primary_key = select_primary_key(all_keys)
    secondary_keys = all_keys -- [primary_key]

    # Calculate min_commit_ts from all involved regions
    min_commit_ts = calculate_min_commit_ts(txn, all_keys)

    # Phase 1: Prewrite all keys with async commit metadata
    case prewrite_async_commit(store_ref, txn, primary_key, secondary_keys, min_commit_ts) do
      :ok ->
        # SUCCESS! Return to client immediately
        # Spawn background commit
        spawn_async_finalize(store_ref, txn, primary_key, min_commit_ts)
        {:ok, min_commit_ts}

      {:error, reason} ->
        # Prewrite failed - rollback
        rollback_all(store_ref, txn)
        {:error, reason}
    end
  end

  @doc """
  Resolve an async commit transaction encountered during read.

  Called when reader encounters lock with use_async_commit=true.
  """
  def resolve_async_commit(store_ref, %Lock{use_async_commit: true} = lock) do
    Logger.debug("Resolving async commit for txn starting at #{lock.start_ts}")

    # Check primary lock status
    case check_primary_status(store_ref, lock.primary_key, lock.start_ts) do
      {:committed, commit_ts} ->
        # Already committed - resolve this secondary
        resolve_to_committed(store_ref, lock.key, lock.start_ts, commit_ts)
        {:committed, commit_ts}

      {:pending, primary_lock} ->
        # Check if all secondaries are locked (implicit commit)
        case check_all_secondaries(store_ref, primary_lock) do
          :all_present ->
            # Transaction is implicitly committed
            commit_ts = calculate_final_commit_ts(primary_lock)
            finalize_async_commit(store_ref, primary_lock, commit_ts)
            {:committed, commit_ts}

          {:missing, _missing_keys} ->
            if Lock.expired?(primary_lock) do
              rollback_async_commit(store_ref, primary_lock)
              :rolled_back
            else
              {:pending, primary_lock.ttl}
            end
        end

      :rolled_back ->
        :rolled_back

      :not_found ->
        case Executor.get_commit_record(store_ref, lock.primary_key, lock.start_ts) do
          {:ok, commit_ts} -> {:committed, commit_ts}
          {:error, :not_found} -> :rolled_back
        end
    end
  end

  def resolve_async_commit(_store_ref, %Lock{use_async_commit: false} = _lock) do
    # Not an async commit lock, use standard resolution
    :not_async
  end

  ## Private Functions

  defp select_primary_key(keys) do
    # Select shortest key as primary (reduces lock storage)
    Enum.min_by(keys, &byte_size/1)
  end

  defp calculate_min_commit_ts(txn, keys) do
    # min_commit_ts = max(start_ts, max_read_ts_of_all_regions) + 1
    regions =
      keys
      |> Enum.map(&get_region_for_key/1)
      |> Enum.uniq()
      |> Enum.reject(&is_nil/1)

    max_read_ts =
      case regions do
        [] -> 0
        rs -> Enum.map(rs, &ReadTsTracker.get_max_read_ts/1) |> Enum.max(fn -> 0 end)
      end

    max(txn.start_ts, max_read_ts) + 1
  end

  defp prewrite_async_commit(store_ref, txn, primary_key, secondary_keys, min_commit_ts) do
    # Prewrite primary first
    case prewrite_key_async(
           store_ref,
           txn,
           primary_key,
           primary_key,
           secondary_keys,
           min_commit_ts
         ) do
      :ok ->
        # Prewrite secondaries in parallel
        results =
          secondary_keys
          |> Task.async_stream(
            fn key ->
              prewrite_key_async(store_ref, txn, key, primary_key, nil, min_commit_ts)
            end,
            max_concurrency: 10,
            timeout: 10_000
          )
          |> Enum.to_list()

        case Enum.find(results, &match?({:ok, {:error, _}}, &1)) do
          nil -> :ok
          {:ok, error} -> error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp prewrite_key_async(store_ref, txn, key, primary_key, secondaries, min_commit_ts) do
    operation = Map.get(txn.write_buffer, key)

    # Check if key belongs to local or remote store
    case get_store_for_key(key) do
      {:local, _region_id} ->
        # Local prewrite
        lock =
          Lock.new_async_commit(key, primary_key, txn.start_ts,
            txn_id: txn.id,
            min_commit_ts: min_commit_ts,
            secondaries: secondaries
          )

        Executor.prewrite_with_lock(store_ref, key, operation, lock, txn.start_ts)

      {:remote, store_address} ->
        # Remote prewrite via gRPC
        prewrite_remote(
          store_address,
          txn,
          key,
          operation,
          primary_key,
          secondaries,
          min_commit_ts
        )
    end
  end

  defp prewrite_remote(
         store_address,
         txn,
         key,
         operation,
         primary_key,
         secondaries,
         min_commit_ts
       ) do
    alias Store.Transaction.InternalClient

    mutation = build_mutation(key, operation)
    secondary_keys = if secondaries, do: secondaries, else: []

    request = %Spiredb.Cluster.AsyncPrewriteRequest{
      mutations: [mutation],
      primary_key: primary_key,
      secondary_keys: secondary_keys,
      start_ts: txn.start_ts,
      min_commit_ts: min_commit_ts,
      lock_ttl: 30_000,
      txn_id: txn.id
    }

    case InternalClient.async_prewrite(store_address, request) do
      {:ok, %{success: true}} ->
        :ok

      {:ok, %{success: false, errors: errors}} ->
        first_error = List.first(errors)
        {:error, {:remote_prewrite_failed, first_error && first_error.error}}

      {:error, reason} ->
        {:error, {:rpc_failed, reason}}
    end
  end

  defp build_mutation(key, {:put, value}) do
    %Spiredb.Cluster.InternalMutation{
      type: :INTERNAL_MUTATION_PUT,
      key: key,
      value: value
    }
  end

  defp build_mutation(key, :delete) do
    %Spiredb.Cluster.InternalMutation{
      type: :INTERNAL_MUTATION_DELETE,
      key: key,
      value: <<>>
    }
  end

  defp get_store_for_key(key) do
    # Check if key belongs to local or remote store
    case get_region_for_key(key) do
      nil ->
        # No region info - assume local
        {:local, nil}

      region_id ->
        # Check if this region is local
        local_regions = get_local_region_ids()

        if region_id in local_regions do
          {:local, region_id}
        else
          # Get store address for this region
          case get_store_address_for_region(region_id) do
            # Fallback to local if unknown
            nil -> {:local, region_id}
            address -> {:remote, address}
          end
        end
    end
  end

  defp get_local_region_ids do
    # Get regions managed by this node
    try do
      {:ok, state} = :sys.get_state(Store.Server)
      Map.keys(state.regions)
    catch
      # Default: assume all local
      _, _ ->
        # Fallback to configured region count
        num_regions = Application.get_env(:spiredb_pd, :num_regions, 16)
        1..num_regions |> Enum.to_list()
    end
  end

  defp get_store_address_for_region(region_id) do
    try do
      case PD.Server.get_region_by_id(region_id) do
        {:ok, region} when region != nil ->
          # Get first store that isn't this node
          case Enum.find(region.stores || [], &(&1 != node())) do
            nil -> nil
            store_node -> Store.Transaction.InternalClient.get_store_address(store_node)
          end

        _ ->
          nil
      end
    catch
      :exit, _ -> nil
    end
  end

  alias Store.Transaction.InternalClient

  defp spawn_async_finalize(store_ref, txn, primary_key, min_commit_ts) do
    spawn(fn ->
      # Small delay to allow any in-flight reads to complete
      Process.sleep(1)

      # Calculate final commit_ts
      commit_ts = calculate_final_commit_ts_for_txn(txn, min_commit_ts)

      # Write commit records
      do_async_finalize(store_ref, txn, primary_key, commit_ts)
    end)
  end

  defp do_async_finalize(store_ref, txn, primary_key, commit_ts) do
    # Write commit record for primary first
    with :ok <- Executor.write_commit_record(store_ref, primary_key, txn.start_ts, commit_ts),
         :ok <- Executor.delete_lock(store_ref, primary_key, txn.start_ts) do
      # Async finalize secondaries
      secondary_keys = Map.keys(txn.write_buffer) -- [primary_key]

      Enum.each(secondary_keys, fn key ->
        # Best effort for secondaries
        with :ok <- Executor.write_commit_record(store_ref, key, txn.start_ts, commit_ts) do
          Executor.delete_lock(store_ref, key, txn.start_ts)
        else
          error ->
            Logger.warning(
              "AsyncCommitCoordinator: failed to finalize secondary #{inspect(key)}: #{inspect(error)}"
            )
        end
      end)
    else
      error ->
        Logger.warning(
          "AsyncCommitCoordinator: failed to finalize primary #{inspect(primary_key)}: #{inspect(error)}"
        )
    end

    Logger.debug("Async commit finalized for txn #{txn.id} at commit_ts=#{commit_ts}")
  end

  defp check_primary_status(store_ref, primary_key, start_ts) do
    case Executor.get_commit_record(store_ref, primary_key, start_ts) do
      {:ok, commit_ts} ->
        {:committed, commit_ts}

      {:error, :not_found} ->
        case Executor.get_lock(store_ref, primary_key) do
          {:ok, nil} -> :not_found
          {:ok, lock} when lock.start_ts == start_ts -> {:pending, lock}
          {:ok, _} -> :rolled_back
          {:error, _} -> :rolled_back
        end
    end
  end

  defp check_all_secondaries(_store_ref, %Lock{secondaries: nil}), do: :all_present
  defp check_all_secondaries(_store_ref, %Lock{secondaries: []}), do: :all_present

  defp check_all_secondaries(store_ref, %Lock{secondaries: secondaries, start_ts: start_ts}) do
    missing =
      Enum.filter(secondaries, fn key ->
        case Executor.get_lock(store_ref, key) do
          {:ok, lock} when lock.start_ts == start_ts ->
            false

          _ ->
            case Executor.get_commit_record(store_ref, key, start_ts) do
              {:ok, _} -> false
              {:error, :not_found} -> true
            end
        end
      end)

    if missing == [], do: :all_present, else: {:missing, missing}
  end

  defp calculate_final_commit_ts(%Lock{min_commit_ts: min_commit_ts, secondaries: secondaries}) do
    all_keys = List.flatten([secondaries || []])

    if all_keys == [] do
      min_commit_ts
    else
      regions = Enum.map(all_keys, &get_region_for_key/1) |> Enum.uniq() |> Enum.reject(&is_nil/1)

      current_max_read =
        case regions do
          [] -> 0
          rs -> Enum.map(rs, &ReadTsTracker.get_max_read_ts/1) |> Enum.max(fn -> 0 end)
        end

      max(min_commit_ts, current_max_read + 1)
    end
  end

  defp calculate_final_commit_ts_for_txn(txn, min_commit_ts) do
    calculate_final_commit_ts(%Lock{
      min_commit_ts: min_commit_ts,
      secondaries: Map.keys(txn.write_buffer)
    })
  end

  defp finalize_async_commit(store_ref, primary_lock, commit_ts) do
    all_keys = [primary_lock.key | primary_lock.secondaries || []]

    Enum.each(all_keys, fn key ->
      Executor.write_commit_record(store_ref, key, primary_lock.start_ts, commit_ts)
      Executor.delete_lock(store_ref, key, primary_lock.start_ts)
    end)
  end

  defp rollback_async_commit(store_ref, primary_lock) do
    all_keys = [primary_lock.key | primary_lock.secondaries || []]

    Enum.each(all_keys, fn key ->
      Executor.delete_lock(store_ref, key, primary_lock.start_ts)
      Executor.delete_data(store_ref, key, primary_lock.start_ts)
      Executor.write_rollback_record(store_ref, key, primary_lock.start_ts)
    end)
  end

  defp resolve_to_committed(store_ref, key, start_ts, commit_ts) do
    Executor.write_commit_record(store_ref, key, start_ts, commit_ts)
    Executor.delete_lock(store_ref, key, start_ts)
  end

  defp rollback_all(store_ref, txn) do
    Enum.each(txn.write_buffer, fn {key, _} ->
      Executor.delete_lock(store_ref, key, txn.start_ts)
      Executor.delete_data(store_ref, key, txn.start_ts)
    end)
  end

  defp get_region_for_key(key) do
    # Try to get region from PD. Falls back gracefully for single-node mode.
    try do
      case PD.Server.find_region(key) do
        {:ok, nil} -> nil
        {:ok, region} -> region.id
        {:error, _} -> nil
      end
    catch
      # PD not available (single-node mode)
      :exit, _ -> nil
    end
  end
end
