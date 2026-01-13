defmodule Store.API.InternalTransaction do
  @moduledoc """
  gRPC server implementation for InternalTransactionService.

  Handles store-to-store transaction coordination:
  - AsyncPrewrite: Prewrite mutations with async commit metadata
  - CheckSecondaryLocks: Check if secondary locks exist
  - ResolveLock: Resolve (commit or rollback) a lock
  """

  use GRPC.Server, service: Spiredb.Cluster.InternalTransactionService.Service

  require Logger
  alias Store.Transaction.Executor
  alias Store.Transaction.Lock
  alias Store.Region.ReadTsTracker

  @doc """
  Handle async prewrite request from coordinator node.
  """
  def async_prewrite(request, _stream) do
    Logger.debug("InternalTransaction.async_prewrite for #{length(request.mutations)} mutations")
    store_ref = get_store_ref()

    results =
      Enum.map(request.mutations, fn mutation ->
        key = mutation.key
        operation = convert_mutation_type(mutation)

        lock =
          Lock.new_async_commit(key, request.primary_key, request.start_ts,
            txn_id: request.txn_id,
            min_commit_ts: request.min_commit_ts,
            secondaries: if(key == request.primary_key, do: request.secondary_keys, else: nil),
            ttl: request.lock_ttl
          )

        case Executor.prewrite_with_lock(store_ref, key, operation, lock, request.start_ts) do
          :ok -> {:ok, key}
          {:error, {:locked_by, txn_id}} -> {:error, key, "locked_by:#{txn_id}"}
          {:error, :write_conflict} -> {:error, key, "write_conflict"}
          {:error, reason} -> {:error, key, inspect(reason)}
        end
      end)

    errors =
      results
      |> Enum.filter(&match?({:error, _, _}, &1))
      |> Enum.map(fn {:error, key, reason} ->
        %Spiredb.Cluster.InternalKeyError{key: key, error: reason}
      end)

    # Calculate actual min_commit_ts based on local read tracker
    actual_min_commit_ts = calculate_local_min_commit_ts(request.min_commit_ts, request.mutations)

    %Spiredb.Cluster.AsyncPrewriteResponse{
      success: errors == [],
      errors: errors,
      actual_min_commit_ts: actual_min_commit_ts
    }
  end

  @doc """
  Check status of secondary locks for async commit resolution.
  """
  def check_secondary_locks(request, _stream) do
    store_ref = get_store_ref()

    statuses =
      Enum.map(request.secondary_keys, fn key ->
        case Executor.get_lock(store_ref, key) do
          {:ok, nil} ->
            # No lock - check if committed
            case Executor.get_commit_record(store_ref, key, request.start_ts) do
              {:ok, commit_ts} ->
                %Spiredb.Cluster.SecondaryLockStatus{
                  key: key,
                  locked: false,
                  committed: true,
                  commit_ts: commit_ts
                }

              {:error, :not_found} ->
                %Spiredb.Cluster.SecondaryLockStatus{
                  key: key,
                  locked: false,
                  committed: false,
                  commit_ts: 0
                }
            end

          {:ok, lock} when lock.start_ts == request.start_ts ->
            %Spiredb.Cluster.SecondaryLockStatus{
              key: key,
              locked: true,
              committed: false,
              commit_ts: 0
            }

          {:ok, _other_lock} ->
            # Different transaction holds the lock
            %Spiredb.Cluster.SecondaryLockStatus{
              key: key,
              locked: false,
              committed: false,
              commit_ts: 0
            }

          {:error, _} ->
            %Spiredb.Cluster.SecondaryLockStatus{
              key: key,
              locked: false,
              committed: false,
              commit_ts: 0
            }
        end
      end)

    %Spiredb.Cluster.CheckSecondaryLocksResponse{statuses: statuses}
  end

  @doc """
  Resolve a lock (commit or rollback).
  """
  def resolve_lock(request, _stream) do
    store_ref = get_store_ref()
    key = request.key
    start_ts = request.start_ts
    commit_ts = request.commit_ts

    if commit_ts > 0 do
      # Commit
      Executor.write_commit_record(store_ref, key, start_ts, commit_ts)
      Executor.delete_lock(store_ref, key, start_ts)
    else
      # Rollback
      Executor.delete_lock(store_ref, key, start_ts)
      Executor.delete_data(store_ref, key, start_ts)
      Executor.write_rollback_record(store_ref, key, start_ts)
    end

    %Spiredb.Cluster.Empty{}
  end

  ## Private helpers

  defp get_store_ref do
    db = :persistent_term.get(:spiredb_rocksdb_ref, nil)
    cfs = :persistent_term.get(:spiredb_rocksdb_cf_map, %{})
    %{db: db, cfs: cfs}
  end

  defp convert_mutation_type(%{type: :INTERNAL_MUTATION_PUT, value: value}), do: {:put, value}
  defp convert_mutation_type(%{type: :INTERNAL_MUTATION_DELETE}), do: :delete
  defp convert_mutation_type(_), do: {:put, <<>>}

  defp calculate_local_min_commit_ts(min_commit_ts, mutations) do
    # Get max read ts from regions that contain these keys
    keys = Enum.map(mutations, & &1.key)

    local_max_read =
      keys
      |> Enum.map(&get_region_id_for_key/1)
      |> Enum.uniq()
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&ReadTsTracker.get_max_read_ts/1)
      |> Enum.max(fn -> 0 end)

    max(min_commit_ts, local_max_read + 1)
  end

  defp get_region_id_for_key(key) do
    try do
      case PD.Server.find_region(key) do
        {:ok, region} when not is_nil(region) -> region.id
        _ -> nil
      end
    catch
      :exit, _ -> nil
    end
  end
end
