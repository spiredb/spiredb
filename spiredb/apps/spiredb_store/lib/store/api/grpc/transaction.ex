defmodule Store.API.GRPC.Transaction do
  @moduledoc """
  gRPC TransactionService implementation.

  Provides Percolator-style 2PC operations for distributed transactions.
  """

  use GRPC.Server, service: Spiredb.Data.TransactionService.Service

  require Logger
  alias Store.Transaction.Executor

  alias Spiredb.Data.{
    PrewriteResponse,
    CommitResponse,
    TxnStatus,
    PessimisticLockResponse,
    KeyError,
    LockInfo,
    Empty
  }

  @doc """
  Prewrite phase of 2PC.

  Acquires locks on all keys and writes data to txn_data CF.
  """
  def prewrite(request, _stream) do
    Logger.debug("Prewrite: #{length(request.mutations)} mutations, start_ts=#{request.start_ts}")
    store_ref = get_store_ref()

    errors =
      request.mutations
      |> Enum.map(fn mutation ->
        case Executor.prewrite_single(store_ref, mutation.key, mutation, request) do
          :ok ->
            nil

          {:error, {:locked_by, lock}} ->
            %KeyError{
              key: mutation.key,
              error: "locked",
              lock_info: %LockInfo{
                primary_key: lock.primary_key,
                start_ts: lock.start_ts,
                ttl: lock.ttl
              }
            }

          {:error, :write_conflict} ->
            %KeyError{key: mutation.key, error: "write_conflict"}

          {:error, reason} ->
            %KeyError{key: mutation.key, error: inspect(reason)}
        end
      end)
      |> Enum.reject(&is_nil/1)

    %PrewriteResponse{
      success: errors == [],
      errors: errors
    }
  end

  @doc """
  Commit phase of 2PC.

  Writes commit record for primary key. Secondary keys are resolved async.
  """
  def commit(request, _stream) do
    Logger.debug(
      "Commit: primary=#{inspect(request.primary_key)}, start_ts=#{request.start_ts}, commit_ts=#{request.commit_ts}"
    )

    store_ref = get_store_ref()

    case Executor.commit_primary(
           store_ref,
           request.primary_key,
           request.start_ts,
           request.commit_ts
         ) do
      :ok ->
        # Spawn async secondary key resolution
        if length(request.keys) > 0 do
          spawn(fn ->
            Enum.each(request.keys, fn key ->
              Executor.commit_secondary(store_ref, key, request.start_ts, request.commit_ts)
            end)
          end)
        end

        %CommitResponse{success: true}

      {:error, reason} ->
        %CommitResponse{success: false, error: inspect(reason)}
    end
  end

  @doc """
  Rollback a transaction.
  """
  def rollback(request, _stream) do
    Logger.debug("Rollback: start_ts=#{request.start_ts}, #{length(request.keys)} keys")
    store_ref = get_store_ref()

    Enum.each(request.keys, fn key ->
      Executor.rollback_key(store_ref, key, request.start_ts)
    end)

    %Empty{}
  end

  @doc """
  Check status of a transaction by its primary key.
  """
  def check_txn_status(request, _stream) do
    Logger.debug(
      "CheckTxnStatus: primary=#{inspect(request.primary_key)}, start_ts=#{request.start_ts}"
    )

    store_ref = get_store_ref()

    case Executor.check_txn_status(store_ref, request.primary_key, request.start_ts) do
      {:committed, commit_ts} ->
        %TxnStatus{state: :TXN_COMMITTED, commit_ts: commit_ts}

      :rolled_back ->
        %TxnStatus{state: :TXN_ROLLED_BACK}

      {:pending, ttl} ->
        %TxnStatus{state: :TXN_PENDING, lock_ttl: ttl}

      {:error, _} ->
        %TxnStatus{state: :TXN_ROLLED_BACK}
    end
  end

  @doc """
  Resolve a lock on a key.
  """
  def resolve_lock(request, _stream) do
    Logger.debug("ResolveLock: key=#{inspect(request.key)}, start_ts=#{request.start_ts}")
    store_ref = get_store_ref()

    if request.commit_ts > 0 do
      # Commit the secondary key
      Executor.commit_secondary(store_ref, request.key, request.start_ts, request.commit_ts)
    else
      # Rollback the key
      Executor.rollback_key(store_ref, request.key, request.start_ts)
    end

    %Empty{}
  end

  @doc """
  Acquire pessimistic locks.
  """
  def acquire_pessimistic_lock(request, _stream) do
    Logger.debug("AcquirePessimisticLock: #{length(request.keys)} keys")
    store_ref = get_store_ref()

    errors =
      request.keys
      |> Enum.map(fn key ->
        case Executor.acquire_pessimistic_lock(
               store_ref,
               key,
               request.start_ts,
               request.for_update_ts,
               request.lock_ttl
             ) do
          :ok ->
            nil

          {:error, {:locked_by, lock}} ->
            %KeyError{
              key: key,
              error: "locked",
              lock_info: %LockInfo{
                primary_key: lock.primary_key,
                start_ts: lock.start_ts,
                ttl: lock.ttl
              }
            }

          {:error, reason} ->
            %KeyError{key: key, error: inspect(reason)}
        end
      end)
      |> Enum.reject(&is_nil/1)

    %PessimisticLockResponse{
      success: errors == [],
      errors: errors
    }
  end

  defp get_store_ref do
    db = :persistent_term.get(:spiredb_rocksdb_ref, nil)
    cfs = :persistent_term.get(:spiredb_rocksdb_cf_map, %{})
    %{db: db, cfs: cfs}
  end
end
