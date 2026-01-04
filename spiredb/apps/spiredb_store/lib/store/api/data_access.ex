defmodule Store.API.DataAccess do
  @moduledoc """
  gRPC service for data access operations.

  Provides:
  - Raw KV: RawGet, RawScan, RawBatchGet
  - Table-aware: TableScan, TableGet, TableInsert, TableUpdate, TableDelete

  Returns Apache Arrow RecordBatches for zero-copy transfer to SpireSQL.
  """

  use GRPC.Server, service: Spiredb.Data.DataAccess.Service

  require Logger
  alias Store.KV.Engine
  alias Store.Arrow.Encoder
  alias Store.Server

  alias Spiredb.Data.{
    RawGetResponse,
    RawScanResponse,
    RawBatchGetResponse,
    TableScanResponse,
    TableGetResponse,
    TableInsertResponse,
    TableUpdateResponse,
    TableDeleteResponse,
    ScanStats
  }

  @default_batch_size 1000

  # ==========================================================================
  # Raw KV Operations
  # ==========================================================================

  @doc """
  Raw point lookup for a single key.
  """
  def raw_get(request, _stream) do
    Logger.debug("RawGet", key: request.key)

    engine = get_engine_for_key(request.key, Map.get(request, :engine))

    case Engine.get(engine, request.key) do
      {:ok, value} ->
        %RawGetResponse{value: value, found: true}

      {:error, :not_found} ->
        %RawGetResponse{value: <<>>, found: false}

      {:error, reason} ->
        Logger.error("RawGet failed", key: request.key, reason: inspect(reason))
        %RawGetResponse{value: <<>>, found: false}
    end
  end

  @doc """
  Raw put (for internal use, transactions use TransactionService).
  """
  def raw_put(request, _stream) do
    engine = get_engine_for_key(request.key, nil)

    case Engine.put(engine, request.key, request.value) do
      :ok ->
        %Spiredb.Data.Empty{}

      {:error, reason} ->
        Logger.error("RawPut failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Put failed"
    end
  end

  @doc """
  Raw delete.
  """
  def raw_delete(request, _stream) do
    engine = get_engine_for_key(request.key, nil)

    case Engine.delete(engine, request.key) do
      :ok ->
        %Spiredb.Data.Empty{}

      {:error, reason} ->
        Logger.error("RawDelete failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Delete failed"
    end
  end

  @doc """
  Raw scan a range of keys.
  Streams Arrow batches back to client.
  """
  def raw_scan(request, stream) do
    start_time = System.monotonic_time(:millisecond)
    Logger.debug("RawScan requested", region_id: request.region_id)

    engine = get_engine_for_key(request.start_key, Map.get(request, :engine))
    batch_size = if request.batch_size == 0, do: @default_batch_size, else: request.batch_size

    opts = [batch_size: batch_size, limit: request.limit]

    case Engine.scan_range(engine, request.start_key, request.end_key, opts) do
      {:ok, batches} ->
        stream_raw_batches(stream, batches, start_time)

      {:error, reason} ->
        Logger.error("RawScan failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Scan failed"
    end
  end

  @doc """
  Batch get multiple keys.
  """
  def raw_batch_get(request, _stream) do
    results =
      Enum.map(request.keys, fn key ->
        engine = get_engine_for_key(key, Map.get(request, :engine))

        case Engine.get(engine, key) do
          {:ok, value} -> {key, value, true}
          {:error, :not_found} -> {key, <<>>, false}
          {:error, _} -> {key, <<>>, false}
        end
      end)

    arrow_batch = Encoder.encode_batch_get_result(results)
    %RawBatchGetResponse{arrow_batch: arrow_batch}
  end

  # ==========================================================================
  # Table-aware Operations
  # ==========================================================================

  @doc """
  Table scan with schema-aware Arrow output.
  """
  def table_scan(request, stream) do
    start_time = System.monotonic_time(:millisecond)
    Logger.debug("TableScan", table: request.table_name, columns: request.columns)

    # TODO: Implement table-aware scan using Store.Schema.Registry
    # For now, delegate to raw scan on table prefix
    table_prefix = "#{request.table_name}:"

    opts = [batch_size: @default_batch_size, limit: request.limit]
    engine = Engine

    case Engine.scan_range(engine, table_prefix, "#{table_prefix}\xFF", opts) do
      {:ok, batches} ->
        stream_table_batches(stream, batches, request.columns, start_time)

      {:error, reason} ->
        Logger.error("TableScan failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Scan failed"
    end
  end

  @doc """
  Table get by primary key.
  """
  def table_get(request, _stream) do
    Logger.debug("TableGet", table: request.table_name, pk: request.primary_key)

    # TODO: Implement proper table key encoding
    key = "#{request.table_name}:#{request.primary_key}"
    engine = get_engine_for_key(key, nil)

    case Engine.get(engine, key) do
      {:ok, value} ->
        # TODO: Decode row and encode as Arrow with schema
        %TableGetResponse{arrow_batch: value, found: true}

      {:error, :not_found} ->
        %TableGetResponse{arrow_batch: <<>>, found: false}

      {:error, reason} ->
        Logger.error("TableGet failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Get failed"
    end
  end

  @doc """
  Table insert.
  """
  def table_insert(request, _stream) do
    Logger.debug("TableInsert", table: request.table_name)

    # TODO: Decode Arrow batch and insert rows with indexes
    # For now, placeholder
    %TableInsertResponse{rows_affected: 0}
  end

  @doc """
  Table update.
  """
  def table_update(request, _stream) do
    Logger.debug("TableUpdate", table: request.table_name, pk: request.primary_key)

    # TODO: Implement
    %TableUpdateResponse{updated: false}
  end

  @doc """
  Table delete.
  """
  def table_delete(request, _stream) do
    Logger.debug("TableDelete", table: request.table_name, pk: request.primary_key)

    # TODO: Implement with index cleanup
    %TableDeleteResponse{deleted: false}
  end

  # ==========================================================================
  # Private Helpers
  # ==========================================================================

  defp get_engine_for_key(key, engine_override) do
    if engine_override do
      engine_override
    else
      case Server.get_engine_for_key(key) do
        {:ok, engine} -> engine
        {:error, _} -> Engine
      end
    end
  end

  defp stream_raw_batches(stream, batches, start_time) do
    total = length(batches)

    Enum.with_index(batches)
    |> Enum.each(fn {{rows, stats}, idx} ->
      arrow_batch = Encoder.encode_scan_batch(rows)

      response = %RawScanResponse{
        arrow_batch: arrow_batch,
        has_more: idx < total - 1,
        stats: %ScanStats{
          rows_returned: stats.rows_returned,
          bytes_read: stats.bytes_read,
          scan_time_ms: stats.scan_time_ms
        }
      }

      grpc_module().send_reply(stream, response)
    end)
  end

  defp stream_table_batches(stream, batches, _columns, _start_time) do
    total = length(batches)

    Enum.with_index(batches)
    |> Enum.each(fn {{rows, stats}, idx} ->
      # TODO: Apply column projection and encode with schema
      arrow_batch = Encoder.encode_scan_batch(rows)

      response = %TableScanResponse{
        arrow_batch: arrow_batch,
        has_more: idx < total - 1,
        stats: %ScanStats{
          rows_returned: stats.rows_returned,
          bytes_read: stats.bytes_read,
          scan_time_ms: stats.scan_time_ms
        }
      }

      grpc_module().send_reply(stream, response)
    end)
  end

  defp grpc_module do
    Application.get_env(:spiredb_store, :grpc_module, GRPC.Server)
  end
end
