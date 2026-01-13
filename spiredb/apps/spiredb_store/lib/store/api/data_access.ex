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
  alias Store.Schema.Encoder
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

    # Use table ID prefix for scan range
    # Table names map to IDs via schema - for now use hash
    table_id = :erlang.phash2(request.table_name)
    table_prefix = Encoder.encode_table_key(table_id, <<>>)
    table_end = <<table_prefix::binary, 0xFF>>

    opts = [batch_size: @default_batch_size, limit: request.limit]
    engine = Engine

    case Engine.scan_range(engine, table_prefix, table_end, opts) do
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

    # Encode table key with table_id and primary_key
    table_id = :erlang.phash2(request.table_name)
    key = Encoder.encode_table_key(table_id, request.primary_key)
    engine = get_engine_for_key(key, nil)

    case Engine.get(engine, key) do
      {:ok, value} ->
        # Return raw value as Arrow batch (client decodes)
        %TableGetResponse{arrow_batch: value, found: true}

      {:error, :not_found} ->
        %TableGetResponse{arrow_batch: <<>>, found: false}

      {:error, reason} ->
        Logger.error("TableGet failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Get failed"
    end
  end

  @doc """
  Table insert with Arrow batch input.
  """
  def table_insert(request, _stream) do
    Logger.debug("TableInsert",
      table: request.table_name,
      batch_size: byte_size(request.arrow_batch)
    )

    table_id = :erlang.phash2(request.table_name)

    # Parse Arrow batch and extract rows
    # For now, treat arrow_batch as serialized rows
    rows = parse_arrow_batch(request.arrow_batch, request.table_name)

    # Insert each row
    inserted =
      Enum.reduce(rows, 0, fn {pk, value}, count ->
        key = Encoder.encode_table_key(table_id, pk)

        case get_db_ref_direct() do
          nil ->
            # No RocksDB
            Logger.error("TableInsert failed: No RocksDB store_ref found")
            count

          db_ref ->
            case :rocksdb.put(db_ref, key, value, []) do
              :ok -> count + 1
              {:error, _} -> count
            end
        end
      end)

    %TableInsertResponse{rows_affected: inserted}
  end

  @doc """
  Table update by primary key.
  """
  def table_update(request, _stream) do
    Logger.debug("TableUpdate", table: request.table_name, pk: request.primary_key)

    table_id = :erlang.phash2(request.table_name)
    key = Encoder.encode_table_key(table_id, request.primary_key)

    case get_db_ref_direct() do
      nil ->
        %TableUpdateResponse{updated: false}

      db_ref ->
        # Check if exists then update
        case :rocksdb.get(db_ref, key, []) do
          {:ok, _} ->
            case :rocksdb.put(db_ref, key, request.arrow_batch, []) do
              :ok -> %TableUpdateResponse{updated: true}
              {:error, _} -> %TableUpdateResponse{updated: false}
            end

          _ ->
            %TableUpdateResponse{updated: false}
        end
    end
  end

  @doc """
  Table delete by primary key.
  """
  def table_delete(request, _stream) do
    Logger.debug("TableDelete", table: request.table_name, pk: request.primary_key)

    table_id = :erlang.phash2(request.table_name)
    key = Encoder.encode_table_key(table_id, request.primary_key)

    case get_db_ref_direct() do
      nil ->
        %TableDeleteResponse{deleted: false}

      db_ref ->
        case :rocksdb.get(db_ref, key, []) do
          {:ok, _} ->
            case :rocksdb.delete(db_ref, key, []) do
              :ok -> %TableDeleteResponse{deleted: true}
              {:error, _} -> %TableDeleteResponse{deleted: false}
            end

          _ ->
            %TableDeleteResponse{deleted: false}
        end
    end
  end

  defp get_db_ref_direct do
    :persistent_term.get(:spiredb_rocksdb_ref, nil)
  end

  defp parse_arrow_batch(batch, _table_name) when is_binary(batch) do
    # Parse Arrow IPC format to extract rows
    # For now, treat as simple format: [pk_len:4][pk][value_len:4][value]...
    parse_simple_batch(batch, [])
  end

  defp parse_simple_batch(<<>>, acc), do: Enum.reverse(acc)

  defp parse_simple_batch(
         <<pk_len::32, pk::binary-size(pk_len), val_len::32, value::binary-size(val_len),
           rest::binary>>,
         acc
       ) do
    parse_simple_batch(rest, [{pk, value} | acc])
  end

  defp parse_simple_batch(_, acc), do: Enum.reverse(acc)

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

  defp stream_raw_batches(stream, batches, _start_time) do
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

  defp stream_table_batches(stream, batches, columns, _start_time) do
    total = length(batches)

    Enum.with_index(batches)
    |> Enum.each(fn {{rows, stats}, idx} ->
      # Apply column projection if columns specified
      projected_rows =
        if columns && columns != [] do
          Enum.map(rows, fn {key, value} ->
            projected_value = apply_column_projection(value, columns)
            {key, projected_value}
          end)
        else
          rows
        end

      arrow_batch = Encoder.encode_scan_batch(projected_rows)

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

  # Apply column projection to row value
  # Value is expected to be term_to_binary encoded map or raw binary
  defp apply_column_projection(value, columns) when is_binary(value) do
    try do
      case :erlang.binary_to_term(value, [:safe]) do
        row when is_map(row) ->
          # Project only requested columns
          projected = Map.take(row, columns)
          :erlang.term_to_binary(projected)

        _other ->
          # Not a map, return as-is
          value
      end
    rescue
      ArgumentError ->
        # Not a valid term, return as-is
        value
    end
  end

  defp apply_column_projection(value, _columns), do: value

  defp grpc_module do
    Application.get_env(:spiredb_store, :grpc_module, GRPC.Server)
  end
end
