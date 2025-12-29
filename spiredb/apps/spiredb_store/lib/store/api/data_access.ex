defmodule Store.API.DataAccess do
  @moduledoc """
  gRPC service for simple data access operations.

  Provides:
  - Scan: Range iteration over RocksDB
  - Get: Point lookup
  - BatchGet: Multi-key retrieval

  Returns Apache Arrow RecordBatches for zero-copy transfer to SpireSQL.
  """

  use GRPC.Server, service: SpireDb.Spiredb.Data.DataAccess.Service

  require Logger
  alias Store.KV.Engine
  alias Store.Arrow.Encoder
  alias Store.Server

  @default_batch_size 1000

  @doc """
  Scan a range of keys in a region.

  Streams Arrow batches back to client.
  """
  def scan(request, stream) do
    start_time = System.monotonic_time(:millisecond)

    Logger.debug("DataAccess.Scan requested",
      region_id: request.region_id,
      batch_size: request.batch_size || @default_batch_size
    )

    # Route to region-specific engine via Store.Server
    engine = get_engine_for_key(request.start_key, Map.get(request, :engine))

    batch_size = if request.batch_size == 0, do: @default_batch_size, else: request.batch_size

    opts = [
      batch_size: batch_size,
      limit: request.limit
    ]

    case Engine.scan_range(engine, request.start_key, request.end_key, opts) do
      {:ok, batches} ->
        Logger.info("Scan found #{length(batches)} batches")

        if length(batches) > 0 do
          {rows, _} = hd(batches)
          Logger.info("First batch has #{length(rows)} rows: #{inspect(rows)}")
        end

        stream_batches_to_client(stream, batches, start_time)

      {:error, reason} ->
        Logger.error("Scan failed", reason: inspect(reason))
        send_error(stream, "Scan failed: #{inspect(reason)}")
    end
  end

  @doc """
  Point lookup for a single key.
  """
  def get(request, _stream) do
    # Route to region-specific engine based on key
    engine = get_engine_for_key(request.key, Map.get(request, :engine))

    case Engine.get(engine, request.key) do
      {:ok, value} ->
        %SpireDb.Spiredb.Data.GetResponse{value: value, found: true}

      {:error, :not_found} ->
        %SpireDb.Spiredb.Data.GetResponse{value: "", found: false}

      {:error, reason} ->
        Logger.error("Get failed", key: request.key, reason: inspect(reason))
        %SpireDb.Spiredb.Data.GetResponse{value: "", found: false}
    end
  end

  @doc """
  Batch get multiple keys.

  Returns Arrow batch with all requested keys.
  """
  def batch_get(request, _stream) do
    # Group keys by region for efficient batching
    keys_by_engine = group_keys_by_engine(request.keys, Map.get(request, :engine))

    # Fetch from each engine in parallel
    results =
      keys_by_engine
      |> Task.async_stream(
        fn {engine, keys} ->
          Enum.map(keys, fn key ->
            case Engine.get(engine, key) do
              {:ok, value} -> {key, value, true}
              {:error, :not_found} -> {key, "", false}
              {:error, _} -> {key, "", false}
            end
          end)
        end,
        ordered: false
      )
      |> Enum.flat_map(fn {:ok, batch_results} -> batch_results end)

    arrow_batch = Encoder.encode_batch_get_result(results)
    %SpireDb.Spiredb.Data.BatchGetResponse{arrow_batch: arrow_batch}
  end

  # Private helpers

  defp get_engine_for_key(key, engine_override \\ nil) do
    if engine_override do
      engine_override
    else
      # Use Store.Server to route to the correct region's engine
      case Server.get_engine_for_key(key) do
        {:ok, engine} -> engine
        # Fallback to global engine
        {:error, _} -> Engine
      end
    end
  end

  defp group_keys_by_engine(keys, engine_override) do
    # Group keys by their region/engine
    keys
    |> Enum.group_by(&get_engine_for_key(&1, engine_override))
  end

  defp stream_batches_to_client(stream, batches, start_time) do
    total_batches = length(batches)

    batches
    |> Enum.with_index()
    |> Enum.each(fn {{rows, stats}, idx} ->
      has_more = idx < total_batches - 1

      arrow_batch = Encoder.encode_scan_batch(rows)

      response = %SpireDb.Spiredb.Data.ScanResponse{
        arrow_batch: arrow_batch,
        has_more: has_more,
        stats: %SpireDb.Spiredb.Data.ScanStats{
          rows_returned: stats.rows_returned,
          bytes_read: stats.bytes_read,
          scan_time_ms: stats.scan_time_ms
        }
      }

      # Real gRPC implementation
      grpc_module().send_reply(stream, response)

      Logger.debug("Streaming batch #{idx + 1}/#{total_batches}",
        rows: stats.rows_returned,
        bytes: stats.bytes_read
      )

      response
    end)
  end

  defp send_error(stream, message) do
    # Error handling for gRPC
    Logger.error("Scan error: #{message}")
    # grpc_module().send_reply(stream, ... error response ...)
    {:error, message}
  end

  defp grpc_module do
    Application.get_env(:spiredb_store, :grpc_module, GRPC.Server)
  end
end
