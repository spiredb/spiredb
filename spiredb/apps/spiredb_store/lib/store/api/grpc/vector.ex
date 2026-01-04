defmodule Store.API.GRPC.Vector do
  @moduledoc """
  gRPC VectorService implementation.

  Provides vector search operations using Anodex backend.
  """

  use GRPC.Server, service: Spiredb.Data.VectorService.Service

  require Logger
  alias Store.VectorIndex

  alias Spiredb.Data.{
    VectorInsertResponse,
    VectorSearchResponse,
    BatchVectorSearchResponse,
    VectorResult,
    Empty
  }

  @doc """
  Create a vector index.
  """
  def create_index(request, _stream) do
    Logger.debug("CreateIndex: #{request.name} on #{request.table_name}.#{request.column_name}")

    algorithm =
      case String.upcase(request.algorithm) do
        "ANODE" -> :anode
        "MANODE" -> :manode
        _ -> :anode
      end

    shards = Map.get(request.params, "shards", "4") |> parse_int(4)
    dimensions = Map.get(request.params, "dimensions", "128") |> parse_int(128)

    case VectorIndex.create_index(request.name, request.table_name, request.column_name,
           algorithm: algorithm,
           shards: shards,
           dimensions: dimensions
         ) do
      {:ok, _id} ->
        %Empty{}

      {:error, :already_exists} ->
        raise GRPC.RPCError, status: :already_exists, message: "Index already exists"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: inspect(reason)
    end
  end

  @doc """
  Drop a vector index.
  """
  def drop_index(request, _stream) do
    Logger.debug("DropIndex: #{request.name}")

    case VectorIndex.drop_index(request.name) do
      :ok ->
        %Empty{}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Index not found"
    end
  end

  @doc """
  Insert a vector with optional payload.
  """
  def insert(request, _stream) do
    Logger.debug("Insert: #{request.index_name}, doc=#{inspect(request.doc_id)}")

    payload = if request.payload == <<>>, do: nil, else: request.payload

    case VectorIndex.insert(request.index_name, request.doc_id, request.vector, payload) do
      {:ok, internal_id} ->
        %VectorInsertResponse{internal_id: internal_id}

      {:error, :index_not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Index not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: inspect(reason)
    end
  end

  @doc """
  Delete a vector.
  """
  def delete(request, _stream) do
    Logger.debug("Delete: #{request.index_name}, doc=#{inspect(request.doc_id)}")

    case VectorIndex.delete(request.index_name, request.doc_id) do
      :ok ->
        %Empty{}

      {:error, :index_not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Index not found"
    end
  end

  @doc """
  Search for nearest neighbors.
  """
  def search(request, _stream) do
    Logger.debug("Search: #{request.index_name}, k=#{request.k}")

    opts = [return_payload: request.return_payload]

    case VectorIndex.search(request.index_name, request.query_vector, request.k, opts) do
      {:ok, results} ->
        %VectorSearchResponse{
          results:
            Enum.map(results, fn {doc_id, distance, payload} ->
              %VectorResult{
                id: ensure_binary(doc_id),
                distance: distance,
                payload: payload || <<>>
              }
            end)
        }

      {:error, :index_not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Index not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: inspect(reason)
    end
  end

  @doc """
  Batch search multiple queries.
  """
  def batch_search(request, _stream) do
    Logger.debug("BatchSearch: #{request.index_name}, #{length(request.query_vectors)} queries")

    opts = [return_payload: request.return_payload]

    results =
      request.query_vectors
      |> Enum.map(fn query_vector ->
        case VectorIndex.search(request.index_name, query_vector, request.k, opts) do
          {:ok, results} ->
            %VectorSearchResponse{
              results:
                Enum.map(results, fn {doc_id, distance, payload} ->
                  %VectorResult{
                    id: ensure_binary(doc_id),
                    distance: distance,
                    payload: payload || <<>>
                  }
                end)
            }

          {:error, _} ->
            %VectorSearchResponse{results: []}
        end
      end)

    %BatchVectorSearchResponse{results: results}
  end

  # Private helpers

  defp parse_int(str, default) when is_binary(str) do
    case Integer.parse(str) do
      {n, ""} -> n
      _ -> default
    end
  end

  defp parse_int(n, _) when is_integer(n), do: n

  defp ensure_binary(v) when is_binary(v), do: v
  defp ensure_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp ensure_binary(v), do: inspect(v)
end
