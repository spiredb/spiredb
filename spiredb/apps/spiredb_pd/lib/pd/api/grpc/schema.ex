defmodule PD.API.GRPC.Schema do
  @moduledoc """
  gRPC SchemaService implementation.
  """

  use GRPC.Server, service: Spiredb.Cluster.SchemaService.Service

  require Logger
  alias PD.Schema.Registry

  alias Spiredb.Cluster.{
    CreateTableResponse,
    CreateIndexResponse,
    TableSchema,
    TableList,
    IndexSchema,
    IndexList,
    ColumnDef,
    Empty,
    TableStats
  }

  # Table operations

  def create_table(request, _stream) do
    columns = Enum.map(request.columns, &proto_to_column/1)

    case Registry.create_table(request.name, columns, request.primary_key) do
      {:ok, table_id} ->
        %CreateTableResponse{table_id: table_id}

      {:error, :already_exists} ->
        raise GRPC.RPCError, status: :already_exists, message: "Table already exists"

      {:error, reason} ->
        Logger.error("CreateTable failed: #{inspect(reason)}")
        raise GRPC.RPCError, status: :internal, message: "Failed to create table"
    end
  end

  def drop_table(request, _stream) do
    case Registry.drop_table(request.name) do
      :ok ->
        %Empty{}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Table not found"
    end
  end

  def get_table(request, _stream) do
    result =
      case request.identifier do
        {:id, id} -> Registry.get_table_by_id(id)
        {:name, name} -> Registry.get_table(name)
        _ -> {:error, :invalid_request}
      end

    case result do
      {:ok, table} ->
        table_to_proto(table)

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Table not found"
    end
  end

  def list_tables(_request, _stream) do
    {:ok, tables} = Registry.list_tables()
    %TableList{tables: Enum.map(tables, &table_to_proto/1)}
  end

  # Index operations

  def create_index(request, _stream) do
    type = proto_to_index_type(request.type)
    params = Map.new(request.params)

    case Registry.create_index(request.name, request.table_name, type, request.columns, params) do
      {:ok, index_id} ->
        %CreateIndexResponse{index_id: index_id}

      {:error, :table_not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Table not found"

      {:error, :already_exists} ->
        raise GRPC.RPCError, status: :already_exists, message: "Index already exists"
    end
  end

  def drop_index(request, _stream) do
    case Registry.drop_index(request.name) do
      :ok ->
        %Empty{}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Index not found"
    end
  end

  def get_index(request, _stream) do
    case Registry.get_index(request.name) do
      {:ok, index} ->
        index_to_proto(index)

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Index not found"
    end
  end

  def get_table_stats(request, _stream) do
    case Registry.get_table(request.table_name) do
      {:ok, table} ->
        %TableStats{
          row_count: table.row_count,
          size_bytes: table.size_bytes,
          last_updated: table.updated_at,
          # Future: retrieve column stats
          column_stats: %{}
        }

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Table not found"
    end
  end

  def update_table_stats(request, _stream) do
    stats = %{
      row_count: request.row_count,
      size_bytes: request.size_bytes
    }

    case Registry.update_table_stats(request.table_name, stats) do
      :ok ->
        %Empty{}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Table not found"
    end
  end

  def list_indexes(request, _stream) do
    table_name = if request.table_name == "", do: nil, else: request.table_name

    case Registry.list_indexes(table_name) do
      {:ok, indexes} ->
        %IndexList{indexes: Enum.map(indexes, &index_to_proto/1)}

      {:error, :table_not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Table not found"
    end
  end

  # Conversion helpers

  defp proto_to_column(%ColumnDef{} = col) do
    %PD.Schema.Column{
      name: col.name,
      type: Common.Schema.Types.from_proto(col.type),
      nullable: col.nullable,
      default: if(col.default_value == <<>>, do: nil, else: col.default_value),
      precision: if(col.precision == 0, do: nil, else: col.precision),
      scale: if(col.scale == 0, do: nil, else: col.scale),
      vector_dim: if(col.vector_dim == 0, do: nil, else: col.vector_dim),
      list_elem:
        if(col.list_elem == :TYPE_INT8,
          do: nil,
          else: Common.Schema.Types.from_proto(col.list_elem)
        )
    }
  end

  defp table_to_proto(table) do
    %TableSchema{
      id: table.id,
      name: table.name,
      columns: Enum.map(table.columns, &column_to_proto/1),
      primary_key: table.primary_key,
      region_prefix: table.region_prefix || <<>>,
      created_at: table.created_at || 0
    }
  end

  defp column_to_proto(col) do
    %ColumnDef{
      name: col.name,
      type: Common.Schema.Types.to_proto(col.type),
      nullable: col.nullable,
      default_value: col.default || <<>>,
      precision: col.precision || 0,
      scale: col.scale || 0,
      vector_dim: col.vector_dim || 0,
      list_elem: Common.Schema.Types.to_proto(col.list_elem || :int8)
    }
  end

  defp proto_to_index_type(:INDEX_BTREE), do: :btree
  defp proto_to_index_type(:INDEX_ANODE), do: :anode
  defp proto_to_index_type(:INDEX_MANODE), do: :manode
  defp proto_to_index_type(_), do: :btree

  defp index_type_to_proto(:btree), do: :INDEX_BTREE
  defp index_type_to_proto(:anode), do: :INDEX_ANODE
  defp index_type_to_proto(:manode), do: :INDEX_MANODE

  defp index_to_proto(index) do
    %IndexSchema{
      id: index.id,
      name: index.name,
      table_id: index.table_id,
      type: index_type_to_proto(index.type),
      columns: index.columns,
      params: index.params
    }
  end
end
