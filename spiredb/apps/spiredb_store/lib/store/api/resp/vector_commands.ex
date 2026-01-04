defmodule Store.API.RESP.VectorCommands do
  @moduledoc """
  RESP handlers for Redis Search compatible vector commands.

  Commands:
  - FT.CREATE <idx> ON <table> SCHEMA <col> VECTOR <algorithm> <shards> DIM <dim>
  - FT.DROPINDEX <idx>
  - FT.ADD <idx> <doc_id> <vector_bytes> [PAYLOAD <json>]
  - FT.DEL <idx> <doc_id>
  - FT.SEARCH <idx> "*=>[KNN k @col $BLOB]" [RETURN_PAYLOAD]
  """

  require Logger
  alias Store.VectorIndex

  @doc """
  Route an FT.* command.
  """
  def execute(["FT.CREATE" | args]) do
    parse_and_create_index(args)
  end

  def execute(["FT.DROPINDEX", name]) do
    case VectorIndex.drop_index(name) do
      :ok -> "OK"
      {:error, :not_found} -> {:error, "ERR index '#{name}' not found"}
    end
  end

  def execute(["FT.ADD" | args]) do
    parse_and_add_vector(args)
  end

  def execute(["FT.DEL", index_name, doc_id]) do
    case VectorIndex.delete(index_name, doc_id) do
      :ok -> 1
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(["FT.SEARCH" | args]) do
    parse_and_search(args)
  end

  def execute(["FT.INFO", name]) do
    case VectorIndex.list_indexes() do
      {:ok, indexes} ->
        case Enum.find(indexes, &(&1.name == name)) do
          nil -> {:error, "ERR index '#{name}' not found"}
          info -> format_index_info(info)
        end
    end
  end

  def execute(["FT._LIST"]) do
    case VectorIndex.list_indexes() do
      {:ok, indexes} -> Enum.map(indexes, & &1.name)
    end
  end

  def execute(_) do
    {:error, "ERR unknown FT command"}
  end

  # FT.CREATE parsing
  # Format: FT.CREATE idx ON table SCHEMA col VECTOR ANODE|MANODE shards DIM dimensions

  defp parse_and_create_index(args) do
    with {:ok, name, rest} <- extract_name(args),
         {:ok, table_name, rest} <- extract_on_table(rest),
         {:ok, column_name, algorithm, shards, dimensions} <- extract_schema(rest) do
      case VectorIndex.create_index(name, table_name, column_name,
             algorithm: algorithm,
             shards: shards,
             dimensions: dimensions
           ) do
        {:ok, _id} -> "OK"
        {:error, :already_exists} -> {:error, "ERR index '#{name}' already exists"}
        {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
      end
    else
      {:error, reason} -> {:error, "ERR #{reason}"}
    end
  end

  defp extract_name([name | rest]), do: {:ok, name, rest}
  defp extract_name([]), do: {:error, "missing index name"}

  defp extract_on_table(["ON", table | rest]), do: {:ok, table, rest}
  defp extract_on_table(_), do: {:error, "expected ON <table>"}

  defp extract_schema(["SCHEMA", col, "VECTOR", alg, shards, "DIM", dim | _]) do
    algorithm =
      case String.upcase(alg) do
        "ANODE" -> :anode
        "MANODE" -> :manode
        _ -> :anode
      end

    {:ok, col, algorithm, parse_int(shards, 4), parse_int(dim, 128)}
  end

  defp extract_schema(["SCHEMA", col, "VECTOR", "DIM", dim | _]) do
    {:ok, col, :anode, 4, parse_int(dim, 128)}
  end

  defp extract_schema(_), do: {:error, "invalid SCHEMA definition"}

  # FT.ADD parsing
  # Format: FT.ADD idx doc_id <vector_bytes> [PAYLOAD <json>]

  defp parse_and_add_vector([index_name, doc_id, vector_data | rest]) do
    payload = extract_payload(rest)

    case VectorIndex.insert(index_name, doc_id, vector_data, payload) do
      {:ok, _id} -> 1
      {:error, :index_not_found} -> {:error, "ERR index not found"}
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  defp parse_and_add_vector(_), do: {:error, "ERR wrong number of arguments"}

  defp extract_payload(["PAYLOAD", json | _]), do: json
  defp extract_payload(_), do: nil

  # FT.SEARCH parsing
  # Format: FT.SEARCH idx "*=>[KNN k @col $BLOB]" BLOB <vector> [RETURN_PAYLOAD]

  defp parse_and_search([index_name, query | rest]) do
    with {:ok, k, _column} <- parse_knn_query(query),
         {:ok, vector, opts} <- extract_search_params(rest) do
      case VectorIndex.search(index_name, vector, k, opts) do
        {:ok, results} ->
          format_search_results(results, Keyword.get(opts, :return_payload, false))

        {:error, :index_not_found} ->
          {:error, "ERR index not found"}

        {:error, reason} ->
          {:error, "ERR #{inspect(reason)}"}
      end
    else
      {:error, reason} -> {:error, "ERR #{reason}"}
    end
  end

  defp parse_and_search(_), do: {:error, "ERR wrong number of arguments"}

  defp parse_knn_query(query) do
    # Parse: *=>[KNN k @col $BLOB]
    regex = ~r/\*=>\[KNN\s+(\d+)\s+@(\w+)\s+\$BLOB\]/i

    case Regex.run(regex, query) do
      [_, k_str, column] ->
        {:ok, parse_int(k_str, 10), column}

      nil ->
        # Simple format: just k
        case Integer.parse(query) do
          {k, ""} -> {:ok, k, "vector"}
          _ -> {:error, "invalid KNN query syntax"}
        end
    end
  end

  defp extract_search_params(params) do
    extract_search_params(params, nil, [])
  end

  defp extract_search_params([], nil, _opts), do: {:error, "missing vector data"}
  defp extract_search_params([], vector, opts), do: {:ok, vector, opts}

  defp extract_search_params(["BLOB", vector | rest], _vec, opts) do
    extract_search_params(rest, vector, opts)
  end

  defp extract_search_params(["RETURN_PAYLOAD" | rest], vec, opts) do
    extract_search_params(rest, vec, [{:return_payload, true} | opts])
  end

  defp extract_search_params([vector | rest], nil, opts) when is_binary(vector) do
    # Assume first binary is the vector if no BLOB keyword
    extract_search_params(rest, vector, opts)
  end

  defp extract_search_params([_ | rest], vec, opts) do
    extract_search_params(rest, vec, opts)
  end

  # Formatting

  defp format_search_results(results, return_payload) do
    # Format: [total, doc_id, [field, value, ...], doc_id, [...], ...]
    count = length(results)

    formatted =
      Enum.flat_map(results, fn {doc_id, distance, payload} ->
        fields =
          if return_payload and payload do
            ["__distance", Float.to_string(distance), "__payload", payload]
          else
            ["__distance", Float.to_string(distance)]
          end

        [doc_id, fields]
      end)

    [count | formatted]
  end

  defp format_index_info(info) do
    [
      "index_name",
      info.name,
      "table_name",
      info.table_name,
      "column_name",
      info.column_name,
      "algorithm",
      Atom.to_string(info.algorithm),
      "dimensions",
      info.dimensions
    ]
  end

  defp parse_int(str, default) when is_binary(str) do
    case Integer.parse(str) do
      {n, ""} -> n
      _ -> default
    end
  end

  defp parse_int(n, _default) when is_integer(n), do: n
end
