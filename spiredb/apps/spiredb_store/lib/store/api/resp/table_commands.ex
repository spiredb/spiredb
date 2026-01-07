defmodule Store.API.RESP.TableCommands do
  @moduledoc """
  RESP handlers for table DDL commands.

  Commands:
  - SPIRE.TABLE.CREATE <name> (<col> <type> [NOT NULL] [DEFAULT <val>], ...) PRIMARY KEY (<cols>)
  - SPIRE.TABLE.DROP <name>
  - SPIRE.TABLE.LIST
  - SPIRE.TABLE.DESCRIBE <name>
  - SPIRE.INDEX.CREATE <name> ON <table> (<cols>) [USING BTREE|ANODE|MANODE] [WITH (...)]
  - SPIRE.INDEX.DROP <name>
  """

  require Logger
  alias PD.Schema.Registry
  alias PD.Schema.Column

  @doc """
  Route a SPIRE.* command to the appropriate handler.
  """
  def execute(["SPIRE.TABLE.CREATE" | args]) do
    parse_and_create_table(args)
  end

  def execute(["SPIRE.TABLE.DROP", name]) do
    case Registry.drop_table(name) do
      :ok -> "OK"
      {:error, :not_found} -> {:error, "ERR table '#{name}' not found"}
    end
  end

  def execute(["SPIRE.TABLE.LIST"]) do
    case Registry.list_tables() do
      {:ok, tables} -> Enum.map(tables, & &1.name)
    end
  end

  def execute(["SPIRE.TABLE.DESCRIBE", name]) do
    case Registry.get_table(name) do
      {:ok, table} -> format_table_description(table)
      {:error, :not_found} -> {:error, "ERR table '#{name}' not found"}
    end
  end

  def execute(["SPIRE.INDEX.CREATE" | args]) do
    parse_and_create_index(args)
  end

  def execute(["SPIRE.INDEX.DROP", name]) do
    case Registry.drop_index(name) do
      :ok -> "OK"
      {:error, :not_found} -> {:error, "ERR index '#{name}' not found"}
    end
  end

  def execute(_) do
    {:error, "ERR unknown SPIRE command"}
  end

  # Table creation parsing

  defp parse_and_create_table(args) do
    # Expected: name (col1 type1, col2 type2, ...) PRIMARY KEY (pk1, ...)
    case args do
      [name | rest] ->
        case parse_table_definition(rest) do
          {:ok, columns, primary_key} ->
            case Registry.create_table(name, columns, primary_key) do
              {:ok, table_id} -> ["OK", table_id]
              {:error, :already_exists} -> {:error, "ERR table '#{name}' already exists"}
              {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
            end

          {:error, reason} ->
            {:error, "ERR #{reason}"}
        end

      [] ->
        {:error, "ERR wrong number of arguments for 'SPIRE.TABLE.CREATE'"}
    end
  end

  defp parse_table_definition(args) do
    # Join args and parse
    full = Enum.join(args, " ")

    # Simple regex to extract columns and primary key
    # Format: (col1 type1 [NOT NULL], ...) PRIMARY KEY (pk1, ...)
    with {:ok, cols_str, pk_str} <- extract_table_parts(full),
         {:ok, columns} <- parse_columns(cols_str),
         {:ok, primary_key} <- parse_primary_key(pk_str) do
      {:ok, columns, primary_key}
    end
  end

  defp extract_table_parts(full) do
    # Format: (col defs with DECIMAL(10,2) etc) PRIMARY KEY (pk cols)
    # Find the balanced ) before PRIMARY KEY by counting parens
    full = String.trim(full)

    case find_balanced_close(full, 0, 0) do
      {:ok, col_end_pos} ->
        # cols_part includes the outer parens
        cols_with_parens = String.slice(full, 0, col_end_pos + 1)
        rest = String.slice(full, col_end_pos + 1, String.length(full))

        # Parse PRIMARY KEY (pk) from rest
        case Regex.run(~r/^\s*PRIMARY\s+KEY\s*\(\s*(.+?)\s*\)\s*$/i, rest) do
          [_, pk] ->
            cols = cols_with_parens |> String.trim_leading("(") |> String.trim_trailing(")")
            {:ok, cols, pk}

          nil ->
            {:error, "invalid table definition syntax - missing PRIMARY KEY"}
        end

      :error ->
        {:error, "invalid table definition syntax - unbalanced parentheses"}
    end
  end

  # Find the position of the balanced closing paren for the first opening paren
  defp find_balanced_close(str, pos, depth) do
    if pos >= String.length(str) do
      :error
    else
      char = String.at(str, pos)

      cond do
        char == "(" ->
          find_balanced_close(str, pos + 1, depth + 1)

        char == ")" and depth == 1 ->
          {:ok, pos}

        char == ")" ->
          find_balanced_close(str, pos + 1, depth - 1)

        true ->
          find_balanced_close(str, pos + 1, depth)
      end
    end
  end

  defp parse_columns(cols_str) do
    # Split by comma, but only outside parentheses (for DECIMAL(10,2) etc)
    columns =
      split_columns(cols_str, [], "", 0)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(&parse_column/1)

    if Enum.all?(columns, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(columns, fn {:ok, col} -> col end)}
    else
      {:error, "invalid column definition"}
    end
  end

  # Split string by comma, but only at depth 0 (outside parentheses)
  defp split_columns("", acc, current, _depth) do
    Enum.reverse([current | acc])
  end

  defp split_columns(<<"(", rest::binary>>, acc, current, depth) do
    split_columns(rest, acc, current <> "(", depth + 1)
  end

  defp split_columns(<<")", rest::binary>>, acc, current, depth) do
    split_columns(rest, acc, current <> ")", max(0, depth - 1))
  end

  defp split_columns(<<",", rest::binary>>, acc, current, 0) do
    split_columns(rest, [current | acc], "", 0)
  end

  defp split_columns(<<char::utf8, rest::binary>>, acc, current, depth) do
    split_columns(rest, acc, current <> <<char::utf8>>, depth)
  end

  defp parse_column(col_str) do
    # Format: name type [NOT NULL] [DEFAULT value]
    # Handle types like DECIMAL(10,2) by extracting name and type first
    trimmed = String.trim(col_str)

    # Match: name type_with_possible_parens rest
    case Regex.run(~r/^(\w+)\s+([\w\(\),]+)(.*)$/, trimmed) do
      [_, name, type_str, rest] ->
        case Common.Schema.Types.from_resp(String.upcase(type_str)) do
          {:ok, type, type_opts} ->
            nullable = not String.contains?(rest, "NOT NULL")
            default = extract_default([rest])

            {:ok,
             %Column{
               name: name,
               type: type,
               nullable: nullable,
               default: default,
               precision: type_opts[:precision],
               scale: type_opts[:scale],
               vector_dim: type_opts[:vector_dim],
               list_elem: type_opts[:list_elem]
             }}

          {:error, _} ->
            {:error, "unknown type: #{type_str}"}
        end

      _ ->
        {:error, "invalid column: #{col_str}"}
    end
  end

  defp extract_default(parts) do
    full = Enum.join(parts, " ")

    case Regex.run(~r/DEFAULT\s+(.+)/i, full) do
      [_, val] -> String.trim(val) |> String.trim("'") |> String.trim("\"")
      nil -> nil
    end
  end

  defp parse_primary_key(pk_str) do
    pk =
      pk_str
      |> String.split(",")
      |> Enum.map(&String.trim/1)

    if Enum.all?(pk, &(String.length(&1) > 0)) do
      {:ok, pk}
    else
      {:error, "invalid primary key"}
    end
  end

  # Index creation parsing

  defp parse_and_create_index(args) do
    # Expected: name ON table (cols) [USING type] [WITH (params)]
    case parse_index_definition(args) do
      {:ok, name, table, columns, type, params} ->
        case Registry.create_index(PD.Schema.Registry, name, table, type, columns, params) do
          {:ok, index_id} -> ["OK", index_id]
          {:error, :table_not_found} -> {:error, "ERR table not found"}
          {:error, :already_exists} -> {:error, "ERR index '#{name}' already exists"}
          {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
        end

      {:error, reason} ->
        {:error, "ERR #{reason}"}
    end
  end

  defp parse_index_definition(args) do
    full = Enum.join(args, " ")

    # Match: name ON table (cols) [USING type]
    regex = ~r/^(\w+)\s+ON\s+(\w+)\s*\(\s*(.+?)\s*\)(?:\s+USING\s+(\w+))?/i

    case Regex.run(regex, full) do
      [_, name, table, cols_str, type_str] ->
        columns = cols_str |> String.split(",") |> Enum.map(&String.trim/1)
        type = parse_index_type(type_str)
        {:ok, name, table, columns, type, %{}}

      [_, name, table, cols_str] ->
        columns = cols_str |> String.split(",") |> Enum.map(&String.trim/1)
        {:ok, name, table, columns, :btree, %{}}

      nil ->
        {:error, "invalid index definition syntax"}
    end
  end

  defp parse_index_type(nil), do: :btree

  defp parse_index_type(str) do
    case String.upcase(str) do
      "BTREE" -> :btree
      "ANODE" -> :anode
      "MANODE" -> :manode
      _ -> :btree
    end
  end

  # Formatting

  defp format_table_description(table) do
    header = ["Column", "Type", "Nullable", "Default", "Extra"]

    rows =
      Enum.map(table.columns, fn col ->
        extra =
          cond do
            col.vector_dim -> "dim=#{col.vector_dim}"
            col.precision -> "precision=#{col.precision},scale=#{col.scale}"
            true -> ""
          end

        [
          col.name,
          Common.Schema.Types.to_resp(col.type,
            precision: col.precision,
            scale: col.scale,
            vector_dim: col.vector_dim
          ),
          if(col.nullable, do: "YES", else: "NO"),
          col.default || "",
          extra
        ]
      end)

    pk_row = ["PRIMARY KEY", Enum.join(table.primary_key, ", "), "", "", ""]

    [header | rows] ++ [pk_row]
  end
end
