defmodule Store.Filter.Parser do
  @moduledoc """
  Parses JSON filter expressions for predicate pushdown.

  Wire Format:
  ```json
  {
    "op": "and",
    "args": [
      {"op": "eq", "col": "id", "val": {"int": 42}},
      {"op": "gt", "col": "age", "val": {"int": 18}}
    ]
  }
  ```

  ## Operators

  | Op | Description | Args |
  |----|-------------|------|
  | `eq` | Equal | `col`, `val` |
  | `ne` | Not equal | `col`, `val` |
  | `lt` | Less than | `col`, `val` |
  | `le` | Less or equal | `col`, `val` |
  | `gt` | Greater than | `col`, `val` |
  | `ge` | Greater or equal | `col`, `val` |
  | `and` | Logical AND | `args[]` |
  | `or` | Logical OR | `args[]` |
  | `not` | Logical NOT | `arg` |
  | `in` | In list | `col`, `vals[]` |
  | `between` | Range | `col`, `low`, `high` |
  | `like` | Pattern match | `col`, `pattern` |
  | `is_null` | Null check | `col` |

  ## Value Types

  | Type | JSON Format |
  |------|-------------|
  | Int64 | `{"int": 42}` |
  | Float64 | `{"float": 3.14}` |
  | String | `{"str": "hello"}` |
  | Bool | `{"bool": true}` |
  | Bytes | `{"bytes": "base64..."}` |
  | Null | `{"null": true}` |
  """

  require Logger

  @type filter :: map() | nil
  @type row :: map()

  @doc """
  Parse JSON filter bytes to filter AST.
  Returns nil if empty or invalid.
  """
  @spec parse(binary()) :: filter()
  def parse(<<>>), do: nil
  def parse(nil), do: nil

  def parse(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, filter} ->
        filter

      {:error, reason} ->
        Logger.warning("Failed to parse filter JSON: #{inspect(reason)}")
        nil
    end
  end

  @doc """
  Evaluate filter against a row.
  Returns true if row matches, false otherwise.
  """
  @spec matches?(filter(), row()) :: boolean()
  def matches?(nil, _row), do: true
  def matches?(%{"op" => op} = filter, row), do: eval(op, filter, row)
  def matches?(_, _row), do: true

  # Comparison operators
  defp eval("eq", %{"col" => col, "val" => val}, row) do
    get_column(row, col) == decode_value(val)
  end

  defp eval("ne", %{"col" => col, "val" => val}, row) do
    get_column(row, col) != decode_value(val)
  end

  defp eval("lt", %{"col" => col, "val" => val}, row) do
    compare(get_column(row, col), decode_value(val)) == :lt
  end

  defp eval("le", %{"col" => col, "val" => val}, row) do
    compare(get_column(row, col), decode_value(val)) in [:lt, :eq]
  end

  defp eval("gt", %{"col" => col, "val" => val}, row) do
    compare(get_column(row, col), decode_value(val)) == :gt
  end

  defp eval("ge", %{"col" => col, "val" => val}, row) do
    compare(get_column(row, col), decode_value(val)) in [:gt, :eq]
  end

  # Logical operators
  defp eval("and", %{"args" => args}, row) when is_list(args) do
    Enum.all?(args, fn arg -> matches?(arg, row) end)
  end

  defp eval("or", %{"args" => args}, row) when is_list(args) do
    Enum.any?(args, fn arg -> matches?(arg, row) end)
  end

  defp eval("not", %{"arg" => arg}, row) do
    not matches?(arg, row)
  end

  # Set operators
  defp eval("in", %{"col" => col, "vals" => vals}, row) when is_list(vals) do
    value = get_column(row, col)
    decoded_vals = Enum.map(vals, &decode_value/1)
    value in decoded_vals
  end

  defp eval("between", %{"col" => col, "low" => low, "high" => high}, row) do
    value = get_column(row, col)
    low_val = decode_value(low)
    high_val = decode_value(high)
    compare(value, low_val) in [:gt, :eq] and compare(value, high_val) in [:lt, :eq]
  end

  # Pattern matching
  defp eval("like", %{"col" => col, "pattern" => pattern}, row) do
    value = get_column(row, col)
    regex = pattern_to_regex(pattern)
    is_binary(value) and Regex.match?(regex, value)
  end

  # Null check
  defp eval("is_null", %{"col" => col}, row) do
    get_column(row, col) == nil
  end

  defp eval(op, _filter, _row) do
    Logger.warning("Unknown filter operator: #{op}")
    true
  end

  # Value decoding
  defp decode_value(%{"int" => v}), do: v
  defp decode_value(%{"float" => v}), do: v
  defp decode_value(%{"str" => v}), do: v
  defp decode_value(%{"bool" => v}), do: v
  defp decode_value(%{"bytes" => v}), do: Base.decode64!(v)
  defp decode_value(%{"null" => true}), do: nil
  defp decode_value(v) when is_integer(v), do: v
  defp decode_value(v) when is_float(v), do: v
  defp decode_value(v) when is_binary(v), do: v
  defp decode_value(v) when is_boolean(v), do: v
  defp decode_value(_), do: nil

  # Column access
  defp get_column(row, col) when is_map(row) do
    Map.get(row, col) || Map.get(row, String.to_atom(col))
  end

  defp get_column(_row, _col), do: nil

  # Safe comparison
  defp compare(nil, _), do: :lt
  defp compare(_, nil), do: :gt
  defp compare(a, b) when a < b, do: :lt
  defp compare(a, b) when a > b, do: :gt
  defp compare(_, _), do: :eq

  # LIKE pattern to regex
  defp pattern_to_regex(pattern) do
    regex_str =
      pattern
      |> String.replace("%", ".*")
      |> String.replace("_", ".")

    Regex.compile!("^#{regex_str}$", [:caseless])
  end
end
