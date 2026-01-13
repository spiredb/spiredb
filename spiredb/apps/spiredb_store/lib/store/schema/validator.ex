defmodule Store.Schema.Validator do
  @moduledoc """
  Schema validation for table writes.

  Validates values against column definitions from PD.Schema.Registry.
  """

  require Logger

  alias PD.Schema.Registry
  alias PD.Schema.Column

  @doc """
  Validate a row against a table schema.

  Returns :ok if valid, {:error, reason} if invalid.
  """
  @spec validate_row(String.t(), map()) :: :ok | {:error, term()}
  def validate_row(table_name, row) when is_map(row) do
    case get_table_schema(table_name) do
      {:ok, table} ->
        validate_row_against_schema(row, table)

      {:error, :not_found} ->
        # No schema defined - allow write (schemaless mode)
        :ok

      {:error, _} = err ->
        err
    end
  end

  def validate_row(_table_name, _row), do: :ok

  @doc """
  Validate a single column value.
  """
  @spec validate_value(term(), Column.t()) :: :ok | {:error, term()}
  def validate_value(nil, %Column{nullable: true}), do: :ok

  def validate_value(nil, %Column{nullable: false, name: name}) do
    {:error, {:not_nullable, name}}
  end

  def validate_value(value, %Column{type: type, name: name} = col) do
    case validate_type(value, type, col) do
      true -> :ok
      false -> {:error, {:type_mismatch, name, expected: type, got: typeof(value)}}
    end
  end

  ## Private

  defp get_table_schema(table_name) do
    try do
      Registry.get_table(table_name)
    catch
      :exit, _ -> {:error, :registry_unavailable}
    end
  end

  defp validate_row_against_schema(row, table) do
    # Build column lookup
    columns_by_name = Map.new(table.columns, &{&1.name, &1})

    # Validate each provided value
    errors =
      Enum.reduce(row, [], fn {key, value}, errs ->
        key_str = to_string(key)

        case Map.get(columns_by_name, key_str) do
          nil ->
            # Unknown column - allow for flexibility
            errs

          column ->
            case validate_value(value, column) do
              :ok -> errs
              {:error, err} -> [err | errs]
            end
        end
      end)

    # Check required columns (primary key + non-nullable without defaults)
    missing =
      table.columns
      |> Enum.filter(fn col ->
        !col.nullable && is_nil(col.default) && col.name in table.primary_key
      end)
      |> Enum.reject(fn col ->
        Map.has_key?(row, col.name) || Map.has_key?(row, String.to_atom(col.name))
      end)
      |> Enum.map(&{:missing_required, &1.name})

    all_errors = errors ++ missing

    case all_errors do
      [] -> :ok
      [single] -> {:error, single}
      multiple -> {:error, {:validation_errors, multiple}}
    end
  end

  defp validate_type(value, :string, _col) when is_binary(value), do: true
  defp validate_type(value, :text, _col) when is_binary(value), do: true
  defp validate_type(value, :varchar, _col) when is_binary(value), do: true

  defp validate_type(value, :int8, _col) when is_integer(value),
    do: value >= -128 and value <= 127

  defp validate_type(value, :int16, _col) when is_integer(value),
    do: value >= -32_768 and value <= 32_767

  defp validate_type(value, :int32, _col) when is_integer(value),
    do: value >= -2_147_483_648 and value <= 2_147_483_647

  defp validate_type(value, :int64, _col) when is_integer(value), do: true
  defp validate_type(value, :uint8, _col) when is_integer(value), do: value >= 0 and value <= 255

  defp validate_type(value, :uint16, _col) when is_integer(value),
    do: value >= 0 and value <= 65_535

  defp validate_type(value, :uint32, _col) when is_integer(value),
    do: value >= 0 and value <= 4_294_967_295

  defp validate_type(value, :uint64, _col) when is_integer(value), do: value >= 0
  defp validate_type(value, :float32, _col) when is_float(value), do: true
  defp validate_type(value, :float64, _col) when is_float(value), do: true
  defp validate_type(value, :double, _col) when is_float(value), do: true
  defp validate_type(value, :boolean, _col) when is_boolean(value), do: true
  defp validate_type(value, :binary, _col) when is_binary(value), do: true
  defp validate_type(value, :bytes, _col) when is_binary(value), do: true
  defp validate_type(value, :timestamp, _col) when is_integer(value), do: true
  defp validate_type(value, :date, _col) when is_binary(value), do: true
  defp validate_type(value, :json, _col) when is_map(value) or is_list(value), do: true

  defp validate_type(value, :vector, %{vector_dim: dim}) when is_list(value),
    do: length(value) == dim

  defp validate_type(value, :list, _col) when is_list(value), do: true
  defp validate_type(_value, _type, _col), do: false

  defp typeof(v) when is_binary(v), do: :binary
  defp typeof(v) when is_integer(v), do: :integer
  defp typeof(v) when is_float(v), do: :float
  defp typeof(v) when is_boolean(v), do: :boolean
  defp typeof(v) when is_list(v), do: :list
  defp typeof(v) when is_map(v), do: :map
  defp typeof(nil), do: nil
  defp typeof(_), do: :unknown
end
