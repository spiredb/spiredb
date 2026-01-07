defmodule Common.Schema.Types do
  @moduledoc """
  SpireDB type system for Arrow/DataFusion compatibility.

  Maps SpireDB column types to:
  - Arrow data types
  - RESP syntax
  - Binary encoding
  """

  @type column_type ::
          :int8
          | :int16
          | :int32
          | :int64
          | :uint8
          | :uint16
          | :uint32
          | :uint64
          | :float32
          | :float64
          | :bool
          | :string
          | :bytes
          | :date
          | :timestamp
          | :decimal
          | :list
          | :vector

  @type column_def :: %{
          name: String.t(),
          type: column_type(),
          nullable: boolean(),
          default: term() | nil,
          # For DECIMAL
          precision: non_neg_integer() | nil,
          # For DECIMAL
          scale: non_neg_integer() | nil,
          # For VECTOR
          vector_dim: non_neg_integer() | nil,
          # For LIST element type
          list_elem: column_type() | nil
        }

  # Proto enum to atom mapping
  @proto_to_atom %{
    :TYPE_INT8 => :int8,
    :TYPE_INT16 => :int16,
    :TYPE_INT32 => :int32,
    :TYPE_INT64 => :int64,
    :TYPE_UINT8 => :uint8,
    :TYPE_UINT16 => :uint16,
    :TYPE_UINT32 => :uint32,
    :TYPE_UINT64 => :uint64,
    :TYPE_FLOAT32 => :float32,
    :TYPE_FLOAT64 => :float64,
    :TYPE_BOOL => :bool,
    :TYPE_STRING => :string,
    :TYPE_BYTES => :bytes,
    :TYPE_DATE => :date,
    :TYPE_TIMESTAMP => :timestamp,
    :TYPE_DECIMAL => :decimal,
    :TYPE_LIST => :list,
    :TYPE_VECTOR => :vector
  }

  @atom_to_proto Map.new(@proto_to_atom, fn {k, v} -> {v, k} end)

  # Arrow type mapping
  @arrow_types %{
    int8: "Int8",
    int16: "Int16",
    int32: "Int32",
    int64: "Int64",
    uint8: "UInt8",
    uint16: "UInt16",
    uint32: "UInt32",
    uint64: "UInt64",
    float32: "Float32",
    float64: "Float64",
    bool: "Boolean",
    string: "Utf8",
    bytes: "Binary",
    date: "Date32",
    timestamp: "Timestamp(Microsecond, Some(\"UTC\"))",
    decimal: "Decimal128(38, 10)",
    list: "List",
    vector: "FixedSizeList(Float32)"
  }

  # RESP syntax
  @resp_syntax %{
    int8: "INT8",
    int16: "INT16",
    int32: "INT32",
    int64: "INT64",
    uint8: "UINT8",
    uint16: "UINT16",
    uint32: "UINT32",
    uint64: "UINT64",
    float32: "FLOAT32",
    float64: "FLOAT64",
    bool: "BOOL",
    string: "STRING",
    bytes: "BYTES",
    date: "DATE",
    timestamp: "TIMESTAMP",
    decimal: "DECIMAL",
    list: "LIST",
    vector: "VECTOR"
  }

  @doc """
  Convert proto ColumnType enum to internal atom.
  """
  def from_proto(proto_type) when is_atom(proto_type) do
    Map.get(@proto_to_atom, proto_type, :string)
  end

  def from_proto(proto_int) when is_integer(proto_int) do
    # Proto enums come as integers sometimes
    proto_type = Spiredb.Cluster.ColumnType.key(proto_int)
    from_proto(proto_type)
  end

  @doc """
  Convert internal atom to proto ColumnType enum.
  """
  def to_proto(type) when is_atom(type) do
    Map.get(@atom_to_proto, type, :TYPE_STRING)
  end

  @doc """
  Get Arrow data type string for a column type.
  """
  def to_arrow(type, opts \\ []) do
    case type do
      :decimal ->
        p = Keyword.get(opts, :precision, 38)
        s = Keyword.get(opts, :scale, 10)
        "Decimal128(#{p}, #{s})"

      :vector ->
        dim = Keyword.get(opts, :vector_dim, 128)
        "FixedSizeList(Float32, #{dim})"

      :list ->
        elem = Keyword.get(opts, :list_elem, :int64)
        "List(#{to_arrow(elem)})"

      _ ->
        Map.get(@arrow_types, type, "Utf8")
    end
  end

  @doc """
  Parse RESP type syntax string to internal type.
  """
  def from_resp(type_str) do
    type_str = String.upcase(type_str)

    cond do
      String.starts_with?(type_str, "DECIMAL") ->
        # DECIMAL(18,4) -> {:decimal, 18, 4}
        case Regex.run(~r/DECIMAL\((\d+),\s*(\d+)\)/, type_str) do
          [_, p, s] ->
            {:ok, :decimal, %{precision: String.to_integer(p), scale: String.to_integer(s)}}

          _ ->
            {:ok, :decimal, %{precision: 38, scale: 10}}
        end

      String.starts_with?(type_str, "VECTOR") ->
        # VECTOR(128)
        case Regex.run(~r/VECTOR\((\d+)\)/, type_str) do
          [_, dim] -> {:ok, :vector, %{vector_dim: String.to_integer(dim)}}
          _ -> {:error, :missing_dimension}
        end

      String.starts_with?(type_str, "LIST") ->
        # LIST(INT64)
        case Regex.run(~r/LIST\((\w+)\)/, type_str) do
          [_, elem] ->
            case from_resp(elem) do
              {:ok, elem_type, _} -> {:ok, :list, %{list_elem: elem_type}}
              _ -> {:error, :invalid_element_type}
            end

          _ ->
            {:error, :missing_element_type}
        end

      true ->
        # Simple types
        type =
          @resp_syntax
          |> Enum.find(fn {_k, v} -> v == type_str end)
          |> case do
            {k, _} -> k
            nil -> nil
          end

        if type, do: {:ok, type, %{}}, else: {:error, :unknown_type}
    end
  end

  @doc """
  Get RESP syntax for a type.
  """
  def to_resp(type, opts \\ []) do
    case type do
      :decimal ->
        p = Keyword.get(opts, :precision, 18)
        s = Keyword.get(opts, :scale, 4)
        "DECIMAL(#{p},#{s})"

      :vector ->
        dim = Keyword.get(opts, :vector_dim, 128)
        "VECTOR(#{dim})"

      :list ->
        elem = Keyword.get(opts, :list_elem, :int64)
        "LIST(#{to_resp(elem)})"

      _ ->
        Map.get(@resp_syntax, type, "STRING")
    end
  end

  @doc """
  Get byte size for fixed-size types.
  Returns nil for variable-size types.
  """
  def byte_size(type) do
    case type do
      :int8 -> 1
      :uint8 -> 1
      :int16 -> 2
      :uint16 -> 2
      :int32 -> 4
      :uint32 -> 4
      :int64 -> 8
      :uint64 -> 8
      :float32 -> 4
      :float64 -> 8
      :bool -> 1
      :date -> 4
      :timestamp -> 8
      _ -> nil
    end
  end

  @doc """
  Check if type is numeric.
  """
  def numeric?(type) do
    type in [
      :int8,
      :int16,
      :int32,
      :int64,
      :uint8,
      :uint16,
      :uint32,
      :uint64,
      :float32,
      :float64,
      :decimal
    ]
  end

  @doc """
  Check if type is signed integer.
  """
  def signed_integer?(type) do
    type in [:int8, :int16, :int32, :int64]
  end

  @doc """
  Check if type is unsigned integer.
  """
  def unsigned_integer?(type) do
    type in [:uint8, :uint16, :uint32, :uint64]
  end

  @doc """
  Check if type is floating point.
  """
  def float?(type) do
    type in [:float32, :float64]
  end
end
