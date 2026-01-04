defmodule Common.Schema.TypesTest do
  @moduledoc """
  Tests for Arrow/DataFusion compatible type system.
  """

  use ExUnit.Case, async: true

  alias Common.Schema.Types

  describe "proto conversion" do
    test "converts proto enum to atom" do
      assert Types.from_proto(:TYPE_INT32) == :int32
      assert Types.from_proto(:TYPE_STRING) == :string
      assert Types.from_proto(:TYPE_VECTOR) == :vector
    end

    test "converts atom to proto enum" do
      assert Types.to_proto(:int32) == :TYPE_INT32
      assert Types.to_proto(:string) == :TYPE_STRING
      assert Types.to_proto(:vector) == :TYPE_VECTOR
    end

    test "round-trips all types" do
      types = [
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
        :bool,
        :string,
        :bytes,
        :date,
        :timestamp,
        :decimal,
        :list,
        :vector
      ]

      for type <- types do
        proto = Types.to_proto(type)
        back = Types.from_proto(proto)
        assert back == type, "Failed round-trip for #{type}"
      end
    end
  end

  describe "Arrow type mapping" do
    test "maps simple types" do
      assert Types.to_arrow(:int32) == "Int32"
      assert Types.to_arrow(:string) == "Utf8"
      assert Types.to_arrow(:bool) == "Boolean"
      assert Types.to_arrow(:bytes) == "Binary"
    end

    test "maps decimal with precision/scale" do
      assert Types.to_arrow(:decimal, precision: 18, scale: 4) == "Decimal128(18, 4)"
      # Default
      assert Types.to_arrow(:decimal) == "Decimal128(38, 10)"
    end

    test "maps vector with dimension" do
      assert Types.to_arrow(:vector, vector_dim: 128) == "FixedSizeList(Float32, 128)"
      # Default
      assert Types.to_arrow(:vector) == "FixedSizeList(Float32, 128)"
    end

    test "maps list with element type" do
      assert Types.to_arrow(:list, list_elem: :int64) == "List(Int64)"
    end
  end

  describe "RESP syntax parsing" do
    test "parses simple types" do
      assert {:ok, :int32, %{}} = Types.from_resp("INT32")
      assert {:ok, :string, %{}} = Types.from_resp("STRING")
      assert {:ok, :bool, %{}} = Types.from_resp("BOOL")
    end

    test "parses case-insensitively" do
      assert {:ok, :int64, %{}} = Types.from_resp("int64")
      assert {:ok, :float32, %{}} = Types.from_resp("Float32")
    end

    test "parses DECIMAL with precision and scale" do
      {:ok, :decimal, opts} = Types.from_resp("DECIMAL(18,4)")
      assert opts.precision == 18
      assert opts.scale == 4
    end

    test "parses VECTOR with dimension" do
      {:ok, :vector, opts} = Types.from_resp("VECTOR(256)")
      assert opts.vector_dim == 256
    end

    test "returns error for VECTOR without dimension" do
      assert {:error, :missing_dimension} = Types.from_resp("VECTOR")
    end

    test "parses LIST with element type" do
      {:ok, :list, opts} = Types.from_resp("LIST(INT64)")
      assert opts.list_elem == :int64
    end

    test "returns error for unknown type" do
      assert {:error, :unknown_type} = Types.from_resp("UNKNOWN_TYPE")
    end
  end

  describe "RESP syntax generation" do
    test "generates simple types" do
      assert Types.to_resp(:int32) == "INT32"
      assert Types.to_resp(:string) == "STRING"
    end

    test "generates DECIMAL with precision/scale" do
      assert Types.to_resp(:decimal, precision: 10, scale: 2) == "DECIMAL(10,2)"
    end

    test "generates VECTOR with dimension" do
      assert Types.to_resp(:vector, vector_dim: 512) == "VECTOR(512)"
    end

    test "generates LIST with element type" do
      assert Types.to_resp(:list, list_elem: :float64) == "LIST(FLOAT64)"
    end
  end

  describe "type properties" do
    test "byte_size returns correct sizes" do
      assert Types.byte_size(:int8) == 1
      assert Types.byte_size(:int16) == 2
      assert Types.byte_size(:int32) == 4
      assert Types.byte_size(:int64) == 8
      assert Types.byte_size(:float32) == 4
      assert Types.byte_size(:float64) == 8
      assert Types.byte_size(:bool) == 1
    end

    test "byte_size returns nil for variable types" do
      assert Types.byte_size(:string) == nil
      assert Types.byte_size(:bytes) == nil
      assert Types.byte_size(:list) == nil
      assert Types.byte_size(:vector) == nil
    end

    test "numeric? recognizes numeric types" do
      assert Types.numeric?(:int32)
      assert Types.numeric?(:float64)
      assert Types.numeric?(:decimal)
      refute Types.numeric?(:string)
      refute Types.numeric?(:bool)
    end

    test "signed_integer? recognizes signed integers" do
      assert Types.signed_integer?(:int8)
      assert Types.signed_integer?(:int64)
      refute Types.signed_integer?(:uint32)
      refute Types.signed_integer?(:float32)
    end

    test "unsigned_integer? recognizes unsigned integers" do
      assert Types.unsigned_integer?(:uint8)
      assert Types.unsigned_integer?(:uint64)
      refute Types.unsigned_integer?(:int32)
    end

    test "float? recognizes floating point types" do
      assert Types.float?(:float32)
      assert Types.float?(:float64)
      refute Types.float?(:int32)
      refute Types.float?(:decimal)
    end
  end
end
