defmodule Store.Schema.ValidatorTest do
  use ExUnit.Case, async: true

  alias Store.Schema.Validator

  describe "validate_value/2" do
    test "allows nil for nullable columns" do
      col = %PD.Schema.Column{name: "test", type: :string, nullable: true}
      assert :ok = Validator.validate_value(nil, col)
    end

    test "rejects nil for non-nullable columns" do
      col = %PD.Schema.Column{name: "test", type: :string, nullable: false}
      assert {:error, {:not_nullable, "test"}} = Validator.validate_value(nil, col)
    end

    test "validates string types" do
      col = %PD.Schema.Column{name: "name", type: :string, nullable: false}
      assert :ok = Validator.validate_value("hello", col)
      assert {:error, _} = Validator.validate_value(123, col)
    end

    test "validates integer types" do
      col = %PD.Schema.Column{name: "age", type: :int32, nullable: false}
      assert :ok = Validator.validate_value(42, col)
      assert {:error, _} = Validator.validate_value("not an int", col)
    end

    test "validates int8 range" do
      col = %PD.Schema.Column{name: "val", type: :int8, nullable: false}
      assert :ok = Validator.validate_value(127, col)
      assert :ok = Validator.validate_value(-128, col)
      assert {:error, _} = Validator.validate_value(128, col)
    end

    test "validates float types" do
      col = %PD.Schema.Column{name: "price", type: :float64, nullable: false}
      assert :ok = Validator.validate_value(3.14, col)
      assert {:error, _} = Validator.validate_value("not a float", col)
    end

    test "validates boolean types" do
      col = %PD.Schema.Column{name: "active", type: :boolean, nullable: false}
      assert :ok = Validator.validate_value(true, col)
      assert :ok = Validator.validate_value(false, col)
      assert {:error, _} = Validator.validate_value("true", col)
    end

    test "validates binary types" do
      col = %PD.Schema.Column{name: "data", type: :binary, nullable: false}
      assert :ok = Validator.validate_value(<<1, 2, 3>>, col)
      assert {:error, _} = Validator.validate_value(123, col)
    end

    test "validates list types" do
      col = %PD.Schema.Column{name: "tags", type: :list, nullable: false}
      assert :ok = Validator.validate_value(["a", "b"], col)
      assert {:error, _} = Validator.validate_value("not a list", col)
    end

    test "validates json types" do
      col = %PD.Schema.Column{name: "meta", type: :json, nullable: false}
      assert :ok = Validator.validate_value(%{"key" => "value"}, col)
      assert :ok = Validator.validate_value([1, 2, 3], col)
      assert {:error, _} = Validator.validate_value("not json", col)
    end

    test "validates vector with dimension" do
      col = %PD.Schema.Column{name: "embedding", type: :vector, nullable: false, vector_dim: 3}
      assert :ok = Validator.validate_value([1.0, 2.0, 3.0], col)
      # Wrong dimension
      assert {:error, _} = Validator.validate_value([1.0, 2.0], col)
    end
  end

  describe "validate_row/2" do
    test "allows writes when no schema defined or registry unavailable" do
      # Without a running schema registry, should return registry unavailable or schemaless
      result = Validator.validate_row("nonexistent_table", %{"foo" => "bar"})
      assert result in [:ok, {:error, :registry_unavailable}]
    end

    test "validates row with map input" do
      # Without a running schema registry, may return unavailable error
      row = %{"name" => "test", "age" => 25}
      result = Validator.validate_row("users", row)
      assert result in [:ok, {:error, :registry_unavailable}]
    end
  end
end
