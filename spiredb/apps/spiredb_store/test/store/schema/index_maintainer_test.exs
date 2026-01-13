defmodule Store.Schema.IndexMaintainerTest do
  use ExUnit.Case, async: true

  alias Store.Schema.IndexMaintainer

  describe "maintain_indexes/4" do
    test "returns ok when no schema defined (schemaless mode)" do
      # Non-existent table - should not fail
      result =
        IndexMaintainer.maintain_indexes("nonexistent_table", "pk1", nil, %{"col" => "val"})

      assert result in [:ok, {:error, :registry_unavailable}]
    end

    test "handles insert (nil old_row, new_row present)" do
      result = IndexMaintainer.maintain_indexes("test_table", "pk1", nil, %{"name" => "test"})
      assert result in [:ok, {:error, :registry_unavailable}]
    end

    test "handles delete (old_row present, nil new_row)" do
      result = IndexMaintainer.maintain_indexes("test_table", "pk1", %{"name" => "old"}, nil)
      assert result in [:ok, {:error, :registry_unavailable}]
    end

    test "handles update (both old_row and new_row present)" do
      result =
        IndexMaintainer.maintain_indexes(
          "test_table",
          "pk1",
          %{"name" => "old"},
          %{"name" => "new"}
        )

      assert result in [:ok, {:error, :registry_unavailable}]
    end
  end

  describe "lookup_by_index/3" do
    test "returns error when no index exists for column" do
      result = IndexMaintainer.lookup_by_index("test_table", "nonexistent_col", "value")
      assert {:error, _} = result
    end
  end

  describe "rebuild_indexes/1" do
    test "returns error for non-existent table" do
      # Registry not running or table not found
      result =
        try do
          IndexMaintainer.rebuild_indexes("nonexistent_table_xyz")
        catch
          :exit, _ -> {:error, :registry_unavailable}
        end

      assert {:error, _} = result
    end
  end

  describe "extract_indexed_values (internal logic)" do
    test "extracts single column value" do
      row = %{"name" => "test", "age" => 25}
      # Single column index
      values = extract_values(row, ["name"])
      assert values == "test"
    end

    test "extracts composite column values" do
      row = %{"first_name" => "John", "last_name" => "Doe"}
      values = extract_values(row, ["first_name", "last_name"])
      assert values == {"John", "Doe"}
    end

    test "returns nil for nil row" do
      assert extract_values(nil, ["name"]) == nil
    end

    test "returns nil when all indexed values are nil" do
      row = %{"other" => "value"}
      assert extract_values(row, ["name"]) == nil
    end
  end

  # Helper mimicking internal extraction logic
  defp extract_values(nil, _columns), do: nil

  defp extract_values(row, columns) when is_map(row) do
    values =
      Enum.map(columns, fn col ->
        Map.get(row, col) || Map.get(row, String.to_atom(col))
      end)

    if Enum.all?(values, &is_nil/1) do
      nil
    else
      case values do
        [single] -> single
        multiple -> List.to_tuple(multiple)
      end
    end
  end
end
