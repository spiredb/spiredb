defmodule PD.Schema.RegistryTest do
  @moduledoc """
  Tests for Schema Registry.
  """

  use ExUnit.Case, async: false

  alias PD.Schema.Registry
  alias PD.Schema.Column

  setup do
    start_supervised!({Registry, name: PD.Schema.Registry})
    :ok
  end

  describe "table management" do
    test "creates table" do
      columns = [
        %Column{name: "id", type: :int64, nullable: false},
        %Column{name: "name", type: :string, nullable: true}
      ]

      {:ok, table_id} = Registry.create_table("test_table", columns, ["id"])

      assert is_integer(table_id)
    end

    test "assigns unique table IDs" do
      columns = [%Column{name: "id", type: :int64}]

      {:ok, id1} = Registry.create_table("table1", columns, ["id"])
      {:ok, id2} = Registry.create_table("table2", columns, ["id"])

      assert id1 != id2
    end

    test "prevents duplicate table names" do
      columns = [%Column{name: "id", type: :int64}]

      {:ok, _} = Registry.create_table("dup_table", columns, ["id"])
      result = Registry.create_table("dup_table", columns, ["id"])

      assert result == {:error, :already_exists}
    end

    test "drops table" do
      columns = [%Column{name: "id", type: :int64}]
      {:ok, _} = Registry.create_table("drop_me", columns, ["id"])

      assert :ok = Registry.drop_table("drop_me")
      assert {:error, :not_found} = Registry.get_table("drop_me")
    end

    test "gets table by name" do
      columns = [%Column{name: "id", type: :int64}]
      {:ok, _} = Registry.create_table("get_by_name", columns, ["id"])

      {:ok, table} = Registry.get_table("get_by_name")

      assert table.name == "get_by_name"
      assert length(table.columns) == 1
    end

    test "gets table by ID" do
      columns = [%Column{name: "id", type: :int64}]
      {:ok, table_id} = Registry.create_table("get_by_id", columns, ["id"])

      {:ok, table} = Registry.get_table_by_id(table_id)

      assert table.id == table_id
    end

    test "lists all tables" do
      columns = [%Column{name: "id", type: :int64}]
      Registry.create_table("list1", columns, ["id"])
      Registry.create_table("list2", columns, ["id"])

      {:ok, tables} = Registry.list_tables()

      names = Enum.map(tables, & &1.name)
      assert "list1" in names
      assert "list2" in names
    end

    test "updates table stats" do
      columns = [%Column{name: "id", type: :int64}]
      Registry.create_table("stats_table", columns, ["id"])

      stats = %{row_count: 100, size_bytes: 1024}
      assert :ok = Registry.update_table_stats("stats_table", stats)

      {:ok, table} = Registry.get_table("stats_table")
      assert table.row_count == 100
      assert table.size_bytes == 1024
      assert table.updated_at > 0
    end

    test "gets table regions" do
      columns = [%Column{name: "id", type: :int64}]
      Registry.create_table("region_table", columns, ["id"])

      # Initially empty or based on prefix
      {:ok, regions} = Registry.get_table_regions("region_table")
      assert is_list(regions)
    end
  end

  describe "index management" do
    setup do
      columns = [
        %Column{name: "id", type: :int64},
        %Column{name: "email", type: :string}
      ]

      {:ok, _} = Registry.create_table("indexed_table", columns, ["id"])
      :ok
    end

    test "creates index" do
      {:ok, index_id} =
        Registry.create_index(
          PD.Schema.Registry,
          "email_idx",
          "indexed_table",
          :btree,
          ["email"]
        )

      assert is_integer(index_id)
    end

    test "creates vector index with params" do
      {:ok, _index_id} =
        Registry.create_index(
          PD.Schema.Registry,
          "vec_idx",
          "indexed_table",
          :anode,
          ["embedding"],
          %{dimensions: 128, shards: 4}
        )

      {:ok, index} = Registry.get_index("vec_idx")
      assert index.params.dimensions == 128
    end

    test "prevents duplicate index names" do
      Registry.create_index(PD.Schema.Registry, "dup_idx", "indexed_table", :btree, ["email"])

      result =
        Registry.create_index(PD.Schema.Registry, "dup_idx", "indexed_table", :btree, ["email"])

      assert result == {:error, :already_exists}
    end

    test "fails for non-existent table" do
      result =
        Registry.create_index(PD.Schema.Registry, "bad_idx", "nonexistent", :btree, ["col"])

      assert result == {:error, :table_not_found}
    end

    test "drops index" do
      Registry.create_index(PD.Schema.Registry, "drop_idx", "indexed_table", :btree, ["email"])

      assert :ok = Registry.drop_index("drop_idx")
      assert {:error, :not_found} = Registry.get_index("drop_idx")
    end

    test "dropping table removes associated indexes" do
      columns = [%Column{name: "id", type: :int64}]
      Registry.create_table("cascade_table", columns, ["id"])
      Registry.create_index(PD.Schema.Registry, "cascade_idx", "cascade_table", :btree, ["id"])

      :ok = Registry.drop_table("cascade_table")

      assert {:error, :not_found} = Registry.get_index("cascade_idx")
    end

    test "lists indexes for table" do
      Registry.create_index(PD.Schema.Registry, "idx1", "indexed_table", :btree, ["email"])
      Registry.create_index(PD.Schema.Registry, "idx2", "indexed_table", :btree, ["id"])

      {:ok, indexes} = Registry.list_indexes(PD.Schema.Registry, "indexed_table")

      names = Enum.map(indexes, & &1.name)
      assert "idx1" in names
      assert "idx2" in names
    end

    test "lists all indexes" do
      columns = [%Column{name: "id", type: :int64}]
      Registry.create_table("other_table", columns, ["id"])

      Registry.create_index(PD.Schema.Registry, "idx_a", "indexed_table", :btree, ["email"])
      Registry.create_index(PD.Schema.Registry, "idx_b", "other_table", :btree, ["id"])

      {:ok, all_indexes} = Registry.list_indexes()

      assert length(all_indexes) >= 2
    end
  end
end
