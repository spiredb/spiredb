defmodule Store.API.RESP.TableCommandsTest do
  @moduledoc """
  Tests for SPIRE.TABLE.* and SPIRE.INDEX.* RESP commands.
  """

  use ExUnit.Case, async: false

  alias Store.API.RESP.TableCommands
  alias PD.Schema.Registry
  alias Store.VectorIndex

  setup do
    # Only start if not already running (may be started by application supervisor)
    unless Process.whereis(PD.Schema.Registry) do
      start_supervised!({Registry, name: PD.Schema.Registry})
    end

    unless Process.whereis(Store.VectorIndex) do
      start_supervised!({VectorIndex, name: Store.VectorIndex})
    end

    :ok
  end

  describe "SPIRE.TABLE.CREATE" do
    test "creates simple table" do
      result =
        TableCommands.execute([
          "SPIRE.TABLE.CREATE",
          "users",
          "(id INT64, name STRING, email STRING NOT NULL) PRIMARY KEY (id)"
        ])

      assert match?(["OK", _id], result)
    end

    test "creates table with decimal type" do
      result =
        TableCommands.execute([
          "SPIRE.TABLE.CREATE",
          "products",
          "(id INT64, price DECIMAL(10,2)) PRIMARY KEY (id)"
        ])

      assert match?(["OK", _], result)
    end

    test "fails for duplicate table" do
      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "dup_table",
        "(id INT64) PRIMARY KEY (id)"
      ])

      result =
        TableCommands.execute([
          "SPIRE.TABLE.CREATE",
          "dup_table",
          "(id INT64) PRIMARY KEY (id)"
        ])

      assert match?({:error, _}, result)
    end

    test "fails for invalid syntax" do
      result =
        TableCommands.execute([
          "SPIRE.TABLE.CREATE",
          "bad_table",
          "invalid syntax"
        ])

      assert match?({:error, _}, result)
    end
  end

  describe "SPIRE.TABLE.DROP" do
    test "drops existing table" do
      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "to_drop",
        "(id INT64) PRIMARY KEY (id)"
      ])

      result = TableCommands.execute(["SPIRE.TABLE.DROP", "to_drop"])
      assert result == "OK"
    end

    test "fails for non-existent table" do
      result = TableCommands.execute(["SPIRE.TABLE.DROP", "nonexistent"])
      assert match?({:error, _}, result)
    end
  end

  describe "SPIRE.TABLE.LIST" do
    test "lists tables" do
      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "list_test1",
        "(id INT64) PRIMARY KEY (id)"
      ])

      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "list_test2",
        "(id INT64) PRIMARY KEY (id)"
      ])

      result = TableCommands.execute(["SPIRE.TABLE.LIST"])

      assert is_list(result)
      assert "list_test1" in result
      assert "list_test2" in result
    end
  end

  describe "SPIRE.TABLE.DESCRIBE" do
    test "describes table" do
      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "describe_me",
        "(id INT64, name STRING NOT NULL) PRIMARY KEY (id)"
      ])

      result = TableCommands.execute(["SPIRE.TABLE.DESCRIBE", "describe_me"])

      assert is_list(result)
      [header | _rows] = result
      assert "Column" in header
      assert "Type" in header
    end

    test "fails for unknown table" do
      result = TableCommands.execute(["SPIRE.TABLE.DESCRIBE", "unknown"])
      assert match?({:error, _}, result)
    end
  end

  describe "SPIRE.INDEX.CREATE" do
    setup do
      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "indexed_table",
        "(id INT64, email STRING) PRIMARY KEY (id)"
      ])

      :ok
    end

    test "creates btree index" do
      result =
        TableCommands.execute([
          "SPIRE.INDEX.CREATE",
          "email_idx",
          "ON indexed_table (email) USING BTREE"
        ])

      assert match?(["OK", _], result)
    end

    test "fails for unknown table" do
      result =
        TableCommands.execute([
          "SPIRE.INDEX.CREATE",
          "bad_idx",
          "ON nonexistent (col)"
        ])

      assert match?({:error, _}, result)
    end
  end

  describe "SPIRE.INDEX.DROP" do
    setup do
      TableCommands.execute([
        "SPIRE.TABLE.CREATE",
        "idx_drop_table",
        "(id INT64, col STRING) PRIMARY KEY (id)"
      ])

      TableCommands.execute([
        "SPIRE.INDEX.CREATE",
        "drop_me_idx",
        "ON idx_drop_table (col)"
      ])

      :ok
    end

    test "drops index" do
      result = TableCommands.execute(["SPIRE.INDEX.DROP", "drop_me_idx"])
      assert result == "OK"
    end

    test "fails for unknown index" do
      result = TableCommands.execute(["SPIRE.INDEX.DROP", "nonexistent"])
      assert match?({:error, _}, result)
    end
  end
end
