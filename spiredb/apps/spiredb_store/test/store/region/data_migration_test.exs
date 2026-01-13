defmodule Store.Region.DataMigrationTest do
  use ExUnit.Case, async: false

  alias Store.Region.DataMigration

  setup do
    # Ensure KV engine is running
    case Store.KV.Engine.start_link(path: "/tmp/datamigration_test_#{System.unique_integer()}") do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    :ok
  end

  describe "count_keys_in_region/1" do
    test "returns error when region not found" do
      # Non-existent region
      assert {:error, _} = DataMigration.count_keys_in_region(99999)
    end
  end

  describe "split_at_key/3" do
    test "returns error for non-existent source region" do
      assert {:error, _} = DataMigration.split_at_key(99999, "split_key", 88888)
    end
  end

  describe "migrate_to_target/2" do
    test "returns error when source region not found" do
      assert {:error, _} = DataMigration.migrate_to_target(99999, 88888)
    end
  end
end
