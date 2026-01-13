defmodule Store.Test.RocksDBHelper do
  @moduledoc """
  Helper for setting up temporary RocksDB instances for testing.
  """

  def setup_rocksdb(name) do
    # Create unique path
    path = "/tmp/spiredb_test_#{name}_#{System.unique_integer([:positive])}"
    File.rm_rf!(path)
    File.mkdir_p!(path)

    # Open RocksDB
    # Note: rocksdb.open with valid column families requires listing them if they exist
    # But since we create a new dir every time, create_if_missing: true should be enough for initial open
    {:ok, db} = :rocksdb.open(String.to_charlist(path), create_if_missing: true)

    # Create Column Families
    cfs = [
      "txn_locks",
      "txn_data",
      "txn_write",
      "meta",
      "change_log",
      "streams",
      "stream_meta",
      "plugin_state"
    ]

    cf_map =
      Enum.reduce(cfs, %{}, fn cf_name, acc ->
        case :rocksdb.create_column_family(db, String.to_charlist(cf_name), []) do
          {:ok, cf} -> Map.put(acc, cf_name, cf)
          # Should not happen in new DB
          {:error, _} -> acc
        end
      end)

    # Register in persistent_term for Executor to find
    # BUT first, save existing (app-global) ref so we can restore it
    old_db = :persistent_term.get(:spiredb_rocksdb_ref, nil)
    old_cfs = :persistent_term.get(:spiredb_rocksdb_cf_map, nil)

    :persistent_term.put(:spiredb_rocksdb_ref, db)
    :persistent_term.put(:spiredb_rocksdb_cf_map, cf_map)

    # Register cleanup on exit
    ExUnit.Callbacks.on_exit(fn ->
      # 1. Close OUR test db
      :rocksdb.close(db)

      try do
        File.rm_rf!(path)
      rescue
        _ -> :ok
      end

      # 2. Restore global app DB if it existed
      if old_db do
        :persistent_term.put(:spiredb_rocksdb_ref, old_db)
        if old_cfs, do: :persistent_term.put(:spiredb_rocksdb_cf_map, old_cfs)
      else
        # If there was nothing, clean up
        :persistent_term.erase(:spiredb_rocksdb_ref)
        :persistent_term.erase(:spiredb_rocksdb_cf_map)
      end
    end)

    {:ok, db, cf_map}
  end
end
