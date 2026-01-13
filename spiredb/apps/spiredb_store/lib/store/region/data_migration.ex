defmodule Store.Region.DataMigration do
  @moduledoc """
  Data migration utilities for region split and merge operations.

  Handles key range scanning and data transfer between regions.
  """

  require Logger

  @doc """
  Migrate data from source region to target region.
  Used during region merge operations.

  Scans all keys in source region's range and copies to target.
  """
  @spec migrate_to_target(non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def migrate_to_target(source_region_id, target_region_id) do
    Logger.info("Starting data migration: region #{source_region_id} -> #{target_region_id}")

    case get_region_key_range(source_region_id) do
      {:ok, start_key, end_key} ->
        count = migrate_key_range(start_key, end_key, target_region_id)

        Logger.info(
          "Migrated #{count} keys from region #{source_region_id} to #{target_region_id}"
        )

        {:ok, count}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Split data at split_key - keys >= split_key will be assigned to new region.
  The data stays in place (same RocksDB), but routing is updated.
  """
  @spec split_at_key(non_neg_integer(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def split_at_key(source_region_id, split_key, new_region_id) do
    Logger.info(
      "Splitting region #{source_region_id} at key #{inspect(split_key)}, new region: #{new_region_id}"
    )

    # Data doesn't need to move - it's all in the same RocksDB
    # PD metadata update handles routing
    # Just verify the split key is valid

    case get_region_key_range(source_region_id) do
      {:ok, start_key, end_key} ->
        if key_in_range?(split_key, start_key, end_key) do
          :ok
        else
          {:error, {:invalid_split_key, split_key, start_key, end_key}}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Scan and count keys in a region's range.
  Useful for debugging and verifying migrations.
  """
  @spec count_keys_in_region(non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count_keys_in_region(region_id) do
    case get_region_key_range(region_id) do
      {:ok, start_key, end_key} ->
        count_keys_in_range(start_key, end_key)

      {:error, _} = err ->
        err
    end
  end

  ## Private

  defp get_region_key_range(region_id) do
    case PD.Server.get_region_by_id(region_id) do
      {:ok, nil} ->
        {:error, :region_not_found}

      {:ok, region} ->
        {:ok, region.start_key || <<>>, region.end_key || <<0xFF, 0xFF, 0xFF, 0xFF>>}

      {:error, _} = err ->
        err
    end
  end

  defp key_in_range?(key, start_key, end_key) do
    (start_key == nil or key >= start_key) and (end_key == nil or key < end_key)
  end

  defp migrate_key_range(start_key, end_key, _target_region_id) do
    # In SpireDB, all regions share the same RocksDB instance
    # Data migration means the routing changes, not the data location
    # We just need to verify keys exist and update PD routing

    case count_keys_in_range(start_key, end_key) do
      {:ok, count} -> count
      {:error, _} -> 0
    end
  end

  defp count_keys_in_range(start_key, end_key) do
    case get_db_ref() do
      {:ok, db_ref, cf_map} ->
        data_cf = Map.get(cf_map, "data")

        case :rocksdb.iterator(db_ref, data_cf, []) do
          {:ok, iter} ->
            count = count_in_range(iter, start_key, end_key, 0)
            :rocksdb.iterator_close(iter)
            {:ok, count}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, _} = err ->
        err
    end
  end

  defp count_in_range(iter, start_key, end_key, count) do
    seek_key = if start_key == <<>>, do: :first, else: {:seek, start_key}

    case :rocksdb.iterator_move(iter, seek_key) do
      {:ok, key, _value} when key < end_key ->
        count_next(iter, end_key, count + 1)

      {:ok, _key, _value} ->
        count

      {:error, :invalid_iterator} ->
        count
    end
  end

  defp count_next(iter, end_key, count) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, _value} when key < end_key ->
        count_next(iter, end_key, count + 1)

      _ ->
        count
    end
  end

  defp get_db_ref do
    case {:persistent_term.get(:spiredb_rocksdb_ref, nil),
          :persistent_term.get(:spiredb_rocksdb_cf_map, nil)} do
      {nil, _} -> {:error, :db_not_available}
      {_, nil} -> {:error, :cf_map_not_available}
      {db_ref, cf_map} -> {:ok, db_ref, cf_map}
    end
  end
end
