defmodule Store.KV.ColumnFamilies do
  @moduledoc """
  Column family management for SpireDB RocksDB.

  Provides version-gated migration and CF handle access.
  """

  require Logger

  # Local schema version for the column families and internal metadata
  @current_schema_version 6

  # Column families for SpireDB
  @column_families [
    # Raw KV (GET/SET)
    "default",
    # Table row data
    "tables",
    # Secondary indexes
    "indexes",
    # Vector data + payloads
    "vectors",
    # Schema version, internal metadata
    "meta",
    # Percolator locks
    "txn_locks",
    # MVCC data
    "txn_data",
    # Commit records
    "txn_write",
    # Pending commits for crash recovery
    "txn_pending",
    # Plugin persistent state
    "plugin_state",
    # Stream entries (key: stream:id, value: encoded event)
    "streams",
    # Stream metadata (key: stream_name, value: last_id, length, etc)
    "stream_meta",
    # CDC change log entries
    "change_log"
  ]

  @doc """
  List of all column families.
  """
  def all, do: @column_families

  @doc """
  Get current schema version.
  """
  def current_version, do: @current_schema_version

  @doc """
  Open RocksDB with all column families.

  Returns {:ok, db_ref, cf_handles_map} or {:error, reason}.
  """
  def open_with_cf(path, db_opts, cf_opts \\ []) do
    path_charlist = to_charlist(path)

    # Check existing column families
    existing_cfs =
      case :rocksdb.list_column_families(path_charlist, db_opts) do
        {:ok, cfs} -> Enum.map(cfs, &to_string/1)
        # New DB
        {:error, _} -> ["default"]
      end

    Logger.info("Existing column families: #{inspect(existing_cfs)}")

    # Build CF descriptors for opening
    cf_descriptors =
      existing_cfs
      |> Enum.map(fn name -> {to_charlist(name), cf_opts} end)

    case :rocksdb.open_with_cf(path_charlist, db_opts, cf_descriptors) do
      {:ok, db_ref, cf_handles} ->
        # Build handle map
        cf_map =
          Enum.zip(existing_cfs, cf_handles)
          |> Map.new()

        # Create any missing column families
        missing = @column_families -- existing_cfs

        cf_map =
          Enum.reduce(missing, cf_map, fn cf_name, acc ->
            Logger.info("Creating missing column family: #{cf_name}")

            case :rocksdb.create_column_family(db_ref, to_charlist(cf_name), cf_opts) do
              {:ok, handle} ->
                Map.put(acc, cf_name, handle)

              {:error, reason} ->
                Logger.error("Failed to create CF #{cf_name}: #{inspect(reason)}")
                acc
            end
          end)

        # Run migrations
        run_migrations(db_ref, cf_map)

        {:ok, db_ref, cf_map}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get a column family handle from the map.
  """
  def get_cf(cf_map, name) when is_binary(name) do
    Map.get(cf_map, name)
  end

  def get_cf(cf_map, name) when is_atom(name) do
    get_cf(cf_map, Atom.to_string(name))
  end

  # Private: Migration support

  defp run_migrations(db_ref, cf_map) do
    meta_cf = get_cf(cf_map, "meta")

    stored_version =
      case :rocksdb.get(db_ref, meta_cf, "schema_version", []) do
        {:ok, bin} ->
          case Integer.parse(bin) do
            {v, _} -> v
            :error -> 1
          end

        :not_found ->
          1

        _ ->
          1
      end

    if stored_version < @current_schema_version do
      Logger.info("Running migrations from v#{stored_version} to v#{@current_schema_version}")
      do_migrations(db_ref, cf_map, stored_version, @current_schema_version)

      # Update version
      :rocksdb.put(
        db_ref,
        meta_cf,
        "schema_version",
        Integer.to_string(@current_schema_version),
        []
      )
    else
      Logger.debug("Schema up to date at v#{stored_version}")
    end
  end

  defp do_migrations(db_ref, cf_map, from, to) when from < to do
    migrate(db_ref, cf_map, from, from + 1)
    do_migrations(db_ref, cf_map, from + 1, to)
  end

  defp do_migrations(_db_ref, _cf_map, v, v), do: :ok

  # V1 → V2: Initial setup with transaction CFs
  defp migrate(_db_ref, _cf_map, 1, 2) do
    # Column families are already created in open_with_cf
    # This migration is a placeholder for any data transformations
    Logger.info("Migration v1→v2: Transaction column families initialized")
    :ok
  end

  # V2 → V3: Plugin state column family
  defp migrate(_db_ref, _cf_map, 2, 3) do
    # Column family already created in open_with_cf
    Logger.info("Migration v2→v3: Plugin state column family initialized")
    :ok
  end

  # V3 → V4: Streams column families
  defp migrate(_db_ref, _cf_map, 3, 4) do
    # Column families already created in open_with_cf
    Logger.info("Migration v3→v4: Streams column families initialized")
    :ok
  end

  # V4 → V5: CDC change log column family
  defp migrate(_db_ref, _cf_map, 4, 5) do
    # Column family already created in open_with_cf
    Logger.info("Migration v4→v5: Change log column family initialized")
    :ok
  end

  # V5 → V6: Transaction pending commits for crash recovery
  defp migrate(_db_ref, _cf_map, 5, 6) do
    # Column family already created in open_with_cf
    Logger.info("Migration v5→v6: Transaction pending commits column family initialized")
    :ok
  end

  # Default fallback
  defp migrate(_db_ref, _cf_map, from, to) do
    Logger.warning("No migration defined for v#{from}→v#{to}")
    :ok
  end
end
