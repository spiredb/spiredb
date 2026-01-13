defmodule PD.Schema.Table do
  @moduledoc """
  Table schema definition.
  """

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          name: String.t(),
          columns: [PD.Schema.Column.t()],
          primary_key: [String.t()],
          region_prefix: String.t() | nil,
          region_ids: [non_neg_integer()],
          row_count: non_neg_integer(),
          size_bytes: non_neg_integer(),
          updated_at: integer(),
          created_at: integer()
        }

  defstruct [
    :id,
    :name,
    :columns,
    :primary_key,
    :region_prefix,
    :created_at,
    region_ids: [],
    row_count: 0,
    size_bytes: 0,
    updated_at: 0
  ]
end

defmodule PD.Schema.Column do
  @moduledoc """
  Column definition.
  """

  @type t :: %__MODULE__{
          name: String.t(),
          type: atom(),
          nullable: boolean(),
          default: term() | nil,
          precision: non_neg_integer() | nil,
          scale: non_neg_integer() | nil,
          vector_dim: non_neg_integer() | nil,
          list_elem: atom() | nil
        }

  defstruct [
    :name,
    :type,
    nullable: true,
    default: nil,
    precision: nil,
    scale: nil,
    vector_dim: nil,
    list_elem: nil
  ]
end

defmodule PD.Schema.Index do
  @moduledoc """
  Index definition.
  """

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          name: String.t(),
          table_id: non_neg_integer(),
          type: :btree | :anode | :manode,
          columns: [String.t()],
          params: map()
        }

  defstruct [:id, :name, :table_id, :type, :columns, params: %{}]
end

defmodule PD.Schema.Registry do
  @moduledoc """
  Schema registry for tables and indexes.

  Backed by Raft for distributed consensus.
  """

  use GenServer

  require Logger

  defstruct [
    # name -> Table
    tables: %{},
    # id -> Table
    tables_by_id: %{},
    # name -> Index
    indexes: %{},
    # id -> Index
    indexes_by_id: %{},
    next_table_id: 1,
    next_index_id: 1
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Create a new table.
  """
  def create_table(pid \\ __MODULE__, name, columns, primary_key) do
    GenServer.call(pid, {:create_table, name, columns, primary_key})
  end

  @doc """
  Drop a table.
  """
  def drop_table(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:drop_table, name})
  end

  @doc """
  Get table by name.
  """
  def get_table(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:get_table, name})
  end

  @doc """
  Get table by ID.
  """
  def get_table_by_id(pid \\ __MODULE__, id) do
    GenServer.call(pid, {:get_table_by_id, id})
  end

  @doc """
  List all tables.
  """
  def list_tables(pid \\ __MODULE__) do
    GenServer.call(pid, :list_tables)
  end

  @doc """
  Add a column to an existing table.
  """
  def add_column(pid \\ __MODULE__, table_name, column) do
    GenServer.call(pid, {:add_column, table_name, column})
  end

  @doc """
  Drop a column from an existing table.
  """
  def drop_column(pid \\ __MODULE__, table_name, column_name) do
    GenServer.call(pid, {:drop_column, table_name, column_name})
  end

  @doc """
  Rename a column.
  """
  def rename_column(pid \\ __MODULE__, table_name, old_name, new_name) do
    GenServer.call(pid, {:rename_column, table_name, old_name, new_name})
  end

  @doc """
  Create an index.
  """
  def create_index(pid \\ __MODULE__, name, table_name, type, columns, params \\ %{}) do
    GenServer.call(pid, {:create_index, name, table_name, type, columns, params})
  end

  @doc """
  Drop an index.
  """
  def drop_index(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:drop_index, name})
  end

  @doc """
  Get index by name.
  """
  def get_index(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:get_index, name})
  end

  @doc """
  Get regions containing data for a table.
  """
  def get_table_regions(pid \\ __MODULE__, table_name) do
    GenServer.call(pid, {:get_table_regions, table_name})
  end

  @doc """
  Update table statistics (row count, size).
  """
  def update_table_stats(pid \\ __MODULE__, table_name, stats) do
    GenServer.call(pid, {:update_table_stats, table_name, stats})
  end

  @doc """
  List all indexes for a table.
  """
  def list_indexes(pid \\ __MODULE__, table_name \\ nil) do
    GenServer.call(pid, {:list_indexes, table_name})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("Schema Registry initialized")
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:create_table, name, columns, primary_key}, _from, state) do
    if Map.has_key?(state.tables, name) do
      {:reply, {:error, :already_exists}, state}
    else
      table = %PD.Schema.Table{
        id: state.next_table_id,
        name: name,
        columns: columns,
        primary_key: primary_key,
        region_prefix: <<state.next_table_id::unsigned-big-32>>,
        created_at: System.os_time(:millisecond)
      }

      new_state = %{
        state
        | tables: Map.put(state.tables, name, table),
          tables_by_id: Map.put(state.tables_by_id, table.id, table),
          next_table_id: state.next_table_id + 1
      }

      Logger.info("Created table #{name} with id=#{table.id}")
      {:reply, {:ok, table.id}, new_state}
    end
  end

  @impl true
  def handle_call({:drop_table, name}, _from, state) do
    case Map.pop(state.tables, name) do
      {nil, _} ->
        {:reply, {:error, :not_found}, state}

      {table, tables} ->
        # Remove associated indexes
        {removed_indexes, kept_indexes} =
          state.indexes
          |> Enum.split_with(fn {_, idx} -> idx.table_id == table.id end)

        new_state = %{
          state
          | tables: tables,
            tables_by_id: Map.delete(state.tables_by_id, table.id),
            indexes: Map.new(kept_indexes),
            indexes_by_id:
              Enum.reduce(removed_indexes, state.indexes_by_id, fn {_, idx}, acc ->
                Map.delete(acc, idx.id)
              end)
        }

        Logger.info("Dropped table #{name} and #{length(removed_indexes)} indexes")
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_table, name}, _from, state) do
    case Map.get(state.tables, name) do
      nil -> {:reply, {:error, :not_found}, state}
      table -> {:reply, {:ok, table}, state}
    end
  end

  @impl true
  def handle_call({:get_table_by_id, id}, _from, state) do
    case Map.get(state.tables_by_id, id) do
      nil -> {:reply, {:error, :not_found}, state}
      table -> {:reply, {:ok, table}, state}
    end
  end

  @impl true
  def handle_call(:list_tables, _from, state) do
    {:reply, {:ok, Map.values(state.tables)}, state}
  end

  @impl true
  def handle_call({:add_column, table_name, column}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, :table_not_found}, state}

      table ->
        # Check if column already exists
        if Enum.any?(table.columns, &(&1.name == column.name)) do
          {:reply, {:error, :column_exists}, state}
        else
          updated_table = %{table | columns: table.columns ++ [column]}

          new_state = %{
            state
            | tables: Map.put(state.tables, table_name, updated_table),
              tables_by_id: Map.put(state.tables_by_id, table.id, updated_table)
          }

          Logger.info("Added column #{column.name} to table #{table_name}")
          {:reply, :ok, new_state}
        end
    end
  end

  @impl true
  def handle_call({:drop_column, table_name, column_name}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, :table_not_found}, state}

      table ->
        # Check if column is part of primary key
        if column_name in table.primary_key do
          {:reply, {:error, :cannot_drop_pk_column}, state}
        else
          new_columns = Enum.reject(table.columns, &(&1.name == column_name))

          if length(new_columns) == length(table.columns) do
            {:reply, {:error, :column_not_found}, state}
          else
            updated_table = %{table | columns: new_columns}

            new_state = %{
              state
              | tables: Map.put(state.tables, table_name, updated_table),
                tables_by_id: Map.put(state.tables_by_id, table.id, updated_table)
            }

            Logger.info("Dropped column #{column_name} from table #{table_name}")
            {:reply, :ok, new_state}
          end
        end
    end
  end

  @impl true
  def handle_call({:rename_column, table_name, old_name, new_name}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, :table_not_found}, state}

      table ->
        # Find and rename column
        {found, new_columns} =
          Enum.map_reduce(table.columns, false, fn col, found ->
            if col.name == old_name do
              {%{col | name: new_name}, true}
            else
              {col, found}
            end
          end)

        if not found do
          {:reply, {:error, :column_not_found}, state}
        else
          # Update primary key if renamed column is part of it
          new_pk =
            Enum.map(table.primary_key, fn pk_col ->
              if pk_col == old_name, do: new_name, else: pk_col
            end)

          updated_table = %{table | columns: new_columns, primary_key: new_pk}

          new_state = %{
            state
            | tables: Map.put(state.tables, table_name, updated_table),
              tables_by_id: Map.put(state.tables_by_id, table.id, updated_table)
          }

          Logger.info("Renamed column #{old_name} to #{new_name} in table #{table_name}")
          {:reply, :ok, new_state}
        end
    end
  end

  @impl true
  def handle_call({:create_index, name, table_name, type, columns, params}, _from, state) do
    with {:table, {:ok, table}} <- {:table, Map.fetch(state.tables, table_name) |> wrap_fetch()},
         {:exists, false} <- {:exists, Map.has_key?(state.indexes, name)} do
      index = %PD.Schema.Index{
        id: state.next_index_id,
        name: name,
        table_id: table.id,
        type: type,
        columns: columns,
        params: params
      }

      new_state = %{
        state
        | indexes: Map.put(state.indexes, name, index),
          indexes_by_id: Map.put(state.indexes_by_id, index.id, index),
          next_index_id: state.next_index_id + 1
      }

      Logger.info("Created index #{name} on #{table_name}.#{inspect(columns)}")
      {:reply, {:ok, index.id}, new_state}
    else
      {:table, :error} -> {:reply, {:error, :table_not_found}, state}
      {:exists, true} -> {:reply, {:error, :already_exists}, state}
    end
  end

  @impl true
  def handle_call({:drop_index, name}, _from, state) do
    case Map.pop(state.indexes, name) do
      {nil, _} ->
        {:reply, {:error, :not_found}, state}

      {index, indexes} ->
        new_state = %{
          state
          | indexes: indexes,
            indexes_by_id: Map.delete(state.indexes_by_id, index.id)
        }

        Logger.info("Dropped index #{name}")
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_index, name}, _from, state) do
    case Map.get(state.indexes, name) do
      nil -> {:reply, {:error, :not_found}, state}
      index -> {:reply, {:ok, index}, state}
    end
  end

  @impl true
  def handle_call({:get_table_regions, table_name}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, :not_found}, state}

      table ->
        # If explicit region_ids not set, find by prefix
        regions =
          if table.region_ids == [] do
            # Find regions where start_key starts with table prefix
            # This is a simplification; ideally we query PD.Server
            []
          else
            table.region_ids
          end

        {:reply, {:ok, regions}, state}
    end
  end

  def handle_call({:update_table_stats, table_name, stats}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, :not_found}, state}

      table ->
        updated_table = %{
          table
          | row_count: stats.row_count,
            size_bytes: stats.size_bytes,
            updated_at: System.system_time(:second)
        }

        new_state = %{
          state
          | tables: Map.put(state.tables, table_name, updated_table),
            tables_by_id: Map.put(state.tables_by_id, table.id, updated_table)
        }

        {:reply, :ok, new_state}
    end
  end

  def handle_call({:list_indexes, nil}, _from, state) do
    {:reply, {:ok, Map.values(state.indexes)}, state}
  end

  @impl true
  def handle_call({:list_indexes, table_name}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, :table_not_found}, state}

      table ->
        indexes =
          state.indexes
          |> Map.values()
          |> Enum.filter(&(&1.table_id == table.id))

        {:reply, {:ok, indexes}, state}
    end
  end

  defp wrap_fetch({:ok, v}), do: {:ok, v}
  defp wrap_fetch(:error), do: :error
end
