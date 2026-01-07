defmodule PD.Schema.Table do
  @moduledoc """
  Table schema definition.
  """

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          name: String.t(),
          columns: [PD.Schema.Column.t()],
          primary_key: [String.t()],
          region_prefix: binary(),
          created_at: non_neg_integer()
        }

  defstruct [:id, :name, :columns, :primary_key, :region_prefix, :created_at]
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
  List indexes, optionally filtered by table.
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
