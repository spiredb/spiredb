defmodule Store.VectorIndex do
  @moduledoc """
  Vector index management using Anodex.

  Wraps Anodex operations and manages index metadata + payloads.
  Supports both ANODE (single shard) and MANODE (multi-shard) indexes.
  """

  use GenServer

  require Logger
  alias Store.Schema.Encoder

  defstruct [
    # name -> index_info
    indexes: %{},
    # name -> anodex reference
    anodex_refs: %{}
  ]

  @type index_info :: %{
          id: non_neg_integer(),
          name: String.t(),
          table_name: String.t(),
          column_name: String.t(),
          algorithm: :anode | :manode,
          dimensions: non_neg_integer(),
          params: map()
        }

  ## Client API

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Create a new vector index.
  """
  def create_index(pid \\ __MODULE__, name, table_name, column_name, opts) do
    GenServer.call(pid, {:create_index, name, table_name, column_name, opts})
  end

  @doc """
  Drop a vector index.
  """
  def drop_index(pid \\ __MODULE__, name) do
    GenServer.call(pid, {:drop_index, name})
  end

  @doc """
  Insert a vector with optional payload.
  """
  def insert(pid \\ __MODULE__, index_name, doc_id, vector, payload \\ nil) do
    GenServer.call(pid, {:insert, index_name, doc_id, vector, payload})
  end

  @doc """
  Delete a vector.
  """
  def delete(pid \\ __MODULE__, index_name, doc_id) do
    GenServer.call(pid, {:delete, index_name, doc_id})
  end

  @doc """
  Search for nearest neighbors.
  """
  def search(pid \\ __MODULE__, index_name, query_vector, k, opts \\ []) do
    GenServer.call(pid, {:search, index_name, query_vector, k, opts}, 30_000)
  end

  @doc """
  List all vector indexes.
  """
  def list_indexes(pid \\ __MODULE__) do
    GenServer.call(pid, :list_indexes)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    data_dir = opts[:data_dir] || "/tmp/spiredb/vectors"
    File.mkdir_p!(data_dir)

    Logger.info("VectorIndex started, data_dir=#{data_dir}")
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:create_index, name, table_name, column_name, opts}, _from, state) do
    if Map.has_key?(state.indexes, name) do
      {:reply, {:error, :already_exists}, state}
    else
      algorithm = Keyword.get(opts, :algorithm, :anode)
      dimensions = Keyword.get(opts, :dimensions, 128)
      shards = Keyword.get(opts, :shards, 4)

      index_info = %{
        id: :erlang.unique_integer([:positive]),
        name: name,
        table_name: table_name,
        column_name: column_name,
        algorithm: algorithm,
        dimensions: dimensions,
        params: %{shards: shards}
      }

      # Initialize Anodex index
      case init_anodex_index(name, algorithm, dimensions, shards) do
        {:ok, ref} ->
          new_state = %{
            state
            | indexes: Map.put(state.indexes, name, index_info),
              anodex_refs: Map.put(state.anodex_refs, name, ref)
          }

          Logger.info("Created vector index #{name} (#{algorithm}, dim=#{dimensions})")
          {:reply, {:ok, index_info.id}, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl true
  def handle_call({:drop_index, name}, _from, state) do
    case Map.get(state.indexes, name) do
      nil ->
        {:reply, {:error, :not_found}, state}

      _info ->
        # Shutdown Anodex index
        case Map.get(state.anodex_refs, name) do
          nil -> :ok
          ref -> shutdown_anodex_index(ref)
        end

        new_state = %{
          state
          | indexes: Map.delete(state.indexes, name),
            anodex_refs: Map.delete(state.anodex_refs, name)
        }

        Logger.info("Dropped vector index #{name}")
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:insert, index_name, doc_id, vector, payload}, _from, state) do
    with {:ok, info} <- get_index(state, index_name),
         {:ok, ref} <- get_anodex_ref(state, index_name) do
      # Store payload in vectors CF
      if payload do
        store_payload(info.id, doc_id, payload)
      end

      # Insert into Anodex
      case insert_into_anodex(ref, doc_id, vector) do
        {:ok, internal_id} ->
          {:reply, {:ok, internal_id}, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, index_name, doc_id}, _from, state) do
    with {:ok, info} <- get_index(state, index_name),
         {:ok, ref} <- get_anodex_ref(state, index_name) do
      # Delete payload
      delete_payload(info.id, doc_id)

      # Delete from Anodex
      delete_from_anodex(ref, doc_id)
      {:reply, :ok, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:search, index_name, query_vector, k, opts}, _from, state) do
    with {:ok, info} <- get_index(state, index_name),
         {:ok, ref} <- get_anodex_ref(state, index_name) do
      return_payload = Keyword.get(opts, :return_payload, false)

      case search_anodex(ref, query_vector, k) do
        {:ok, results} ->
          # Optionally fetch payloads
          results_with_payload =
            if return_payload do
              Enum.map(results, fn {doc_id, distance} ->
                payload = get_payload(info.id, doc_id)
                {doc_id, distance, payload}
              end)
            else
              Enum.map(results, fn {doc_id, distance} -> {doc_id, distance, nil} end)
            end

          {:reply, {:ok, results_with_payload}, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:list_indexes, _from, state) do
    {:reply, {:ok, Map.values(state.indexes)}, state}
  end

  # Private helpers

  defp get_index(state, name) do
    case Map.get(state.indexes, name) do
      nil -> {:error, :index_not_found}
      info -> {:ok, info}
    end
  end

  defp get_anodex_ref(state, name) do
    case Map.get(state.anodex_refs, name) do
      nil -> {:error, :anodex_not_initialized}
      ref -> {:ok, ref}
    end
  end

  # Anodex operations (placeholder - will use actual Anodex NIF)

  defp init_anodex_index(name, algorithm, dimensions, shards) do
    # TODO: Use actual Anodex.new() or Anodex.Manode.new()
    # For now, use ETS as placeholder
    table_name = :"vector_#{name}"

    try do
      :ets.new(table_name, [:named_table, :public, :set])
      {:ok, table_name}
    rescue
      ArgumentError ->
        :ets.delete(table_name)
        :ets.new(table_name, [:named_table, :public, :set])
        {:ok, table_name}
    end
  end

  defp shutdown_anodex_index(ref) do
    try do
      :ets.delete(ref)
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  defp insert_into_anodex(ref, doc_id, vector) do
    # TODO: Use Anodex.insert()
    internal_id = :erlang.phash2({doc_id, System.os_time()})

    try do
      :ets.insert(ref, {doc_id, vector, internal_id})
      {:ok, internal_id}
    rescue
      e -> {:error, e}
    end
  end

  defp delete_from_anodex(ref, doc_id) do
    try do
      :ets.delete(ref, doc_id)
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  defp search_anodex(ref, query_vector, k) do
    # TODO: Use Anodex.search()
    # For now, brute force search in ETS
    try do
      all = :ets.tab2list(ref)

      results =
        all
        |> Enum.map(fn {doc_id, vector, _} ->
          distance = euclidean_distance(query_vector, vector)
          {doc_id, distance}
        end)
        |> Enum.sort_by(fn {_, d} -> d end)
        |> Enum.take(k)

      {:ok, results}
    rescue
      e -> {:error, e}
    end
  end

  defp euclidean_distance(v1, v2) when is_list(v1) and is_list(v2) do
    Enum.zip(v1, v2)
    |> Enum.map(fn {a, b} -> (a - b) * (a - b) end)
    |> Enum.sum()
    |> :math.sqrt()
  end

  defp euclidean_distance(v1, v2) when is_binary(v1) and is_binary(v2) do
    # Decode float32 arrays
    list1 = for <<f::float-32-native <- v1>>, do: f
    list2 = for <<f::float-32-native <- v2>>, do: f
    euclidean_distance(list1, list2)
  end

  # Payload storage (using vectors CF)

  defp store_payload(index_id, doc_id, payload) do
    key = Encoder.encode_vector_key(index_id, doc_id)
    value = if is_binary(payload), do: payload, else: Jason.encode!(payload)

    try do
      :ets.insert(:vector_payloads, {key, value})
    rescue
      ArgumentError ->
        :ets.new(:vector_payloads, [:named_table, :public, :set])
        :ets.insert(:vector_payloads, {key, value})
    end
  end

  defp get_payload(index_id, doc_id) do
    key = Encoder.encode_vector_key(index_id, doc_id)

    try do
      case :ets.lookup(:vector_payloads, key) do
        [{^key, value}] -> value
        [] -> nil
      end
    rescue
      ArgumentError -> nil
    end
  end

  defp delete_payload(index_id, doc_id) do
    key = Encoder.encode_vector_key(index_id, doc_id)

    try do
      :ets.delete(:vector_payloads, key)
    rescue
      ArgumentError -> :ok
    end
  end
end
