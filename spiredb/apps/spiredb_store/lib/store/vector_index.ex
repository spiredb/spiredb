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
    anodex_refs: %{},
    # name -> %{int_id => doc_id}
    id_mappings: %{}
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
          # Load any existing id mappings from persistence
          existing_mappings = load_id_mappings(name)

          new_state = %{
            state
            | indexes: Map.put(state.indexes, name, index_info),
              anodex_refs: Map.put(state.anodex_refs, name, ref),
              id_mappings: Map.put(state.id_mappings, name, existing_mappings)
          }

          Logger.info(
            "Created vector index #{name} (#{algorithm}, dim=#{dimensions}), loaded #{map_size(existing_mappings)} mappings"
          )

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
        {:ok, int_id} ->
          # Persist mapping to RocksDB for recovery after restart
          store_id_mapping(index_name, int_id, doc_id)

          # Update in-memory mapping
          index_mapping = Map.get(state.id_mappings, index_name, %{})
          new_mapping = Map.put(index_mapping, int_id, doc_id)
          new_state = %{state | id_mappings: Map.put(state.id_mappings, index_name, new_mapping)}
          {:reply, {:ok, int_id}, new_state}

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

      # Normalize query vector to list of floats
      query_list = normalize_vector(query_vector)
      index_mapping = Map.get(state.id_mappings, index_name, %{})

      case search_anodex(ref, query_list, k) do
        {:ok, results} ->
          # Map int_ids back to doc_ids and optionally fetch payloads
          # Note: Anodex returns {distance, id} tuples
          results_with_payload =
            Enum.map(results, fn {distance, int_id} ->
              doc_id = Map.get(index_mapping, int_id, int_id)
              payload = if return_payload, do: get_payload(info.id, doc_id), else: nil
              {doc_id, distance, payload}
            end)

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

  # Anodex operations

  defp init_anodex_index(name, _algorithm, dimensions, _shards) do
    # Create index-specific directory
    base_dir = Application.get_env(:spiredb_store, :vector_data_dir, "/tmp/spiredb/vectors")
    index_dir = Path.join(base_dir, to_string(name))
    File.mkdir_p!(index_dir)

    opts = %Anodex.Options{
      dimension: dimensions,
      data_dir: index_dir,
      name: to_string(name)
    }

    # Use Manode for all indexes (recommended for databases)
    case Anodex.Manode.new(opts) do
      {:ok, index} -> {:ok, index}
      {:error, reason} -> {:error, reason}
    end
  end

  defp shutdown_anodex_index(ref) do
    Anodex.Manode.shutdown(ref)
  end

  defp insert_into_anodex(ref, doc_id, vector) do
    # Anodex expects {integer_id, vector} tuple
    # Convert string doc_id to integer using hash
    int_id = if is_integer(doc_id), do: doc_id, else: :erlang.phash2(doc_id)

    # Convert binary vectors to list of floats if needed
    vector_list = normalize_vector(vector)

    case Anodex.Manode.insert(ref, {int_id, vector_list}) do
      # Return int_id for mapping
      :ok -> {:ok, int_id}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_vector(vector) when is_list(vector), do: vector

  defp normalize_vector(vector) when is_binary(vector) do
    # Decode float32 binary to list of floats
    for <<f::float-32-native <- vector>>, do: f
  end

  defp delete_from_anodex(ref, doc_id) do
    # Convert doc_id to int_id (same as insert)
    int_id = if is_integer(doc_id), do: doc_id, else: :erlang.phash2(doc_id)

    case Anodex.Manode.delete(ref, int_id) do
      :ok ->
        :ok

      # Some versions return empty tuple on success
      {} ->
        :ok

      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Anodex delete failed for id #{int_id}: #{inspect(reason)}")
        # Don't fail the operation
        :ok

      # Handle any other return value gracefully
      _ ->
        :ok
    end
  end

  defp search_anodex(ref, query_vector, k) do
    case Anodex.Manode.search(ref, query_vector, k) do
      {:ok, results} -> {:ok, results}
      {:error, reason} -> {:error, reason}
    end
  end

  # Payload storage (using vectors CF)

  defp get_db_ref do
    :persistent_term.get(:spiredb_rocksdb_ref, nil)
  end

  defp get_vectors_cf do
    case :persistent_term.get(:spiredb_rocksdb_cf_map, nil) do
      nil -> nil
      cf_map -> Map.get(cf_map, "vectors")
    end
  end

  defp store_payload(index_id, doc_id, payload) do
    key = Encoder.encode_vector_key(index_id, doc_id)
    value = if is_binary(payload), do: payload, else: Jason.encode!(payload)

    case {get_db_ref(), get_vectors_cf()} do
      {nil, _} ->
        Logger.error("Cannot store payload: No RocksDB store_ref found")
        :error

      {db_ref, cf} when not is_nil(cf) ->
        :rocksdb.put(db_ref, cf, key, value, [])

      {db_ref, nil} ->
        # Fallback to default CF if vectors CF not available
        :rocksdb.put(db_ref, key, value, [])
    end
  end

  defp get_payload(index_id, doc_id) do
    key = Encoder.encode_vector_key(index_id, doc_id)

    case {get_db_ref(), get_vectors_cf()} do
      {nil, _} ->
        nil

      {db_ref, cf} when not is_nil(cf) ->
        case :rocksdb.get(db_ref, cf, key, []) do
          {:ok, value} -> value
          _ -> nil
        end

      {db_ref, nil} ->
        case :rocksdb.get(db_ref, key, []) do
          {:ok, value} -> value
          _ -> nil
        end
    end
  end

  defp delete_payload(index_id, doc_id) do
    key = Encoder.encode_vector_key(index_id, doc_id)

    case {get_db_ref(), get_vectors_cf()} do
      {nil, _} ->
        :ok

      {db_ref, cf} when not is_nil(cf) ->
        :rocksdb.delete(db_ref, cf, key, [])

      {db_ref, nil} ->
        :rocksdb.delete(db_ref, key, [])
    end
  end

  # ID mapping persistence (for int_id <-> doc_id)

  defp encode_mapping_key(index_name, int_id) do
    # Key format: "vec_map:{index_name}:{int_id}"
    "vec_map:#{index_name}:#{int_id}"
  end

  defp store_id_mapping(index_name, int_id, doc_id) do
    key = encode_mapping_key(index_name, int_id)
    value = :erlang.term_to_binary(doc_id)

    case {get_db_ref(), get_vectors_cf()} do
      {nil, _} ->
        Logger.error("Cannot store ID mapping: No RocksDB store_ref found")
        :error

      {db_ref, cf} when not is_nil(cf) ->
        :rocksdb.put(db_ref, cf, key, value, [])

      {db_ref, nil} ->
        :rocksdb.put(db_ref, key, value, [])
    end
  end

  defp load_id_mappings(index_name) do
    prefix = "vec_map:#{index_name}:"

    case {get_db_ref(), get_vectors_cf()} do
      {nil, _} ->
        %{}

      {db_ref, cf} when not is_nil(cf) ->
        # Scan RocksDB with prefix using vectors CF
        case :rocksdb.iterator(db_ref, cf, [{:prefix_same_as_start, true}]) do
          {:ok, iter} ->
            result = scan_mappings(iter, prefix, %{})
            :rocksdb.iterator_close(iter)
            result

          {:error, _} ->
            %{}
        end

      {db_ref, nil} ->
        # Fallback to default CF
        case :rocksdb.iterator(db_ref, [{:prefix_same_as_start, true}]) do
          {:ok, iter} ->
            result = scan_mappings(iter, prefix, %{})
            :rocksdb.iterator_close(iter)
            result

          {:error, _} ->
            %{}
        end
    end
  end

  defp scan_mappings(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, value} when is_binary(key) ->
        if String.starts_with?(key, prefix) do
          int_id = key |> String.replace(prefix, "") |> String.to_integer()
          doc_id = :erlang.binary_to_term(value)
          new_acc = Map.put(acc, int_id, doc_id)
          scan_next_mappings(iter, prefix, new_acc)
        else
          acc
        end

      _ ->
        acc
    end
  end

  defp scan_next_mappings(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} when is_binary(key) ->
        if String.starts_with?(key, prefix) do
          int_id = key |> String.replace(prefix, "") |> String.to_integer()
          doc_id = :erlang.binary_to_term(value)
          new_acc = Map.put(acc, int_id, doc_id)
          scan_next_mappings(iter, prefix, new_acc)
        else
          acc
        end

      _ ->
        acc
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("VectorIndex received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end
end
