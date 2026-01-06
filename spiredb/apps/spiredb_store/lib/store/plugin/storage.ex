defmodule Store.Plugin.Storage do
  @moduledoc """
  Persistent storage API for plugins.

  Provides a namespaced key-value store backed by RocksDB's `plugin_state` column family.
  Each plugin gets its own namespace to prevent key collisions.

  ## Usage

      # In your plugin's init/1:
      def init(opts) do
        # Load previous state
        case Store.Plugin.Storage.get("my_plugin", "counter") do
          {:ok, value} -> {:ok, %{counter: value}}
          {:error, :not_found} -> {:ok, %{counter: 0}}
        end
      end

      # During operation:
      Store.Plugin.Storage.put("my_plugin", "counter", state.counter)

  """

  require Logger

  @cf_name "plugin_state"

  @doc """
  Get a value for a plugin.

  Keys are namespaced by plugin name to prevent collisions.
  """
  @spec get(plugin_name :: String.t(), key :: String.t()) ::
          {:ok, binary()} | {:error, :not_found | term()}
  def get(plugin_name, key) when is_binary(plugin_name) and is_binary(key) do
    full_key = encode_key(plugin_name, key)

    case {get_db_ref(), get_cf_handle()} do
      {nil, _} ->
        Logger.warning("Plugin Storage: RocksDB not available")
        {:error, :not_available}

      {_, nil} ->
        Logger.warning("Plugin Storage: CF not available")
        {:error, :not_available}

      {db_ref, cf_handle} ->
        case :rocksdb.get(db_ref, cf_handle, full_key, []) do
          {:ok, value} -> {:ok, value}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Put a value for a plugin.

  Values must be binaries. Use `:erlang.term_to_binary/1` for complex terms.
  """
  @spec put(plugin_name :: String.t(), key :: String.t(), value :: binary()) ::
          :ok | {:error, term()}
  def put(plugin_name, key, value)
      when is_binary(plugin_name) and is_binary(key) and is_binary(value) do
    full_key = encode_key(plugin_name, key)

    case {get_db_ref(), get_cf_handle()} do
      {nil, _} ->
        {:error, :not_available}

      {_, nil} ->
        {:error, :not_available}

      {db_ref, cf_handle} ->
        :rocksdb.put(db_ref, cf_handle, full_key, value, [])
    end
  end

  @doc """
  Put an Elixir term (serialized automatically).
  """
  @spec put_term(plugin_name :: String.t(), key :: String.t(), term :: term()) ::
          :ok | {:error, term()}
  def put_term(plugin_name, key, term) do
    put(plugin_name, key, :erlang.term_to_binary(term))
  end

  @doc """
  Get an Elixir term (deserialized automatically).
  """
  @spec get_term(plugin_name :: String.t(), key :: String.t()) ::
          {:ok, term()} | {:error, :not_found | term()}
  def get_term(plugin_name, key) do
    case get(plugin_name, key) do
      {:ok, binary} ->
        try do
          {:ok, :erlang.binary_to_term(binary)}
        rescue
          _ -> {:error, :decode_failed}
        end

      error ->
        error
    end
  end

  @doc """
  Delete a key for a plugin.
  """
  @spec delete(plugin_name :: String.t(), key :: String.t()) :: :ok | {:error, term()}
  def delete(plugin_name, key) when is_binary(plugin_name) and is_binary(key) do
    full_key = encode_key(plugin_name, key)

    case {get_db_ref(), get_cf_handle()} do
      {nil, _} ->
        {:error, :not_available}

      {_, nil} ->
        {:error, :not_available}

      {db_ref, cf_handle} ->
        :rocksdb.delete(db_ref, cf_handle, full_key, [])
    end
  end

  @doc """
  List all keys for a plugin.
  """
  @spec list_keys(plugin_name :: String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_keys(plugin_name) when is_binary(plugin_name) do
    prefix = "#{plugin_name}:"

    case {get_db_ref(), get_cf_handle()} do
      {nil, _} ->
        {:error, :not_available}

      {_, nil} ->
        {:error, :not_available}

      {db_ref, cf_handle} ->
        case :rocksdb.iterator(db_ref, cf_handle, []) do
          {:ok, iter} ->
            keys = scan_prefix(iter, prefix, [])
            :rocksdb.iterator_close(iter)
            {:ok, keys}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Clear all data for a plugin.
  """
  @spec clear_all(plugin_name :: String.t()) :: :ok | {:error, term()}
  def clear_all(plugin_name) when is_binary(plugin_name) do
    case list_keys(plugin_name) do
      {:ok, keys} ->
        Enum.each(keys, fn key -> delete(plugin_name, key) end)
        :ok

      error ->
        error
    end
  end

  ## Private

  defp encode_key(plugin_name, key) do
    "#{plugin_name}:#{key}"
  end

  defp get_db_ref do
    :persistent_term.get(:spiredb_rocksdb_ref, nil)
  end

  defp get_cf_handle do
    case :persistent_term.get(:spiredb_rocksdb_cf_map, nil) do
      nil -> nil
      cf_map -> Map.get(cf_map, @cf_name)
    end
  end

  defp scan_prefix(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, {:seek, prefix}) do
      {:ok, key, _value} ->
        if String.starts_with?(key, prefix) do
          # Extract local key (remove prefix)
          local_key = String.slice(key, String.length(prefix)..-1//1)
          collect_keys(iter, prefix, [local_key | acc])
        else
          Enum.reverse(acc)
        end

      {:error, :invalid_iterator} ->
        Enum.reverse(acc)
    end
  end

  defp collect_keys(iter, prefix, acc) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, _value} ->
        if String.starts_with?(key, prefix) do
          local_key = String.slice(key, String.length(prefix)..-1//1)
          collect_keys(iter, prefix, [local_key | acc])
        else
          Enum.reverse(acc)
        end

      {:error, :invalid_iterator} ->
        Enum.reverse(acc)
    end
  end
end
