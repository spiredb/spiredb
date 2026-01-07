defmodule Store.API.RESP.Commands do
  @moduledoc """
  Redis command router and handlers.

  Routes parsed RESP commands to appropriate Raft operations via Store.Server.
  """

  require Logger
  # Remove alias Store.Server

  @doc """
  Execute a command parsed from RESP.

  Returns a response in Redix-compatible format.
  """
  @spec execute(list()) :: term()
  def execute(["PING"]), do: "PONG"
  def execute(["PING", message]), do: message

  def execute(["COMMAND"]) do
    # Return minimal command list for compatibility
    # Most clients work fine with empty
    []
  end

  def execute(["GET", key]) do
    handle_get(key)
  end

  def execute(["SET", key, value]) do
    handle_set(key, value)
  end

  def execute(["SET", key, value | opts]) do
    # Handle SET with options (EX, PX, NX, XX, etc.)
    # For now, ignore options and do basic SET
    Logger.debug("SET options ignored: #{inspect(opts)}")
    handle_set(key, value)
  end

  def execute(["DEL" | keys]) when length(keys) > 0 do
    handle_del(keys)
  end

  def execute(["EXISTS" | keys]) when length(keys) > 0 do
    handle_exists(keys)
  end

  def execute(["MGET" | keys]) when length(keys) > 0 do
    handle_mget(keys)
  end

  def execute(["MSET" | args]) when rem(length(args), 2) == 0 and length(args) > 0 do
    handle_mset(args)
  end

  def execute(["INCR", key]) do
    handle_incr(key, 1)
  end

  def execute(["DECR", key]) do
    handle_incr(key, -1)
  end

  def execute(["INCRBY", key, increment]) do
    case Integer.parse(increment) do
      {num, ""} -> handle_incr(key, num)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def execute(["DECRBY", key, decrement]) do
    case Integer.parse(decrement) do
      {num, ""} -> handle_incr(key, -num)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def execute(["APPEND", key, value]) do
    handle_append(key, value)
  end

  def execute(["STRLEN", key]) do
    case handle_get(key) do
      nil -> 0
      value when is_binary(value) -> byte_size(value)
      _ -> 0
    end
  end

  def execute(["FLUSHALL"]) do
    # For testing - doesn't actually flush in distributed mode
    # Just return OK for compatibility
    "OK"
  end

  # Route SPIRE.* commands to TableCommands
  def execute(["SPIRE.TABLE.CREATE" | _] = cmd), do: Store.API.RESP.TableCommands.execute(cmd)
  def execute(["SPIRE.TABLE.DROP" | _] = cmd), do: Store.API.RESP.TableCommands.execute(cmd)
  def execute(["SPIRE.TABLE.LIST" | _] = cmd), do: Store.API.RESP.TableCommands.execute(cmd)
  def execute(["SPIRE.TABLE.DESCRIBE" | _] = cmd), do: Store.API.RESP.TableCommands.execute(cmd)
  def execute(["SPIRE.INDEX.CREATE" | _] = cmd), do: Store.API.RESP.TableCommands.execute(cmd)
  def execute(["SPIRE.INDEX.DROP" | _] = cmd), do: Store.API.RESP.TableCommands.execute(cmd)

  # Route transaction commands
  def execute(["MULTI" | _] = cmd), do: Store.API.RESP.TxnCommands.execute(cmd)
  def execute(["EXEC" | _] = cmd), do: Store.API.RESP.TxnCommands.execute(cmd)
  def execute(["DISCARD" | _] = cmd), do: Store.API.RESP.TxnCommands.execute(cmd)
  def execute(["SAVEPOINT" | _] = cmd), do: Store.API.RESP.TxnCommands.execute(cmd)
  def execute(["ROLLBACK", "TO" | _] = cmd), do: Store.API.RESP.TxnCommands.execute(cmd)

  # Route vector search commands (Redis Search compatible)
  def execute(["FT.CREATE" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)
  def execute(["FT.DROPINDEX" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)
  def execute(["FT.ADD" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)
  def execute(["FT.DEL" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)
  def execute(["FT.SEARCH" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)
  def execute(["FT.INFO" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)
  def execute(["FT._LIST" | _] = cmd), do: Store.API.RESP.VectorCommands.execute(cmd)

  # Route plugin commands
  def execute(["SPIRE.PLUGIN.LIST" | _] = cmd), do: Store.API.RESP.PluginCommands.execute(cmd)
  def execute(["SPIRE.PLUGIN.INFO" | _] = cmd), do: Store.API.RESP.PluginCommands.execute(cmd)
  def execute(["SPIRE.PLUGIN.RELOAD" | _] = cmd), do: Store.API.RESP.PluginCommands.execute(cmd)

  # Route stream commands
  def execute(["XADD" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XREAD" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XRANGE" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XREVRANGE" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XLEN" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XINFO" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XTRIM" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)
  def execute(["XDEL" | _] = cmd), do: Store.API.RESP.StreamCommands.execute(cmd)

  def execute([command | _]) do
    {:error, "ERR unknown command '#{String.downcase(command)}'"}
  end

  def execute([]) do
    {:error, "ERR empty command"}
  end

  # Command Handlers

  defp handle_get(key) do
    case store_module().get(key) do
      {:ok, value} -> value
      {:error, :not_found} -> nil
      {:error, _} -> nil
    end
  end

  defp handle_set(key, value) do
    case store_module().put(key, value) do
      {:ok, _} -> "OK"
      {:error, :regions_initializing} -> {:error, "LOADING SpireDB is loading, please wait"}
      {:error, _} -> {:error, "ERR failed to set key"}
    end
  end

  defp handle_del(keys) do
    count =
      Enum.count(keys, fn key ->
        case store_module().delete(key) do
          {:ok, :ok} -> true
          _ -> false
        end
      end)

    count
  end

  defp handle_exists(keys) do
    count =
      Enum.count(keys, fn key ->
        case store_module().get(key) do
          {:ok, _} -> true
          _ -> false
        end
      end)

    count
  end

  defp handle_mget(keys) do
    Enum.map(keys, fn key ->
      case store_module().get(key) do
        {:ok, value} -> value
        _ -> nil
      end
    end)
  end

  defp handle_mset(args) do
    args
    |> Enum.chunk_every(2)
    |> Enum.each(fn [key, value] ->
      store_module().put(key, value)
    end)

    "OK"
  end

  defp handle_incr(key, delta) do
    # Note: This is NOT atomic across regions in distributed mode
    # For true atomicity, need to implement INCR as a special Raft command
    case store_module().get(key) do
      {:ok, value} ->
        case Integer.parse(value) do
          {num, ""} ->
            new_value = num + delta
            new_value_str = Integer.to_string(new_value)

            case store_module().put(key, new_value_str) do
              {:ok, _} -> new_value
              _ -> {:error, "ERR failed to update value"}
            end

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end

      {:error, :not_found} ->
        # Key doesn't exist, initialize to delta
        new_value_str = Integer.to_string(delta)

        case store_module().put(key, new_value_str) do
          {:ok, _} -> delta
          _ -> {:error, "ERR failed to set value"}
        end

      _ ->
        {:error, "ERR internal error"}
    end
  end

  defp handle_append(key, value) do
    case store_module().get(key) do
      {:ok, existing} ->
        new_value = existing <> value

        case store_module().put(key, new_value) do
          {:ok, _} -> byte_size(new_value)
          _ -> {:error, "ERR failed to append"}
        end

      {:error, :not_found} ->
        case store_module().put(key, value) do
          {:ok, _} -> byte_size(value)
          _ -> {:error, "ERR failed to set value"}
        end

      _ ->
        {:error, "ERR internal error"}
    end
  end

  defp store_module do
    Application.get_env(:spiredb_store, :store_module, Store.Server)
  end
end
