defmodule Store.API.RESP.TxnCommands do
  @moduledoc """
  RESP handlers for transaction commands.

  Commands:
  - MULTI [ISOLATION level] [LOCK PESSIMISTIC] - Start transaction
  - EXEC - Commit transaction
  - DISCARD - Rollback transaction
  - SAVEPOINT <name> - Create savepoint
  - ROLLBACK TO <name> - Rollback to savepoint

  Within a transaction, GET/SET/DEL are buffered.
  """

  require Logger
  alias Store.Transaction.Manager

  # Connection state is stored in process dictionary
  @txn_key :spiredb_current_txn

  @doc """
  Start a transaction.
  """
  def execute(["MULTI"]) do
    execute_multi([])
  end

  def execute(["MULTI" | opts]) do
    execute_multi(opts)
  end

  def execute(["EXEC"]) do
    case get_current_txn() do
      nil -> {:error, "ERR EXEC without MULTI"}
      txn_id -> execute_commit(txn_id)
    end
  end

  def execute(["DISCARD"]) do
    case get_current_txn() do
      nil -> {:error, "ERR DISCARD without MULTI"}
      txn_id -> execute_discard(txn_id)
    end
  end

  def execute(["SAVEPOINT", name]) do
    case get_current_txn() do
      nil ->
        {:error, "ERR SAVEPOINT outside transaction"}

      txn_id ->
        case Manager.savepoint(txn_id, name) do
          :ok -> "OK"
          {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
        end
    end
  end

  def execute(["ROLLBACK", "TO", name]) do
    case get_current_txn() do
      nil ->
        {:error, "ERR ROLLBACK TO outside transaction"}

      txn_id ->
        case Manager.rollback_to(txn_id, name) do
          :ok -> "OK"
          {:error, :savepoint_not_found} -> {:error, "ERR savepoint '#{name}' not found"}
          {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
        end
    end
  end

  def execute(_) do
    {:error, "ERR unknown transaction command"}
  end

  @doc """
  Check if we're in a transaction context.
  """
  def in_transaction? do
    get_current_txn() != nil
  end

  @doc """
  Get current transaction ID.
  """
  def current_txn_id do
    get_current_txn()
  end

  @doc """
  Execute GET within a transaction.
  """
  def txn_get(key) do
    case get_current_txn() do
      nil ->
        {:error, :not_in_transaction}

      txn_id ->
        case Manager.get(txn_id, key) do
          {:ok, value} -> {:ok, value}
          {:error, :not_found} -> {:ok, nil}
          {:error, {:locked, _lock}} -> {:error, "ERR key locked by another transaction"}
          {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
        end
    end
  end

  @doc """
  Execute SET within a transaction.
  """
  def txn_set(key, value) do
    case get_current_txn() do
      nil ->
        {:error, :not_in_transaction}

      txn_id ->
        case Manager.put(txn_id, key, value) do
          :ok -> "QUEUED"
          {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
        end
    end
  end

  @doc """
  Execute DEL within a transaction.
  """
  def txn_delete(key) do
    case get_current_txn() do
      nil ->
        {:error, :not_in_transaction}

      txn_id ->
        case Manager.delete(txn_id, key) do
          :ok -> "QUEUED"
          {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
        end
    end
  end

  # Private helpers

  defp execute_multi(opts) do
    if get_current_txn() do
      {:error, "ERR MULTI calls can not be nested"}
    else
      parsed_opts = parse_multi_opts(opts)

      case Manager.begin_transaction(parsed_opts) do
        {:ok, txn_id} ->
          set_current_txn(txn_id)
          "OK"

        {:error, reason} ->
          {:error, "ERR #{inspect(reason)}"}
      end
    end
  end

  defp parse_multi_opts(opts) do
    opts
    |> Enum.chunk_every(2)
    |> Enum.reduce([], fn
      ["ISOLATION", level], acc ->
        isolation =
          case String.upcase(level) do
            "READ_COMMITTED" -> :read_committed
            "REPEATABLE_READ" -> :repeatable_read
            "SERIALIZABLE" -> :serializable
            _ -> :repeatable_read
          end

        [{:isolation, isolation} | acc]

      ["LOCK", "PESSIMISTIC"], acc ->
        [{:lock_type, :pessimistic} | acc]

      _, acc ->
        acc
    end)
  end

  defp execute_commit(txn_id) do
    case Manager.commit(txn_id) do
      {:ok, commit_ts} ->
        clear_current_txn()
        ["OK", commit_ts]

      {:error, {:conflict, key}} ->
        clear_current_txn()
        {:error, "ERR write conflict on key '#{inspect(key)}'"}

      {:error, reason} ->
        clear_current_txn()
        {:error, "ERR commit failed: #{inspect(reason)}"}
    end
  end

  defp execute_discard(txn_id) do
    case Manager.rollback(txn_id) do
      :ok ->
        clear_current_txn()
        "OK"

      {:error, reason} ->
        clear_current_txn()
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  defp get_current_txn do
    Process.get(@txn_key)
  end

  defp set_current_txn(txn_id) do
    Process.put(@txn_key, txn_id)
  end

  defp clear_current_txn do
    Process.delete(@txn_key)
  end
end
