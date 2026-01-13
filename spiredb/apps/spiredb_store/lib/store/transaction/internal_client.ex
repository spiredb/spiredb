defmodule Store.Transaction.InternalClient do
  @moduledoc """
  Client for InternalTransactionService gRPC calls to remote stores.

  Used by AsyncCommitCoordinator to perform cross-node operations.
  Uses ConnectionPool for connection reuse.
  """

  require Logger
  alias Spiredb.Cluster.InternalTransactionService.Stub
  alias Store.Transaction.ConnectionPool

  @default_timeout 10_000

  @doc """
  Async prewrite mutations to a remote store.
  """
  def async_prewrite(store_address, request, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    ConnectionPool.with_connection(store_address, fn channel ->
      case Stub.async_prewrite(channel, request, timeout: timeout) do
        {:ok, response} ->
          {:ok, response}

        {:error, reason} ->
          Logger.warning(
            "InternalClient.async_prewrite failed for #{store_address}: #{inspect(reason)}"
          )

          {:error, reason}
      end
    end)
  end

  @doc """
  Check secondary locks on a remote store.
  """
  def check_secondary_locks(store_address, request, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    ConnectionPool.with_connection(store_address, fn channel ->
      case Stub.check_secondary_locks(channel, request, timeout: timeout) do
        {:ok, response} ->
          {:ok, response}

        {:error, reason} ->
          Logger.warning("InternalClient.check_secondary_locks failed: #{inspect(reason)}")
          {:error, reason}
      end
    end)
  end

  @doc """
  Resolve a lock on a remote store.
  """
  def resolve_lock(store_address, request, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    ConnectionPool.with_connection(store_address, fn channel ->
      case Stub.resolve_lock(channel, request, timeout: timeout) do
        {:ok, _response} ->
          :ok

        {:error, reason} ->
          Logger.warning("InternalClient.resolve_lock failed: #{inspect(reason)}")
          {:error, reason}
      end
    end)
  end

  @doc """
  Get the gRPC address for a store node.
  Returns address in format "host:port".
  """
  def get_store_address(store_node) when is_atom(store_node) do
    # Extract hostname from node name (e.g., spiredb@host -> host)
    node_string = Atom.to_string(store_node)

    host =
      case String.split(node_string, "@") do
        [_name, host] -> host
        [host] -> host
      end

    # Internal gRPC port (same as DataAccess port)
    port = Application.get_env(:spiredb_store, :grpc_port, 50_052)

    "#{host}:#{port}"
  end

  def get_store_address(store_node) when is_binary(store_node) do
    store_node
  end
end
