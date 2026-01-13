defmodule Store.Transaction.ConnectionPool do
  @moduledoc """
  Connection pool for internal gRPC connections to remote stores.

  Uses a simple ETS-backed pool with connection reuse to avoid
  per-request connection overhead (100-500ms per connection).
  """

  use GenServer

  require Logger

  @pool_table :grpc_connection_pool
  @connection_ttl_ms 60_000
  @cleanup_interval_ms 30_000

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get a pooled connection to a store address.
  Creates a new connection if none available.
  """
  @spec checkout(String.t()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def checkout(address) do
    GenServer.call(__MODULE__, {:checkout, address})
  catch
    :exit, _ ->
      # Pool not running, create direct connection
      create_connection(address)
  end

  @doc """
  Return a connection to the pool.
  """
  @spec checkin(String.t(), GRPC.Channel.t()) :: :ok
  def checkin(address, channel) do
    GenServer.cast(__MODULE__, {:checkin, address, channel})
  catch
    :exit, _ ->
      # Pool not running, just close
      try do
        GRPC.Stub.disconnect(channel)
      catch
        _, _ -> :ok
      end

      :ok
  end

  @doc """
  Execute a function with a pooled connection.
  Automatically checks out and checks in the connection.
  """
  @spec with_connection(String.t(), (GRPC.Channel.t() -> result)) :: result when result: term()
  def with_connection(address, fun) do
    case checkout(address) do
      {:ok, channel} ->
        try do
          fun.(channel)
        after
          checkin(address, channel)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    # Create ETS table for pool
    :ets.new(@pool_table, [:named_table, :public, :bag])

    # Schedule periodic cleanup
    schedule_cleanup()

    Logger.info("gRPC connection pool started")
    {:ok, %{}}
  end

  @impl true
  def handle_call({:checkout, address}, _from, state) do
    result =
      case get_available_connection(address) do
        {:ok, channel} ->
          {:ok, channel}

        :none ->
          create_connection(address)
      end

    {:reply, result, state}
  end

  @impl true
  def handle_cast({:checkin, address, channel}, state) do
    # Check if connection is still valid
    if connection_valid?(channel) do
      # Return to pool with timestamp
      :ets.insert(@pool_table, {address, channel, System.monotonic_time(:millisecond)})
    else
      # Connection is dead, close it
      try do
        GRPC.Stub.disconnect(channel)
      catch
        _, _ -> :ok
      end
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_stale_connections()
    schedule_cleanup()
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private

  defp get_available_connection(address) do
    case :ets.take(@pool_table, address) do
      [{^address, channel, _timestamp} | rest] ->
        # Put back the rest
        Enum.each(rest, fn entry -> :ets.insert(@pool_table, entry) end)

        if connection_valid?(channel) do
          {:ok, channel}
        else
          # Try next one
          try do
            GRPC.Stub.disconnect(channel)
          catch
            _, _ -> :ok
          end

          get_available_connection(address)
        end

      [] ->
        :none
    end
  end

  defp create_connection(address) do
    try do
      case GRPC.Stub.connect(address, interceptors: []) do
        {:ok, channel} -> {:ok, channel}
        {:error, reason} -> {:error, reason}
      end
    rescue
      RuntimeError -> {:error, :grpc_supervisor_not_running}
    catch
      :exit, reason -> {:error, {:exit, reason}}
    end
  end

  defp connection_valid?(_channel) do
    # Simple check - could be enhanced to actually ping
    true
  end

  defp cleanup_stale_connections do
    now = System.monotonic_time(:millisecond)
    cutoff = now - @connection_ttl_ms

    # Get all stale connections
    stale =
      :ets.select(@pool_table, [
        {{:"$1", :"$2", :"$3"}, [{:<, :"$3", cutoff}], [{{:"$1", :"$2", :"$3"}}]}
      ])

    Enum.each(stale, fn {address, channel, timestamp} ->
      :ets.delete_object(@pool_table, {address, channel, timestamp})

      try do
        GRPC.Stub.disconnect(channel)
      catch
        _, _ -> :ok
      end
    end)

    if stale != [] do
      Logger.debug("Cleaned up #{length(stale)} stale gRPC connections")
    end
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval_ms)
  end
end
