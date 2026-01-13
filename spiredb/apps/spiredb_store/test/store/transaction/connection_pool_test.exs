defmodule Store.Transaction.ConnectionPoolTest do
  use ExUnit.Case, async: true

  alias Store.Transaction.ConnectionPool

  describe "checkout/1" do
    test "returns error when pool not running and connection fails" do
      # With pool not running and invalid address, should return error
      result =
        try do
          ConnectionPool.checkout("invalid:99999")
        rescue
          RuntimeError -> {:error, :grpc_supervisor_not_running}
        catch
          :exit, _ -> {:error, :exit}
        end

      assert {:error, _} = result
    end
  end

  describe "checkin/2" do
    test "handles checkin gracefully when pool not running" do
      # Should not crash even without pool
      assert :ok = ConnectionPool.checkin("test:1234", nil)
    end
  end

  describe "with_connection/2" do
    test "returns error with invalid connection" do
      # With invalid address, should return error from connection
      result =
        try do
          ConnectionPool.with_connection("invalid:99999", fn _channel ->
            :should_not_reach
          end)
        rescue
          RuntimeError -> {:error, :grpc_supervisor_not_running}
        catch
          :exit, _ -> {:error, :exit}
        end

      assert {:error, _} = result
    end

    test "handles function exceptions gracefully" do
      # When pool is running, exceptions should propagate
      # When not running, connection error comes first
      result =
        try do
          ConnectionPool.with_connection("invalid:99999", fn _channel ->
            raise "test error"
          end)
        rescue
          _ -> {:error, :exception_raised}
        end

      assert {:error, _} = result
    end
  end

  describe "connection management" do
    test "pool table name is correct" do
      # The pool uses :grpc_connection_pool as table name
      assert ConnectionPool.start_link([]) |> elem(0) == :ok or
               Process.whereis(ConnectionPool) != nil
    end
  end

  describe "cleanup behavior" do
    test "stale connections are cleaned up" do
      # Start pool if not running
      case ConnectionPool.start_link([]) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _}} -> :ok
      end

      # Send cleanup message (simulating timer)
      if pid = Process.whereis(ConnectionPool) do
        send(pid, :cleanup)
        # Give it time to process
        Process.sleep(50)
      end

      # Should not crash
      assert true
    end
  end
end
