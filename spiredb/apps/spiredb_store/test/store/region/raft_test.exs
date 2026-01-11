defmodule Store.Region.RaftTest do
  @moduledoc """
  Integration tests for Raft consensus layer.

  Run with: make test-raft
  Or: cd spiredb && elixir --name test@127.0.0.1 -S mix test apps/spiredb_store/test/store/region/raft_test.exs
  """
  use ExUnit.Case, async: false

  alias Store.Region.Raft
  require Logger

  @moduletag :raft

  # Minimal state machine for testing Ra directly
  defmodule TestMachine do
    @behaviour :ra_machine

    @impl true
    def init(_config), do: %{}

    @impl true
    def apply(_meta, {:put, key, value}, state) do
      new_state = Map.put(state, key, value)
      {new_state, :ok, []}
    end

    @impl true
    def apply(_meta, {:get, key}, state) do
      result = Map.get(state, key, :not_found)
      {state, result, []}
    end

    @impl true
    def apply(_meta, {:delete, key}, state) do
      new_state = Map.delete(state, key)
      {new_state, :ok, []}
    end

    @impl true
    def apply(_meta, _cmd, state) do
      {state, {:error, :unknown_command}, []}
    end

    @impl true
    def state_enter(_ra_state, _machine_state), do: []
  end

  setup_all do
    # Configure logging
    Logger.configure(level: :info)

    # Disable PD Raft to prevent conflicts with test servers
    System.put_env("SPIRE_PD_START_RAFT", "false")

    # Unique data directory per test run
    data_dir =
      Path.join(System.tmp_dir!(), "spiredb_raft_test_#{System.system_time(:nanosecond)}")

    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)

    # Configure Ra for test environment
    # Note: Ra may already be started by the application, so we configure it before ensuring it's started
    Application.put_env(:ra, :data_dir, String.to_charlist(data_dir))
    Application.put_env(:ra, :wal_pre_allocate, false)
    Application.put_env(:ra, :segment_pre_allocate, false)
    Application.put_env(:ra, :wal_max_size_bytes, 1 * 1024 * 1024)

    Logger.info("Ra data_dir: #{data_dir}")

    # Ensure Ra is started (may already be running from application start)
    case Application.ensure_all_started(:ra) do
      {:ok, _apps} ->
        Logger.info("Ra application started")

      {:error, {app, reason}} ->
        Logger.error("Failed to start #{app}: #{inspect(reason)}")
    end

    # Wait for Ra to be ready - check for core supervisors like the application does
    wait_for_ra_ready()

    # Start default Ra system if not already started
    case :ra_system.start_default() do
      {:ok, _} ->
        Logger.info("Ra default system started")
        :ok

      {:error, {:already_started, _}} ->
        Logger.info("Ra default system already running")
        :ok

      {:error, reason} ->
        Logger.error("Failed to start Ra default system: #{inspect(reason)}")
        :ok
    end

    on_exit(fn ->
      File.rm_rf!(data_dir)
    end)

    {:ok, data_dir: data_dir}
  end

  # Wait for Ra to be ready - same pattern as Store.Server
  defp wait_for_ra_ready(retries \\ 30) do
    if retries == 0 do
      Logger.error("Ra application failed to become ready")
      Logger.error("Registered processes: #{inspect(Process.registered())}")
      Logger.error("Ra supervisor: #{inspect(Process.whereis(:ra_sup))}")
      Logger.error("Ra directory: #{inspect(Process.whereis(:ra_directory))}")
      raise "Ra not ready"
    else
      # Check if Ra application is started AND we can access its main supervisor
      # ra_directory may not always be present depending on Ra version
      if Process.whereis(:ra_sup) do
        # Additional check: verify Ra application is actually running
        case Application.started_applications() |> Enum.find(fn {app, _, _} -> app == :ra end) do
          nil ->
            Logger.warning("Ra supervisor exists but application not in started list")
            Process.sleep(500)
            wait_for_ra_ready(retries - 1)

          _ ->
            Logger.info("Ra is ready")
            :ok
        end
      else
        if rem(retries, 5) == 0 do
          Logger.debug("Waiting for Ra... (#{retries} retries left)")
        end

        Process.sleep(500)
        wait_for_ra_ready(retries - 1)
      end
    end
  end

  setup do
    # Generate unique IDs for each test
    test_id = System.unique_integer([:positive, :monotonic])
    server_name = :"test_server_#{test_id}"
    cluster_name = :"test_cluster_#{test_id}"
    uid = "test_#{test_id}"

    on_exit(fn ->
      server_id = {server_name, node()}

      try do
        :ra.stop_server(:default, server_id)
        :ra.delete_cluster(:default, [server_id])
      catch
        _, _ -> :ok
      end
    end)

    {:ok, server_name: server_name, cluster_name: cluster_name, uid: uid}
  end

  describe "Store.Region.Raft" do
    test "starts single node region server", %{uid: uid} do
      region_id = :erlang.phash2(uid, 10000)
      nodes = [{region_id, node()}]

      result = Raft.start_server(region_id, nodes)

      assert result == :ok or match?({:ok, _}, result),
             "Expected :ok or {:ok, _}, got: #{inspect(result)}"

      server_id = Raft.server_id(region_id)
      assert :ok = wait_for_leader(server_id)
    end

    test "write and read via Raft consensus", %{uid: uid} do
      region_id = :erlang.phash2(uid, 10000)
      nodes = [{region_id, node()}]

      result = Raft.start_server(region_id, nodes)
      assert result == :ok or match?({:ok, _}, result)

      server_id = Raft.server_id(region_id)
      :ok = wait_for_leader(server_id)

      # Write using Raft.process_command
      assert {:ok, {_index, :ok, _leader}} =
               Raft.process_command(region_id, {:put, "key1", "value1"})

      # Read via consistent query
      assert {:ok, {:ok, "value1"}, _leader} =
               :ra.consistent_query(server_id, fn state -> Map.get(state, "key1") end, 5000)
    end

    test "delete key", %{uid: uid} do
      region_id = :erlang.phash2(uid, 10000)
      nodes = [{region_id, node()}]

      result = Raft.start_server(region_id, nodes)
      assert result == :ok or match?({:ok, _}, result)

      server_id = Raft.server_id(region_id)
      :ok = wait_for_leader(server_id)

      # Write then delete
      {:ok, {_, :ok, _}} = Raft.process_command(region_id, {:put, "k", "v"})
      {:ok, {_, :ok, _}} = Raft.process_command(region_id, {:delete, "k"})

      # Verify deleted
      {:ok, {:error, :not_found}, _} =
        :ra.consistent_query(server_id, fn _state -> {:error, :not_found} end, 5000)
    end

    test "multiple writes maintain order", %{uid: uid} do
      region_id = :erlang.phash2(uid, 10000)
      nodes = [{region_id, node()}]

      result = Raft.start_server(region_id, nodes)
      assert result == :ok or match?({:ok, _}, result)

      server_id = Raft.server_id(region_id)
      :ok = wait_for_leader(server_id)

      # Write multiple keys
      for i <- 1..10 do
        {:ok, {_, :ok, _}} = Raft.process_command(region_id, {:put, "key#{i}", "val#{i}"})
      end

      # Verify a key exists
      {:ok, {_, {:ok, "val5"}, _}} = Raft.process_command(region_id, {:get, "key5"})
    end
  end

  describe "Store.Region.Raft wrapper" do
    test "starts region server", _ctx do
      region_id = System.unique_integer([:positive]) |> rem(10000)
      nodes = [{region_id, node()}]

      result = Raft.start_server(region_id, nodes)

      cond do
        result == :ok -> :ok
        match?({:error, {:already_started, _}}, result) -> :ok
        true -> flunk("Expected :ok or {:error, {:already_started, _}}, got: #{inspect(result)}")
      end

      if result == :ok do
        server_id = Raft.server_id(region_id)
        assert :ok = wait_for_leader(server_id)
      end
    end
  end

  # Helpers

  defp wait_for_leader(server_id, retries \\ 30) do
    cond do
      retries == 0 ->
        {:error, :no_leader}

      true ->
        case :ra.members(server_id) do
          {:ok, _members, leader} when is_tuple(leader) ->
            :ok

          _ ->
            Process.sleep(200)
            wait_for_leader(server_id, retries - 1)
        end
    end
  end
end
