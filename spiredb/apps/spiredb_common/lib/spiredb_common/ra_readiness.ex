defmodule SpiredbCommon.RaReadiness do
  @moduledoc """
  Shared utilities for Ra cluster readiness checks.

  Provides consistent waiting logic for Ra systems and servers
  used by both PD and Store applications.
  """
  require Logger

  @default_timeout 30_000
  @check_interval 500

  @doc """
  Wait for Ra system to be ready.

  ## Options
  - `:timeout` - Maximum time to wait in ms (default: 30_000)

  Returns `:ok` or `{:error, :timeout}`
  """
  @spec wait_for_ra_system(atom(), non_neg_integer()) :: :ok | {:error, :timeout}
  def wait_for_ra_system(system \\ :default, timeout \\ @default_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_ra_system(system, deadline)
  end

  defp do_wait_ra_system(system, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      if ra_system_ready?(system) do
        :ok
      else
        Process.sleep(@check_interval)
        do_wait_ra_system(system, deadline)
      end
    end
  end

  defp ra_system_ready?(:default) do
    Process.whereis(:ra_sup) != nil
  end

  defp ra_system_ready?(system) when is_atom(system) do
    # Check if named Ra system is running by looking up its log supervisor
    case :ra_system.lookup_name(system, :ra_log_sup) do
      {:ok, pid} when is_pid(pid) -> true
      _ -> false
    end
  end

  @doc """
  Wait for a specific Ra server to be ready (able to respond to queries).

  Returns `:ok` or `{:error, :timeout}`
  """
  @spec wait_for_server(term(), non_neg_integer()) :: :ok | {:error, :timeout}
  def wait_for_server(server_id, timeout \\ @default_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_server(server_id, deadline)
  end

  defp do_wait_server(server_id, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      case safe_members_query(server_id) do
        :ok ->
          :ok

        :not_ready ->
          Process.sleep(@check_interval)
          do_wait_server(server_id, deadline)
      end
    end
  end

  defp safe_members_query(server_id) do
    try do
      case :ra.members(server_id, 1000) do
        {:ok, _members, _leader} -> :ok
        {:ok, _members} -> :ok
        _ -> :not_ready
      end
    catch
      :exit, _ -> :not_ready
      _, _ -> :not_ready
    end
  end

  @doc """
  Check if Ra system is currently ready (non-blocking).
  """
  @spec ra_ready?(atom()) :: boolean()
  def ra_ready?(system \\ :default) do
    ra_system_ready?(system)
  end
end
