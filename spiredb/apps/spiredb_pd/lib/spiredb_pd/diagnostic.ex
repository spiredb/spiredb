defmodule SpiredbPd.Diagnostic do
  @moduledoc false
  require Logger

  def run do
    Logger.warning("=== SpireDB Diagnostic Mode Enabled ===")

    check_io_latency()
    check_raft_status()

    # Note: Minimal raft start test omitted as it might conflict with running system if not careful
    # But simple IO check is most valuable here.
  end

  defp check_io_latency do
    Logger.info("--- Checking Disk I/O Latency ---")

    # Use the configured Raft dir
    base_dir = Application.get_env(:spiredb_pd, :pd_data_dir, "/var/lib/spiredb/ra/pd")
    path = Path.join(Path.dirname(base_dir), "diag_test.bin")

    # Ensure dir exists
    File.mkdir_p!(Path.dirname(path))

    {u_time, _} =
      :timer.tc(fn ->
        File.write!(path, :binary.copy(<<0>>, 64 * 1024 * 1024), [:sync])
      end)

    ms = u_time / 1000
    Logger.info("Diagnostic: Write 64MB (sync) took #{ms} ms")

    if ms > 5000 do
      Logger.error("Diagnostic WARNING: Disk write is extremely slow! (#{ms}ms)")
    else
      Logger.info("Diagnostic: Disk write speed looks acceptable.")
    end

    File.rm(path)
  end

  defp check_raft_status do
    Logger.info("--- Checking Raft Process Status ---")

    case Process.whereis(:ra_sup) do
      nil -> Logger.error("Diagnostic ERROR: :ra_sup is NOT running!")
      pid -> Logger.info("Diagnostic: :ra_sup is running at #{inspect(pid)}")
    end
  end
end
