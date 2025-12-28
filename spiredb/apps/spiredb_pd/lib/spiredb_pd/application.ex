defmodule SpiredbPd.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Run diagnostics if enabled
    if System.get_env("SPIRE_DIAG") == "true" do
      SpiredbPd.Diagnostic.run()
    end

    # PD.Server is a Ra machine, managed by Ra cluster
    # It's started via Ra APIs, not supervised here

    # In releases, always start production services
    # Use config :spiredb_pd, :disable_services, true to disable in tests

    # Explicitly start Ra default system if not already running
    # Directory creation and config handled in runtime.exs before applications start

    # Start isolated Ra system for PD
    # Correction: Use start/1 with full config including derived names
    base_dir =
      Application.get_env(:ra, :data_dir) ||
        String.to_charlist(System.get_env("SPIRE_RA_DATA_DIR", "/tmp/spiredb/ra"))

    pd_dir = Path.join(List.to_string(base_dir), "pd")
    File.mkdir_p!(pd_dir)
    pd_dir_charlist = String.to_charlist(pd_dir)

    sys_name = :pd_system
    names = :ra_system.derive_names(sys_name)

    pd_config =
      :ra_system.default_config()
      |> Map.merge(%{
        name: sys_name,
        names: names,
        data_dir: pd_dir_charlist,
        wal_data_dir: pd_dir_charlist,
        wal_max_size_bytes: 64 * 1024 * 1024,
        wal_pre_allocate: false,
        wal_write_strategy: :default
      })

    case :ra_system.start(pd_config) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      error -> IO.warn("Failed to start PD Raft system: #{inspect(error)}")
    end

    case :ra_system.start_default() do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :ok

      {:error, reason} ->
        # Log error but don't crash yet, let supervisors handle it
        IO.warn("Failed to start Ra default system: #{inspect(reason)}")
    end

    children =
      if Application.get_env(:spiredb_pd, :disable_services, false) do
        # Services disabled (for testing)
        []
      else
        # Start gRPC API server and Scheduler
        [
          PD.Supervisor,
          PD.API.GRPCServerSupervisor,
          PD.Scheduler
        ]
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SpiredbPd.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
