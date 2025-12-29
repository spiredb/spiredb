defmodule PD.API.GRPCServerSupervisor do
  @moduledoc """
  Supervisor for PD gRPC metadata service.

  Manages the gRPC server lifecycle for Placement Driver metadata queries.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Get gRPC port from config, default to 50051
    port = Application.get_env(:spiredb_pd, :grpc_port, 50051)

    children = [
      # gRPC server for PD metadata service
      {GRPC.Server.Supervisor, endpoint: PD.API.Endpoint, port: port, start_server: true}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
