defmodule PD.API.GRPC.TSO do
  @moduledoc """
  gRPC TSOService implementation.
  """

  use GRPC.Server, service: Spiredb.Cluster.TSOService.Service

  require Logger
  alias PD.TSO

  alias Spiredb.Cluster.GetTimestampResponse

  def get_timestamp(request, _stream) do
    count = max(request.count, 1)

    case TSO.get_timestamps(count) do
      {:ok, start_ts, count} ->
        %GetTimestampResponse{start_ts: start_ts, count: count}

      {:error, reason} ->
        Logger.error("TSO allocation failed: #{inspect(reason)}")
        raise GRPC.RPCError, status: :internal, message: "TSO allocation failed"
    end
  end
end
