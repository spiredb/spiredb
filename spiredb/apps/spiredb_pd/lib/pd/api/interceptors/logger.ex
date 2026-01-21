defmodule PD.API.Interceptors.Logger do
  @moduledoc """
  Custom logger interceptor that reduces verbosity for frequent polling methods.
  """

  require Logger

  @behaviour GRPC.Server.Interceptor

  @impl true
  def init(opts) do
    Keyword.validate!(opts, level: :info)
  end

  @impl true
  def call(req, stream, next, opts) do
    default_level = Keyword.fetch!(opts, :level)

    # Determine level based on RPC method
    # Downgrade frequent polling methods to :debug to reduce log noise
    level =
      case {stream.server, elem(stream.rpc, 0)} do
        {PD.API.GRPC.Schema, :list_tables} -> :debug
        {PD.API.GRPC.Cluster, :list_stores} -> :debug
        _ -> default_level
      end

    if Logger.compare_levels(level, Logger.level()) != :lt do
      Logger.metadata(request_id: Logger.metadata()[:request_id] || stream.request_id)

      Logger.log(level, "Handled by #{inspect(stream.server)}.#{elem(stream.rpc, 0)}")

      start = System.monotonic_time()
      result = next.(req, stream)
      stop = System.monotonic_time()

      status = elem(result, 0)
      diff = System.convert_time_unit(stop - start, :native, :microsecond)

      Logger.log(level, "Response #{inspect(status)} in #{formatted_diff(diff)}")

      result
    else
      next.(req, stream)
    end
  end

  def formatted_diff(diff) when diff > 1000, do: [diff |> div(1000) |> Integer.to_string(), "ms"]
  def formatted_diff(diff), do: [Integer.to_string(diff), "µs"]
end
