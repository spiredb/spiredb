defmodule PD.API.Endpoint do
  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  run(PD.API.GRPC)
  run(PD.API.Reflection)
end
