defmodule PD.API.Endpoint do
  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  # gRPC services
  run(PD.API.GRPC.Cluster)
  run(PD.API.GRPC.TSO)
  run(PD.API.GRPC.Schema)
  run(PD.API.GRPC.Plugin)

  # gRPC reflection (v1 and v1alpha for compatibility)
  run(PD.API.Reflection)
  run(PD.API.ReflectionV1Alpha)
end
