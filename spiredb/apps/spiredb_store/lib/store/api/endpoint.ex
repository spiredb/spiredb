defmodule Store.API.Endpoint do
  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  # gRPC services
  run(Store.API.DataAccess)
  run(Store.API.GRPC.Transaction)
  run(Store.API.GRPC.Vector)

  # Internal store-to-store transaction coordination
  run(Store.API.InternalTransaction)

  # gRPC reflection (v1 and v1alpha for compatibility)
  run(Store.API.Reflection)
  run(Store.API.ReflectionV1Alpha)
end
