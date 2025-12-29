defmodule Store.API.Endpoint do
  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  run(Store.API.DataAccess)
  run(Store.API.Reflection)
end
