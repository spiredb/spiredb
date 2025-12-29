defmodule Store.API.ReflectionV1Alpha do
  use GrpcReflection.Server,
    version: :v1alpha,
    services: [
      SpireDb.Spiredb.Data.DataAccess.Service
    ]
end
