defmodule Store.API.ReflectionV1Alpha do
  use GrpcReflection.Server,
    version: :v1alpha,
    services: [
      Spiredb.Data.DataAccess.Service,
      Spiredb.Data.TransactionService.Service,
      Spiredb.Data.VectorService.Service
    ]
end
