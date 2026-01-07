defmodule Store.API.Reflection do
  use GrpcReflection.Server,
    version: :v1,
    services: [
      Spiredb.Data.DataAccess.Service,
      Spiredb.Data.TransactionService.Service,
      Spiredb.Data.VectorService.Service
    ]
end
