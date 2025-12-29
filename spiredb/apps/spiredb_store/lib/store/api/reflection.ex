defmodule Store.API.Reflection do
  use GrpcReflection.Server,
    version: :v1,
    services: [
      SpireDb.Spiredb.Data.DataAccess.Service
    ]
end
