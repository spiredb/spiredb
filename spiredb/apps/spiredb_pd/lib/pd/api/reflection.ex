defmodule PD.API.Reflection do
  use GrpcReflection.Server,
    version: :v1,
    services: [
      SpireDb.Spiredb.Pd.PlacementDriver.Service
    ]
end
