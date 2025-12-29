defmodule PD.API.ReflectionV1Alpha do
  use GrpcReflection.Server,
    version: :v1alpha,
    services: [
      SpireDb.Spiredb.Pd.PlacementDriver.Service
    ]
end
