defmodule PD.API.Reflection do
  @moduledoc false
  use GrpcReflection.Server,
    version: :v1,
    services: [
      Spiredb.Cluster.ClusterService.Service,
      Spiredb.Cluster.SchemaService.Service,
      Spiredb.Cluster.TSOService.Service,
      Spiredb.Cluster.PluginService.Service
    ]
end
