defmodule PD.API.ReflectionV1Alpha do
  @moduledoc false
  use GrpcReflection.Server,
    version: :v1alpha,
    services: [
      Spiredb.Cluster.ClusterService.Service,
      Spiredb.Cluster.SchemaService.Service,
      Spiredb.Cluster.TSOService.Service,
      Spiredb.Cluster.PluginService.Service
    ]
end
