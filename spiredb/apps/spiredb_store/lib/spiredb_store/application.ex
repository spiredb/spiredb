defmodule SpiredbStore.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Ensure Ra is started first (required for PD and regions)
    Application.ensure_all_started(:ra)

    # Build cluster topology based on SPIRE_DISCOVERY env var
    topology = build_cluster_topology()

    resp_port = Application.get_env(:spiredb_store, :resp_port, 6379)

    children = [
      # Node discovery (libcluster)
      {Cluster.Supervisor, [topology, [name: Spiredb.ClusterSupervisor]]},

      # PD for cluster metadata (moved to spiredb_pd)
      # {PD.Supervisor, []},

      # Store for data management (manages regions)
      {Store.Supervisor, []},

      # Iterator Pool for frequent scans
      {Store.KV.IteratorPool, []},

      # Task executor for scheduled tasks from PD
      {Store.TaskExecutor, []},

      # RESP server
      {Store.API.RESP.Supervisor, [port: resp_port]},

      # DataAccess gRPC server
      {GRPC.Server.Supervisor, endpoint: Store.API.Endpoint, port: 50052, start_server: true}
    ]

    opts = [strategy: :one_for_one, name: SpiredbStore.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp build_cluster_topology do
    discovery = System.get_env("SPIRE_DISCOVERY", "epmd")

    strategy_config =
      case discovery do
        "k8sdns" ->
          # DNS discovery using headless service - will discover spiredb-0, spiredb-1, etc.
          [
            strategy: Cluster.Strategy.Kubernetes.DNS,
            config: [
              service: System.get_env("SPIRE_SERVICE_NAME", "spiredb-headless"),
              application_name: "spiredb",
              kubernetes_namespace: System.get_env("SPIRE_NAMESPACE", "default"),
              polling_interval: 5_000
            ]
          ]

        "gossip" ->
          [
            strategy: Cluster.Strategy.Gossip,
            config: [
              port: 45892,
              multicast_addr: System.get_env("SPIRE_MULTICAST_ADDR", "230.1.1.251"),
              multicast_ttl: 1
            ]
          ]

        "dnspoll" ->
          [
            strategy: Cluster.Strategy.DNSPoll,
            config: [
              query: System.get_env("SPIRE_DNS_QUERY", "spiredb.local"),
              interval: 5_000
            ]
          ]

        "epmd" ->
          # For local development/testing with EPMD
          hosts =
            System.get_env("SPIRE_CLUSTER_NODES", "")
            |> String.split(",", trim: true)
            |> Enum.map(&String.to_atom/1)

          [
            strategy: Cluster.Strategy.Epmd,
            config: [hosts: hosts]
          ]

        _ ->
          IO.puts("Unknown SPIRE_DISCOVERY: #{discovery}, using epmd with no hosts")
          [strategy: Cluster.Strategy.Epmd, config: [hosts: []]]
      end

    [spiredb: strategy_config]
  end
end
