defmodule SpiredbStore.MixProject do
  use Mix.Project

  def project do
    [
      app: :spiredb_store,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ra],
      mod: {SpiredbStore.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:spiredb_common, in_umbrella: true},
      {:spiredb_pd, in_umbrella: true},
      {:rocksdb, "~> 1.9.0"},
      {:ra, "2.15.3", override: true},
      {:ranch, "~> 2.1"},
      {:redix, "~> 1.5"},
      {:libcluster, "~> 3.5"},
      # Coprocessor API dependencies
      {:grpc, "~> 0.8"},
      # Note: protobuf already includes google_protos, no need to add it separately
      {:telemetry, "~> 1.0"},
      # Arrow data interchange
      {:explorer, "~> 0.11"},
      {:adbc, "~> 0.8"},
      {:anodex, "~> 0.1.3"}
    ]
  end
end
