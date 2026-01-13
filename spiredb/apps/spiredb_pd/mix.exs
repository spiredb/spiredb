defmodule SpiredbPd.MixProject do
  use Mix.Project

  def project do
    [
      app: :spiredb_pd,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # Ignore xref warnings for Ra (compile-time module resolution)
      xref: [exclude: [:ra, :ra_system, :ra_directory]]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ra],
      mod: {SpiredbPd.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:spiredb_common, in_umbrella: true},
      {:ra, "2.15.3", override: true},
      {:libcluster, "~> 3.5"},
      {:prometheus_ex, "~> 3.1"},
      {:opentelemetry, "~> 1.3"},
      {:opentelemetry_exporter, "~> 1.3"}
    ]
  end
end
