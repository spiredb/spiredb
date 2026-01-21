defmodule Spiredb.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  defp releases do
    [
      spiredb: [
        applications: [
          spiredb_common: :permanent,
          spiredb_pd: :permanent,
          spiredb_store: :permanent,
          observer_cli: :permanent,
          recon: :load
        ],
        include_executables_for: [:unix],
        steps: [:assemble, :tar],
        # Default cookie - can be overridden at runtime via RELEASE_COOKIE env var
        # All nodes MUST use the same cookie for cluster communication
        cookie: "spiredb_cluster_cookie"
      ]
    ]
  end

  # Dependencies listed here are available only for this
  # project and cannot be accessed from applications inside
  # the apps folder.
  #
  # Run "mix help deps" for examples and options.
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end
end
