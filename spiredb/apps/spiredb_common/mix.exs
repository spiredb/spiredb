defmodule SpiredbCommon.MixProject do
  use Mix.Project

  def project do
    [
      app: :spiredb_common,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # Ignore xref warnings for optional Ra dependency
      xref: [exclude: [:ra, :ra_system, :ra_directory]]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {SpiredbCommon.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:protobuf, "~> 0.12"},
      {:grpc, "~> 0.7"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:logger_json, "~> 5.1"},
      {:grpc_reflection, "~> 0.3.0"},
      {:ra, "~> 2.6", optional: true}
    ]
  end
end
