defmodule FastDedupe.MixProject do
  use Mix.Project

  def project do
    [
      app: :fast_dedupe,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      escript: [main_module: FastDedupe.CLI]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:exqlite, "~> 0.35"},
      {:jason, "~> 1.4"}
    ]
  end
end
