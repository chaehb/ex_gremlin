defmodule ExGremlin.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_gremlin,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      source_url: "https://github.com/chaehb/ex_gremlin",
      docs: [
        main: "ExGremlin",
        logo: "logo.png",
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :eex],
      mod: {ExGremlin.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gun, "~> 2.0.0-rc.1"},
      {:uuid, "~> 2.0", hex: :uuid_erl},
      {:jason, "~> 1.2"},
      {:ex_doc, "~> 0.23.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    "An Elixir client for Gremlin (Apache TinkerPop™), inspired by Gremlex."
  end

  defp package() do
    [
      name: "ex_gremlin",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/chaehb/ex_gremlin"}
    ]
  end
end
