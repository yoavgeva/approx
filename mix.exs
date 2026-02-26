defmodule Approx.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/yoavgeva/approx"

  def project do
    [
      app: :approx,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      name: "Approx",
      description:
        "Probabilistic data structures for Elixir — Bloom filter, Count-Min Sketch, HyperLogLog, t-digest, and more."
    ]
  end

  def application do
    []
  end

  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extra_section: "GUIDES",
      extras: [
        "README.md",
        "guides/getting-started.md",
        "guides/choosing-a-data-structure.md",
        "guides/distributed-merging.md",
        "guides/accuracy-and-tuning.md",
        "cheatsheets/approx.cheatmd",
        "LICENSE"
      ],
      groups_for_extras: [
        Guides: [
          "guides/getting-started.md",
          "guides/choosing-a-data-structure.md",
          "guides/distributed-merging.md",
          "guides/accuracy-and-tuning.md"
        ],
        Cheatsheets: [
          "cheatsheets/approx.cheatmd"
        ]
      ],
      groups_for_modules: [
        "Set Membership": [Approx.BloomFilter, Approx.CuckooFilter],
        "Frequency & Ranking": [Approx.CountMinSketch, Approx.TopK],
        Cardinality: [Approx.HyperLogLog],
        "Sampling & Similarity": [Approx.Reservoir, Approx.MinHash],
        Distribution: [Approx.TDigest]
      ]
    ]
  end
end
