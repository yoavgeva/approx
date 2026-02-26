defmodule Sketch.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/yoavgeva/sketch"

  def project do
    [
      app: :sketch,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      name: "Sketch",
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
        "cheatsheets/sketch.cheatmd",
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
          "cheatsheets/sketch.cheatmd"
        ]
      ],
      groups_for_modules: [
        "Set Membership": [Sketch.BloomFilter, Sketch.CuckooFilter],
        "Frequency & Ranking": [Sketch.CountMinSketch, Sketch.TopK],
        Cardinality: [Sketch.HyperLogLog],
        "Sampling & Similarity": [Sketch.Reservoir, Sketch.MinHash],
        Distribution: [Sketch.TDigest]
      ]
    ]
  end
end
