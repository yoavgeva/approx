defmodule Sketch do
  @moduledoc """
  Probabilistic data structures for Elixir.

  Sketch provides a collection of space-efficient probabilistic data structures for
  streaming and large-scale data processing. Every structure is immutable, supports
  merging for distributed use, and uses pure Elixir with zero runtime dependencies.

  ## Data Structures

    * `Sketch.BloomFilter` — Set membership testing (no false negatives)
    * `Sketch.CountMinSketch` — Frequency estimation
    * `Sketch.HyperLogLog` — Cardinality estimation (count distinct)
    * `Sketch.TopK` — Most frequent items tracking
    * `Sketch.CuckooFilter` — Set membership with deletion support
    * `Sketch.Reservoir` — Uniform random sampling from a stream
    * `Sketch.MinHash` — Jaccard similarity estimation between sets
    * `Sketch.TDigest` — Streaming percentile estimation (tail-accurate)

  ## Common Patterns

  All structures follow a consistent API pattern:

      structure = Module.new(...)      # Create
      structure = Module.add(s, elem)  # Insert
      result    = Module.query(s)      # Query
      merged    = Module.merge(a, b)   # Merge for distributed use
  """
end
