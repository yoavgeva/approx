defmodule Sketch do
  @moduledoc """
  Probabilistic data structures for Elixir.

  Sketch provides space-efficient data structures for the problems that come up
  in every data-intensive application — deduplication, frequency counting,
  cardinality estimation, percentile tracking, similarity detection, and
  sampling — all in constant or sub-linear space.

  Pure Elixir. Zero runtime dependencies. Every structure is immutable,
  mergeable, and serializable.

  ## Installation

  Add `sketch` to your list of dependencies in `mix.exs`:

      def deps do
        [
          {:sketch, "~> 0.1.0"}
        ]
      end

  ## Choosing a data structure

  | Structure | Question it answers | Space |
  |---|---|---|
  | `Sketch.BloomFilter` | "Have I seen this before?" (no false negatives) | O(n) bits |
  | `Sketch.CuckooFilter` | "Have I seen this before?" (with deletion) | ~12 bits/elem |
  | `Sketch.CountMinSketch` | "How many times has X appeared?" | Fixed by error params |
  | `Sketch.TopK` | "What are the K most frequent items?" | CMS + O(k) |
  | `Sketch.HyperLogLog` | "How many distinct items have I seen?" | O(2^p) bytes |
  | `Sketch.Reservoir` | "Give me a uniform random sample of K items" | O(k) |
  | `Sketch.MinHash` | "How similar are these two sets?" | O(num_hashes) |
  | `Sketch.TDigest` | "What is the p99 latency?" | O(delta) centroids |

  ## Quick start

  ### Bloom filter — set membership

      bf = Sketch.BloomFilter.new(10_000, 0.01)

      bf =
        bf
        |> Sketch.BloomFilter.add("alice@example.com")
        |> Sketch.BloomFilter.add("bob@example.com")

      Sketch.BloomFilter.member?(bf, "alice@example.com")
      # => true

      Sketch.BloomFilter.member?(bf, "unknown@example.com")
      # => false (definitely not in the set)

  ### Cuckoo filter — set membership with deletion

      cf = Sketch.CuckooFilter.new(10_000)

      {:ok, cf} = Sketch.CuckooFilter.add(cf, "session_abc")
      Sketch.CuckooFilter.member?(cf, "session_abc")
      # => true

      {:ok, cf} = Sketch.CuckooFilter.delete(cf, "session_abc")
      Sketch.CuckooFilter.member?(cf, "session_abc")
      # => false

  ### Count-Min Sketch — frequency estimation

      cms = Sketch.CountMinSketch.new(0.001, 0.01)

      cms =
        cms
        |> Sketch.CountMinSketch.add("page_view", 3)
        |> Sketch.CountMinSketch.add("click", 1)

      Sketch.CountMinSketch.count(cms, "page_view")
      # => 3 (or slightly higher — never undercounts)

  ### Top-K — most frequent items

      tk = Sketch.TopK.new(10)

      tk =
        tk
        |> Sketch.TopK.add("/home", 100)
        |> Sketch.TopK.add("/pricing", 80)
        |> Sketch.TopK.add("/docs", 120)

      Sketch.TopK.top(tk)
      # => [{"/docs", 120}, {"/home", 100}, {"/pricing", 80}]

  ### HyperLogLog — cardinality estimation

      hll = Sketch.HyperLogLog.new(14)
      hll = Enum.reduce(1..1_000_000, hll, &Sketch.HyperLogLog.add(&2, &1))

      Sketch.HyperLogLog.count(hll)
      # => ~1_000_000 (within ~1% error with precision 14)

  ### Reservoir — uniform random sampling

      reservoir = Sketch.Reservoir.new(100)
      reservoir = Sketch.Reservoir.add_all(reservoir, 1..1_000_000)

      Sketch.Reservoir.sample(reservoir)
      # => 100 uniformly random elements from the stream

      Sketch.Reservoir.count(reservoir)
      # => 1_000_000 (total elements seen)

  ### MinHash — Jaccard similarity

      mh = Sketch.MinHash.new(128, seed: 42)

      sig_a = Sketch.MinHash.signature(mh, MapSet.new(["cat", "dog", "fish"]))
      sig_b = Sketch.MinHash.signature(mh, MapSet.new(["cat", "dog", "bird"]))

      Sketch.MinHash.similarity(sig_a, sig_b)
      # => ~0.5 (true Jaccard is 2/4 = 0.5)

  ### t-digest — streaming percentiles

      td = Sketch.TDigest.new()
      td = Enum.reduce(1..10_000, td, &Sketch.TDigest.add(&2, &1))

      Sketch.TDigest.percentile(td, 0.50)   # median
      Sketch.TDigest.percentile(td, 0.99)   # p99
      Sketch.TDigest.percentile(td, 0.999)  # p99.9

  ## Common API patterns

  All structures follow a consistent API:

      # 1. Create with parameters that control accuracy vs. space
      cms = Sketch.CountMinSketch.new(0.001, 0.01)

      # 2. Insert elements
      cms = Sketch.CountMinSketch.add(cms, "page_view")
      cms = Sketch.CountMinSketch.add(cms, "page_view")

      # 3. Query
      Sketch.CountMinSketch.count(cms, "page_view")
      # => 2

      # 4. Merge structures from different nodes
      {:ok, merged} = Sketch.CountMinSketch.merge(cms_node_a, cms_node_b)

      # 5. Serialize for storage or network transfer
      bin = Sketch.CountMinSketch.to_binary(cms)
      {:ok, restored} = Sketch.CountMinSketch.from_binary(bin)

  ## Features

    * **Pure Elixir** — no NIFs, no ports, no runtime dependencies
    * **Immutable** — all operations return new structures; safe for concurrent reads
    * **Mergeable** — combine structures from distributed nodes with `merge/2`
    * **Serializable** — every structure supports `to_binary/1` and `from_binary/1`
    * **Tested** — 600+ tests including statistical accuracy bounds and round-trip serialization
  """
end
