defmodule Approx do
  @moduledoc """
  Probabilistic data structures for Elixir.

  Approx provides space-efficient data structures for the problems that come up
  in every data-intensive application — deduplication, frequency counting,
  cardinality estimation, percentile tracking, similarity detection, and
  sampling — all in constant or sub-linear space.

  Pure Elixir. Zero runtime dependencies. Every structure is immutable,
  mergeable, and serializable.

  ## Installation

  Add `approx` to your list of dependencies in `mix.exs`:

      def deps do
        [
          {:approx, "~> 0.1.0"}
        ]
      end

  ## Choosing a data structure

  | Structure | Question it answers | Space |
  |---|---|---|
  | `Approx.BloomFilter` | "Have I seen this before?" (no false negatives) | O(n) bits |
  | `Approx.CuckooFilter` | "Have I seen this before?" (with deletion) | ~12 bits/elem |
  | `Approx.CountMinSketch` | "How many times has X appeared?" | Fixed by error params |
  | `Approx.TopK` | "What are the K most frequent items?" | CMS + O(k) |
  | `Approx.HyperLogLog` | "How many distinct items have I seen?" | O(2^p) bytes |
  | `Approx.Reservoir` | "Give me a uniform random sample of K items" | O(k) |
  | `Approx.MinHash` | "How similar are these two sets?" | O(num_hashes) |
  | `Approx.TDigest` | "What is the p99 latency?" | O(delta) centroids |

  ## Quick start

  ### Bloom filter — set membership

      bf = Approx.BloomFilter.new(10_000, 0.01)

      bf =
        bf
        |> Approx.BloomFilter.add("alice@example.com")
        |> Approx.BloomFilter.add("bob@example.com")

      Approx.BloomFilter.member?(bf, "alice@example.com")
      # => true

      Approx.BloomFilter.member?(bf, "unknown@example.com")
      # => false (definitely not in the set)

  ### Cuckoo filter — set membership with deletion

      cf = Approx.CuckooFilter.new(10_000)

      {:ok, cf} = Approx.CuckooFilter.add(cf, "session_abc")
      Approx.CuckooFilter.member?(cf, "session_abc")
      # => true

      {:ok, cf} = Approx.CuckooFilter.delete(cf, "session_abc")
      Approx.CuckooFilter.member?(cf, "session_abc")
      # => false

  ### Count-Min Sketch — frequency estimation

      cms = Approx.CountMinSketch.new(0.001, 0.01)

      cms =
        cms
        |> Approx.CountMinSketch.add("page_view", 3)
        |> Approx.CountMinSketch.add("click", 1)

      Approx.CountMinSketch.count(cms, "page_view")
      # => 3 (or slightly higher — never undercounts)

  ### Top-K — most frequent items

      tk = Approx.TopK.new(10)

      tk =
        tk
        |> Approx.TopK.add("/home", 100)
        |> Approx.TopK.add("/pricing", 80)
        |> Approx.TopK.add("/docs", 120)

      Approx.TopK.top(tk)
      # => [{"/docs", 120}, {"/home", 100}, {"/pricing", 80}]

  ### HyperLogLog — cardinality estimation

      hll = Approx.HyperLogLog.new(14)
      hll = Enum.reduce(1..1_000_000, hll, &Approx.HyperLogLog.add(&2, &1))

      Approx.HyperLogLog.count(hll)
      # => ~1_000_000 (within ~1% error with precision 14)

  ### Reservoir — uniform random sampling

      reservoir = Approx.Reservoir.new(100)
      reservoir = Approx.Reservoir.add_all(reservoir, 1..1_000_000)

      Approx.Reservoir.sample(reservoir)
      # => 100 uniformly random elements from the stream

      Approx.Reservoir.count(reservoir)
      # => 1_000_000 (total elements seen)

  ### MinHash — Jaccard similarity

      mh = Approx.MinHash.new(128, seed: 42)

      sig_a = Approx.MinHash.signature(mh, MapSet.new(["cat", "dog", "fish"]))
      sig_b = Approx.MinHash.signature(mh, MapSet.new(["cat", "dog", "bird"]))

      Approx.MinHash.similarity(sig_a, sig_b)
      # => ~0.5 (true Jaccard is 2/4 = 0.5)

  ### t-digest — streaming percentiles

      td = Approx.TDigest.new()
      td = Enum.reduce(1..10_000, td, &Approx.TDigest.add(&2, &1))

      Approx.TDigest.percentile(td, 0.50)   # median
      Approx.TDigest.percentile(td, 0.99)   # p99
      Approx.TDigest.percentile(td, 0.999)  # p99.9

  ## Common API patterns

  All structures follow a consistent API:

      # 1. Create with parameters that control accuracy vs. space
      cms = Approx.CountMinSketch.new(0.001, 0.01)

      # 2. Insert elements
      cms = Approx.CountMinSketch.add(cms, "page_view")
      cms = Approx.CountMinSketch.add(cms, "page_view")

      # 3. Query
      Approx.CountMinSketch.count(cms, "page_view")
      # => 2

      # 4. Merge structures from different nodes
      {:ok, merged} = Approx.CountMinSketch.merge(cms_node_a, cms_node_b)

      # 5. Serialize for storage or network transfer
      bin = Approx.CountMinSketch.to_binary(cms)
      {:ok, restored} = Approx.CountMinSketch.from_binary(bin)

  ## Features

    * **Pure Elixir** — no NIFs, no ports, no runtime dependencies
    * **Mergeable** — combine structures from distributed nodes with `merge/2`
    * **Serializable** — every structure supports `to_binary/1` and `from_binary/1`
    * **Tested** — 600+ tests including statistical accuracy bounds and round-trip serialization
  """
end
