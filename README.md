# Approx

[![CI](https://github.com/yoavgeva/approx/actions/workflows/ci.yml/badge.svg)](https://github.com/yoavgeva/approx/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/approx.svg)](https://hex.pm/packages/approx)
[![Docs](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/approx)

**Probabilistic data structures for Elixir.** Pure Elixir, zero runtime
dependencies, immutable, mergeable, and serializable.

Approx gives you battle-tested sketches and samplers for the problems that come
up in every data-intensive Elixir application — deduplication, frequency
counting, cardinality estimation, percentile tracking, similarity detection, and
sampling — all in constant or sub-linear space.

## Installation

```elixir
def deps do
  [
    {:approx, "~> 0.1.0"}
  ]
end
```

## What's Included

| Structure | Answers the question... | Module |
|---|---|---|
| **Bloom Filter** | "Have I seen this before?" (no false negatives) | `Approx.BloomFilter` |
| **Cuckoo Filter** | "Have I seen this before?" (supports deletion) | `Approx.CuckooFilter` |
| **Count-Min Sketch** | "How many times has X appeared?" | `Approx.CountMinSketch` |
| **Top-K** | "What are the K most frequent items?" | `Approx.TopK` |
| **HyperLogLog** | "How many distinct items have I seen?" | `Approx.HyperLogLog` |
| **Reservoir Sampling** | "Give me a uniform random sample of K items" | `Approx.Reservoir` |
| **MinHash** | "How similar are these two sets?" | `Approx.MinHash` |
| **t-digest** | "What is the p99 latency?" | `Approx.TDigest` |

## Examples

### Bloom Filter — deduplicate webhook deliveries

Prevent processing the same webhook twice. The filter uses a few KB of memory
instead of storing every event ID in a database or ETS table.

```elixir
bf = Approx.BloomFilter.new(1_000_000, 0.001)

# On each incoming webhook
event_id = "evt_abc123"

if Approx.BloomFilter.member?(bf, event_id) do
  # Already processed — skip
  :duplicate
else
  bf = Approx.BloomFilter.add(bf, event_id)
  # Process the webhook...
  :ok
end
```

### Cuckoo Filter — manage a blocklist with removals

Like a Bloom filter, but you can remove entries — useful for a dynamic IP
blocklist where addresses are blocked and later unblocked.

```elixir
cf = Approx.CuckooFilter.new(100_000)

# Block an IP
{:ok, cf} = Approx.CuckooFilter.add(cf, "192.168.1.42")
Approx.CuckooFilter.member?(cf, "192.168.1.42")  # => true

# Unblock it later
{:ok, cf} = Approx.CuckooFilter.delete(cf, "192.168.1.42")
Approx.CuckooFilter.member?(cf, "192.168.1.42")  # => false
```

### Count-Min Sketch — rate limiting by API key

Track how many requests each API key has made without storing per-key counters
for millions of keys.

```elixir
cms = Approx.CountMinSketch.new(0.001, 0.01)

# On each API request
api_key = "key_xyz789"
cms = Approx.CountMinSketch.add(cms, api_key)

if Approx.CountMinSketch.count(cms, api_key) > 1000 do
  # Rate limit exceeded
  {:error, :rate_limited}
else
  {:ok, :allowed}
end
```

### Top-K — find the most popular pages in real time

Track the top 10 most viewed pages across your application without storing
every page view.

```elixir
tk = Approx.TopK.new(10)

# On each page view
tk = Approx.TopK.add(tk, "/docs/getting-started")
tk = Approx.TopK.add(tk, "/pricing")
tk = Approx.TopK.add(tk, "/docs/getting-started")

Approx.TopK.top(tk)
# => [{"/docs/getting-started", 2}, {"/pricing", 1}]
```

### HyperLogLog — count unique visitors across nodes

Each web server maintains its own HyperLogLog. At the end of the day, merge
them for a global distinct count using only ~16 KB of memory.

```elixir
hll_node_a = Approx.HyperLogLog.new(14)
hll_node_a = Enum.reduce(user_ids_node_a, hll_node_a, &Approx.HyperLogLog.add(&2, &1))

hll_node_b = Approx.HyperLogLog.new(14)
hll_node_b = Enum.reduce(user_ids_node_b, hll_node_b, &Approx.HyperLogLog.add(&2, &1))

{:ok, merged} = Approx.HyperLogLog.merge(hll_node_a, hll_node_b)
Approx.HyperLogLog.count(merged)
# => ~2_500_000 unique visitors (estimated, <1% error)
```

### Reservoir Sampling — sample logs for debugging

Keep a random sample of 100 log entries from a stream of millions, without
knowing the stream length in advance. Every entry has an equal chance of being
in the sample.

```elixir
reservoir = Approx.Reservoir.new(100)

# In a log processing pipeline
reservoir = Enum.reduce(log_entries, reservoir, &Approx.Reservoir.add(&2, &1))

sampled = Approx.Reservoir.sample(reservoir)
# => 100 uniformly random log entries from the full stream
```

### MinHash — detect near-duplicate articles

Compute compact signatures of article word sets, then estimate similarity
in constant time — no need to compare full texts.

```elixir
mh = Approx.MinHash.new(128, seed: 42)

words_a = article_a |> String.split() |> MapSet.new()
words_b = article_b |> String.split() |> MapSet.new()

sig_a = Approx.MinHash.signature(mh, words_a)
sig_b = Approx.MinHash.signature(mh, words_b)

Approx.MinHash.similarity(sig_a, sig_b)
# => 0.82 (82% similar — likely a near-duplicate)
```

### t-digest — monitor API latency SLAs

Track latency percentiles with high accuracy at the tails — exactly where SLA
violations happen. Two digests from different time windows can be merged.

```elixir
td = Approx.TDigest.new()

# Record latencies in milliseconds
td = Enum.reduce(request_latencies_ms, td, &Approx.TDigest.add(&2, &1))

Approx.TDigest.percentile(td, 0.50)   # => p50 (median)
Approx.TDigest.percentile(td, 0.95)   # => p95
Approx.TDigest.percentile(td, 0.99)   # => p99
Approx.TDigest.percentile(td, 0.999)  # => p99.9

# Merge hourly digests into a daily summary
daily = Approx.TDigest.merge(hour_1_digest, hour_2_digest)
```

## Key Features

- **Pure Elixir** — no NIFs, no ports, no runtime dependencies
- **Immutable** — all operations return new structures; safe for concurrent reads
- **Mergeable** — combine structures from distributed nodes with `merge/2`
- **Serializable** — every structure supports `to_binary/1` / `from_binary/1`
- **Tested** — 600+ tests including statistical accuracy bounds and round-trip serialization

## Documentation

Full API docs with examples on every function: [hexdocs.pm/approx](https://hexdocs.pm/approx)

## License

MIT — see [LICENSE](LICENSE) for details.
