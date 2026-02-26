# Choosing a Data Structure

Approx includes 8 data structures. This guide helps you pick the right one for your problem.

## Decision Tree

**"I need to check if an item is in a set"**

- Can items be removed? -> `Approx.CuckooFilter`
- No deletion needed? -> `Approx.BloomFilter` (simpler, slightly more space-efficient)

**"I need to count how often items appear"**

- Track all items' frequencies? -> `Approx.CountMinSketch`
- Only need the top K most frequent? -> `Approx.TopK`

**"I need to count how many distinct items I've seen"**

-> `Approx.HyperLogLog`

**"I need percentile estimates (p50, p99, p99.9)"**

-> `Approx.TDigest`

**"I need a random sample from a stream"**

-> `Approx.Reservoir`

**"I need to estimate how similar two sets are"**

-> `Approx.MinHash`

## Comparison Table

| Structure | Space | Error Bound | Supports Merge | Supports Serialization | Supports Deletion | Best For |
|---|---|---|---|---|---|---|
| `Approx.BloomFilter` | O(n) bits, tuned by capacity and FPP | False positive rate configurable (e.g. 1%, 0.1%); no false negatives | Yes (bitwise OR) | Yes (`to_binary` / `from_binary`) | No | Pre-filtering expensive lookups, deduplication |
| `Approx.CuckooFilter` | ~12 bits per element (8-bit fingerprints, 4 slots/bucket) | False positive rate ~3% with 8-bit fingerprints; no false negatives | No | Yes (`to_binary` / `from_binary`) | Yes | Set membership when you also need to remove items |
| `Approx.CountMinSketch` | `width x depth` counters, fixed by epsilon and delta | Overestimates by at most epsilon * N with probability 1 - delta; never undercounts | Yes (element-wise addition) | Yes (`to_binary` / `from_binary`) | No | Frequency estimation for arbitrary stream elements |
| `Approx.TopK` | CMS + O(k) candidate map | Inherits CMS error bounds; heavy hitters are tracked accurately | Yes (CMS merge + candidate reunion) | No | No | Finding the most frequent items in a data stream |
| `Approx.HyperLogLog` | 2^p bytes (e.g. 16 KiB at p=14) | Standard error ~1.04 / sqrt(2^p) | Yes (element-wise max) | Yes (`to_binary` / `from_binary`) | No | Counting unique visitors, distinct IPs, unique events |
| `Approx.TDigest` | O(delta) centroids | High accuracy at tails (p99, p99.9); bounded by delta parameter | Yes (centroid merge + recompress) | Yes (`to_binary` / `from_binary`) | No | Latency percentiles, SLA monitoring, distribution summary |
| `Approx.Reservoir` | O(k) elements | Exact uniform sample of k items from a stream of any length | Yes (weighted random selection) | No | No | Random sampling from unbounded streams |
| `Approx.MinHash` | O(num_hashes) integers per signature | Standard error ~1 / sqrt(num_hashes) on Jaccard similarity | Yes (element-wise min on signatures) | No | No | Near-duplicate detection, set similarity estimation |

## "Why not just use..."

### MapSet / Map

Exact collections give you perfect answers, but they store every element. A `MapSet` holding 100 million strings will consume gigabytes of memory. A `Approx.BloomFilter` configured for the same capacity at 1% false-positive rate uses roughly 114 MB of bits -- and a `Approx.HyperLogLog` at precision 14 counts those 100 million distinct items in just 16 KiB.

**Use Approx when**: your data volume makes O(n) memory impractical, or when an approximate answer is acceptable to avoid a database round-trip.

**Stick with MapSet/Map when**: your data fits comfortably in memory and you need exact answers (exact membership, exact counts, exact set operations).

### ETS

ETS tables are mutable, node-local, and still O(n) in memory. They cannot be merged across nodes in a distributed system without writing custom synchronization logic. Approx structures are immutable, safely shareable across processes, and every one supports a `merge/2` operation for combining partial results from different nodes or time windows.

**Use Approx when**: you need to aggregate across distributed nodes, want immutable data you can pass through message-passing pipelines, or need sub-linear memory.

**Stick with ETS when**: you need mutable shared state with concurrent read/write access on a single node, or you need exact key-value lookups.

### Redis HyperLogLog

Redis provides a built-in `PFADD` / `PFCOUNT` / `PFMERGE` for HyperLogLog, but it requires a Redis dependency, network round-trips, and operational overhead. Approx is pure Elixir, runs in-process with zero latency, and gives you 7 additional data structures beyond cardinality estimation. Approx structures can be serialized and stored in Redis, Postgres, or anywhere else if you need persistence.

**Use Approx when**: you want to avoid an external dependency, need in-process speed, or need structures beyond HyperLogLog.

**Stick with Redis when**: you already have Redis in your stack and need a shared mutable counter accessible from multiple application instances without custom merge logic.

### :counters / :atomics

These BEAM primitives provide fast mutable integer arrays, but they are fixed-size -- you must know the number of slots (keys) in advance. They cannot dynamically handle arbitrary keys from a stream. A `Approx.CountMinSketch` maps any term to a fixed-size table using hash functions, giving you frequency estimation for an unbounded key space in bounded memory.

**Use Approx when**: the set of keys is unbounded or unknown in advance, or when you need mergeability and serialization.

**Stick with :counters/:atomics when**: you have a small, fixed set of known keys and need maximum throughput for concurrent increments on a single node.

## Parameter Tuning

### BloomFilter: `capacity` and `fpp`

The two parameters that control a Bloom filter are `capacity` (expected number of distinct elements) and `false_positive_probability` (FPP). Together they determine the bit-array size and number of hash functions.

| Capacity | FPP | Bit-array size | Hash functions | Bytes |
|---|---|---|---|---|
| 10,000 | 0.01 (1%) | ~95,851 bits | 7 | ~11.7 KiB |
| 10,000 | 0.001 (0.1%) | ~143,776 bits | 10 | ~17.6 KiB |
| 10,000 | 0.0001 (0.01%) | ~191,702 bits | 14 | ~23.4 KiB |
| 1,000,000 | 0.01 (1%) | ~9,585,059 bits | 7 | ~1.14 MiB |
| 1,000,000 | 0.001 (0.1%) | ~14,377,588 bits | 10 | ~1.71 MiB |

**Rule of thumb**: halving the FPP adds roughly 44% more bits. Choose the highest FPP you can tolerate for your use case. If you are unsure, 1% (`0.01`) is a solid default.

**What happens when you exceed capacity**: the actual false-positive rate rises above the configured FPP. The filter still works -- it never produces false negatives -- but more queries will incorrectly return `true`. If you expect growth, size for peak capacity.

```elixir
# 1% FPP, good default
bf = Approx.BloomFilter.new(100_000, 0.01)

# 0.1% FPP, tighter but uses ~50% more memory
bf = Approx.BloomFilter.new(100_000, 0.001)
```

### CountMinSketch: `epsilon` and `delta`

`epsilon` controls accuracy: the maximum overestimate is `epsilon * N` where N is total insertions. `delta` controls confidence: the accuracy guarantee holds with probability `1 - delta`.

| epsilon | delta | Width (columns) | Depth (rows) | Memory (64-bit counters) |
|---|---|---|---|---|
| 0.001 | 0.01 | 2,719 | 5 | ~106 KiB |
| 0.001 | 0.001 | 2,719 | 7 | ~149 KiB |
| 0.0001 | 0.01 | 27,183 | 5 | ~1.04 MiB |
| 0.01 | 0.01 | 272 | 5 | ~10.6 KiB |

**Rule of thumb**: `epsilon = 0.001` and `delta = 0.01` is a good starting point. Decrease epsilon for tighter count estimates (at the cost of more columns). Decrease delta if you need higher confidence (at the cost of more rows).

```elixir
# Good default: overestimates by at most 0.1% of total, 99% confidence
cms = Approx.CountMinSketch.new(0.001, 0.01)

# Tighter accuracy, higher memory
cms = Approx.CountMinSketch.new(0.0001, 0.001)
```

### HyperLogLog: `precision`

The `precision` parameter `p` (4 to 16) controls the number of registers (`2^p`) and thus the trade-off between memory and accuracy.

| Precision | Registers | Memory | Standard Error |
|---|---|---|---|
| 4 | 16 | 16 B | ~26% |
| 8 | 256 | 256 B | ~6.5% |
| 10 | 1,024 | 1 KiB | ~3.25% |
| 12 | 4,096 | 4 KiB | ~1.625% |
| 14 | 16,384 | 16 KiB | ~0.8125% |
| 16 | 65,536 | 64 KiB | ~0.40625% |

**Rule of thumb**: precision 14 (the default) gives less than 1% error in 16 KiB -- sufficient for most production use cases. Use precision 10-12 if memory is very constrained. Only go below 10 for rough estimates.

```elixir
# Default: precision 14, ~0.8% error, 16 KiB
hll = Approx.HyperLogLog.new()

# Lower memory: precision 10, ~3.25% error, 1 KiB
hll = Approx.HyperLogLog.new(10)
```

### TopK: `k`

`k` is the number of top-frequent items to track. The TopK module maintains a candidate map of at most `k` items backed by a Count-Min Sketch.

**Rule of thumb**: set `k` to the number of items you actually need to display or act on. Tracking more items than you need wastes memory in the candidate map (though the CMS size is independent of `k`). For dashboards showing "top 10 pages", `k = 10` (or slightly higher, like 20, to handle ties and churn).

You can also tune the underlying CMS via `:epsilon` and `:delta` options.

```elixir
# Track top 10 items
tk = Approx.TopK.new(10)

# Track top 100 with tighter CMS accuracy
tk = Approx.TopK.new(100, epsilon: 0.0001, delta: 0.001)
```

### TDigest: `delta`

The `delta` parameter controls the compression level. Higher delta values allow more centroids, giving better accuracy at the cost of more memory and slightly slower queries.

| delta | Approximate max centroids | Accuracy | Use case |
|---|---|---|---|
| 25 | ~25 | Lower, but adequate for rough estimates | Memory-constrained environments |
| 50 | ~50 | Good for general-purpose percentiles | Quick dashboards |
| 100 | ~100 | High accuracy at tails | Production latency monitoring (default) |
| 200 | ~200 | Very high accuracy | SLA tracking where p99.9 precision matters |
| 300 | ~300 | Highest accuracy | When you need tight tail bounds |

**Rule of thumb**: the default (`delta = 100`) is appropriate for most applications. Increase to 200-300 only if you need very precise tail estimates (e.g., contractual SLA tracking at p99.9). Decrease to 25-50 for rough estimates or when tracking many independent distributions.

```elixir
# Default: delta 100, good tail accuracy
td = Approx.TDigest.new()

# Higher accuracy for SLA monitoring
td = Approx.TDigest.new(200)

# Lower memory for many independent metrics
td = Approx.TDigest.new(50)
```

### Reservoir: `k`

`k` is the sample size -- the number of elements retained from the stream. Every element in the stream has an equal `k/n` probability of being in the final sample.

**Rule of thumb**: choose `k` based on the downstream analysis. For statistical estimates (mean, variance), `k = 1000` gives tight confidence intervals via the Central Limit Theorem. For display purposes ("show 10 random examples"), a small `k` suffices. The Reservoir stores the actual elements, so `k` directly determines memory usage.

```elixir
# Small sample for display
r = Approx.Reservoir.new(10)

# Larger sample for statistical analysis
r = Approx.Reservoir.new(1_000)
```

### MinHash: `num_hashes`

`num_hashes` controls signature size and accuracy. The standard error of the Jaccard similarity estimate is approximately `1 / sqrt(num_hashes)`.

| num_hashes | Signature size | Standard error |
|---|---|---|
| 32 | 32 integers (~128 B) | ~17.7% |
| 64 | 64 integers (~256 B) | ~12.5% |
| 128 | 128 integers (~512 B) | ~8.8% |
| 256 | 256 integers (~1 KiB) | ~6.25% |
| 512 | 512 integers (~2 KiB) | ~4.4% |

**Rule of thumb**: 128 hashes (the default) gives a good balance of accuracy and compactness. Use 256+ when precision matters (e.g., setting a similarity threshold for deduplication). Use 64 or fewer when you are comparing many pairs and need to minimize signature storage.

**Important**: all signatures you want to compare must come from the same MinHash instance (same coefficients). When comparing across nodes, pass the same `:seed` to `new/2` on each node.

```elixir
# Default: 128 hashes, ~8.8% error
mh = Approx.MinHash.new(128, seed: 42)

# Higher accuracy for deduplication pipelines
mh = Approx.MinHash.new(256, seed: 42)
```
