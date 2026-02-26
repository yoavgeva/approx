# Distributed Merging

Every probabilistic data structure in Sketch is designed from the ground up to
be **mergeable** and **serializable**. This makes Sketch a natural fit for
distributed systems where data arrives at many nodes and you need a unified
global view without ever shipping raw data.

## Why Merge?

Consider a cluster of web servers, each tracking unique visitors with a
HyperLogLog. At the end of each minute, you want a single "unique visitors"
count across the entire fleet. You have two options:

1. **Ship raw data** -- collect every visitor ID from every node and count them
   centrally. This is simple, but the data volume scales with traffic.
2. **Ship sketches** -- each node serializes its 16 KiB HyperLogLog, sends it
   to an aggregator, and the aggregator merges them. The data volume is
   constant regardless of traffic.

Option 2 is the merge-serialize pattern, and it brings three major benefits:

- **Network-efficient** -- a serialized sketch is a small fixed-size binary
  (bytes to kilobytes), whereas raw data grows with the stream. A HyperLogLog
  with precision 14 is always 16 KiB, whether it has seen 1,000 or
  1,000,000,000 elements.

- **Privacy-preserving** -- raw data (user IDs, IP addresses, request bodies)
  never leaves the node. Only opaque statistical summaries are transmitted. The
  original elements cannot be recovered from a sketch.

- **Horizontally scalable** -- add more nodes, merge more sketches. The merge
  operation is associative and commutative, so you can merge in any order, use
  tree-shaped aggregation, or merge incrementally as sketches arrive.

## The Merge-Serialize Pattern

The pattern follows five steps. Here is the complete flow using HyperLogLog as
an example:

### 1. Build locally

Each node builds its own sketch from its local data stream:

```elixir
# On node A
hll_a = Sketch.HyperLogLog.new(14)
hll_a = Enum.reduce(events_a, hll_a, &Sketch.HyperLogLog.add(&2, &1))
```

### 2. Serialize

Convert the sketch to a compact binary with `to_binary/1`:

```elixir
bin_a = Sketch.HyperLogLog.to_binary(hll_a)
# => <<1, 14, 0, 0, 3, ...>>  (16 KiB + 2-byte header)
```

### 3. Transfer

Send the binary over any transport -- a GenServer call, Phoenix PubSub,
`:erpc.call/4`, TCP socket, HTTP POST, message queue, or even a database
column:

```elixir
GenServer.cast(aggregator, {:sketch, :node_a, bin_a})
```

### 4. Deserialize

On the receiving side, restore the sketch from the binary with `from_binary/1`:

```elixir
{:ok, restored_a} = Sketch.HyperLogLog.from_binary(bin_a)
```

### 5. Merge

Combine two sketches into one with `merge/2`:

```elixir
{:ok, merged} = Sketch.HyperLogLog.merge(restored_a, restored_b)
Sketch.HyperLogLog.count(merged)
# => the estimated cardinality of the union of both streams
```

## Which Structures Support What

Not every structure supports all three operations. The table below summarizes
the current state:

| Structure | `merge/2` | `to_binary/1` | `from_binary/1` | Notes |
|---|---|---|---|---|
| `BloomFilter` | Yes | Yes | Yes | Bitwise OR of bit arrays |
| `CuckooFilter` | No | Yes | Yes | Merging fingerprint tables is not well-defined |
| `CountMinSketch` | Yes | Yes | Yes | Element-wise addition of counters |
| `TopK` | Yes | No | No | Merges underlying CMS, rebuilds top-k list |
| `HyperLogLog` | Yes | Yes | Yes | Register-wise max |
| `Reservoir` | Yes | No | No | Weighted random selection from both samples |
| `MinHash` | Yes\* | No | No | \*Operates on signature tuples, not structs |
| `TDigest` | Yes | Yes | Yes | Combines centroids and recompresses |

Structures that support all three (`BloomFilter`, `CountMinSketch`,
`HyperLogLog`, `TDigest`) are the most natural fit for the full
merge-serialize pattern over the network. Structures with `merge/2` but no
serialization (`TopK`, `Reservoir`, `MinHash`) can still be merged within the
same BEAM node or cluster using distributed Erlang.

## Detailed Examples

### HyperLogLog -- count unique users across web servers

Two web servers each see a partially overlapping set of users. After merging,
the HyperLogLog approximates the cardinality of the union.

```elixir
# Build on node A -- users 1..500_000
hll_a = Sketch.HyperLogLog.new(14)
hll_a = Enum.reduce(1..500_000, hll_a, &Sketch.HyperLogLog.add(&2, &1))

# Build on node B -- users 250_000..750_000 (overlapping)
hll_b = Sketch.HyperLogLog.new(14)
hll_b = Enum.reduce(250_000..750_000, hll_b, &Sketch.HyperLogLog.add(&2, &1))

# Serialize, transfer, deserialize
bin_a = Sketch.HyperLogLog.to_binary(hll_a)
bin_b = Sketch.HyperLogLog.to_binary(hll_b)
{:ok, restored_a} = Sketch.HyperLogLog.from_binary(bin_a)
{:ok, restored_b} = Sketch.HyperLogLog.from_binary(bin_b)

# Merge
{:ok, merged} = Sketch.HyperLogLog.merge(restored_a, restored_b)
Sketch.HyperLogLog.count(merged)
# => ~750_000 (the union of 1..500k and 250k..750k)
```

### Count-Min Sketch -- aggregate rate-limit counters

Each API gateway node tracks request counts per client. The aggregator merges
all nodes to get global counts for rate limiting decisions.

```elixir
# Node A -- 50 requests from "client_42"
cms_a = Sketch.CountMinSketch.new(0.001, 0.01)
cms_a = Sketch.CountMinSketch.add(cms_a, "client_42", 50)

# Node B -- 30 more requests from the same client
cms_b = Sketch.CountMinSketch.new(0.001, 0.01)
cms_b = Sketch.CountMinSketch.add(cms_b, "client_42", 30)

# Serialize, transfer, deserialize
bin_a = Sketch.CountMinSketch.to_binary(cms_a)
bin_b = Sketch.CountMinSketch.to_binary(cms_b)
{:ok, restored_a} = Sketch.CountMinSketch.from_binary(bin_a)
{:ok, restored_b} = Sketch.CountMinSketch.from_binary(bin_b)

# Merge -- counters are summed element-wise
{:ok, merged} = Sketch.CountMinSketch.merge(restored_a, restored_b)
Sketch.CountMinSketch.count(merged, "client_42")
# => 80 (50 + 30)
```

### Bloom Filter -- combine dedup filters from workers

Each worker maintains a Bloom filter of processed event IDs. After a batch,
you merge them so the next round of workers can skip already-processed events.

```elixir
# Worker 1 -- processed events "evt_1" through "evt_100"
bf1 = Sketch.BloomFilter.new(1_000)
bf1 = Enum.reduce(1..100, bf1, fn i, bf ->
  Sketch.BloomFilter.add(bf, "evt_#{i}")
end)

# Worker 2 -- processed events "evt_50" through "evt_200"
bf2 = Sketch.BloomFilter.new(1_000)
bf2 = Enum.reduce(50..200, bf2, fn i, bf ->
  Sketch.BloomFilter.add(bf, "evt_#{i}")
end)

# Serialize, transfer, deserialize
bin1 = Sketch.BloomFilter.to_binary(bf1)
bin2 = Sketch.BloomFilter.to_binary(bf2)
{:ok, restored1} = Sketch.BloomFilter.from_binary(bin1)
{:ok, restored2} = Sketch.BloomFilter.from_binary(bin2)

# Merge -- bitwise OR of the underlying bit arrays
{:ok, merged} = Sketch.BloomFilter.merge(restored1, restored2)

# The merged filter knows about all events from both workers
Sketch.BloomFilter.member?(merged, "evt_1")    # => true
Sketch.BloomFilter.member?(merged, "evt_150")   # => true
Sketch.BloomFilter.member?(merged, "evt_999")   # => false (definitely not seen)
```

### t-digest -- merge hourly latency digests into a daily view

Each hour produces a t-digest of request latencies. At the end of the day,
merge all 24 digests to compute daily percentiles.

```elixir
# Hour 1 -- fast responses (10-50ms)
td_hour1 = Sketch.TDigest.new(100)
td_hour1 = Enum.reduce(1..10_000, td_hour1, fn _, td ->
  latency = 10.0 + :rand.uniform() * 40.0
  Sketch.TDigest.add(td, latency)
end)

# Hour 2 -- peak traffic with some slow responses (10-200ms)
td_hour2 = Sketch.TDigest.new(100)
td_hour2 = Enum.reduce(1..10_000, td_hour2, fn _, td ->
  latency = 10.0 + :rand.uniform() * 190.0
  Sketch.TDigest.add(td, latency)
end)

# Serialize, transfer, deserialize
bin1 = Sketch.TDigest.to_binary(td_hour1)
bin2 = Sketch.TDigest.to_binary(td_hour2)
{:ok, restored1} = Sketch.TDigest.from_binary(bin1)
{:ok, restored2} = Sketch.TDigest.from_binary(bin2)

# Merge -- combines centroids and recompresses
merged = Sketch.TDigest.merge(restored1, restored2)

Sketch.TDigest.count(merged)
# => 20_000.0

Sketch.TDigest.percentile(merged, 0.50)   # median
Sketch.TDigest.percentile(merged, 0.99)   # p99
Sketch.TDigest.percentile(merged, 0.999)  # p99.9
```

Note that `TDigest.merge/2` returns the merged struct directly (not a tagged
`{:ok, merged}` tuple), because any two t-digests can always be merged
regardless of their parameters. The result uses the `delta` from the first
digest.

## Compatibility Requirements

`merge/2` requires that both structures were created with compatible
parameters. If they do not match, you get an error tuple back instead of a
merged result.

### What must match

| Structure | Must match | Error on mismatch |
|---|---|---|
| `BloomFilter` | `size` and `hash_count` | `{:error, :incompatible_filters}` |
| `CountMinSketch` | `width` and `depth` (i.e., same `epsilon` and `delta`) | `{:error, :dimension_mismatch}` |
| `HyperLogLog` | `precision` | `{:error, :incompatible_precision}` |
| `TopK` | `k` and compatible CMS dimensions | `{:error, :incompatible_k}` or `{:error, :dimension_mismatch}` |
| `Reservoir` | `k` | `{:error, :incompatible_size}` |
| `TDigest` | Nothing -- any two digests can be merged | (always succeeds) |
| `MinHash` | Signature length (`num_hashes`) | Raises if tuple sizes differ |

### Examples of error returns

```elixir
# HyperLogLog -- different precisions
hll_a = Sketch.HyperLogLog.new(10)
hll_b = Sketch.HyperLogLog.new(14)
Sketch.HyperLogLog.merge(hll_a, hll_b)
# => {:error, :incompatible_precision}

# BloomFilter -- different capacities produce different sizes
bf_a = Sketch.BloomFilter.new(100)
bf_b = Sketch.BloomFilter.new(10_000)
Sketch.BloomFilter.merge(bf_a, bf_b)
# => {:error, :incompatible_filters}

# CountMinSketch -- different epsilon values produce different widths
cms_a = Sketch.CountMinSketch.new(0.001, 0.01)
cms_b = Sketch.CountMinSketch.new(0.01, 0.01)
Sketch.CountMinSketch.merge(cms_a, cms_b)
# => {:error, :dimension_mismatch}

# Reservoir -- different k values
r_a = Sketch.Reservoir.new(10)
r_b = Sketch.Reservoir.new(20)
Sketch.Reservoir.merge(r_a, r_b)
# => {:error, :incompatible_size}
```

The fix is simple: make sure all nodes use the same creation parameters. Define
them in a shared configuration module:

```elixir
defmodule MyApp.SketchConfig do
  @moduledoc """
  Centralized sketch configuration so all nodes use compatible parameters.
  """

  def hll_precision, do: 14
  def cms_epsilon, do: 0.001
  def cms_delta, do: 0.01
  def bloom_capacity, do: 100_000
  def bloom_fpp, do: 0.01
end
```

## Architecture Pattern: GenServer Aggregator

A common architecture is to run a GenServer on an aggregator node that receives
serialized sketches from worker processes and maintains a merged global sketch.
Here is a minimal but complete example for HyperLogLog:

```elixir
defmodule MyApp.SketchAggregator do
  @moduledoc """
  Aggregates HyperLogLog sketches from multiple worker nodes into a single
  global cardinality estimate.

  Workers periodically serialize their local sketch and send it to this
  process. The aggregator deserializes and merges each incoming sketch
  into its running global state.
  """

  use GenServer

  require Logger

  # -- Client API --

  @doc """
  Starts the aggregator with the given HyperLogLog precision.
  """
  def start_link(opts \\ []) do
    precision = Keyword.get(opts, :precision, 14)
    GenServer.start_link(__MODULE__, precision, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Submits a serialized HyperLogLog binary from a worker node.
  """
  def submit(server \\ __MODULE__, binary) when is_binary(binary) do
    GenServer.cast(server, {:submit, binary})
  end

  @doc """
  Returns the current estimated cardinality from the merged global sketch.
  """
  def count(server \\ __MODULE__) do
    GenServer.call(server, :count)
  end

  @doc """
  Returns the serialized global sketch binary.
  """
  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  end

  # -- Server Callbacks --

  @impl true
  def init(precision) do
    {:ok, Sketch.HyperLogLog.new(precision)}
  end

  @impl true
  def handle_cast({:submit, binary}, global_hll) do
    case Sketch.HyperLogLog.from_binary(binary) do
      {:ok, incoming_hll} ->
        case Sketch.HyperLogLog.merge(global_hll, incoming_hll) do
          {:ok, merged} ->
            {:noreply, merged}

          {:error, :incompatible_precision} ->
            Logger.warning("Rejected sketch with incompatible precision")
            {:noreply, global_hll}
        end

      {:error, :invalid_binary} ->
        Logger.warning("Rejected invalid sketch binary")
        {:noreply, global_hll}
    end
  end

  @impl true
  def handle_call(:count, _from, global_hll) do
    {:reply, Sketch.HyperLogLog.count(global_hll), global_hll}
  end

  @impl true
  def handle_call(:snapshot, _from, global_hll) do
    {:reply, Sketch.HyperLogLog.to_binary(global_hll), global_hll}
  end
end
```

### Using the aggregator

```elixir
# In your application supervision tree
children = [
  {MyApp.SketchAggregator, precision: 14}
]

# On each worker node -- periodically send the local sketch
binary = Sketch.HyperLogLog.to_binary(local_hll)
MyApp.SketchAggregator.submit(binary)

# From anywhere -- query the global estimate
MyApp.SketchAggregator.count()
# => ~750_000
```

### Extending the pattern

The same GenServer pattern works for any mergeable and serializable structure.
Swap `HyperLogLog` for `CountMinSketch`, `BloomFilter`, or `TDigest` and
adjust the `merge/2` and `from_binary/1` calls accordingly. For structures
that are mergeable but not serializable (`TopK`, `Reservoir`), you can send
the struct directly over distributed Erlang instead of serializing to binary.

For production use, consider adding:

- **Periodic reset** -- clear the global sketch on a schedule (e.g., every
  minute) to get rolling windows.
- **Telemetry** -- emit `:telemetry` events on merge so you can track sketch
  submission rates and detect stale workers.
- **Multi-sketch** -- store a map of `%{sketch_name => sketch}` to aggregate
  multiple independent sketches in a single process.
- **Persistence** -- periodically call `to_binary/1` on the global sketch and
  write it to disk or a database, so the aggregator survives restarts.
