# Getting Started with Approx

This guide walks you through installing Approx, building your first
probabilistic data structure, and applying common patterns you will encounter in
production Elixir applications.

By the end you will have working examples of:

- A **Bloom filter** for set membership testing
- A **GenServer** wrapper for stream deduplication
- **Distributed merging** with HyperLogLog for cross-node cardinality
- **Latency monitoring** with t-digest percentiles

## Installation

Add `approx` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:approx, "~> 0.1.0"}
  ]
end
```

Then fetch the dependency:

```bash
mix deps.get
```

Approx is a pure-Elixir library with zero runtime dependencies. There is nothing
else to configure -- no NIFs to compile, no external services to start.

## Your First Sketch -- Bloom Filter

A Bloom filter answers the question _"Have I seen this before?"_ using a
fraction of the memory that a `MapSet` would require. The trade-off is that it
can occasionally report a false positive ("maybe yes" when the answer is really
"no"), but it will **never** report a false negative (it will never say "no"
when the answer is really "yes").

### 1. Create a filter

Specify the expected number of distinct elements (capacity) and the desired
false-positive probability (FPP):

```elixir
# A filter for up to 10,000 items with a 1% false-positive rate
bf = Approx.BloomFilter.new(10_000, 0.01)
```

The library computes the optimal bit-array size and number of hash functions
automatically from these two parameters.

### 2. Add some items

`add/2` returns an updated (immutable) filter:

```elixir
bf =
  bf
  |> Approx.BloomFilter.add("alice@example.com")
  |> Approx.BloomFilter.add("bob@example.com")
  |> Approx.BloomFilter.add("charlie@example.com")
```

You can add any Erlang/Elixir term -- strings, atoms, integers, tuples, etc.

### 3. Query membership

```elixir
Approx.BloomFilter.member?(bf, "alice@example.com")
# => true

Approx.BloomFilter.member?(bf, "alice@example.com")
# => true  (always -- no false negatives)

Approx.BloomFilter.member?(bf, "unknown@example.com")
# => false (definitely not in the set)
```

### 4. False negatives never happen

This is the fundamental guarantee of a Bloom filter. Every element you have
added will always be reported as present:

```elixir
emails = for i <- 1..1_000, do: "user_#{i}@example.com"
bf = Enum.reduce(emails, Approx.BloomFilter.new(10_000, 0.01), &Approx.BloomFilter.add(&2, &1))

# Every single inserted element is found -- guaranteed
Enum.all?(emails, &Approx.BloomFilter.member?(bf, &1))
# => true
```

### 5. False positives can happen (but rarely)

When you query an element that was never added, the filter _usually_ returns
`false`, but occasionally returns `true`. The rate is controlled by the FPP you
chose at creation time:

```elixir
# Query 100,000 items that were never added
false_positives =
  1..100_000
  |> Enum.count(fn i -> Approx.BloomFilter.member?(bf, "never_added_#{i}") end)

# With a 1% FPP and 1,000 items inserted into a 10,000-capacity filter,
# you can expect roughly 1% of these probes to be false positives.
IO.puts("False positives: #{false_positives} out of 100,000")
# => typically around 1,000 (i.e. ~1%)
```

The false-positive rate stays at or below the configured FPP as long as you
do not exceed the filter's capacity.

## Using in a GenServer

A common pattern is wrapping a Bloom filter in a GenServer so that multiple
processes can check and mark items as "seen" through a single shared filter.
This is useful for deduplication of events, webhooks, or message IDs.

```elixir
defmodule MyApp.Deduplicator do
  use GenServer

  @doc """
  Starts the deduplicator GenServer.

  ## Options

    * `:capacity` - expected number of distinct items (default: 100,000)
    * `:fpp` - false-positive probability (default: 0.01)

  """
  def start_link(opts \\ []) do
    capacity = Keyword.get(opts, :capacity, 100_000)
    fpp = Keyword.get(opts, :fpp, 0.01)
    GenServer.start_link(__MODULE__, {capacity, fpp}, name: __MODULE__)
  end

  @doc "Returns `true` if the item has (probably) been seen before."
  def seen?(item), do: GenServer.call(__MODULE__, {:seen?, item})

  @doc "Marks the item as seen and returns `:ok`."
  def mark(item), do: GenServer.call(__MODULE__, {:mark, item})

  @doc """
  Checks and marks in one call. Returns `:new` if the item was not
  previously seen (and is now marked), or `:duplicate` if it was.
  """
  def check_and_mark(item), do: GenServer.call(__MODULE__, {:check_and_mark, item})

  # --- Callbacks ---

  @impl true
  def init({capacity, fpp}) do
    {:ok, Approx.BloomFilter.new(capacity, fpp)}
  end

  @impl true
  def handle_call({:seen?, item}, _from, bf) do
    {:reply, Approx.BloomFilter.member?(bf, item), bf}
  end

  def handle_call({:mark, item}, _from, bf) do
    {:reply, :ok, Approx.BloomFilter.add(bf, item)}
  end

  def handle_call({:check_and_mark, item}, _from, bf) do
    if Approx.BloomFilter.member?(bf, item) do
      {:reply, :duplicate, bf}
    else
      {:reply, :new, Approx.BloomFilter.add(bf, item)}
    end
  end
end
```

Add it to your application's supervision tree:

```elixir
# In your Application module
children = [
  {MyApp.Deduplicator, capacity: 500_000, fpp: 0.001},
  # ...other children
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Then use it anywhere in your application:

```elixir
case MyApp.Deduplicator.check_and_mark(webhook_event_id) do
  :new       -> process_webhook(event)
  :duplicate -> Logger.debug("Skipping duplicate event #{webhook_event_id}")
end
```

> **Note:** Because the filter is immutable under the hood, reads inside the
> GenServer are safe even during garbage collection. However, all writes are
> serialized through the GenServer's mailbox. If you need higher write
> throughput, consider sharding across multiple GenServer instances by hashing
> the item to a shard.

## Distributed Merging

Most Approx data structures support serialization via `to_binary/1` and
`from_binary/1`, and merging via `merge/2`. This makes it straightforward to
build sketches on individual nodes and combine them on an aggregator. See
the [distributed merging guide](distributed-merging.md) for a per-structure
compatibility table.

Here is a complete example using HyperLogLog to count distinct users across a
cluster:

### On each worker node

Each node builds its own HyperLogLog from local data and serializes it:

```elixir
# Node A
hll_a = Approx.HyperLogLog.new(14)
hll_a = Enum.reduce(local_user_ids, hll_a, &Approx.HyperLogLog.add(&2, &1))
binary_a = Approx.HyperLogLog.to_binary(hll_a)

# Send binary_a to the aggregator node.
# This is just a binary -- you can send it via :rpc.call, Phoenix PubSub,
# a GenServer message, an HTTP POST, or anything that carries bytes.
```

### On the aggregator node

The aggregator receives serialized binaries from each node, deserializes them,
and merges:

```elixir
# Receive binaries from nodes A and B (however you transport them)
{:ok, hll_a} = Approx.HyperLogLog.from_binary(binary_a)
{:ok, hll_b} = Approx.HyperLogLog.from_binary(binary_b)

# Merge into a single estimator
{:ok, merged} = Approx.HyperLogLog.merge(hll_a, hll_b)

# Query the combined cardinality
Approx.HyperLogLog.count(merged)
# => estimated number of distinct users across both nodes
```

### Merging more than two

When you have many nodes, fold them together:

```elixir
binaries = [binary_a, binary_b, binary_c, binary_d]

merged =
  binaries
  |> Enum.map(fn bin ->
    {:ok, hll} = Approx.HyperLogLog.from_binary(bin)
    hll
  end)
  |> Enum.reduce(fn hll, acc ->
    {:ok, merged} = Approx.HyperLogLog.merge(acc, hll)
    merged
  end)

Approx.HyperLogLog.count(merged)
# => estimated distinct count across all four nodes
```

> **Precision matters:** All HyperLogLog instances must be created with the same
> precision (e.g., `14`) for `merge/2` to succeed. If precisions differ, you
> will get `{:error, :incompatible_precision}`.

## Monitoring Latency with t-digest

The t-digest is purpose-built for tracking percentiles of a numeric
distribution -- especially at the tails (p99, p99.9) where SLA violations
happen. It uses far less memory than storing every observation.

### 1. Create a digest

```elixir
td = Approx.TDigest.new()
```

The default compression parameter (`delta = 100`) works well for most use
cases. Increase it for higher accuracy at the cost of more memory.

### 2. Add latency measurements

Record API response times as they arrive:

```elixir
# Simulate some latency measurements (in milliseconds)
latencies = [
  12.3, 15.1, 11.8, 45.2, 13.0, 14.7, 200.5, 11.2, 16.9, 12.0,
  13.5, 14.1, 18.3, 11.0, 310.7, 12.8, 15.5, 13.2, 12.1, 11.9
]

td = Enum.reduce(latencies, td, &Approx.TDigest.add(&2, &1))
```

In production you would call `Approx.TDigest.add/2` on each request:

```elixir
{elapsed_ms, result} = :timer.tc(fn -> handle_request(conn) end, :millisecond)
updated_td = Approx.TDigest.add(td, elapsed_ms)
```

### 3. Query percentiles

```elixir
Approx.TDigest.percentile(td, 0.50)   # p50 -- median response time
Approx.TDigest.percentile(td, 0.95)   # p95
Approx.TDigest.percentile(td, 0.99)   # p99
Approx.TDigest.percentile(td, 0.999)  # p99.9
```

The t-digest is especially accurate at the extreme tails. A p99.9 estimate from
a t-digest is far more reliable than one computed from a fixed-bucket histogram.

### 4. Merge hourly digests into a daily summary

Because t-digests are mergeable, you can build one per time window and combine
them later without losing accuracy:

```elixir
# Each hour, build a digest from that hour's latencies
hour_00 = Enum.reduce(hour_00_latencies, Approx.TDigest.new(), &Approx.TDigest.add(&2, &1))
hour_01 = Enum.reduce(hour_01_latencies, Approx.TDigest.new(), &Approx.TDigest.add(&2, &1))
hour_02 = Enum.reduce(hour_02_latencies, Approx.TDigest.new(), &Approx.TDigest.add(&2, &1))

# Merge into a daily digest
daily = hour_00 |> Approx.TDigest.merge(hour_01) |> Approx.TDigest.merge(hour_02)

# Query the full day's percentiles
Approx.TDigest.percentile(daily, 0.99)
# => p99 latency across all three hours

Approx.TDigest.count(daily)
# => total number of requests across all three hours
```

This pattern scales to any number of windows -- merge 24 hourly digests for a
daily summary, merge 7 daily digests for a weekly summary, and so on. The
merged digest maintains the same accuracy guarantees as one built from the raw
data.

## Next Steps

Now that you have the basics, here are some resources to go deeper:

- **[Choosing a Data Structure](choosing-a-data-structure.md)** -- a decision
  guide that helps you pick the right structure for your problem (Bloom filter vs.
  Cuckoo filter, Count-Min Sketch vs. Top-K, etc.)

- **Module documentation** -- every module has detailed docs with examples on
  every function:
  - `Approx.BloomFilter` -- set membership (no false negatives)
  - `Approx.CuckooFilter` -- set membership with deletion support
  - `Approx.CountMinSketch` -- frequency estimation
  - `Approx.TopK` -- heavy hitters / most frequent items
  - `Approx.HyperLogLog` -- cardinality (distinct count) estimation
  - `Approx.TDigest` -- streaming percentiles
  - `Approx.Reservoir` -- uniform random sampling
  - `Approx.MinHash` -- Jaccard similarity estimation

- **[Cheatsheet](approx.cheatmd)** -- a single-page quick reference with the
  most common operations for every data structure
