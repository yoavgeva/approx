# Accuracy and Tuning

Probabilistic data structures trade exactness for dramatically less memory. Each
structure in Approx has parameters that control this tradeoff: you choose how much
error you can tolerate, and the structure determines how much memory it needs.

This guide explains what each parameter does, how the parameters interact, and how
to choose good values for your use case.

## Bloom Filter

A Bloom filter answers "is this element in the set?" with either **definitely no**
or **probably yes**. The probability of a false "yes" is controlled by two
parameters.

### Parameters

- **`capacity`** -- the maximum number of elements you expect to insert.
- **`false_positive_probability`** (FPP) -- the target probability that `member?/2`
  incorrectly returns `true` for an element that was never inserted.

### How they interact

Given capacity `n` and FPP `p`:

- Bit array size: `m = ceil(-n * ln(p) / (ln 2)^2)`
- Hash functions: `k = ceil(m/n * ln 2)`

A lower FPP requires a larger bit array and more hash functions. A larger capacity
requires a proportionally larger bit array but the same number of hash functions.

### Tuning table

| Capacity    | FPP           | Memory    | Hash functions |
| ----------- | ------------- | --------- | -------------- |
| 10,000      | 0.01 (1%)     | ~12 KB    | 7              |
| 10,000      | 0.001 (0.1%)  | ~18 KB    | 10             |
| 100,000     | 0.01 (1%)     | ~117 KB   | 7              |
| 1,000,000   | 0.01 (1%)     | ~1.14 MB  | 7              |
| 1,000,000   | 0.001 (0.1%) | ~1.71 MB  | 10             |

### Rule of thumb

Start with FPP = 0.01 (1%). Only decrease if false positives are expensive in your
application (for example, unnecessary database lookups or cache invalidations).

## Count-Min Approx

A Count-Min Approx estimates the frequency of elements in a stream. It only
overcounts, never undercounts.

### Parameters

- **`epsilon`** -- controls accuracy. The maximum overcount is `epsilon * N` where
  `N` is the total number of insertions.
- **`delta`** -- controls confidence. The probability that the overcount exceeds
  `epsilon * N` is at most `delta`.

### Tuning table

| epsilon | delta | Width  | Depth | Memory (64-bit counters) |
| ------- | ----- | ------ | ----- | ------------------------ |
| 0.01    | 0.01  | 272    | 5     | ~10 KB                   |
| 0.001   | 0.01  | 2,719  | 5     | ~104 KB                  |
| 0.0001  | 0.01  | 27,183 | 5     | ~1 MB                    |
| 0.001   | 0.001 | 2,719  | 7     | ~146 KB                  |

### Rule of thumb

`epsilon = 0.001`, `delta = 0.01` is a good default for most applications. Tighten
epsilon (smaller value) if you need more precise frequency counts; tighten delta if
you need higher confidence that the error bound holds.

## HyperLogLog

HyperLogLog estimates the number of distinct elements in a stream (cardinality).
It has a single tuning parameter.

### Parameter

- **`precision`** (4 to 16) -- the number of bits used to index into the register
  array. Higher precision means more registers, more memory, and lower standard
  error.

### Tuning table

| Precision | Registers | Memory | Std Error  |
| --------- | --------- | ------ | ---------- |
| 4         | 16        | 16 B   | ~26%       |
| 8         | 256       | 256 B  | ~6.5%      |
| 10        | 1,024     | 1 KB   | ~3.25%     |
| 12        | 4,096     | 4 KB   | ~1.625%    |
| 14        | 16,384    | 16 KB  | ~0.8125%   |
| 16        | 65,536    | 64 KB  | ~0.40625%  |

### Rule of thumb

Use precision 14 (the default) for production use. Precision 10--12 works well for
space-constrained environments. Precision 4--8 is only appropriate for rough
order-of-magnitude estimates.

## Top-K

Top-K tracks the most frequent elements in a stream. It combines a Count-Min Approx
for frequency estimation with a bounded heap for tracking the top items.

### Parameters

- **`k`** -- the number of top items to track.
- **`epsilon`** and **`delta`** -- the underlying Count-Min Approx parameters
  (see the Count-Min Approx section above).

The CMS accuracy determines how well heavy hitters are identified versus noise. If
the CMS overcounts a rare element enough, it may displace a true heavy hitter from
the top-k list.

### Rule of thumb

Use `k = 10-100` for dashboards and reporting. For noisy streams with many
low-frequency elements, use a tighter CMS epsilon (smaller value) to reduce the
chance of rare elements being mistaken for heavy hitters.

## Cuckoo Filter

A Cuckoo filter is similar to a Bloom filter but supports deletion and can be more
space-efficient for low false positive rates.

### Parameter

- **`capacity`** -- the expected number of elements. The filter allocates
  `ceil(capacity / 4)` buckets (rounded up to the nearest power of 2), with 4 slots
  per bucket. Each slot stores an 8-bit fingerprint.

With 8-bit fingerprints the false positive rate is approximately 3%, and storage
cost is roughly 12 bits per element.

### Rule of thumb

Set capacity to 2x your expected number of elements to leave headroom. Cuckoo
filters that approach full capacity have increasing insertion failure rates due to
bucket contention.

## Reservoir

Reservoir sampling maintains a fixed-size uniform random sample from a stream of
unknown length.

### Parameter

- **`k`** (sample size) -- the number of elements to retain. After the stream
  exceeds `k` elements, every element in the stream has exactly `k/n` probability of
  being in the sample, where `n` is the total number of elements seen.

### Rule of thumb

Use `k = 100-1000` for debugging and lightweight sampling. Use `k = 10,000+` for
statistical analysis where you need the sample to closely approximate the
distribution of the full stream.

## MinHash

MinHash estimates the Jaccard similarity between two sets using compact fixed-size
signatures.

### Parameter

- **`num_hashes`** (signature size) -- the number of independent hash functions used
  to build the signature. The standard error of the similarity estimate is
  `1 / sqrt(num_hashes)`.

### Tuning table

| num_hashes | Std Error | Signature Size |
| ---------- | --------- | -------------- |
| 64         | ~12.5%    | 256 bytes      |
| 128        | ~8.8%     | 512 bytes      |
| 256        | ~6.25%    | 1 KB           |
| 512        | ~4.4%     | 2 KB           |

### Rule of thumb

Use 128 hashes for rough similarity checks (for example, near-duplicate detection
with a generous threshold). Use 256 or more when you need precise similarity
estimates.

## t-digest

A t-digest computes approximate quantiles (median, p95, p99, etc.) from a stream.
It is especially accurate at the tails of the distribution, which is exactly where
you typically need it most (for example, p99 latency).

### Parameter

- **`delta`** (compression) -- controls how many centroids the t-digest maintains.
  Higher delta means more centroids, more memory, and higher accuracy. Tail accuracy
  (p99, p99.9) is always high regardless of delta because the t-digest concentrates
  centroids at the extremes.

### Tuning table

| delta | Max centroids | Memory (approx) | Accuracy                 |
| ----- | ------------- | ---------------- | ------------------------ |
| 25    | ~50           | ~800 B           | Good for rough estimates |
| 100   | ~200          | ~3.2 KB          | Good default             |
| 200   | ~400          | ~6.4 KB          | High accuracy            |
| 500   | ~1000         | ~16 KB           | Very high accuracy       |

### Rule of thumb

Use `delta = 100` (the default) for most applications. Increase to 200--300 for SLA
monitoring where you need high confidence in tail latency percentiles.

## Memory Comparison: Approx vs. Exact

The following table shows why probabilistic data structures exist. The memory
savings are dramatic -- often 1000x or more -- for a small, controlled amount of
error.

| Task                          | Exact approach | Exact memory | Approx approach            | Approx memory |
| ----------------------------- | -------------- | ------------ | -------------------------- | ------------- |
| Count 1M distinct users       | MapSet         | ~48 MB       | HyperLogLog (p=14)         | 16 KB         |
| Track frequencies of 1M keys  | Map            | ~72 MB       | Count-Min Approx           | 104 KB        |
| Deduplicate 1M events         | MapSet         | ~48 MB       | Bloom Filter (1% FPP)      | 1.14 MB       |
| p99 of 10M latency values     | Sorted list    | ~80 MB       | t-digest (delta=100)       | ~3.2 KB       |
