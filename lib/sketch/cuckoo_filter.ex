defmodule Sketch.CuckooFilter do
  @moduledoc """
  A Cuckoo filter — a space-efficient probabilistic set that supports
  insertion, membership queries, **and deletion**.

  Cuckoo filters provide the same probabilistic membership guarantees as Bloom
  filters (no false negatives, tunable false positive rate) while also
  supporting element removal. They achieve this by storing compact fingerprints
  in a cuckoo-hashing table with buckets of four slots each.

  ## When to use

    * You need set membership with deletion (Bloom filters cannot delete)
    * You want better locality of reference than a Bloom filter (bucket
      lookups access one or two contiguous cache lines)
    * You need a compact serialization format for storage or transfer

  ## Creating a filter

  Provide the expected number of elements. The filter allocates buckets as
  the smallest power of 2 that fits `ceil(capacity / 4)` (minimum 2 buckets),
  giving roughly 95 % load factor headroom before eviction pressure rises.

      cf = Sketch.CuckooFilter.new(1_000)

  ## Inserting, querying, and deleting

      {:ok, cf} = Sketch.CuckooFilter.insert(cf, "hello")
      Sketch.CuckooFilter.member?(cf, "hello")   # => true

      {:ok, cf} = Sketch.CuckooFilter.delete(cf, "hello")
      Sketch.CuckooFilter.member?(cf, "hello")   # => false

  ## Serialization

      bin = Sketch.CuckooFilter.to_binary(cf)
      {:ok, cf} = Sketch.CuckooFilter.from_binary(bin)

  The wire format is:

      <<version::8, num_buckets::big-32, count::big-32, bucket_data::binary>>

  where `bucket_data` contains 4 bytes per bucket (one byte per fingerprint
  slot, 0 for empty).

  ## Implementation notes

    * Fingerprints are 8-bit, derived from the element hash. A zero
      fingerprint is mapped to 1 to distinguish it from an empty slot.
    * Alternate bucket indices are computed via XOR:
      `i2 = (i1 ^^^ hash(fingerprint)) band (num_buckets - 1)`.
      This requires `num_buckets` to be a power of 2.
    * When both candidate buckets are full, a random existing fingerprint
      is evicted and relocated to its alternate bucket. Up to 500 such
      kicks are attempted before returning `{:error, :full}`.
    * The `:seed` option controls the PRNG state used for random eviction,
      making tests deterministic.
  """

  import Bitwise

  @enforce_keys [:buckets, :num_buckets, :bucket_size, :fingerprint_size, :count, :hash_fn, :seed]
  defstruct [:buckets, :num_buckets, :bucket_size, :fingerprint_size, :count, :hash_fn, :seed]

  @typedoc "A Cuckoo filter struct."
  @type t :: %__MODULE__{
          buckets: tuple(),
          num_buckets: pos_integer(),
          bucket_size: 4,
          fingerprint_size: 8,
          count: non_neg_integer(),
          hash_fn: (term() -> non_neg_integer()),
          seed: :rand.state()
        }

  @serialization_version 1
  @default_bucket_size 4
  @default_fingerprint_size 8
  @max_kicks 500

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new Cuckoo filter sized for `capacity` elements.

  The number of buckets is the smallest power of 2 >= `ceil(capacity / 4)`,
  with a minimum of 2 buckets.

  ## Parameters

    * `capacity` — the expected number of distinct elements to insert
      (positive integer).
    * `opts` — keyword list of options:
      * `:hash_fn` — a 1-arity function `(term -> non_neg_integer())`.
        Defaults to `&Sketch.Hash.hash32/1`.
      * `:seed` — an initial seed for `:rand`. Accepts any term accepted
        by `:rand.seed/2` or a pre-existing `:rand.state()`. Defaults to
        `:rand.seed(:exsss)`.

  ## Examples

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> cf.num_buckets >= 2
      true

      iex> cf = Sketch.CuckooFilter.new(16)
      iex> cf.count
      0
  """
  @spec new(pos_integer(), keyword()) :: t()
  def new(capacity, opts \\ []) when is_integer(capacity) and capacity > 0 do
    num_buckets = next_power_of_two(ceil(capacity / @default_bucket_size))
    num_buckets = max(num_buckets, 2)
    hash_fn = Keyword.get(opts, :hash_fn, &Sketch.Hash.hash32/1)

    seed =
      case Keyword.get(opts, :seed) do
        nil -> :rand.seed_s(:exsss)
        {%{type: _} = _alg, _state} = rand_state -> rand_state
        seed_value -> :rand.seed_s(:exsss, seed_value)
      end

    empty_bucket = {0, 0, 0, 0}
    buckets = Tuple.duplicate(empty_bucket, num_buckets)

    %__MODULE__{
      buckets: buckets,
      num_buckets: num_buckets,
      bucket_size: @default_bucket_size,
      fingerprint_size: @default_fingerprint_size,
      count: 0,
      hash_fn: hash_fn,
      seed: seed
    }
  end

  # ---------------------------------------------------------------------------
  # Insertion
  # ---------------------------------------------------------------------------

  @doc """
  Inserts an element into the Cuckoo filter.

  Returns `{:ok, updated_filter}` on success, or `{:error, :full}` if the
  filter cannot place the element after up to 500 eviction kicks.

  ## Parameters

    * `cf` — a `%Sketch.CuckooFilter{}` struct.
    * `element` — any Erlang/Elixir term.

  ## Examples

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:ok, cf} = Sketch.CuckooFilter.insert(cf, "hello")
      iex> Sketch.CuckooFilter.member?(cf, "hello")
      true

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:ok, cf} = Sketch.CuckooFilter.insert(cf, 42)
      iex> cf.count
      1
  """
  @spec insert(t(), term()) :: {:ok, t()} | {:error, :full}
  def insert(%__MODULE__{} = cf, element) do
    fp = fingerprint(cf, element)
    i1 = primary_index(cf, element)
    i2 = alternate_index(cf, i1, fp)

    cond do
      bucket_has_empty_slot?(cf.buckets, i1) ->
        buckets = bucket_insert(cf.buckets, i1, fp)
        {:ok, %{cf | buckets: buckets, count: cf.count + 1}}

      bucket_has_empty_slot?(cf.buckets, i2) ->
        buckets = bucket_insert(cf.buckets, i2, fp)
        {:ok, %{cf | buckets: buckets, count: cf.count + 1}}

      true ->
        evict_and_insert(cf, fp, i1, i2)
    end
  end

  # ---------------------------------------------------------------------------
  # Membership query
  # ---------------------------------------------------------------------------

  @doc """
  Checks whether `element` might be a member of the filter.

  Returns `true` if the element's fingerprint is found in either of its
  candidate buckets (possible false positive), or `false` if the element
  is **definitely not** in the filter.

  ## Parameters

    * `cf` — a `%Sketch.CuckooFilter{}` struct.
    * `element` — any Erlang/Elixir term.

  ## Examples

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> Sketch.CuckooFilter.member?(cf, "ghost")
      false

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:ok, cf} = Sketch.CuckooFilter.insert(cf, "present")
      iex> Sketch.CuckooFilter.member?(cf, "present")
      true
  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{} = cf, element) do
    fp = fingerprint(cf, element)
    i1 = primary_index(cf, element)
    i2 = alternate_index(cf, i1, fp)

    bucket_contains?(cf.buckets, i1, fp) or bucket_contains?(cf.buckets, i2, fp)
  end

  # ---------------------------------------------------------------------------
  # Deletion
  # ---------------------------------------------------------------------------

  @doc """
  Deletes one occurrence of `element` from the Cuckoo filter.

  Returns `{:ok, updated_filter}` on success, or `{:error, :not_found}` if
  the element's fingerprint is not present in either candidate bucket.

  **Note**: if the same element was inserted multiple times, each call to
  `delete/2` removes only one occurrence. You must call `delete/2` once per
  insertion to fully remove a duplicated element.

  ## Parameters

    * `cf` — a `%Sketch.CuckooFilter{}` struct.
    * `element` — any Erlang/Elixir term.

  ## Examples

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:ok, cf} = Sketch.CuckooFilter.insert(cf, "bye")
      iex> {:ok, cf} = Sketch.CuckooFilter.delete(cf, "bye")
      iex> Sketch.CuckooFilter.member?(cf, "bye")
      false

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:error, :not_found} = Sketch.CuckooFilter.delete(cf, "missing")
      iex> cf.count
      0
  """
  @spec delete(t(), term()) :: {:ok, t()} | {:error, :not_found}
  def delete(%__MODULE__{} = cf, element) do
    fp = fingerprint(cf, element)
    i1 = primary_index(cf, element)
    i2 = alternate_index(cf, i1, fp)

    cond do
      bucket_contains?(cf.buckets, i1, fp) ->
        buckets = bucket_remove(cf.buckets, i1, fp)
        {:ok, %{cf | buckets: buckets, count: cf.count - 1}}

      bucket_contains?(cf.buckets, i2, fp) ->
        buckets = bucket_remove(cf.buckets, i2, fp)
        {:ok, %{cf | buckets: buckets, count: cf.count - 1}}

      true ->
        {:error, :not_found}
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  @doc """
  Serializes the Cuckoo filter to a binary.

  The format is:

      <<version::8, num_buckets::big-unsigned-32, count::big-unsigned-32, bucket_data::binary>>

  where `bucket_data` contains 4 bytes per bucket (one byte per fingerprint
  slot, 0 for empty). The `hash_fn` and `seed` are **not** serialized; on
  deserialization the defaults are restored.

  ## Examples

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:ok, cf} = Sketch.CuckooFilter.insert(cf, "test")
      iex> bin = Sketch.CuckooFilter.to_binary(cf)
      iex> is_binary(bin)
      true
  """
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{} = cf) do
    bucket_iodata =
      for i <- 0..(cf.num_buckets - 1) do
        {s0, s1, s2, s3} = elem(cf.buckets, i)
        <<s0::8, s1::8, s2::8, s3::8>>
      end

    bucket_data = :erlang.iolist_to_binary(bucket_iodata)

    <<@serialization_version::8, cf.num_buckets::big-unsigned-32, cf.count::big-unsigned-32,
      bucket_data::binary>>
  end

  @doc """
  Deserializes a Cuckoo filter from a binary produced by `to_binary/1`.

  Returns `{:ok, cuckoo_filter}` on success or `{:error, :invalid_binary}`
  on failure.

  The default hash function (`&Sketch.Hash.hash32/1`) and a fresh PRNG seed
  are used for the restored filter. Pass `:hash_fn` or `:seed` in `opts` to
  override.

  ## Options

    * `:hash_fn` — a 1-arity hash function to attach to the deserialized
      filter. Defaults to `&Sketch.Hash.hash32/1`.
    * `:seed` — PRNG seed for the restored filter.

  ## Examples

      iex> cf = Sketch.CuckooFilter.new(100)
      iex> {:ok, cf} = Sketch.CuckooFilter.insert(cf, "round_trip")
      iex> {:ok, restored} = cf |> Sketch.CuckooFilter.to_binary() |> Sketch.CuckooFilter.from_binary()
      iex> Sketch.CuckooFilter.member?(restored, "round_trip")
      true
  """
  @spec from_binary(binary(), keyword()) :: {:ok, t()} | {:error, :invalid_binary}
  def from_binary(binary, opts \\ [])

  def from_binary(
        <<@serialization_version::8, num_buckets::big-unsigned-32, _count::big-unsigned-32,
          bucket_data::binary>>,
        opts
      ) do
    expected_bytes = num_buckets * @default_bucket_size

    if byte_size(bucket_data) == expected_bytes and num_buckets >= 2 and
         power_of_two?(num_buckets) do
      buckets = deserialize_buckets(bucket_data, num_buckets)
      hash_fn = Keyword.get(opts, :hash_fn, &Sketch.Hash.hash32/1)

      seed =
        case Keyword.get(opts, :seed) do
          nil -> :rand.seed_s(:exsss)
          {%{type: _} = _alg, _state} = rand_state -> rand_state
          seed_value -> :rand.seed_s(:exsss, seed_value)
        end

      # Compute actual count from bucket contents instead of trusting the header
      actual_count = count_fingerprints(buckets, num_buckets)

      {:ok,
       %__MODULE__{
         buckets: buckets,
         num_buckets: num_buckets,
         bucket_size: @default_bucket_size,
         fingerprint_size: @default_fingerprint_size,
         count: actual_count,
         hash_fn: hash_fn,
         seed: seed
       }}
    else
      {:error, :invalid_binary}
    end
  end

  def from_binary(_binary, _opts), do: {:error, :invalid_binary}

  # ---------------------------------------------------------------------------
  # Private — bucket operations
  # ---------------------------------------------------------------------------

  # Returns true if the bucket at `index` has at least one empty (0) slot.
  @spec bucket_has_empty_slot?(tuple(), non_neg_integer()) :: boolean()
  defp bucket_has_empty_slot?(buckets, index) do
    {s0, s1, s2, s3} = elem(buckets, index)
    s0 == 0 or s1 == 0 or s2 == 0 or s3 == 0
  end

  # Inserts `fp` into the first empty slot of the bucket at `index`.
  # Assumes the bucket has at least one empty slot.
  @spec bucket_insert(tuple(), non_neg_integer(), pos_integer()) :: tuple()
  defp bucket_insert(buckets, index, fp) do
    bucket = elem(buckets, index)
    new_bucket = insert_into_bucket(bucket, fp)
    put_elem(buckets, index, new_bucket)
  end

  @spec insert_into_bucket(tuple(), pos_integer()) :: tuple()
  defp insert_into_bucket({0, s1, s2, s3}, fp), do: {fp, s1, s2, s3}
  defp insert_into_bucket({s0, 0, s2, s3}, fp), do: {s0, fp, s2, s3}
  defp insert_into_bucket({s0, s1, 0, s3}, fp), do: {s0, s1, fp, s3}
  defp insert_into_bucket({s0, s1, s2, 0}, fp), do: {s0, s1, s2, fp}

  # Returns true if the bucket at `index` contains `fp`.
  @spec bucket_contains?(tuple(), non_neg_integer(), pos_integer()) :: boolean()
  defp bucket_contains?(buckets, index, fp) do
    {s0, s1, s2, s3} = elem(buckets, index)
    s0 == fp or s1 == fp or s2 == fp or s3 == fp
  end

  # Removes the first occurrence of `fp` from the bucket at `index`.
  # Assumes the bucket contains `fp`.
  @spec bucket_remove(tuple(), non_neg_integer(), pos_integer()) :: tuple()
  defp bucket_remove(buckets, index, fp) do
    bucket = elem(buckets, index)
    new_bucket = remove_from_bucket(bucket, fp)
    put_elem(buckets, index, new_bucket)
  end

  @spec remove_from_bucket(tuple(), pos_integer()) :: tuple()
  defp remove_from_bucket({fp, s1, s2, s3}, fp), do: {0, s1, s2, s3}
  defp remove_from_bucket({s0, fp, s2, s3}, fp), do: {s0, 0, s2, s3}
  defp remove_from_bucket({s0, s1, fp, s3}, fp), do: {s0, s1, 0, s3}
  defp remove_from_bucket({s0, s1, s2, fp}, fp), do: {s0, s1, s2, 0}

  # ---------------------------------------------------------------------------
  # Private — eviction loop
  # ---------------------------------------------------------------------------

  # Randomly pick one of the two candidate buckets, evict a random fingerprint,
  # insert the incoming fingerprint, then try to place the evicted one.
  # The mutable parts (buckets, seed) are threaded as plain variables through
  # the recursion to avoid rebuilding the struct on every kick iteration.
  @spec evict_and_insert(t(), pos_integer(), non_neg_integer(), non_neg_integer()) ::
          {:ok, t()} | {:error, :full}
  defp evict_and_insert(cf, fp, i1, i2) do
    {chosen_index, seed} = random_choice(cf.seed, i1, i2)

    case do_evict(cf.buckets, cf.num_buckets, cf.hash_fn, fp, chosen_index, seed, @max_kicks) do
      {:ok, buckets, seed} ->
        {:ok, %{cf | buckets: buckets, seed: seed, count: cf.count + 1}}

      :full ->
        {:error, :full}
    end
  end

  @spec do_evict(
          tuple(),
          pos_integer(),
          (term() -> non_neg_integer()),
          pos_integer(),
          non_neg_integer(),
          :rand.state(),
          non_neg_integer()
        ) ::
          {:ok, tuple(), :rand.state()} | :full
  defp do_evict(_buckets, _num_buckets, _hash_fn, _fp, _index, _seed, 0), do: :full

  defp do_evict(buckets, num_buckets, hash_fn, fp, index, seed, kicks_remaining) do
    # Pick a random slot from the bucket to evict.
    {slot, seed} = random_slot(seed)

    bucket = elem(buckets, index)
    evicted_fp = elem(bucket, slot)

    # Replace the evicted fingerprint with the incoming one.
    new_bucket = put_elem(bucket, slot, fp)
    buckets = put_elem(buckets, index, new_bucket)

    # Compute the alternate index for the evicted fingerprint.
    fp_hash = hash_fn.(evicted_fp)
    alt_index = band(bxor(index, fp_hash), num_buckets - 1)

    if bucket_has_empty_slot?(buckets, alt_index) do
      buckets = bucket_insert(buckets, alt_index, evicted_fp)
      {:ok, buckets, seed}
    else
      do_evict(buckets, num_buckets, hash_fn, evicted_fp, alt_index, seed, kicks_remaining - 1)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — hashing and indexing
  # ---------------------------------------------------------------------------

  # Computes an 8-bit fingerprint for an element. Guarantees non-zero result.
  @spec fingerprint(t(), term()) :: pos_integer()
  defp fingerprint(%__MODULE__{hash_fn: hash_fn}, element) do
    hash = hash_fn.(element)
    fp = band(hash, 0xFF)
    max(fp, 1)
  end

  # Computes the primary bucket index for an element.
  @spec primary_index(t(), term()) :: non_neg_integer()
  defp primary_index(%__MODULE__{hash_fn: hash_fn, num_buckets: num_buckets}, element) do
    hash = hash_fn.({:__cuckoo_index__, element})
    band(hash, num_buckets - 1)
  end

  # Computes the alternate bucket index via XOR.
  @spec alternate_index(t(), non_neg_integer(), pos_integer()) :: non_neg_integer()
  defp alternate_index(%__MODULE__{hash_fn: hash_fn, num_buckets: num_buckets}, index, fp) do
    fp_hash = hash_fn.(fp)
    band(bxor(index, fp_hash), num_buckets - 1)
  end

  # ---------------------------------------------------------------------------
  # Private — randomness helpers
  # ---------------------------------------------------------------------------

  @spec random_choice(:rand.state(), non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), :rand.state()}
  defp random_choice(seed, a, b) do
    {val, new_seed} = :rand.uniform_s(2, seed)

    case val do
      1 -> {a, new_seed}
      2 -> {b, new_seed}
    end
  end

  @spec random_slot(:rand.state()) :: {0..3, :rand.state()}
  defp random_slot(seed) do
    {val, new_seed} = :rand.uniform_s(@default_bucket_size, seed)
    {val - 1, new_seed}
  end

  # ---------------------------------------------------------------------------
  # Private — numeric helpers
  # ---------------------------------------------------------------------------

  @spec next_power_of_two(pos_integer()) :: pos_integer()
  defp next_power_of_two(n) when n <= 1, do: 1

  defp next_power_of_two(n) do
    # Subtract 1 to handle the case where n is already a power of 2.
    n = n - 1
    n = bor(n, bsr(n, 1))
    n = bor(n, bsr(n, 2))
    n = bor(n, bsr(n, 4))
    n = bor(n, bsr(n, 8))
    n = bor(n, bsr(n, 16))
    n + 1
  end

  @spec power_of_two?(pos_integer()) :: boolean()
  defp power_of_two?(n) when n > 0, do: band(n, n - 1) == 0

  # Counts non-zero fingerprints across all buckets.
  @spec count_fingerprints(tuple(), pos_integer()) :: non_neg_integer()
  defp count_fingerprints(buckets, num_buckets) do
    Enum.reduce(0..(num_buckets - 1), 0, fn i, acc ->
      {s0, s1, s2, s3} = elem(buckets, i)
      acc + non_zero(s0) + non_zero(s1) + non_zero(s2) + non_zero(s3)
    end)
  end

  defp non_zero(0), do: 0
  defp non_zero(_), do: 1

  # Deserializes bucket data binary into a tuple of bucket tuples.
  @spec deserialize_buckets(binary(), pos_integer()) :: tuple()
  defp deserialize_buckets(data, num_buckets) do
    do_deserialize_buckets(data, num_buckets, [])
    |> Enum.reverse()
    |> List.to_tuple()
  end

  @spec do_deserialize_buckets(binary(), non_neg_integer(), [tuple()]) :: [tuple()]
  defp do_deserialize_buckets(<<>>, 0, acc), do: acc

  defp do_deserialize_buckets(<<s0::8, s1::8, s2::8, s3::8, rest::binary>>, remaining, acc) do
    do_deserialize_buckets(rest, remaining - 1, [{s0, s1, s2, s3} | acc])
  end
end
