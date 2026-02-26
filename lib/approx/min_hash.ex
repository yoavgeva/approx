defmodule Approx.MinHash do
  @moduledoc """
  MinHash — a locality-sensitive hashing scheme for estimating Jaccard similarity.

  MinHash produces compact fixed-size signatures for sets so that the
  probability two signatures agree at any given position equals the Jaccard
  similarity of the original sets. This makes it possible to estimate set
  similarity in constant time and space, regardless of set size.

  ## When to use

    * Near-duplicate detection in large document collections
    * Finding similar users or items in recommendation systems
    * Clustering sets by similarity without pairwise comparison
    * Locality-sensitive hashing (LSH) pipelines for approximate nearest neighbors

  ## How it works

  A MinHash instance holds `num_hashes` random hash functions from a universal
  hash family. For each hash function, the signature of a set is the minimum
  hash value over all elements. Two sets that share many elements will tend to
  produce matching minimums, and the fraction of matching positions is an
  unbiased estimator of the Jaccard similarity `|A intersection B| / |A union B|`.

  ## Creating a MinHash instance

      mh = Approx.MinHash.new(128)
      mh = Approx.MinHash.new(256, seed: 42)

  ## Computing signatures and similarity

      sig_a = Approx.MinHash.signature(mh, MapSet.new(["cat", "dog", "fish"]))
      sig_b = Approx.MinHash.signature(mh, MapSet.new(["cat", "dog", "bird"]))
      Approx.MinHash.similarity(sig_a, sig_b)
      # => ~0.5 (true Jaccard is 2/4 = 0.5)

  ## Merging signatures

  Signatures from the same MinHash instance can be merged element-wise. The
  merged signature is equivalent to the signature of the union of the original
  sets:

      merged = Approx.MinHash.merge(sig_a, sig_b)

  ## Implementation notes

    * Hash functions use the universal hash family:
      `h_i(x) = ((a_i * hash(x) + b_i) mod p) mod max_val`
      where `p = 2^61 - 1` (a Mersenne prime) and `a_i`, `b_i` are random
      coefficients drawn uniformly.
    * Signatures are plain tuples of `non_neg_integer()` for fast positional
      access and compact representation.
    * All operations are pure and the struct is immutable.
  """

  import Bitwise

  @enforce_keys [:num_hashes, :coefficients_a, :coefficients_b, :hash_fn, :seed]
  defstruct [:num_hashes, :coefficients_a, :coefficients_b, :hash_fn, :seed]

  # Mersenne prime 2^61 - 1, used as the modulus for universal hashing.
  @mersenne_prime bsl(1, 61) - 1

  # Maximum hash output value. Signatures store values in 0..(max_val - 1).
  @max_val bsl(1, 32)

  @typedoc "A MinHash instance holding hash function coefficients."
  @type t :: %__MODULE__{
          num_hashes: pos_integer(),
          coefficients_a: tuple(),
          coefficients_b: tuple(),
          hash_fn: (term() -> non_neg_integer()),
          seed: :rand.state()
        }

  @typedoc "A signature produced by `signature/2` — a tuple of minimum hash values."
  @type signature :: tuple()

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new MinHash instance with `num_hashes` hash functions.

  More hash functions yield a more accurate similarity estimate at the cost of
  larger signatures. 128 is a reasonable default; 256 or higher may be used
  when precision matters.

  ## Options

    * `:hash_fn` — a 1-arity function `(term -> non_neg_integer())`.
      Defaults to the built-in 32-bit hash.
    * `:seed` — an integer seed for reproducible coefficient generation.
      When omitted, a random seed is used. Passing the same seed always
      produces the same coefficients, which is required when comparing
      signatures produced on different nodes.

  ## Raises

    * `FunctionClauseError` if `num_hashes` is not a positive integer.

  ## Returns

  A `%Approx.MinHash{}` struct containing the generated hash function
  coefficients. The struct itself does not hold any set data; pass it to
  `signature/2` to compute signatures for specific sets.

  ## Examples

      iex> mh = Approx.MinHash.new(64)
      iex> mh.num_hashes
      64

      iex> mh = Approx.MinHash.new(128, seed: 42)
      iex> mh.num_hashes
      128
  """
  @spec new(pos_integer(), keyword()) :: t()
  def new(num_hashes \\ 128, opts \\ [])

  def new(num_hashes, opts) when is_integer(num_hashes) and num_hashes > 0 do
    hash_fn = Keyword.get(opts, :hash_fn, &Approx.Hash.hash32/1)

    seed =
      case Keyword.get(opts, :seed) do
        nil -> :rand.seed_s(:exsss)
        seed_value -> :rand.seed_s(:exsss, seed_value)
      end

    {a_values, b_values, final_seed} = generate_coefficients(num_hashes, seed)

    %__MODULE__{
      num_hashes: num_hashes,
      coefficients_a: List.to_tuple(a_values),
      coefficients_b: List.to_tuple(b_values),
      hash_fn: hash_fn,
      seed: final_seed
    }
  end

  # ---------------------------------------------------------------------------
  # Signature computation
  # ---------------------------------------------------------------------------

  @doc """
  Computes the MinHash signature for a set of elements.

  The signature is a tuple of `num_hashes` non-negative integers, where each
  position holds the minimum hash value observed across all set elements for
  the corresponding hash function.

  For an empty set, every position in the signature is set to the maximum
  hash value (`2^32 - 1`), since no element was observed.

  ## Parameters

    * `mh` — a `%Approx.MinHash{}` struct.
    * `set` — any `Enumerable` of terms (e.g., `MapSet`, list, `Stream`).

  ## Returns

  A `t:signature/0` tuple of `num_hashes` non-negative integers.

  ## Examples

      iex> mh = Approx.MinHash.new(4, seed: 1)
      iex> sig = Approx.MinHash.signature(mh, MapSet.new(["a", "b", "c"]))
      iex> tuple_size(sig)
      4

      iex> mh = Approx.MinHash.new(4, seed: 1)
      iex> sig = Approx.MinHash.signature(mh, [])
      iex> sig == Tuple.duplicate(4_294_967_295, 4)
      true
  """
  @spec signature(t(), Enumerable.t()) :: signature()
  def signature(%__MODULE__{} = mh, set) do
    max = @max_val - 1
    initial = Tuple.duplicate(max, mh.num_hashes)

    coeffs_a = mh.coefficients_a
    coeffs_b = mh.coefficients_b
    num = mh.num_hashes

    Enum.reduce(set, initial, fn element, acc ->
      raw_hash = mh.hash_fn.(element)
      update_signature(acc, coeffs_a, coeffs_b, num, raw_hash)
    end)
  end

  # ---------------------------------------------------------------------------
  # Similarity estimation
  # ---------------------------------------------------------------------------

  @doc """
  Estimates the Jaccard similarity between two sets from their signatures.

  The estimate is the fraction of positions where the two signatures agree.
  Both signatures must have the same length (i.e., produced by MinHash
  instances with the same `num_hashes`).

  > **Note**: Unlike other Approx modules where `similarity/2` operates on struct
  > pairs, MinHash's `similarity/2` operates on **signature tuples** produced by
  > `signature/2`. This is because the MinHash struct holds only the hash
  > function coefficients, not any set data.

  ## Parameters

    * `sig1` — a signature tuple from `signature/2`.
    * `sig2` — a signature tuple from `signature/2`, same length as `sig1`.

  ## Returns

  A float between `0.0` and `1.0` inclusive, representing the estimated
  Jaccard similarity `|A intersection B| / |A union B|`.

  ## Examples

      iex> mh = Approx.MinHash.new(128, seed: 42)
      iex> sig = Approx.MinHash.signature(mh, MapSet.new([1, 2, 3]))
      iex> Approx.MinHash.similarity(sig, sig)
      1.0

      iex> mh = Approx.MinHash.new(128, seed: 42)
      iex> sig1 = Approx.MinHash.signature(mh, MapSet.new(1..100))
      iex> sig2 = Approx.MinHash.signature(mh, MapSet.new(200..300))
      iex> Approx.MinHash.similarity(sig1, sig2) < 0.1
      true
  """
  @spec similarity(signature(), signature()) :: float()
  def similarity(sig1, sig2)
      when is_tuple(sig1) and is_tuple(sig2) and tuple_size(sig1) == tuple_size(sig2) do
    size = tuple_size(sig1)

    if size == 0 do
      1.0
    else
      matches = count_matches(sig1, sig2, size, 0, 0)
      matches / size
    end
  end

  # ---------------------------------------------------------------------------
  # Merge
  # ---------------------------------------------------------------------------

  @doc """
  Merges two signatures by taking the element-wise minimum.

  The merged signature estimates the similarity of the union of the original
  sets against any other set. Both signatures must have the same length.

  > **Note**: Unlike other Approx modules where `merge/2` operates on struct
  > pairs, MinHash's `merge/2` operates on **signature tuples** produced by
  > `signature/2`. This is because the MinHash struct holds only the hash
  > function coefficients, not any set data.

  ## Parameters

    * `sig1` — a signature tuple from `signature/2`.
    * `sig2` — a signature tuple from `signature/2`, same length as `sig1`.

  ## Returns

  A new `t:signature/0` tuple where each position holds the minimum of the
  corresponding positions in `sig1` and `sig2`.

  ## Examples

      iex> mh = Approx.MinHash.new(4, seed: 99)
      iex> sig1 = Approx.MinHash.signature(mh, MapSet.new(["a", "b"]))
      iex> sig2 = Approx.MinHash.signature(mh, MapSet.new(["c", "d"]))
      iex> merged = Approx.MinHash.merge(sig1, sig2)
      iex> tuple_size(merged)
      4

      iex> mh = Approx.MinHash.new(4, seed: 99)
      iex> sig1 = Approx.MinHash.signature(mh, MapSet.new(["a"]))
      iex> sig2 = Approx.MinHash.signature(mh, MapSet.new(["a"]))
      iex> Approx.MinHash.merge(sig1, sig2) == sig1
      true
  """
  @spec merge(signature(), signature()) :: signature()
  def merge(sig1, sig2)
      when is_tuple(sig1) and is_tuple(sig2) and tuple_size(sig1) == tuple_size(sig2) do
    size = tuple_size(sig1)
    merge_tuples(sig1, sig2, size)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Generate `count` pairs of random coefficients {a, b} for the universal
  # hash family. `a` is drawn from 1..(p-1) and `b` from 0..(p-1).
  # Returns {a_values, b_values, final_state} where a_values and b_values are
  # lists suitable for conversion to tuples.
  @spec generate_coefficients(pos_integer(), :rand.state()) ::
          {[pos_integer()], [non_neg_integer()], :rand.state()}
  defp generate_coefficients(count, seed) do
    {a_rev, b_rev, final_state} =
      Enum.reduce(1..count, {[], [], seed}, fn _i, {a_acc, b_acc, state} ->
        {a, state} = :rand.uniform_s(@mersenne_prime - 1, state)
        {b, state} = :rand.uniform_s(@mersenne_prime, state)
        # a must be >= 1; :rand.uniform_s(n, state) returns 1..n
        # b must be >= 0; shift down by 1
        {[a | a_acc], [b - 1 | b_acc], state}
      end)

    {Enum.reverse(a_rev), Enum.reverse(b_rev), final_state}
  end

  # Update every position in the signature tuple for a single element's raw hash.
  # Builds a new list via comprehension and converts to tuple once, avoiding
  # O(num_hashes^2) word copies from iterative put_elem on tuples.
  @spec update_signature(signature(), tuple(), tuple(), non_neg_integer(), non_neg_integer()) ::
          signature()
  defp update_signature(signature, coefficients_a, coefficients_b, n, hash_val) do
    list =
      for i <- 0..(n - 1) do
        a = elem(coefficients_a, i)
        b = elem(coefficients_b, i)
        candidate = rem(rem(a * hash_val + b, @mersenne_prime), @max_val)
        min(elem(signature, i), candidate)
      end

    List.to_tuple(list)
  end

  # Count the number of positions where both signatures have the same value.
  @spec count_matches(
          signature(),
          signature(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          non_neg_integer()
  defp count_matches(_sig1, _sig2, size, index, acc) when index >= size, do: acc

  defp count_matches(sig1, sig2, size, index, acc) do
    new_acc =
      if elem(sig1, index) == elem(sig2, index) do
        acc + 1
      else
        acc
      end

    count_matches(sig1, sig2, size, index + 1, new_acc)
  end

  # Merge two signature tuples by taking element-wise minimums.
  # Builds a list via comprehension and converts to tuple once, avoiding
  # O(n^2) word copies from iterative put_elem on tuples.
  @spec merge_tuples(signature(), signature(), non_neg_integer()) :: signature()
  defp merge_tuples(sig1, sig2, size) do
    list = for i <- 0..(size - 1), do: min(elem(sig1, i), elem(sig2, i))
    List.to_tuple(list)
  end
end
