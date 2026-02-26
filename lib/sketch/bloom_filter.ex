defmodule Sketch.BloomFilter do
  @moduledoc """
  A Bloom filter — a space-efficient probabilistic set membership structure.

  A Bloom filter can tell you with certainty that an element is **not** in the
  set (no false negatives), or that it **might** be in the set (possible false
  positives at a controlled rate).

  ## When to use

    * Checking if a username is already taken before hitting the database
    * Pre-filtering expensive lookups — skip work when the element is
      definitely absent
    * Deduplicating a stream of events without storing every event

  ## Creating a filter

  Create a filter by specifying the expected number of elements (capacity) and
  the desired false positive probability:

      bf = Sketch.BloomFilter.new(1_000)              # 1 % FPP (default)
      bf = Sketch.BloomFilter.new(1_000, 0.001)       # 0.1 % FPP

  ## Adding elements and querying

      bf = Sketch.BloomFilter.new(100)
      bf = Sketch.BloomFilter.add(bf, "hello")
      Sketch.BloomFilter.member?(bf, "hello")   # => true
      Sketch.BloomFilter.member?(bf, "world")   # => false (probably)

  ## Merging filters

  Two filters created with the same parameters can be merged (bitwise OR).
  This is useful in distributed or parallel pipelines where each node builds
  its own filter and you combine them afterwards:

      merged = Sketch.BloomFilter.merge(bf1, bf2)

  ## Serialization

  Filters can be serialized to and from binaries for storage or network
  transfer:

      bin = Sketch.BloomFilter.to_binary(bf)
      {:ok, bf} = Sketch.BloomFilter.from_binary(bin)

  The wire format is:

      <<version::8, size::big-64, hash_count::big-16, bits::binary>>

  ## Implementation notes

  * Bit storage uses a plain binary; individual bits are addressed via
    byte-offset and `bor`/`band` with `Bitwise`.
  * Hash positions are derived using the Kirsch-Mitzenmacker double-hashing
    scheme: `h1(x) + i * h2(x)` for `i` in `0..k-1`, which provides
    the same theoretical guarantees as `k` independent hash functions.
  * The optimal bit-array size `m` and hash-function count `k` are computed
    from the requested capacity `n` and false-positive probability `p`:
    - `m = ceil(-n * ln(p) / (ln(2))^2)`
    - `k = ceil(m / n * ln(2))`
  """

  import Bitwise

  @enforce_keys [:bits, :size, :hash_count, :hash_fn]
  defstruct [:bits, :size, :hash_count, :hash_fn]

  @typedoc "A Bloom filter struct."
  @type t :: %__MODULE__{
          bits: binary(),
          size: pos_integer(),
          hash_count: pos_integer(),
          hash_fn: (term() -> non_neg_integer())
        }

  @serialization_version 1

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new Bloom filter sized for `capacity` elements at the given
  false-positive probability.

  ## Parameters

    * `capacity` — the expected number of distinct elements to insert
      (positive integer).
    * `false_positive_probability` — desired FPP, a float between 0 and 1
      exclusive. Defaults to `0.01` (1 %).
    * `opts` — keyword list of options:
      * `:hash_fn` — a 1-arity function `(term -> non_neg_integer())`.
        Defaults to `&Sketch.Hash.hash32/1`. Useful for injecting a
        deterministic hash in tests.

  ## Examples

      iex> bf = Sketch.BloomFilter.new(100)
      iex> bf.hash_count > 0
      true

      iex> bf = Sketch.BloomFilter.new(1_000, 0.001)
      iex> bf.size > 0
      true
  """
  @spec new(pos_integer(), float(), keyword()) :: t()
  def new(capacity, false_positive_probability \\ 0.01, opts \\ [])

  def new(capacity, false_positive_probability, opts)
      when is_integer(capacity) and capacity > 0 and
             is_float(false_positive_probability) and
             false_positive_probability > 0.0 and
             false_positive_probability < 1.0 do
    m = optimal_bit_count(capacity, false_positive_probability)
    k = optimal_hash_count(m, capacity)
    hash_fn = Keyword.get(opts, :hash_fn, &Sketch.Hash.hash32/1)
    byte_count = div(m + 7, 8)

    %__MODULE__{
      bits: <<0::size(byte_count * 8)>>,
      size: m,
      hash_count: k,
      hash_fn: hash_fn
    }
  end

  # ---------------------------------------------------------------------------
  # Insertion
  # ---------------------------------------------------------------------------

  @doc """
  Adds an element to the Bloom filter, returning an updated filter.

  The element can be any Erlang/Elixir term.

  ## Examples

      iex> bf = Sketch.BloomFilter.new(100)
      iex> bf = Sketch.BloomFilter.add(bf, "hello")
      iex> Sketch.BloomFilter.member?(bf, "hello")
      true
  """
  @spec add(t(), term()) :: t()
  def add(%__MODULE__{size: m, hash_count: k, hash_fn: hash_fn} = bf, element) do
    # Compute h1 and h2 once, then build a map of byte_index => accumulated OR mask.
    # This fuses hash_positions (Issue 2) and avoids per-bit binary rebuilds (Issue 1).
    h1 = hash_fn.(element)
    h2 = hash_fn.({:__sketch_h2__, element})

    mask_map = build_mask_map(h1, h2, k, m, 0, %{})

    # Single pass over the binary: walk byte-by-byte, OR in masks where needed.
    # This is O(m/8) regardless of k, versus the previous O(m/8 * k).
    new_bits = apply_masks(bf.bits, mask_map)

    %{bf | bits: new_bits}
  end

  # ---------------------------------------------------------------------------
  # Membership query
  # ---------------------------------------------------------------------------

  @doc """
  Checks whether `element` might be a member of the filter.

  Returns `true` if the element is **possibly** in the set (with the
  configured false-positive rate), or `false` if the element is **definitely
  not** in the set.

  ## Examples

      iex> bf = Sketch.BloomFilter.new(100)
      iex> Sketch.BloomFilter.member?(bf, "ghost")
      false

      iex> bf = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("present")
      iex> Sketch.BloomFilter.member?(bf, "present")
      true
  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{size: m, hash_count: k, hash_fn: hash_fn, bits: bits}, element) do
    # Short-circuiting check: compute one position at a time and return false
    # immediately when an unset bit is found, avoiding intermediate list
    # allocation (Issue 3).
    h1 = hash_fn.(element)
    h2 = hash_fn.({:__sketch_h2__, element})
    check_bits(bits, h1, h2, m, k, 0)
  end

  # ---------------------------------------------------------------------------
  # Merge / Union
  # ---------------------------------------------------------------------------

  @doc """
  Merges two Bloom filters by performing a bitwise OR of their bit arrays.

  Both filters **must** have the same `size` and `hash_count`; otherwise an
  error tuple is returned. After merging, `member?/2` returns `true` for any
  element that was added to either filter.

  ## Examples

      iex> bf1 = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("a")
      iex> bf2 = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("b")
      iex> {:ok, merged} = Sketch.BloomFilter.merge(bf1, bf2)
      iex> Sketch.BloomFilter.member?(merged, "a") and Sketch.BloomFilter.member?(merged, "b")
      true
  """
  @spec merge(t(), t()) :: {:ok, t()} | {:error, :incompatible_filters}
  def merge(
        %__MODULE__{size: size, hash_count: k} = bf1,
        %__MODULE__{size: size, hash_count: k} = bf2
      ) do
    merged_bits = bitwise_or_binaries(bf1.bits, bf2.bits)
    {:ok, %{bf1 | bits: merged_bits}}
  end

  def merge(%__MODULE__{}, %__MODULE__{}), do: {:error, :incompatible_filters}

  @doc """
  Alias for `merge/2`.

  Merges two compatible Bloom filters via bitwise OR.

  ## Examples

      iex> bf1 = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("x")
      iex> bf2 = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("y")
      iex> {:ok, merged} = Sketch.BloomFilter.union(bf1, bf2)
      iex> Sketch.BloomFilter.member?(merged, "x")
      true
  """
  @spec union(t(), t()) :: {:ok, t()} | {:error, :incompatible_filters}
  def union(bf1, bf2), do: merge(bf1, bf2)

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  @doc """
  Serializes the Bloom filter to a binary.

  The format is:

      <<version::8, size::big-unsigned-64, hash_count::big-unsigned-16, bits::binary>>

  This representation is deterministic and suitable for storage in a database
  column, file, or network message. The `hash_fn` is **not** serialized; on
  deserialization the default hash function is restored.

  ## Examples

      iex> bf = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("test")
      iex> bin = Sketch.BloomFilter.to_binary(bf)
      iex> is_binary(bin)
      true
  """
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{} = bf) do
    <<@serialization_version::8, bf.size::big-unsigned-64, bf.hash_count::big-unsigned-16,
      bf.bits::binary>>
  end

  @doc """
  Deserializes a Bloom filter from a binary produced by `to_binary/1`.

  Returns `{:ok, bloom_filter}` on success or `{:error, reason}` on failure.

  The default hash function (`&Sketch.Hash.hash32/1`) is used for the
  restored filter. Pass `:hash_fn` in `opts` to override.

  ## Options

    * `:hash_fn` — a 1-arity hash function to attach to the deserialized
      filter. Defaults to `&Sketch.Hash.hash32/1`.

  ## Examples

      iex> bf = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("round_trip")
      iex> {:ok, restored} = bf |> Sketch.BloomFilter.to_binary() |> Sketch.BloomFilter.from_binary()
      iex> Sketch.BloomFilter.member?(restored, "round_trip")
      true
  """
  @spec from_binary(binary(), keyword()) :: {:ok, t()} | {:error, :invalid_binary}
  def from_binary(binary, opts \\ [])

  def from_binary(
        <<@serialization_version::8, size::big-unsigned-64, hash_count::big-unsigned-16,
          bits::binary>>,
        opts
      )
      when size > 0 and hash_count > 0 do
    expected_bytes = div(size + 7, 8)

    if byte_size(bits) == expected_bytes do
      hash_fn = Keyword.get(opts, :hash_fn, &Sketch.Hash.hash32/1)

      {:ok,
       %__MODULE__{
         bits: bits,
         size: size,
         hash_count: hash_count,
         hash_fn: hash_fn
       }}
    else
      {:error, :invalid_binary}
    end
  end

  def from_binary(_binary, _opts), do: {:error, :invalid_binary}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  @doc false
  @spec optimal_bit_count(pos_integer(), float()) :: pos_integer()
  def optimal_bit_count(capacity, fpp) do
    m = -capacity * :math.log(fpp) / (:math.log(2) * :math.log(2))
    max(ceil(m), 8)
  end

  @doc false
  @spec optimal_hash_count(pos_integer(), pos_integer()) :: pos_integer()
  def optimal_hash_count(bit_count, capacity) do
    k = bit_count / capacity * :math.log(2)
    max(ceil(k), 1)
  end

  # Build a map of %{byte_index => accumulated_bit_mask} for all k hash positions.
  # Fuses hash position computation into a single pass, accumulating OR masks
  # per byte so each byte is written at most once.
  @spec build_mask_map(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          non_neg_integer(),
          map()
        ) :: map()
  defp build_mask_map(_h1, _h2, k, _m, i, acc) when i == k, do: acc

  defp build_mask_map(h1, h2, k, m, i, acc) do
    pos = Sketch.Hash.double_hash(h1, h2, i) |> rem(m) |> abs()
    byte_index = div(pos, 8)
    bit_mask = bsl(1, rem(pos, 8))
    updated = Map.update(acc, byte_index, bit_mask, &bor(&1, bit_mask))
    build_mask_map(h1, h2, k, m, i + 1, updated)
  end

  # Apply bit masks to specific byte positions in the binary.
  # Instead of walking every byte (O(m/8)), we sort the k modified positions
  # and splice only those bytes using binary_part for unchanged ranges.
  # This gives O(k log k) per add, which is critical for large filters.
  @spec apply_masks(binary(), map()) :: binary()
  defp apply_masks(bits, mask_map) when map_size(mask_map) == 0, do: bits

  defp apply_masks(bits, mask_map) do
    sorted = mask_map |> Map.to_list() |> List.keysort(0)
    total = byte_size(bits)
    iodata = apply_masks_splice(bits, sorted, 0, total, [])
    :erlang.iolist_to_binary(iodata)
  end

  # Build an iolist by copying unchanged byte ranges with binary_part and
  # inserting modified bytes at each position in the sorted mask list.
  defp apply_masks_splice(bits, [], pos, total, acc) do
    # Append the remaining unchanged tail.
    if pos < total do
      Enum.reverse([binary_part(bits, pos, total - pos) | acc])
    else
      Enum.reverse(acc)
    end
  end

  defp apply_masks_splice(bits, [{byte_idx, mask} | rest], pos, total, acc) do
    # Copy unchanged bytes before this position.
    acc =
      if byte_idx > pos do
        [binary_part(bits, pos, byte_idx - pos) | acc]
      else
        acc
      end

    # Read the original byte, OR in the mask, and append.
    original = :binary.at(bits, byte_idx)
    acc = [bor(original, mask) | acc]
    apply_masks_splice(bits, rest, byte_idx + 1, total, acc)
  end

  # Short-circuiting membership check: compute one hash position at a time
  # and return false immediately when an unset bit is found, avoiding
  # intermediate list allocation.
  @spec check_bits(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: boolean()
  defp check_bits(_bits, _h1, _h2, _m, k, i) when i == k, do: true

  defp check_bits(bits, h1, h2, m, k, i) do
    pos = Sketch.Hash.double_hash(h1, h2, i) |> rem(m) |> abs()
    byte_index = div(pos, 8)
    bit_offset = rem(pos, 8)
    byte = :binary.at(bits, byte_index)

    if band(byte, bsl(1, bit_offset)) != 0 do
      check_bits(bits, h1, h2, m, k, i + 1)
    else
      false
    end
  end

  # Bitwise OR two binaries of the same length.
  # Uses recursive binary pattern matching to avoid :binary.at per-byte overhead
  # and builds the result as an iolist for O(n) performance.
  @spec bitwise_or_binaries(binary(), binary()) :: binary()
  defp bitwise_or_binaries(a, b) when byte_size(a) == byte_size(b) do
    or_binaries_match(a, b, [])
  end

  defp or_binaries_match(<<>>, <<>>, acc) do
    acc |> Enum.reverse() |> :erlang.iolist_to_binary()
  end

  defp or_binaries_match(<<byte_a::8, rest_a::binary>>, <<byte_b::8, rest_b::binary>>, acc) do
    or_binaries_match(rest_a, rest_b, [bor(byte_a, byte_b) | acc])
  end
end
