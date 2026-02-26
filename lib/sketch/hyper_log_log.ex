defmodule Sketch.HyperLogLog do
  @moduledoc """
  A HyperLogLog cardinality estimator.

  HyperLogLog is a probabilistic data structure for estimating the number of
  distinct elements (cardinality) in a multiset. It uses dramatically less
  memory than exact counting while providing estimates with a standard error
  of approximately `1.04 / sqrt(m)`, where `m = 2^precision`.

  ## How It Works

  Each element is hashed to a 32-bit value. The first `p` bits determine a
  register index (one of `m = 2^p` registers), and the remaining `32 - p` bits
  are scanned for the position of the first 1-bit (leading zeros + 1). Each
  register stores the maximum such value seen. The harmonic mean of the
  register values produces the cardinality estimate.

  ## Precision and Accuracy

  The `precision` parameter (`p`) controls the trade-off between memory usage
  and estimation accuracy:

  | Precision | Registers | Memory  | Std Error |
  |-----------|-----------|---------|-----------|
  | 4         | 16        | 16 B    | ~26%      |
  | 8         | 256       | 256 B   | ~6.5%     |
  | 10        | 1024      | 1 KiB   | ~3.25%    |
  | 12        | 4096      | 4 KiB   | ~1.625%   |
  | 14        | 16384     | 16 KiB  | ~0.8125%  |
  | 16        | 65536     | 64 KiB  | ~0.40625% |

  ## Examples

      iex> hll = Sketch.HyperLogLog.new(10)
      iex> hll = Enum.reduce(1..1000, hll, &Sketch.HyperLogLog.add(&2, &1))
      iex> count = Sketch.HyperLogLog.count(hll)
      iex> abs(count - 1000) < 1000 * 0.1
      true

  ## Merging

  HyperLogLog structures with the same precision can be merged, which is useful
  for distributed counting. The merged result approximates the cardinality of
  the union of both input sets.

      iex> hll1 = Enum.reduce(1..500, Sketch.HyperLogLog.new(10), &Sketch.HyperLogLog.add(&2, &1))
      iex> hll2 = Enum.reduce(501..1000, Sketch.HyperLogLog.new(10), &Sketch.HyperLogLog.add(&2, &1))
      iex> {:ok, merged} = Sketch.HyperLogLog.merge(hll1, hll2)
      iex> count = Sketch.HyperLogLog.count(merged)
      iex> abs(count - 1000) < 1000 * 0.1
      true

  ## Serialization

  Structures can be serialized to a compact binary for storage or transmission
  and deserialized back with no loss of information.

      iex> hll = Sketch.HyperLogLog.new(4)
      iex> hll = Sketch.HyperLogLog.add(hll, "hello")
      iex> binary = Sketch.HyperLogLog.to_binary(hll)
      iex> {:ok, restored} = Sketch.HyperLogLog.from_binary(binary)
      iex> Sketch.HyperLogLog.count(restored) == Sketch.HyperLogLog.count(hll)
      true
  """

  import Bitwise

  @enforce_keys [:registers, :precision, :hash_fn]
  defstruct [:registers, :precision, :hash_fn]

  @typedoc """
  A HyperLogLog cardinality estimator struct.

  Fields:

    * `:registers` — A tuple of `2^precision` register values (non-negative
      integers). Each register stores the maximum observed leading-zeros-plus-one
      value for its hash bucket.
    * `:precision` — An integer from `4` to `16` controlling the number of
      registers and thus the trade-off between memory usage and estimation
      accuracy.
    * `:hash_fn` — A function `(term() -> non_neg_integer())` used to hash
      elements into 32-bit values.
  """
  @type t :: %__MODULE__{
          registers: tuple(),
          precision: 4..16,
          hash_fn: (term() -> non_neg_integer())
        }

  @serialization_version 1
  @min_precision 4
  @max_precision 16
  @two_pow_32 bsl(1, 32)

  # ── Public API ──────────────────────────────────────────────────────────

  @doc """
  Creates a new HyperLogLog estimator with the given precision.

  ## Parameters

    * `precision` — An integer from 4 to 16 (default 14). Higher precision uses
      more memory but produces more accurate estimates. The number of registers
      is `2^precision`.
    * `opts` — A keyword list of options:
      * `:hash_fn` — A function `(term() -> non_neg_integer())` that returns a
        32-bit hash. Defaults to the built-in 32-bit hash.

  ## Returns

  A new `%Sketch.HyperLogLog{}` struct with all registers initialized to zero.

  ## Raises

    * `ArgumentError` if `precision` is an integer outside `4..16`.

  ## Examples

      iex> hll = Sketch.HyperLogLog.new()
      iex> hll.precision
      14

      iex> hll = Sketch.HyperLogLog.new(8)
      iex> hll.precision
      8

      iex> hll = Sketch.HyperLogLog.new(4, hash_fn: fn term -> :erlang.phash2(term, 4294967296) end)
      iex> hll.precision
      4
  """
  @spec new(precision :: 4..16, opts :: keyword()) :: t()
  def new(precision \\ 14, opts \\ [])

  def new(precision, opts)
      when is_integer(precision) and precision >= @min_precision and precision <= @max_precision do
    m = bsl(1, precision)
    hash_fn = Keyword.get(opts, :hash_fn, &Sketch.Hash.hash32/1)

    %__MODULE__{
      registers: :erlang.make_tuple(m, 0),
      precision: precision,
      hash_fn: hash_fn
    }
  end

  def new(precision, _opts) when is_integer(precision) do
    raise ArgumentError,
          "precision must be between #{@min_precision} and #{@max_precision}, got: #{precision}"
  end

  @doc """
  Adds an element to the HyperLogLog estimator.

  The element is hashed, and the resulting hash determines which register is
  updated. Duplicate elements have no effect on the estimator beyond what was
  recorded on first insertion.

  ## Parameters

    * `hll` — A `%Sketch.HyperLogLog{}` struct.
    * `element` — Any term to add.

  ## Returns

  An updated `%Sketch.HyperLogLog{}` struct.

  ## Examples

      iex> hll = Sketch.HyperLogLog.new(4)
      iex> hll = Sketch.HyperLogLog.add(hll, "hello")
      iex> Sketch.HyperLogLog.count(hll) >= 1
      true

      iex> hll = Sketch.HyperLogLog.new(4)
      iex> hll = Sketch.HyperLogLog.add(hll, :atom_value)
      iex> Sketch.HyperLogLog.count(hll) >= 1
      true
  """
  @spec add(t(), term()) :: t()
  def add(%__MODULE__{registers: registers, precision: p, hash_fn: hash_fn} = hll, element) do
    hash = hash_fn.(element)

    # First p bits determine the register index
    index = hash >>> (32 - p)

    # Remaining 32-p bits are used to compute rho (leading zeros + 1)
    remaining_bits = 32 - p
    # Mask off the first p bits, keeping only the lower (32-p) bits
    w = band(hash, bsl(1, remaining_bits) - 1)
    rho = leading_zeros(w, remaining_bits) + 1

    current = elem(registers, index)

    if rho > current do
      %{hll | registers: put_elem(registers, index, rho)}
    else
      hll
    end
  end

  @doc """
  Returns the estimated cardinality (number of distinct elements).

  Uses the harmonic mean estimator with small-range and large-range corrections
  as described in the original HyperLogLog paper.

  ## Parameters

    * `hll` — A `%Sketch.HyperLogLog{}` struct.

  ## Returns

  A non-negative number (the estimated cardinality), rounded to the nearest integer.

  ## Examples

      iex> Sketch.HyperLogLog.count(Sketch.HyperLogLog.new())
      0

      iex> hll = Enum.reduce(1..100, Sketch.HyperLogLog.new(14), &Sketch.HyperLogLog.add(&2, &1))
      iex> count = Sketch.HyperLogLog.count(hll)
      iex> count > 0
      true
  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{registers: registers, precision: p}) do
    m = bsl(1, p)
    alpha = alpha_m(m)

    # Compute the harmonic mean: sum of 2^(-register[j])
    {sum, zero_count} = harmonic_sum(registers, m)

    # Raw estimate
    raw_estimate = alpha * m * m / sum

    estimate =
      cond do
        # Small range correction: linear counting
        raw_estimate <= 5.0 / 2.0 * m and zero_count > 0 ->
          m * :math.log(m / zero_count)

        # Large range correction (only when estimate is below 2^32)
        raw_estimate > @two_pow_32 / 30.0 and raw_estimate < @two_pow_32 ->
          -@two_pow_32 * :math.log(1.0 - raw_estimate / @two_pow_32)

        # No correction needed
        true ->
          raw_estimate
      end

    round(estimate)
  end

  @doc """
  Merges two HyperLogLog estimators into one.

  The merged estimator approximates the cardinality of the union of both input
  sets. Both estimators must have the same precision.

  ## Parameters

    * `hll1` — A `%Sketch.HyperLogLog{}` struct.
    * `hll2` — A `%Sketch.HyperLogLog{}` struct with the same precision as `hll1`.

  ## Returns

  `{:ok, merged}` on success, or `{:error, :incompatible_precision}` if the
  precisions do not match.

  ## Examples

      iex> hll1 = Sketch.HyperLogLog.add(Sketch.HyperLogLog.new(4), "a")
      iex> hll2 = Sketch.HyperLogLog.add(Sketch.HyperLogLog.new(4), "b")
      iex> {:ok, merged} = Sketch.HyperLogLog.merge(hll1, hll2)
      iex> Sketch.HyperLogLog.count(merged) >= 1
      true
  """
  @spec merge(t(), t()) :: {:ok, t()} | {:error, :incompatible_precision}
  def merge(
        %__MODULE__{precision: p, registers: r1} = hll1,
        %__MODULE__{precision: p, registers: r2}
      ) do
    m = bsl(1, p)
    merged_registers = merge_registers(r1, r2, m)
    {:ok, %{hll1 | registers: merged_registers}}
  end

  def merge(%__MODULE__{}, %__MODULE__{}) do
    {:error, :incompatible_precision}
  end

  @doc """
  Serializes the HyperLogLog estimator to a binary.

  The binary format is:

    * 1 byte — serialization version (currently 1)
    * 1 byte — precision
    * `2^precision` bytes — one byte per register

  ## Parameters

    * `hll` — A `%Sketch.HyperLogLog{}` struct.

  ## Returns

  A binary representing the serialized estimator.

  ## Examples

      iex> hll = Sketch.HyperLogLog.new(4)
      iex> binary = Sketch.HyperLogLog.to_binary(hll)
      iex> is_binary(binary)
      true

      iex> hll = Sketch.HyperLogLog.new(4)
      iex> binary = Sketch.HyperLogLog.to_binary(hll)
      iex> byte_size(binary)
      18
  """
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{registers: registers, precision: p}) do
    m = bsl(1, p)
    register_bytes = registers_to_binary(registers, m)
    <<@serialization_version::8, p::8, register_bytes::binary>>
  end

  @doc """
  Deserializes a HyperLogLog estimator from a binary.

  Restores a HyperLogLog struct previously serialized with `to_binary/1`. By
  default the hash function is reset to the built-in 32-bit hash; pass the
  `:hash_fn` option to override.

  ## Parameters

    * `binary` — A binary produced by `to_binary/1`.
    * `opts` — A keyword list of options:
      * `:hash_fn` — A function `(term() -> non_neg_integer())` that returns a
        32-bit hash. Defaults to the built-in 32-bit hash.

  ## Returns

  `{:ok, hll}` on success, or `{:error, :invalid_binary}` on failure.

  ## Examples

      iex> hll = Sketch.HyperLogLog.new(4)
      iex> hll = Sketch.HyperLogLog.add(hll, "test")
      iex> {:ok, restored} = Sketch.HyperLogLog.from_binary(Sketch.HyperLogLog.to_binary(hll))
      iex> Sketch.HyperLogLog.count(restored) == Sketch.HyperLogLog.count(hll)
      true
  """
  @spec from_binary(binary(), keyword()) :: {:ok, t()} | {:error, :invalid_binary}
  def from_binary(binary, opts \\ [])

  def from_binary(<<@serialization_version::8, p::8, register_bytes::binary>>, opts)
      when p >= @min_precision and p <= @max_precision do
    m = bsl(1, p)

    if byte_size(register_bytes) != m do
      {:error, :invalid_binary}
    else
      registers = binary_to_registers(register_bytes, m)
      hash_fn = Keyword.get(opts, :hash_fn, &Sketch.Hash.hash32/1)

      {:ok,
       %__MODULE__{
         registers: registers,
         precision: p,
         hash_fn: hash_fn
       }}
    end
  end

  def from_binary(<<_version::8, _rest::binary>>, _opts) do
    {:error, :invalid_binary}
  end

  def from_binary(_binary, _opts) do
    {:error, :invalid_binary}
  end

  # ── Private Helpers ─────────────────────────────────────────────────────

  # Counts the number of leading zeros in the lower `bits` bits of `value`.
  # If all bits are zero, returns `bits`.
  # Uses :math.log2 for O(1) computation instead of a bit-by-bit linear scan.
  @spec leading_zeros(non_neg_integer(), pos_integer()) :: non_neg_integer()
  defp leading_zeros(0, bits), do: bits

  defp leading_zeros(value, bits) do
    bits - 1 - trunc(:math.log2(value))
  end

  # Computes the harmonic sum and counts zero registers.
  @spec harmonic_sum(tuple(), pos_integer()) :: {float(), non_neg_integer()}
  defp harmonic_sum(registers, m) do
    do_harmonic_sum(registers, m, 0, 0.0, 0)
  end

  defp do_harmonic_sum(_registers, m, m, sum, zero_count), do: {sum, zero_count}

  defp do_harmonic_sum(registers, m, index, sum, zero_count) do
    val = elem(registers, index)

    new_zero_count = if val == 0, do: zero_count + 1, else: zero_count
    # 2^(-val) = 1 / 2^val — bit shift is faster than :math.pow for integer exponents
    new_sum = sum + 1.0 / bsl(1, val)

    do_harmonic_sum(registers, m, index + 1, new_sum, new_zero_count)
  end

  # Alpha constant for the bias correction, depends on the number of registers.
  @spec alpha_m(pos_integer()) :: float()
  defp alpha_m(16), do: 0.673
  defp alpha_m(32), do: 0.697
  defp alpha_m(64), do: 0.709
  defp alpha_m(m), do: 0.7213 / (1.0 + 1.079 / m)

  # Element-wise max of two register tuples.
  # Builds a list via comprehension and converts to tuple in one pass,
  # avoiding O(m²) from repeated put_elem on a large tuple.
  @spec merge_registers(tuple(), tuple(), pos_integer()) :: tuple()
  defp merge_registers(r1, r2, m) do
    merged = for i <- 0..(m - 1), do: max(elem(r1, i), elem(r2, i))
    List.to_tuple(merged)
  end

  # Converts a register tuple to a binary (one byte per register).
  # Uses an IO list to avoid O(m²) repeated binary concatenation.
  @spec registers_to_binary(tuple(), pos_integer()) :: binary()
  defp registers_to_binary(registers, m) do
    iodata = for i <- 0..(m - 1), do: <<elem(registers, i)::8>>
    :erlang.iolist_to_binary(iodata)
  end

  # Converts a binary back to a register tuple.
  @spec binary_to_registers(binary(), pos_integer()) :: tuple()
  defp binary_to_registers(binary, _m) do
    binary
    |> :binary.bin_to_list()
    |> List.to_tuple()
  end
end
