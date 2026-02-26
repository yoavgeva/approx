defmodule Approx.CountMinSketch do
  @moduledoc """
  A Count-Min Sketch for frequency estimation of elements in a stream.

  The Count-Min Sketch is a probabilistic data structure that answers
  point queries of the form "how many times has element *x* been seen?"
  It uses sub-linear space and guarantees that the returned count is
  **never less** than the true count (no false negatives), but may
  overestimate by at most `epsilon * N` with probability `1 - delta`,
  where `N` is the total number of items added.

  ## Parameters

    * `epsilon` -- controls accuracy. Smaller values give tighter estimates
      at the cost of more memory. Default: `0.001`.
    * `delta` -- controls confidence. Smaller values make the accuracy
      guarantee hold with higher probability. Default: `0.01`.

  ## Storage

  The approx is stored as a tuple of tuples (a 2-D array) with `depth` rows
  and `width` columns, where:

    * `width  = ceil(e / epsilon)`
    * `depth  = ceil(ln(1 / delta))`

  Each cell is a non-negative integer counter.

  ## Hash Functions

  Row hashing uses the Kirsch-Mitzenmacker double-hashing technique.
  Two independent 32-bit hashes of the element (`h1` and `h2`) are
  computed, and the position in row `i` is `rem(h1 + i * h2, width)`.

  The hash function defaults to an internal 32-bit hash but can be
  overridden via the `:hash_fn` option for deterministic testing.

  ## Examples

      iex> cms = Approx.CountMinSketch.new()
      iex> cms = Approx.CountMinSketch.add(cms, "hello", 5)
      iex> Approx.CountMinSketch.count(cms, "hello") >= 5
      true

      iex> cms = Approx.CountMinSketch.new()
      iex> Approx.CountMinSketch.count(cms, "missing")
      0
  """

  @enforce_keys [:table, :width, :depth, :hash_fn]
  defstruct [:table, :width, :depth, :hash_fn]

  @typedoc "A Count-Min Sketch for sub-linear frequency estimation of stream elements.\n\nSee `new/3` for creation options."
  @type t :: %__MODULE__{
          table: tuple(),
          width: pos_integer(),
          depth: pos_integer(),
          hash_fn: (term() -> non_neg_integer())
        }

  @serialization_version 1

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new Count-Min Sketch with the given error parameters.

  ## Parameters

    * `epsilon` -- relative error bound. Must be positive. Default: `0.001`.
    * `delta` -- failure probability. Must be in `(0, 1)`. Default: `0.01`.
    * `opts` -- keyword list of options.

  ## Options

    * `:hash_fn` -- a function of arity 1 that maps a term to a
      non-negative integer. Defaults to the built-in 32-bit hash.

  ## Returns

    A `%Approx.CountMinSketch{}` struct.

  ## Raises

    * `ArgumentError` if `epsilon` is not positive or `delta` is not in `(0, 1)`.

  ## Examples

      iex> cms = Approx.CountMinSketch.new()
      iex> cms.width
      2719

      iex> cms = Approx.CountMinSketch.new(0.01, 0.05)
      iex> cms.depth
      3
  """
  @spec new(float(), float(), keyword()) :: t()
  def new(epsilon \\ 0.001, delta \\ 0.01, opts \\ []) do
    validate_params!(epsilon, delta)

    width = (:math.exp(1) / epsilon) |> ceil()
    depth = :math.log(1.0 / delta) |> ceil()
    hash_fn = Keyword.get(opts, :hash_fn, &Approx.Hash.hash32/1)

    table = build_table(width, depth)

    %__MODULE__{
      table: table,
      width: width,
      depth: depth,
      hash_fn: hash_fn
    }
  end

  # ---------------------------------------------------------------------------
  # Updating
  # ---------------------------------------------------------------------------

  @doc """
  Adds `amount` occurrences of `element` to the approx.

  The `amount` must be a positive integer (negative amounts are not
  supported because they would violate the "never undercount" guarantee).

  ## Parameters

    * `cms` -- the Count-Min Sketch.
    * `element` -- any term to record.
    * `amount` -- a positive integer count to add. Default: `1`.

  ## Returns

    An updated `%Approx.CountMinSketch{}` struct.

  ## Raises

    * `ArgumentError` if `amount` is not a positive integer.

  ## Examples

      iex> cms = Approx.CountMinSketch.new()
      iex> cms = Approx.CountMinSketch.add(cms, "apple")
      iex> Approx.CountMinSketch.count(cms, "apple") >= 1
      true

      iex> cms = Approx.CountMinSketch.new()
      iex> cms = Approx.CountMinSketch.add(cms, "banana", 10)
      iex> Approx.CountMinSketch.count(cms, "banana") >= 10
      true
  """
  @spec add(t(), term(), pos_integer()) :: t()
  def add(%__MODULE__{} = cms, element, amount \\ 1) do
    validate_amount!(amount)

    {h1, h2} = hash_pair(cms.hash_fn, element)
    table = do_add(cms.table, cms.width, h1, h2, amount, 0, cms.depth)
    %{cms | table: table}
  end

  defp do_add(table, _width, _h1, _h2, _amount, row, depth) when row >= depth, do: table

  defp do_add(table, width, h1, h2, amount, row, depth) do
    col = column_for(h1, h2, row, width)
    current_row = elem(table, row)
    old_val = elem(current_row, col)
    new_row = put_elem(current_row, col, old_val + amount)
    do_add(put_elem(table, row, new_row), width, h1, h2, amount, row + 1, depth)
  end

  @doc false
  @spec add_and_count(t(), term(), pos_integer()) :: {t(), non_neg_integer()}
  def add_and_count(%__MODULE__{} = cms, element, amount \\ 1) do
    validate_amount!(amount)
    {h1, h2} = hash_pair(cms.hash_fn, element)

    {table, min_val} = add_and_min(cms.table, cms.depth, cms.width, h1, h2, amount, 0, nil)

    {%{cms | table: table}, min_val}
  end

  # Tail-recursive helper that updates each row and tracks the minimum
  # counter value simultaneously. Avoids a second pass over the rows.
  defp add_and_min(table, depth, _width, _h1, _h2, _amount, row, current_min)
       when row >= depth do
    {table, current_min}
  end

  defp add_and_min(table, depth, width, h1, h2, amount, row, current_min) do
    col = column_for(h1, h2, row, width)
    row_tuple = elem(table, row)
    new_val = elem(row_tuple, col) + amount
    new_row = put_elem(row_tuple, col, new_val)
    new_table = put_elem(table, row, new_row)

    new_min =
      if current_min == nil do
        new_val
      else
        min(current_min, new_val)
      end

    add_and_min(new_table, depth, width, h1, h2, amount, row + 1, new_min)
  end

  # ---------------------------------------------------------------------------
  # Querying
  # ---------------------------------------------------------------------------

  @doc """
  Returns the estimated count for `element`.

  The returned value is the **minimum** across all rows in the approx,
  which is guaranteed to be >= the true count. In the worst case, the
  overestimate is bounded by `epsilon * N` with probability `1 - delta`.

  ## Parameters

    * `cms` -- the Count-Min Sketch.
    * `element` -- the term to query.

  ## Returns

    A non-negative integer representing the estimated frequency of `element`.

  ## Examples

      iex> cms = Approx.CountMinSketch.new()
      iex> cms = cms |> Approx.CountMinSketch.add("x", 3) |> Approx.CountMinSketch.add("y", 7)
      iex> Approx.CountMinSketch.count(cms, "x") >= 3
      true

      iex> cms = Approx.CountMinSketch.new()
      iex> Approx.CountMinSketch.count(cms, "never_added")
      0
  """
  @spec count(t(), term()) :: non_neg_integer()
  def count(%__MODULE__{} = cms, element) do
    {h1, h2} = hash_pair(cms.hash_fn, element)

    col = column_for(h1, h2, 0, cms.width)
    first = elem(elem(cms.table, 0), col)
    min_count(cms, h1, h2, 1, first)
  end

  # Tail-recursive helper that walks rows 1..depth-1, maintaining a running
  # minimum. Avoids allocating an intermediate list.
  defp min_count(%{depth: depth}, _h1, _h2, row, current_min) when row >= depth do
    current_min
  end

  defp min_count(cms, h1, h2, row, current_min) do
    col = column_for(h1, h2, row, cms.width)
    value = elem(elem(cms.table, row), col)
    min_count(cms, h1, h2, row + 1, min(value, current_min))
  end

  # ---------------------------------------------------------------------------
  # Merging
  # ---------------------------------------------------------------------------

  @doc """
  Merges two Count-Min Sketches by element-wise addition of counters.

  Both approxes must have the same `width` and `depth`. The resulting
  approx contains counts equivalent to having inserted all elements from
  both source approxes.

  ## Parameters

    * `cms1` -- the first Count-Min Sketch.
    * `cms2` -- the second Count-Min Sketch.

  ## Returns

    `{:ok, merged}` on success, or `{:error, :dimension_mismatch}` if the approxes have different `width` or `depth`.

  ## Examples

      iex> cms1 = Approx.CountMinSketch.new() |> Approx.CountMinSketch.add("a", 3)
      iex> cms2 = Approx.CountMinSketch.new() |> Approx.CountMinSketch.add("a", 7)
      iex> {:ok, merged} = Approx.CountMinSketch.merge(cms1, cms2)
      iex> Approx.CountMinSketch.count(merged, "a") >= 10
      true
  """
  @spec merge(t(), t()) :: {:ok, t()} | {:error, :dimension_mismatch}
  def merge(%__MODULE__{width: w, depth: d} = cms1, %__MODULE__{width: w, depth: d} = cms2) do
    merged_table =
      0..(d - 1)
      |> Enum.map(fn i ->
        row1 = elem(cms1.table, i)
        row2 = elem(cms2.table, i)

        0..(w - 1)
        |> Enum.map(fn j ->
          elem(row1, j) + elem(row2, j)
        end)
        |> List.to_tuple()
      end)
      |> List.to_tuple()

    {:ok, %{cms1 | table: merged_table}}
  end

  def merge(%__MODULE__{}, %__MODULE__{}) do
    {:error, :dimension_mismatch}
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  @doc """
  Serializes the approx to a binary.

  The binary format is:

    * 1 byte  -- version (`#{@serialization_version}`)
    * 4 bytes -- width (big-endian unsigned 32-bit)
    * 4 bytes -- depth (big-endian unsigned 32-bit)
    * `width * depth * 8` bytes -- counters as big-endian unsigned 64-bit
      integers, written row by row (row 0 first, left to right)

  ## Parameters

    * `cms` -- the Count-Min Sketch to serialize.

  ## Returns

    A binary suitable for storage or network transfer.

  ## Examples

      iex> cms = Approx.CountMinSketch.new(0.1, 0.1) |> Approx.CountMinSketch.add("test", 5)
      iex> {:ok, restored} = cms |> Approx.CountMinSketch.to_binary() |> Approx.CountMinSketch.from_binary()
      iex> Approx.CountMinSketch.count(restored, "test") >= 5
      true
  """
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{} = cms) do
    header = <<@serialization_version::8, cms.width::big-unsigned-32, cms.depth::big-unsigned-32>>

    counters =
      for i <- 0..(cms.depth - 1), j <- 0..(cms.width - 1) do
        row = elem(cms.table, i)
        <<elem(row, j)::big-unsigned-64>>
      end

    :erlang.iolist_to_binary([header | counters])
  end

  @doc """
  Deserializes a approx from the binary format produced by `to_binary/1`.

  The deserialized approx uses the default hash function
  (built-in 32-bit hash).

  ## Parameters

    * `binary` -- a binary previously produced by `to_binary/1`.

  ## Returns

    `{:ok, approx}` on success, or `{:error, :invalid_binary}` if the binary is malformed or uses an unsupported version.

  ## Options

    * `:hash_fn` -- override the hash function on the restored approx.
      Defaults to the built-in 32-bit hash.

  ## Examples

      iex> cms = Approx.CountMinSketch.new(0.1, 0.1) |> Approx.CountMinSketch.add("x", 42)
      iex> {:ok, restored} = cms |> Approx.CountMinSketch.to_binary() |> Approx.CountMinSketch.from_binary()
      iex> Approx.CountMinSketch.count(restored, "x") == Approx.CountMinSketch.count(cms, "x")
      true
  """
  @spec from_binary(binary(), keyword()) :: {:ok, t()} | {:error, :invalid_binary}
  def from_binary(binary, opts \\ [])

  def from_binary(
        <<@serialization_version::8, width::big-unsigned-32, depth::big-unsigned-32,
          rest::binary>>,
        opts
      )
      when width > 0 and depth > 0 do
    expected_bytes = width * depth * 8

    if byte_size(rest) != expected_bytes do
      {:error, :invalid_binary}
    else
      hash_fn = Keyword.get(opts, :hash_fn, &Approx.Hash.hash32/1)
      table = decode_counters(rest, width, depth)

      {:ok,
       %__MODULE__{
         table: table,
         width: width,
         depth: depth,
         hash_fn: hash_fn
       }}
    end
  end

  def from_binary(
        <<@serialization_version::8, _width::big-unsigned-32, _depth::big-unsigned-32,
          _rest::binary>>,
        _opts
      ) do
    {:error, :invalid_binary}
  end

  def from_binary(<<_version::8, _rest::binary>> = binary, _opts)
      when byte_size(binary) >= 9 do
    {:error, :invalid_binary}
  end

  def from_binary(_binary, _opts) do
    {:error, :invalid_binary}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp build_table(width, depth) do
    row = Tuple.duplicate(0, width)
    Tuple.duplicate(row, depth)
  end

  # Generates two independent 32-bit hashes for the Kirsch-Mitzenmacker
  # double-hashing scheme, allowing positions to address the full column space
  # even when width > 65536.
  defp hash_pair(hash_fn, element) do
    h1 = hash_fn.(element)
    h2 = hash_fn.({:__approx_h2__, element})
    {h1, h2}
  end

  defp column_for(h1, h2, i, width) do
    Approx.Hash.double_hash(h1, h2, i) |> rem(width) |> abs()
  end

  defp decode_counters(binary, width, depth) do
    0..(depth - 1)
    |> Enum.map(fn i ->
      offset = i * width * 8

      0..(width - 1)
      |> Enum.map(fn j ->
        pos = offset + j * 8
        <<value::big-unsigned-64>> = binary_part(binary, pos, 8)
        value
      end)
      |> List.to_tuple()
    end)
    |> List.to_tuple()
  end

  defp validate_params!(epsilon, delta) do
    unless is_number(epsilon) and epsilon > 0 do
      raise ArgumentError, "epsilon must be a positive number, got: #{inspect(epsilon)}"
    end

    unless is_number(delta) and delta > 0 and delta < 1 do
      raise ArgumentError, "delta must be a number in (0, 1), got: #{inspect(delta)}"
    end
  end

  defp validate_amount!(amount) do
    unless is_integer(amount) and amount > 0 do
      raise ArgumentError, "amount must be a positive integer, got: #{inspect(amount)}"
    end
  end
end
