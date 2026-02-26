defmodule Sketch.TDigest do
  @moduledoc """
  A t-digest for streaming percentile estimation with high tail accuracy.

  The t-digest is a probabilistic data structure that estimates quantiles
  (percentiles) of a numeric distribution from a stream of observations.
  It is especially accurate at the extreme tails (p99, p99.9, p1, etc.),
  making it ideal for latency monitoring, SLA tracking, and anomaly
  detection.

  ## How it works

  The t-digest maintains a sorted list of **centroids**, where each centroid
  is a `{mean, weight}` pair representing a cluster of nearby values. A
  **scale function** controls how much weight each centroid may accumulate
  based on its quantile position: centroids near the tails (q close to 0
  or 1) are kept small for precision, while centroids in the middle of the
  distribution are allowed to grow larger.

  New data points are first accumulated in an unsorted **buffer**. When the
  buffer reaches a configurable threshold, it is sorted and merged into the
  main centroid list in a single compression pass. This amortizes the cost
  of maintaining sorted centroids.

  ## Scale function

  This implementation uses the `k_1` scale function:

      k(q, delta) = (delta / (2 * pi)) * arcsin(2*q - 1)

  Its derivative gives the weight limit at quantile `q`:

      limit(q) ~= 4 * total_weight * q * (1 - q) / delta

  This naturally produces small centroids at the tails and larger centroids
  in the middle.

  ## Features

    * **Fixed memory**: The number of centroids is bounded by `delta`.
    * **High tail accuracy**: p99, p99.9, p1 estimates are tightly bounded.
    * **Buffered insertion**: Amortized O(1) insert via batched compression.
    * **Mergeable**: Two digests can be merged for distributed aggregation.
    * **Serializable**: Compact binary format for storage or network transfer.

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> td = Enum.reduce(1..1000, td, fn x, acc -> Sketch.TDigest.add(acc, x) end)
      iex> Sketch.TDigest.count(td)
      1000.0
      iex> abs(Sketch.TDigest.median(td) - 500.5) < 10
      true

  ## Algorithm Complexity

    * **add/3**: Amortized O(1); worst case O(n log n) during compression
    * **percentile/2**: O(n) where n is the number of centroids
    * **merge/2**: O((n1 + n2) log(n1 + n2)) for compression
  """

  @typedoc """
  A centroid is a `{mean, weight}` pair representing a cluster of values.
  """
  @type centroid :: {float(), float()}

  @typedoc "A t-digest struct."
  @type t :: %__MODULE__{
          centroids: [centroid()],
          buffer: [centroid()],
          buffer_size: non_neg_integer(),
          delta: number(),
          total_weight: float(),
          min: float() | nil,
          max: float() | nil,
          buffer_limit: pos_integer(),
          compress_direction: :forward | :reverse
        }

  @enforce_keys [:delta, :buffer_limit]
  defstruct centroids: [],
            buffer: [],
            buffer_size: 0,
            delta: 100,
            total_weight: 0.0,
            min: nil,
            max: nil,
            buffer_limit: 500,
            compress_direction: :forward

  @serialization_version 1

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new, empty t-digest.

  ## Parameters

    * `delta` -- the compression parameter controlling the trade-off between
      accuracy and memory. Larger values use more centroids but give better
      accuracy. Typical values are 50--300. Defaults to `100`.
    * `opts` -- keyword list of options:
      * `:buffer_limit` -- the number of buffered points that triggers
        automatic compression. Defaults to `delta * 5`.

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> td.delta
      100

      iex> td = Sketch.TDigest.new(200)
      iex> td.delta
      200

      iex> td = Sketch.TDigest.new(50, buffer_limit: 100)
      iex> td.buffer_limit
      100

  ## Raises

    * `FunctionClauseError` if `delta` is not a positive number.

  ## Returns

  A new, empty `%Sketch.TDigest{}` struct ready to accept data via `add/3`.
  """
  @spec new(number(), keyword()) :: t()
  def new(delta \\ 100, opts \\ []) when is_number(delta) and delta > 0 do
    buffer_limit = Keyword.get(opts, :buffer_limit, max(1, trunc(delta * 5)))

    %__MODULE__{
      delta: delta,
      buffer_limit: buffer_limit
    }
  end

  # ---------------------------------------------------------------------------
  # Insertion
  # ---------------------------------------------------------------------------

  @doc """
  Adds a numeric value to the t-digest with an optional weight.

  Values are accumulated in an internal buffer. When the buffer reaches the
  configured limit, it is automatically compressed into the main centroid
  list.

  ## Parameters

    * `td` -- the t-digest struct
    * `value` -- the numeric value to add
    * `weight` -- the weight of the observation (defaults to `1`)

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> td = Sketch.TDigest.add(td, 42.0)
      iex> Sketch.TDigest.count(td)
      1.0

      iex> td = Sketch.TDigest.new()
      iex> td = Sketch.TDigest.add(td, 10.0, 5)
      iex> Sketch.TDigest.count(td)
      5.0

  ## Raises

    * `FunctionClauseError` if `value` is not a number, or `weight` is not a positive number.

  ## Returns

  An updated `%Sketch.TDigest{}` struct with the new value incorporated.
  If the internal buffer has reached its limit, the returned struct will
  have been automatically compressed.
  """
  @spec add(t(), number(), number()) :: t()
  def add(%__MODULE__{} = td, value, weight \\ 1)
      when is_number(value) and is_number(weight) and weight > 0 do
    float_value = value / 1
    float_weight = weight / 1

    new_min = if td.min == nil, do: float_value, else: Kernel.min(td.min, float_value)
    new_max = if td.max == nil, do: float_value, else: Kernel.max(td.max, float_value)

    new_buffer_size = td.buffer_size + 1

    updated = %{
      td
      | buffer: [{float_value, float_weight} | td.buffer],
        buffer_size: new_buffer_size,
        total_weight: td.total_weight + float_weight,
        min: new_min,
        max: new_max
    }

    if new_buffer_size >= updated.buffer_limit do
      compress(updated)
    else
      updated
    end
  end

  # ---------------------------------------------------------------------------
  # Queries
  # ---------------------------------------------------------------------------

  @doc """
  Estimates the value at the given quantile.

  The quantile `q` must be a float between `0.0` and `1.0` inclusive, where
  `0.0` represents the minimum, `0.5` the median, and `1.0` the maximum.

  The buffer is flushed (compressed) before computing the percentile to
  ensure all data points are considered.

  ## Parameters

    * `td` -- the t-digest struct
    * `q` -- the target quantile in `0.0..1.0`

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> td = Enum.reduce(1..100, td, fn x, acc -> Sketch.TDigest.add(acc, x) end)
      iex> Sketch.TDigest.percentile(td, 0.0)
      1.0

      iex> td = Sketch.TDigest.new()
      iex> td = Enum.reduce(1..100, td, fn x, acc -> Sketch.TDigest.add(acc, x) end)
      iex> Sketch.TDigest.percentile(td, 1.0)
      100.0

      iex> td = Sketch.TDigest.new()
      iex> td = Sketch.TDigest.add(td, 42.0)
      iex> Sketch.TDigest.percentile(td, 0.5)
      42.0

  ## Returns

  The estimated value at quantile `q` as a float, or `nil` if the digest
  is empty.
  """
  @spec percentile(t(), float()) :: float() | nil
  def percentile(%__MODULE__{total_weight: tw}, _q) when tw == 0.0, do: nil

  def percentile(%__MODULE__{} = td, q) when is_number(q) and q >= 0.0 and q <= 1.0 do
    flushed = compress(td)

    cond do
      flushed.centroids == [] ->
        nil

      q == 0.0 ->
        flushed.min

      q == 1.0 ->
        flushed.max

      true ->
        interpolate_percentile(flushed.centroids, flushed.total_weight, q)
    end
  end

  @doc """
  Returns the minimum value that has been added to the digest.

  Returns `nil` if the digest is empty.

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> Sketch.TDigest.min(td)
      nil

      iex> td = Sketch.TDigest.new() |> Sketch.TDigest.add(5.0) |> Sketch.TDigest.add(3.0) |> Sketch.TDigest.add(7.0)
      iex> Sketch.TDigest.min(td)
      3.0

  ## Returns

  The minimum value as a float, or `nil` if the digest is empty.
  """
  @spec min(t()) :: float() | nil
  def min(%__MODULE__{min: min}), do: min

  @doc """
  Returns the maximum value that has been added to the digest.

  Returns `nil` if the digest is empty.

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> Sketch.TDigest.max(td)
      nil

      iex> td = Sketch.TDigest.new() |> Sketch.TDigest.add(5.0) |> Sketch.TDigest.add(3.0) |> Sketch.TDigest.add(7.0)
      iex> Sketch.TDigest.max(td)
      7.0

  ## Returns

  The maximum value as a float, or `nil` if the digest is empty.
  """
  @spec max(t()) :: float() | nil
  def max(%__MODULE__{max: max}), do: max

  @doc """
  Returns the total weight (count) of all values added to the digest.

  When all values are added with the default weight of 1, this equals the
  number of observations.

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> Sketch.TDigest.count(td)
      0.0

      iex> td = Sketch.TDigest.new()
      iex> td = Enum.reduce(1..50, td, fn x, acc -> Sketch.TDigest.add(acc, x) end)
      iex> Sketch.TDigest.count(td)
      50.0

  ## Returns

  The total weight as a float. When all values are added with the default
  weight of 1, this equals the number of observations.
  """
  @spec count(t()) :: float()
  def count(%__MODULE__{total_weight: tw}), do: tw

  @doc """
  Returns the estimated median (50th percentile).

  This is a convenience function equivalent to `percentile(td, 0.5)`.

  ## Examples

      iex> td = Sketch.TDigest.new()
      iex> td = Sketch.TDigest.add(td, 10.0) |> Sketch.TDigest.add(20.0) |> Sketch.TDigest.add(30.0)
      iex> Sketch.TDigest.median(td)
      20.0

  ## Returns

  The estimated median as a float, or `nil` if the digest is empty.
  """
  @spec median(t()) :: float() | nil
  def median(%__MODULE__{} = td), do: percentile(td, 0.5)

  # ---------------------------------------------------------------------------
  # Merge
  # ---------------------------------------------------------------------------

  @doc """
  Merges two t-digests into one.

  All centroids and buffered points from both digests are combined and
  then compressed. The resulting digest uses the `delta` value from the
  first digest. The min and max are the extremes of both digests.

  ## Parameters

    * `td1` -- the first t-digest (its `delta` is used for the result)
    * `td2` -- the second t-digest

  ## Examples

      iex> td1 = Enum.reduce(1..50, Sketch.TDigest.new(), fn x, acc -> Sketch.TDigest.add(acc, x) end)
      iex> td2 = Enum.reduce(51..100, Sketch.TDigest.new(), fn x, acc -> Sketch.TDigest.add(acc, x) end)
      iex> merged = Sketch.TDigest.merge(td1, td2)
      iex> Sketch.TDigest.count(merged)
      100.0
      iex> Sketch.TDigest.min(merged)
      1.0
      iex> Sketch.TDigest.max(merged)
      100.0

  ## Returns

  A new `%Sketch.TDigest{}` struct containing all data from both input
  digests, compressed using the `delta` from the first digest.
  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = td1, %__MODULE__{} = td2) do
    merged_min = combine_extremes(td1.min, td2.min, &Kernel.min/2)
    merged_max = combine_extremes(td1.max, td2.max, &Kernel.max/2)
    merged_weight = td1.total_weight + td2.total_weight

    # Combine all centroids and buffer points, then compress
    all_points = td1.centroids ++ td1.buffer ++ td2.centroids ++ td2.buffer

    merged = %__MODULE__{
      centroids: [],
      buffer: all_points,
      buffer_size: length(all_points),
      delta: td1.delta,
      total_weight: merged_weight,
      min: merged_min,
      max: merged_max,
      buffer_limit: td1.buffer_limit
    }

    compress(merged)
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  @doc """
  Serializes the t-digest to a compact binary format.

  The buffer is flushed before serialization so all data points are
  included in the centroid list.

  ## Wire format

      <<version::8, delta::float-64, buffer_limit::unsigned-32,
        total_weight::float-64, min::float-64, max::float-64,
        num_centroids::unsigned-32, centroids::binary>>

  Each centroid is encoded as two big-endian float-64 values (mean, weight).

  ## Returns

  A binary containing the serialized t-digest data.

  ## Examples

      iex> td = Sketch.TDigest.new() |> Sketch.TDigest.add(1.0) |> Sketch.TDigest.add(2.0)
      iex> {:ok, restored} = td |> Sketch.TDigest.to_binary() |> Sketch.TDigest.from_binary()
      iex> Sketch.TDigest.count(restored)
      2.0
  """
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{} = td) do
    flushed = compress(td)

    num_centroids = length(flushed.centroids)

    # Encode min/max as 0.0 when nil (empty digest)
    min_val = flushed.min || 0.0
    max_val = flushed.max || 0.0

    header =
      <<@serialization_version::8, flushed.delta::big-float-64,
        flushed.buffer_limit::big-unsigned-32, flushed.total_weight::big-float-64,
        min_val::big-float-64, max_val::big-float-64, num_centroids::big-unsigned-32>>

    centroid_iodata =
      for {mean, weight} <- flushed.centroids do
        <<mean::big-float-64, weight::big-float-64>>
      end

    <<header::binary, :erlang.iolist_to_binary(centroid_iodata)::binary>>
  end

  @doc """
  Deserializes a t-digest from a binary produced by `to_binary/1`.

  ## Returns

  `{:ok, t_digest}` on success or `{:error, :invalid_binary}` on failure
  (e.g., truncated data, unknown version byte, or malformed input).

  ## Examples

      iex> td = Sketch.TDigest.new() |> Sketch.TDigest.add(1.0) |> Sketch.TDigest.add(2.0)
      iex> {:ok, restored} = td |> Sketch.TDigest.to_binary() |> Sketch.TDigest.from_binary()
      iex> Sketch.TDigest.count(restored)
      2.0
  """
  @spec from_binary(binary()) :: {:ok, t()} | {:error, :invalid_binary}
  def from_binary(
        <<@serialization_version::8, delta::big-float-64, buffer_limit::big-unsigned-32,
          total_weight::big-float-64, min_val::big-float-64, max_val::big-float-64,
          num_centroids::big-unsigned-32, rest::binary>>
      ) do
    expected_bytes = num_centroids * 16

    if byte_size(rest) == expected_bytes do
      centroids = decode_centroids(rest, num_centroids, [])

      # Restore nil for min/max if total_weight is 0 (empty digest)
      {restored_min, restored_max} =
        if total_weight == 0.0, do: {nil, nil}, else: {min_val, max_val}

      {:ok,
       %__MODULE__{
         centroids: centroids,
         buffer: [],
         delta: delta,
         total_weight: total_weight,
         min: restored_min,
         max: restored_max,
         buffer_limit: buffer_limit
       }}
    else
      {:error, :invalid_binary}
    end
  end

  def from_binary(_binary), do: {:error, :invalid_binary}

  # ---------------------------------------------------------------------------
  # Compression (private)
  # ---------------------------------------------------------------------------

  @doc """
  Flushes the internal buffer by sorting it and merging into the main
  centroid list using the scale-function compression algorithm.

  This is called automatically by `add/3` when the buffer reaches its
  limit, by `percentile/2` before querying, and by `to_binary/1` before
  serialization. You typically do not need to call it manually, but it
  is available for testing and inspection purposes.

  Compression alternates direction (forward / reverse) on each invocation
  to avoid systematic bias toward either tail.

  If the buffer is already empty, this is a no-op and the compression
  direction is **not** flipped.

  ## Returns

  An updated `%Sketch.TDigest{}` struct with an empty buffer and a
  compressed centroid list.
  """
  @spec compress(t()) :: t()
  def compress(%__MODULE__{buffer: []} = td), do: td

  def compress(%__MODULE__{} = td) do
    # Sort buffer by mean value
    sorted_buffer = Enum.sort_by(td.buffer, fn {mean, _weight} -> mean end)

    # Merge sorted buffer into existing centroids
    merged_centroids = merge_sorted(td.centroids, sorted_buffer)

    # Compress the combined centroid list using the scale function,
    # alternating direction between compressions to avoid bias toward
    # either tail (matches the reference t-digest implementation).
    {compressed, next_direction} =
      case td.compress_direction do
        :forward ->
          {compress_forward(merged_centroids, td.total_weight, td.delta), :reverse}

        :reverse ->
          reversed = Enum.reverse(merged_centroids)
          result = compress_forward(reversed, td.total_weight, td.delta)
          {Enum.reverse(result), :forward}
      end

    %{td | centroids: compressed, buffer: [], buffer_size: 0, compress_direction: next_direction}
  end

  # Merge two sorted centroid lists into one sorted list.
  # Uses tail recursion with an accumulator to avoid stack buildup on large lists.
  @spec merge_sorted([centroid()], [centroid()]) :: [centroid()]
  defp merge_sorted(list1, list2) do
    do_merge_sorted(list1, list2, [])
  end

  @spec do_merge_sorted([centroid()], [centroid()], [centroid()]) :: [centroid()]
  defp do_merge_sorted([], rest, acc), do: Enum.reverse(acc, rest)
  defp do_merge_sorted(rest, [], acc), do: Enum.reverse(acc, rest)

  defp do_merge_sorted([{m1, _} = h1 | t1], [{m2, _} = h2 | t2], acc) do
    if m1 <= m2 do
      do_merge_sorted(t1, [h2 | t2], [h1 | acc])
    else
      do_merge_sorted([h1 | t1], t2, [h2 | acc])
    end
  end

  # Compress centroids left-to-right by merging adjacent ones that fit
  # within the scale function's weight limit.
  @spec compress_forward([centroid()], float(), number()) :: [centroid()]
  defp compress_forward([], _total_weight, _delta), do: []
  defp compress_forward(centroids, total_weight, _delta) when total_weight == 0.0, do: centroids

  defp compress_forward(centroids, total_weight, delta) do
    centroids
    |> Enum.reduce({[], 0.0}, fn {mean, weight}, {acc, weight_so_far} ->
      compress_centroid({mean, weight}, acc, weight_so_far, total_weight, delta)
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp compress_centroid({mean, weight}, [], weight_so_far, _total_weight, _delta) do
    {[{mean, weight}], weight_so_far + weight}
  end

  defp compress_centroid(
         {mean, weight},
         [{last_mean, last_weight} | rest] = acc,
         weight_so_far,
         total_weight,
         delta
       ) do
    q = (weight_so_far + weight / 2.0) / total_weight
    max_weight = weight_limit(q, total_weight, delta)

    if last_weight + weight <= max_weight do
      new_weight = last_weight + weight
      new_mean = (last_mean * last_weight + mean * weight) / new_weight
      {[{new_mean, new_weight} | rest], weight_so_far + weight}
    else
      {[{mean, weight} | acc], weight_so_far + weight}
    end
  end

  # The weight limit for a centroid at quantile q using the k_1 scale function.
  # This is the derivative of the inverse of k(q) = (delta / 2*pi) * asin(2q - 1),
  # which gives:  limit ~= 4 * total_weight * q * (1 - q) / delta
  #
  # We ensure a minimum weight of 1.0 so that single-point centroids are always
  # allowed.
  @spec weight_limit(float(), float(), number()) :: float()
  defp weight_limit(q, total_weight, delta) do
    Kernel.max(1.0, 4.0 * total_weight * q * (1.0 - q) / delta)
  end

  # ---------------------------------------------------------------------------
  # Percentile interpolation (private)
  # ---------------------------------------------------------------------------

  # Walk centroids, accumulating weight, and interpolate between centroid
  # means at the target quantile position.
  @spec interpolate_percentile([centroid()], float(), float()) :: float()
  defp interpolate_percentile(centroids, total_weight, q) do
    target = q * total_weight

    # Build a list of {mean, cumulative_weight_at_center} for interpolation.
    # The "center" of each centroid is at cumulative_weight_before + weight/2.
    {centers, _} =
      Enum.map_reduce(centroids, 0.0, fn {mean, weight}, cum ->
        center = cum + weight / 2.0
        {{mean, center}, cum + weight}
      end)

    interpolate_in_centers(centers, target)
  end

  # Given a sorted list of {mean, cumulative_center_weight} pairs,
  # find where `target` falls and linearly interpolate.
  # Uses a recursive walk carrying the previous element to avoid
  # allocating intermediate sublists (replacing Enum.chunk_every).
  @spec interpolate_in_centers([{float(), float()}], float()) :: float()
  defp interpolate_in_centers([{mean, _center}], _target), do: mean

  defp interpolate_in_centers([{_m, _c} = first | rest], target) do
    walk_centers(first, rest, target)
  end

  @spec walk_centers({float(), float()}, [{float(), float()}], float()) :: float()
  defp walk_centers({m, _c}, [], _target), do: m

  defp walk_centers({m1, c1}, [{m2, c2} | rest], target) do
    cond do
      target <= c1 ->
        m1

      target <= c2 ->
        interpolate_between(m1, c1, m2, c2, target)

      true ->
        walk_centers({m2, c2}, rest, target)
    end
  end

  defp interpolate_between(m1, c1, m2, c2, target) do
    if c2 == c1 do
      (m1 + m2) / 2.0
    else
      fraction = (target - c1) / (c2 - c1)
      m1 + fraction * (m2 - m1)
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization helpers (private)
  # ---------------------------------------------------------------------------

  @spec decode_centroids(binary(), non_neg_integer(), [centroid()]) :: [centroid()]
  defp decode_centroids(_rest, 0, acc), do: Enum.reverse(acc)

  defp decode_centroids(
         <<mean::big-float-64, weight::big-float-64, rest::binary>>,
         remaining,
         acc
       ) do
    decode_centroids(rest, remaining - 1, [{mean, weight} | acc])
  end

  # ---------------------------------------------------------------------------
  # Min/max combination helpers (private)
  # ---------------------------------------------------------------------------

  @spec combine_extremes(float() | nil, float() | nil, (float(), float() -> float())) ::
          float() | nil
  defp combine_extremes(nil, nil, _fun), do: nil
  defp combine_extremes(nil, val, _fun), do: val
  defp combine_extremes(val, nil, _fun), do: val
  defp combine_extremes(val1, val2, fun), do: fun.(val1, val2)
end
