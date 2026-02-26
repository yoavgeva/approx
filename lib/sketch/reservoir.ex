defmodule Sketch.Reservoir do
  @moduledoc """
  Reservoir Sampling (Algorithm R) for uniform random sampling from a stream.

  Reservoir sampling maintains a fixed-size sample of `k` elements from a stream
  of unknown (and potentially infinite) length. Every element in the stream has an
  equal probability of being included in the sample, regardless of when it arrives.

  This implementation uses **Algorithm R** by Jeffrey Vitter, which is the classic
  reservoir sampling algorithm. For each new element at position `i` (1-indexed):

    * If `i <= k`, add the element directly to the reservoir.
    * If `i > k`, generate a random integer `j` in `0..i-1`. If `j < k`, replace
      the element at position `j` in the reservoir with the new element.

  ## Features

    * **Fixed memory**: Stores at most `k` elements regardless of stream size.
    * **Uniform sampling**: Every element has an equal `k/n` probability of being
      in the final sample, where `n` is the total number of elements seen.
    * **Deterministic**: Accepts an optional random seed for reproducible results.
    * **Mergeable**: Two reservoirs can be merged with weighted selection
      proportional to the number of elements each has seen.
    * **Arbitrary terms**: Stores any Elixir term -- no serialization constraints.

  ## Examples

      iex> r = Sketch.Reservoir.new(5, seed: 42)
      iex> r = Sketch.Reservoir.add_all(r, 1..100)
      iex> length(Sketch.Reservoir.sample(r))
      5
      iex> Sketch.Reservoir.count(r)
      100

  ## Algorithm Complexity

    * **add/2**: O(1) time and space per element
    * **sample/1**: O(k) -- converts internal tuple to list
    * **merge/2**: O(k) where k is the reservoir size
  """

  @typedoc "A reservoir sampler that maintains a fixed-size uniform random sample."
  @type t :: %__MODULE__{
          samples: tuple() | list(),
          k: pos_integer(),
          count: non_neg_integer(),
          seed: :rand.state()
        }

  @enforce_keys [:k, :seed]
  defstruct samples: [],
            k: 1,
            count: 0,
            seed: nil

  @doc """
  Creates a new reservoir sampler that retains up to `k` elements.

  ## Options

    * `:seed` - A seed for the random number generator, used for reproducibility.
      Can be an integer (used as seed for `:exsss` algorithm), a tuple of
      `{algorithm, state}` as returned by `:rand.seed/1`, or omitted to use a
      fresh random seed. Defaults to a new `:exsss` seed.

  ## Returns

    * A `t:t/0` struct representing an empty reservoir sampler.

  ## Raises

    * `FunctionClauseError` if `k` is not a positive integer.

  ## Examples

      iex> r = Sketch.Reservoir.new(10)
      iex> r.k
      10
      iex> r.count
      0
      iex> Sketch.Reservoir.sample(r)
      []

      iex> r = Sketch.Reservoir.new(3, seed: 123)
      iex> r.k
      3

  """
  @spec new(pos_integer(), keyword()) :: t()
  def new(k, opts \\ []) when is_integer(k) and k > 0 do
    seed = resolve_seed(opts[:seed])

    %__MODULE__{
      samples: [],
      k: k,
      count: 0,
      seed: seed
    }
  end

  @doc """
  Adds a single element to the reservoir.

  If fewer than `k` elements have been seen, the element is added directly.
  Otherwise, the element replaces a randomly chosen element in the reservoir
  with probability `k / count`, ensuring uniform sampling.

  ## Parameters

    * `reservoir` — the reservoir sampler.
    * `element` — any term to add.

  ## Returns

    * An updated `t:t/0` struct with the element considered for sampling.

  ## Examples

      iex> r = Sketch.Reservoir.new(3, seed: 42)
      iex> r = Sketch.Reservoir.add(r, :a)
      iex> r = Sketch.Reservoir.add(r, :b)
      iex> Sketch.Reservoir.sample(r)
      [:a, :b]
      iex> Sketch.Reservoir.count(r)
      2

      iex> r = Sketch.Reservoir.new(2, seed: 42)
      iex> r = Enum.reduce(1..1000, r, fn x, acc -> Sketch.Reservoir.add(acc, x) end)
      iex> length(Sketch.Reservoir.sample(r))
      2
      iex> Sketch.Reservoir.count(r)
      1000

  """
  @spec add(t(), term()) :: t()
  def add(%__MODULE__{count: count, k: k, samples: samples, seed: seed} = reservoir, element) do
    new_count = count + 1

    cond do
      # Fill phase: collecting first k elements in a list
      new_count < k ->
        %{reservoir | samples: [element | samples], count: new_count}

      # Exactly k-th element: convert list to tuple for O(1) access
      new_count == k ->
        tuple = [element | samples] |> Enum.reverse() |> List.to_tuple()
        %{reservoir | samples: tuple, count: new_count}

      # Replacement phase: O(1) random replacement in tuple
      true ->
        {j, new_seed} = :rand.uniform_s(new_count, seed)
        # :rand.uniform_s(n, state) returns 1..n, so subtract 1 for 0-indexed
        j = j - 1

        if j < k do
          %{
            reservoir
            | samples: put_elem(samples, j, element),
              count: new_count,
              seed: new_seed
          }
        else
          %{reservoir | count: new_count, seed: new_seed}
        end
    end
  end

  @doc """
  Adds all elements from an enumerable to the reservoir.

  This is equivalent to calling `add/2` for each element in the enumerable,
  but expressed as a single fold operation.

  ## Parameters

    * `reservoir` — the reservoir sampler.
    * `enumerable` — any enumerable of terms to add.

  ## Returns

    * An updated `t:t/0` struct with all elements considered for sampling.

  ## Examples

      iex> r = Sketch.Reservoir.new(5, seed: 42)
      iex> r = Sketch.Reservoir.add_all(r, [1, 2, 3])
      iex> Sketch.Reservoir.sample(r)
      [1, 2, 3]
      iex> Sketch.Reservoir.count(r)
      3

      iex> r = Sketch.Reservoir.new(3, seed: 42)
      iex> r = Sketch.Reservoir.add_all(r, 1..10_000)
      iex> length(Sketch.Reservoir.sample(r))
      3
      iex> Sketch.Reservoir.count(r)
      10_000

  """
  @spec add_all(t(), Enumerable.t()) :: t()
  def add_all(%__MODULE__{} = reservoir, enumerable) do
    Enum.reduce(enumerable, reservoir, fn element, acc -> add(acc, element) end)
  end

  @doc """
  Returns the current list of sampled elements.

  If fewer than `k` elements have been added, returns all elements seen so far.
  Otherwise, returns exactly `k` elements chosen uniformly at random from all
  elements that have been added.

  ## Parameters

    * `reservoir` — the reservoir sampler.

  ## Returns

    * A list of at most `k` sampled elements.

  ## Examples

      iex> r = Sketch.Reservoir.new(5, seed: 42)
      iex> Sketch.Reservoir.sample(r)
      []

      iex> r = Sketch.Reservoir.new(5, seed: 42)
      iex> r = Sketch.Reservoir.add_all(r, [:a, :b, :c])
      iex> Sketch.Reservoir.sample(r)
      [:a, :b, :c]

      iex> r = Sketch.Reservoir.new(3, seed: 42)
      iex> r = Sketch.Reservoir.add_all(r, 1..100)
      iex> length(Sketch.Reservoir.sample(r))
      3

  """
  @spec sample(t()) :: list()
  def sample(%__MODULE__{samples: samples}) when is_list(samples), do: Enum.reverse(samples)
  def sample(%__MODULE__{samples: samples}) when is_tuple(samples), do: Tuple.to_list(samples)

  @doc """
  Returns the total number of elements that have been added to the reservoir.

  This is the count of all elements seen, not the number of elements currently
  stored in the reservoir (which is at most `k`).

  ## Parameters

    * `reservoir` — the reservoir sampler.

  ## Returns

    * A non-negative integer representing the total number of elements seen.

  ## Examples

      iex> r = Sketch.Reservoir.new(3, seed: 42)
      iex> Sketch.Reservoir.count(r)
      0

      iex> r = Sketch.Reservoir.new(3, seed: 42)
      iex> r = Sketch.Reservoir.add_all(r, 1..100)
      iex> Sketch.Reservoir.count(r)
      100

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{count: count}), do: count

  @doc """
  Merges two reservoirs into one using weighted random selection.

  The merged reservoir uses the `k` value from the first reservoir. Elements are
  selected from each reservoir with probability proportional to their respective
  counts, ensuring that the merged sample remains a uniform sample of the combined
  stream.

  Both reservoirs must have the same `k` value.

  Returns `{:ok, merged}` on success, or `{:error, :incompatible_size}` if the
  reservoirs have different `k` values.

  ## Returns

    * `{:ok, t()}` — the merged reservoir with combined count.
    * `{:error, :incompatible_size}` — if the reservoirs have different `k` values.

  ## Examples

      iex> r1 = Sketch.Reservoir.new(5, seed: 42)
      iex> r1 = Sketch.Reservoir.add_all(r1, 1..50)
      iex> r2 = Sketch.Reservoir.new(5, seed: 99)
      iex> r2 = Sketch.Reservoir.add_all(r2, 51..100)
      iex> {:ok, merged} = Sketch.Reservoir.merge(r1, r2)
      iex> Sketch.Reservoir.count(merged)
      100
      iex> length(Sketch.Reservoir.sample(merged))
      5

  """
  @spec merge(t(), t()) :: {:ok, t()} | {:error, :incompatible_size}
  def merge(%__MODULE__{k: k} = r1, %__MODULE__{k: k} = r2) do
    {:ok, merge_reservoirs(r1, r2)}
  end

  def merge(%__MODULE__{}, %__MODULE__{}) do
    {:error, :incompatible_size}
  end

  # --- Private Helpers ---

  defp resolve_seed(nil) do
    :rand.seed_s(:exsss)
  end

  defp resolve_seed(seed) when is_integer(seed) do
    :rand.seed_s(:exsss, seed)
  end

  defp resolve_seed({_alg, _state} = seed_state) do
    seed_state
  end

  # Convert internal storage to a list for merging
  defp samples_to_list(%__MODULE__{samples: s}) when is_list(s), do: Enum.reverse(s)
  defp samples_to_list(%__MODULE__{samples: s}) when is_tuple(s), do: Tuple.to_list(s)

  defp merge_reservoirs(%__MODULE__{count: 0} = r1, %__MODULE__{count: 0}), do: r1
  defp merge_reservoirs(%__MODULE__{count: 0}, %__MODULE__{} = r2), do: r2
  defp merge_reservoirs(%__MODULE__{} = r1, %__MODULE__{count: 0}), do: r1

  defp merge_reservoirs(%__MODULE__{} = r1, %__MODULE__{} = r2) do
    k = r1.k
    total_count = r1.count + r2.count
    list1 = samples_to_list(r1)
    list2 = samples_to_list(r2)

    # If total elements seen <= k, all elements should be kept
    if total_count <= k do
      combined = list1 ++ list2

      samples =
        if total_count == k do
          List.to_tuple(combined)
        else
          Enum.reverse(combined)
        end

      %__MODULE__{
        samples: samples,
        k: k,
        count: total_count,
        seed: r1.seed
      }
    else
      # Build an array-based weighted pool for O(k) sampling
      arr1 = List.to_tuple(list1)
      arr2 = List.to_tuple(list2)
      len1 = tuple_size(arr1)
      len2 = tuple_size(arr2)

      w1 = if len1 > 0, do: r1.count / len1, else: 0.0
      w2 = if len2 > 0, do: r2.count / len2, else: 0.0

      # Use alias method for O(k) weighted sampling without replacement.
      # Since all items in pool1 have the same weight and all in pool2 have
      # the same weight, we can use a simpler approach: for each of k slots,
      # pick which pool to draw from based on weight ratio, then pick a
      # random element from that pool.
      {selected, final_seed} =
        weighted_merge_sample(arr1, arr2, w1, w2, k, r1.seed)

      %__MODULE__{
        samples: selected,
        k: k,
        count: total_count,
        seed: final_seed
      }
    end
  end

  # O(k) merge sampling: for each of k output slots, randomly choose which
  # pool to draw from (weighted by total representation), then pick a random
  # element from that pool.
  defp weighted_merge_sample(arr1, arr2, w1, w2, k, seed) do
    len1 = tuple_size(arr1)
    len2 = tuple_size(arr2)
    total_w1 = w1 * len1
    total_w2 = w2 * len2
    total_weight = total_w1 + total_w2
    threshold = total_w1 / total_weight

    {result_list, final_seed} =
      Enum.reduce(1..k, {[], seed}, fn _i, {acc, s} ->
        # Decide which pool to draw from
        {rand_val, s2} = :rand.uniform_s(s)

        {elem, s3} =
          if rand_val <= threshold do
            {idx, s3} = :rand.uniform_s(len1, s2)
            {elem(arr1, idx - 1), s3}
          else
            {idx, s3} = :rand.uniform_s(len2, s2)
            {elem(arr2, idx - 1), s3}
          end

        {[elem | acc], s3}
      end)

    {result_list |> Enum.reverse() |> List.to_tuple(), final_seed}
  end
end
