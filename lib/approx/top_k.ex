defmodule Approx.TopK do
  @moduledoc """
  A Top-K frequent items tracker backed by a Count-Min Sketch.

  This module maintains an approximate list of the `k` most frequent items
  seen in a data stream. It combines a `Approx.CountMinSketch` for frequency
  estimation with a bounded map of the current top-k candidates.

  ## How It Works

  Every element added to the tracker is first recorded in the underlying
  Count-Min Sketch. The CMS provides an estimated count for any element.
  A candidate map of at most `k` items is maintained:

    * If the element is already in the map, its count is updated in O(1).
    * If the map has fewer than `k` entries, the element is inserted in O(1).
    * If the map is full and the element's estimated count exceeds the
      tracked minimum count, the minimum entry is evicted and the new
      element is inserted.

  The minimum element and count are tracked explicitly to avoid scanning
  the full map on every update. The minimum is only recomputed (an O(k)
  scan) when an eviction occurs or when the current minimum's count changes.

  Because the CMS may overestimate counts, the top-k list is an
  **approximation**. Heavy hitters with significantly higher counts than
  others will be tracked accurately.

  ## Parameters

    * `k` -- the number of top items to track. Must be a positive integer.
    * `epsilon` -- relative error bound for the CMS. Default: `0.001`.
    * `delta` -- failure probability for the CMS. Default: `0.01`.

  ## Examples

      iex> tk = Approx.TopK.new(3)
      iex> tk = tk |> Approx.TopK.add("a", 10) |> Approx.TopK.add("b", 5) |> Approx.TopK.add("c", 8)
      iex> top = Approx.TopK.top(tk)
      iex> length(top) == 3
      true

      iex> tk = Approx.TopK.new(2)
      iex> tk = tk |> Approx.TopK.add("x", 100) |> Approx.TopK.add("y", 50) |> Approx.TopK.add("z", 1)
      iex> Approx.TopK.member?(tk, "x")
      true
      iex> Approx.TopK.member?(tk, "z")
      false
  """

  alias Approx.CountMinSketch

  @enforce_keys [:k, :cms, :items, :hash_fn]
  defstruct [:k, :cms, :items, :hash_fn, :min_elem, :min_count]

  @typedoc """
  A Top-K frequent items tracker struct.

  Fields:

    * `:k` — The maximum number of top items to track.
    * `:cms` — The underlying `Approx.CountMinSketch` used for frequency
      estimation.
    * `:items` — A map of the current top-k candidates, where keys are
      elements and values are their estimated counts.
    * `:hash_fn` — The hash function shared with the underlying CMS.
    * `:min_elem` — The element with the lowest estimated count currently
      in the candidate map, or `nil` if the map is empty.
    * `:min_count` — The estimated count of `:min_elem`, or `nil` if the
      map is empty.
  """
  @type t :: %__MODULE__{
          k: pos_integer(),
          cms: CountMinSketch.t(),
          items: %{optional(term()) => non_neg_integer()},
          hash_fn: (term() -> non_neg_integer()),
          min_elem: term() | nil,
          min_count: non_neg_integer() | nil
        }

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new Top-K tracker that maintains the `k` most frequent items.

  ## Parameters

    * `k` -- the number of top items to track. Must be a positive integer.
    * `opts` -- keyword list of options.

  ## Options

    * `:epsilon` -- relative error bound for the Count-Min Sketch.
      Default: `0.001`.
    * `:delta` -- failure probability for the Count-Min Sketch.
      Default: `0.01`.
    * `:hash_fn` -- a function of arity 1 that maps a term to a
      non-negative integer. Passed through to the Count-Min Sketch.
      Defaults to the built-in 32-bit hash.

  ## Returns

  A new `%Approx.TopK{}` struct with an empty candidate map.

  ## Examples

      iex> tk = Approx.TopK.new(10)
      iex> tk.k
      10

      iex> tk = Approx.TopK.new(5, epsilon: 0.01, delta: 0.05)
      iex> tk.k
      5
  """
  @spec new(pos_integer(), keyword()) :: t()
  def new(k, opts \\ []) when is_integer(k) and k > 0 do
    epsilon = Keyword.get(opts, :epsilon, 0.001)
    delta = Keyword.get(opts, :delta, 0.01)
    hash_fn = Keyword.get(opts, :hash_fn)

    cms_opts = if hash_fn, do: [hash_fn: hash_fn], else: []
    cms = CountMinSketch.new(epsilon, delta, cms_opts)

    %__MODULE__{
      k: k,
      cms: cms,
      items: %{},
      hash_fn: cms.hash_fn,
      min_elem: nil,
      min_count: nil
    }
  end

  # ---------------------------------------------------------------------------
  # Updating
  # ---------------------------------------------------------------------------

  @doc """
  Adds `amount` occurrences of `element` to the tracker.

  The element is recorded in the underlying Count-Min Sketch, and then the
  top-k candidate map is updated based on the new estimated count.

  ## Parameters

    * `tk` -- the Top-K tracker.
    * `element` -- any term to record.
    * `amount` -- a positive integer count to add. Default: `1`.

  ## Returns

  An updated `%Approx.TopK{}` struct with the element recorded in the CMS
  and the candidate map potentially updated.

  ## Examples

      iex> tk = Approx.TopK.new(3)
      iex> tk = Approx.TopK.add(tk, "apple", 10)
      iex> Approx.TopK.count(tk, "apple") >= 10
      true

      iex> tk = Approx.TopK.new(2)
      iex> tk = tk |> Approx.TopK.add("a", 5) |> Approx.TopK.add("a", 3)
      iex> Approx.TopK.count(tk, "a") >= 8
      true
  """
  @spec add(t(), term(), pos_integer()) :: t()
  def add(%__MODULE__{} = tk, element, amount \\ 1) do
    {updated_cms, estimated_count} = CountMinSketch.add_and_count(tk.cms, element, amount)

    {updated_items, updated_min_elem, updated_min_count} =
      update_items(tk.items, tk.k, element, estimated_count, tk.min_elem, tk.min_count)

    %{
      tk
      | cms: updated_cms,
        items: updated_items,
        min_elem: updated_min_elem,
        min_count: updated_min_count
    }
  end

  # ---------------------------------------------------------------------------
  # Querying
  # ---------------------------------------------------------------------------

  @doc """
  Returns the current top-k items as a list of `{element, estimated_count}` tuples.

  The list is sorted in descending order by estimated count. Its length is
  at most `k`, and may be shorter if fewer than `k` distinct elements have
  been added.

  ## Parameters

    * `tk` -- the Top-K tracker.

  ## Returns

  A list of `{element, estimated_count}` tuples sorted in descending order
  by estimated count. The list contains at most `k` entries.

  ## Examples

      iex> tk = Approx.TopK.new(2)
      iex> tk = tk |> Approx.TopK.add("a", 10) |> Approx.TopK.add("b", 20)
      iex> [{first_elem, _}, {second_elem, _}] = Approx.TopK.top(tk)
      iex> first_elem
      "b"
      iex> second_elem
      "a"

      iex> tk = Approx.TopK.new(5)
      iex> Approx.TopK.top(tk)
      []
  """
  @spec top(t()) :: list({term(), non_neg_integer()})
  def top(%__MODULE__{items: items}) do
    items
    |> Map.to_list()
    |> Enum.sort_by(fn {_elem, count} -> count end, :desc)
  end

  @doc """
  Returns the estimated count for `element` from the underlying Count-Min Sketch.

  This queries the CMS directly, so it works for any element -- not just
  those currently in the top-k list.

  ## Parameters

    * `tk` -- the Top-K tracker.
    * `element` -- the term to query.

  ## Returns

  A non-negative integer representing the estimated frequency of `element`.
  Returns `0` for elements that have never been added.

  ## Examples

      iex> tk = Approx.TopK.new(3) |> Approx.TopK.add("x", 42)
      iex> Approx.TopK.count(tk, "x") >= 42
      true

      iex> tk = Approx.TopK.new(3)
      iex> Approx.TopK.count(tk, "never_added")
      0
  """
  @spec count(t(), term()) :: non_neg_integer()
  def count(%__MODULE__{cms: cms}, element) do
    CountMinSketch.count(cms, element)
  end

  @doc """
  Returns `true` if `element` is currently in the top-k candidate list.

  Note that this only checks the maintained map of top candidates, not
  the Count-Min Sketch. An element may have been added but not be in the
  top-k map if its count is too low.

  ## Parameters

    * `tk` -- the Top-K tracker.
    * `element` -- the term to check.

  ## Returns

  `true` if `element` is currently in the top-k candidate map, `false`
  otherwise.

  ## Examples

      iex> tk = Approx.TopK.new(2)
      iex> tk = tk |> Approx.TopK.add("a", 100) |> Approx.TopK.add("b", 50) |> Approx.TopK.add("c", 1)
      iex> Approx.TopK.member?(tk, "a")
      true
      iex> Approx.TopK.member?(tk, "c")
      false
  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{items: items}, element) do
    Map.has_key?(items, element)
  end

  # ---------------------------------------------------------------------------
  # Merging
  # ---------------------------------------------------------------------------

  @doc """
  Merges two Top-K trackers into one.

  The underlying Count-Min Sketches are merged, and then the top-k map is
  rebuilt by querying the merged CMS for all unique elements that appeared
  in either tracker's candidate map. The top `k` elements by estimated
  count are retained.

  Both trackers must have the same `k` value and compatible CMS dimensions
  (same `width` and `depth`).

  ## Parameters

    * `tk1` -- the first Top-K tracker.
    * `tk2` -- the second Top-K tracker.

  ## Returns

  `{:ok, merged}` on success, or `{:error, :incompatible_k}` if the `k`
  values differ, or `{:error, :dimension_mismatch}` if the underlying CMS
  dimensions are incompatible.

  ## Examples

      iex> tk1 = Approx.TopK.new(2) |> Approx.TopK.add("a", 10) |> Approx.TopK.add("b", 5)
      iex> tk2 = Approx.TopK.new(2) |> Approx.TopK.add("a", 20) |> Approx.TopK.add("c", 15)
      iex> {:ok, merged} = Approx.TopK.merge(tk1, tk2)
      iex> Approx.TopK.member?(merged, "a")
      true
  """
  @spec merge(t(), t()) :: {:ok, t()} | {:error, :incompatible_k | :dimension_mismatch}
  def merge(%__MODULE__{k: k} = tk1, %__MODULE__{k: k} = tk2) do
    case CountMinSketch.merge(tk1.cms, tk2.cms) do
      {:ok, merged_cms} ->
        # Collect all unique elements from both candidate maps
        all_elements =
          (Map.keys(tk1.items) ++ Map.keys(tk2.items))
          |> Enum.uniq()

        # Query the merged CMS for each element's new estimated count,
        # then keep the top k as a map with tracked minimum.
        scored =
          all_elements
          |> Enum.map(fn elem -> {elem, CountMinSketch.count(merged_cms, elem)} end)
          |> Enum.sort_by(fn {_elem, count} -> count end, :desc)
          |> Enum.take(k)

        items_map = Map.new(scored)
        {min_elem, min_count} = compute_min(items_map)

        {:ok,
         %{tk1 | cms: merged_cms, items: items_map, min_elem: min_elem, min_count: min_count}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def merge(%__MODULE__{}, %__MODULE__{}) do
    {:error, :incompatible_k}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  @spec update_items(
          %{optional(term()) => non_neg_integer()},
          pos_integer(),
          term(),
          non_neg_integer(),
          term() | nil,
          non_neg_integer() | nil
        ) :: {%{optional(term()) => non_neg_integer()}, term() | nil, non_neg_integer() | nil}
  defp update_items(items, k, element, estimated_count, min_elem, min_count) do
    cond do
      Map.has_key?(items, element) ->
        update_existing(items, element, estimated_count, min_elem, min_count)

      map_size(items) < k ->
        insert_with_room(items, element, estimated_count, min_elem, min_count)

      estimated_count > min_count ->
        evict_and_insert(items, element, estimated_count, min_elem)

      true ->
        {items, min_elem, min_count}
    end
  end

  # Element already tracked -- update count in O(1), recompute min only when needed.
  defp update_existing(items, element, estimated_count, min_elem, min_count) do
    updated = Map.put(items, element, estimated_count)

    cond do
      element == min_elem ->
        {new_min_elem, new_min_count} = compute_min(updated)
        {updated, new_min_elem, new_min_count}

      estimated_count < min_count ->
        {updated, element, estimated_count}

      true ->
        {updated, min_elem, min_count}
    end
  end

  # Map has room -- insert in O(1), update min tracking.
  defp insert_with_room(items, element, estimated_count, min_elem, min_count) do
    updated = Map.put(items, element, estimated_count)

    if min_count == nil or estimated_count < min_count do
      {updated, element, estimated_count}
    else
      {updated, min_elem, min_count}
    end
  end

  # Map full, new element beats minimum -- evict and recompute min in O(k).
  defp evict_and_insert(items, element, estimated_count, min_elem) do
    updated = items |> Map.delete(min_elem) |> Map.put(element, estimated_count)
    {new_min_elem, new_min_count} = compute_min(updated)
    {updated, new_min_elem, new_min_count}
  end

  # Computes the element with the minimum count in the map.
  # Returns `{nil, nil}` for an empty map, otherwise `{element, count}`.
  @spec compute_min(%{optional(term()) => non_neg_integer()}) ::
          {term() | nil, non_neg_integer() | nil}
  defp compute_min(items) when map_size(items) == 0, do: {nil, nil}

  defp compute_min(items) do
    Enum.min_by(items, fn {_elem, count} -> count end)
  end
end
