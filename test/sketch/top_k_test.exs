defmodule Sketch.TopKTest do
  use ExUnit.Case, async: true

  alias Sketch.TopK

  doctest TopK

  # ---------------------------------------------------------------------------
  # new/2
  # ---------------------------------------------------------------------------

  describe "new/2" do
    test "creates a tracker with the given k" do
      tk = TopK.new(10)
      assert tk.k == 10
    end

    test "creates a tracker with empty items map" do
      tk = TopK.new(5)
      assert tk.items == %{}
    end

    test "creates a tracker with a valid CMS" do
      tk = TopK.new(3)
      assert %Sketch.CountMinSketch{} = tk.cms
    end

    test "accepts custom epsilon and delta" do
      tk = TopK.new(5, epsilon: 0.01, delta: 0.05)
      # width = ceil(e / 0.01) = 272
      assert tk.cms.width == 272
      # depth = ceil(ln(1/0.05)) = 3
      assert tk.cms.depth == 3
    end

    test "accepts a custom hash function" do
      custom_fn = fn _term -> 42 end
      tk = TopK.new(3, hash_fn: custom_fn)
      assert tk.hash_fn == custom_fn
      assert tk.cms.hash_fn == custom_fn
    end

    test "uses default CMS parameters when no options given" do
      tk = TopK.new(10)
      # Default epsilon=0.001 -> width=2719, default delta=0.01 -> depth=5
      assert tk.cms.width == 2719
      assert tk.cms.depth == 5
    end

    test "raises on non-positive k" do
      assert_raise FunctionClauseError, fn ->
        TopK.new(0)
      end

      assert_raise FunctionClauseError, fn ->
        TopK.new(-1)
      end
    end

    test "raises on non-integer k" do
      assert_raise FunctionClauseError, fn ->
        TopK.new(1.5)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # add/3
  # ---------------------------------------------------------------------------

  describe "add/3" do
    test "adds element with default amount of 1" do
      tk = TopK.new(3) |> TopK.add("hello")
      assert TopK.count(tk, "hello") >= 1
      assert TopK.member?(tk, "hello")
    end

    test "adds element with specified amount" do
      tk = TopK.new(3) |> TopK.add("hello", 42)
      assert TopK.count(tk, "hello") >= 42
      assert TopK.member?(tk, "hello")
    end

    test "accumulates counts across multiple adds" do
      tk =
        TopK.new(3)
        |> TopK.add("x", 5)
        |> TopK.add("x", 10)
        |> TopK.add("x", 3)

      assert TopK.count(tk, "x") >= 18
    end

    test "updates count in items list when element already tracked" do
      tk =
        TopK.new(3)
        |> TopK.add("a", 10)
        |> TopK.add("a", 5)

      [{elem, count}] = TopK.top(tk)
      assert elem == "a"
      assert count >= 15
    end

    test "inserts new element when list has room" do
      tk =
        TopK.new(5)
        |> TopK.add("a", 10)
        |> TopK.add("b", 5)
        |> TopK.add("c", 8)

      top = TopK.top(tk)
      assert length(top) == 3
      elements = Enum.map(top, fn {elem, _} -> elem end)
      assert "a" in elements
      assert "b" in elements
      assert "c" in elements
    end

    test "evicts minimum when list is full and new element has higher count" do
      tk =
        TopK.new(2)
        |> TopK.add("a", 100)
        |> TopK.add("b", 50)
        |> TopK.add("c", 200)

      top = TopK.top(tk)
      assert length(top) == 2
      elements = Enum.map(top, fn {elem, _} -> elem end)
      assert "c" in elements
      assert "a" in elements
      refute "b" in elements
    end

    test "does not evict when new element has lower count than minimum" do
      tk =
        TopK.new(2)
        |> TopK.add("a", 100)
        |> TopK.add("b", 50)
        |> TopK.add("c", 1)

      top = TopK.top(tk)
      assert length(top) == 2
      elements = Enum.map(top, fn {elem, _} -> elem end)
      assert "a" in elements
      assert "b" in elements
      refute "c" in elements
    end

    test "handles various element types" do
      tk =
        TopK.new(5)
        |> TopK.add(:atom_key, 10)
        |> TopK.add(123, 20)
        |> TopK.add({:tuple, "value"}, 30)
        |> TopK.add([1, 2, 3], 40)

      assert TopK.member?(tk, :atom_key)
      assert TopK.member?(tk, 123)
      assert TopK.member?(tk, {:tuple, "value"})
      assert TopK.member?(tk, [1, 2, 3])
    end
  end

  # ---------------------------------------------------------------------------
  # top/1
  # ---------------------------------------------------------------------------

  describe "top/1" do
    test "returns empty list for fresh tracker" do
      tk = TopK.new(5)
      assert TopK.top(tk) == []
    end

    test "returns items sorted descending by count" do
      tk =
        TopK.new(5)
        |> TopK.add("low", 1)
        |> TopK.add("mid", 50)
        |> TopK.add("high", 100)
        |> TopK.add("medium", 75)

      top = TopK.top(tk)
      counts = Enum.map(top, fn {_elem, count} -> count end)
      assert counts == Enum.sort(counts, :desc)
    end

    test "returns at most k items" do
      tk =
        Enum.reduce(1..20, TopK.new(5), fn i, acc ->
          TopK.add(acc, "item_#{i}", i * 10)
        end)

      assert length(TopK.top(tk)) == 5
    end

    test "items are tuples of {element, count}" do
      tk = TopK.new(3) |> TopK.add("a", 10)
      [{elem, count}] = TopK.top(tk)
      assert is_binary(elem)
      assert is_integer(count)
      assert count >= 10
    end

    test "tracks highest frequency items from a stream" do
      # Add elements with known, well-separated frequencies
      # The top-3 should be clearly identifiable
      tk =
        TopK.new(3)
        |> TopK.add("rare", 1)
        |> TopK.add("uncommon", 10)
        |> TopK.add("common", 100)
        |> TopK.add("frequent", 1000)
        |> TopK.add("dominant", 10_000)

      top = TopK.top(tk)
      elements = Enum.map(top, fn {elem, _} -> elem end)

      assert "dominant" in elements
      assert "frequent" in elements
      assert "common" in elements
      refute "rare" in elements
      refute "uncommon" in elements
    end
  end

  # ---------------------------------------------------------------------------
  # count/2
  # ---------------------------------------------------------------------------

  describe "count/2" do
    test "returns 0 for element never added" do
      tk = TopK.new(5)
      assert TopK.count(tk, "ghost") == 0
    end

    test "returns estimated count from CMS for added element" do
      tk = TopK.new(3) |> TopK.add("x", 42)
      assert TopK.count(tk, "x") >= 42
    end

    test "works for elements not in top-k list" do
      tk =
        TopK.new(1)
        |> TopK.add("top", 1000)
        |> TopK.add("bottom", 1)

      # "bottom" is not in top-k list, but CMS still has its count
      refute TopK.member?(tk, "bottom")
      assert TopK.count(tk, "bottom") >= 1
    end

    test "never undercounts (CMS guarantee)" do
      elements = for i <- 1..50, do: {"item_#{i}", i * 3}

      tk =
        Enum.reduce(elements, TopK.new(10), fn {elem, freq}, acc ->
          TopK.add(acc, elem, freq)
        end)

      for {elem, freq} <- elements do
        assert TopK.count(tk, elem) >= freq,
               "count for #{elem} was #{TopK.count(tk, elem)}, expected >= #{freq}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # member?/2
  # ---------------------------------------------------------------------------

  describe "member?/2" do
    test "returns false for empty tracker" do
      tk = TopK.new(5)
      refute TopK.member?(tk, "anything")
    end

    test "returns true for element in top-k list" do
      tk = TopK.new(3) |> TopK.add("present", 10)
      assert TopK.member?(tk, "present")
    end

    test "returns false for element not in top-k list" do
      tk =
        TopK.new(1)
        |> TopK.add("winner", 1000)
        |> TopK.add("loser", 1)

      assert TopK.member?(tk, "winner")
      refute TopK.member?(tk, "loser")
    end

    test "returns false for element never added" do
      tk = TopK.new(3) |> TopK.add("x", 10)
      refute TopK.member?(tk, "never_seen")
    end

    test "reflects evictions correctly" do
      # Fill up k=2 slots, then add something that evicts
      tk =
        TopK.new(2)
        |> TopK.add("a", 50)
        |> TopK.add("b", 100)

      assert TopK.member?(tk, "a")
      assert TopK.member?(tk, "b")

      # Add "c" with higher count than "a", evicting "a"
      tk = TopK.add(tk, "c", 200)

      assert TopK.member?(tk, "c")
      assert TopK.member?(tk, "b")
      refute TopK.member?(tk, "a")
    end
  end

  # ---------------------------------------------------------------------------
  # merge/2
  # ---------------------------------------------------------------------------

  describe "merge/2" do
    test "merged tracker contains top-k from combined data" do
      tk1 =
        TopK.new(3)
        |> TopK.add("a", 100)
        |> TopK.add("b", 50)
        |> TopK.add("c", 25)

      tk2 =
        TopK.new(3)
        |> TopK.add("a", 200)
        |> TopK.add("d", 150)
        |> TopK.add("e", 10)

      {:ok, merged} = TopK.merge(tk1, tk2)

      top = TopK.top(merged)
      elements = Enum.map(top, fn {elem, _} -> elem end)

      # "a" has highest combined count (300+), "d" has 150, "b" has 50
      assert "a" in elements
      assert "d" in elements
      assert length(top) == 3
    end

    test "merged CMS counts reflect both sources" do
      tk1 = TopK.new(3) |> TopK.add("x", 10)
      tk2 = TopK.new(3) |> TopK.add("x", 20)

      {:ok, merged} = TopK.merge(tk1, tk2)

      assert TopK.count(merged, "x") >= 30
    end

    test "merge is commutative for top elements" do
      tk1 =
        TopK.new(3)
        |> TopK.add("a", 100)
        |> TopK.add("b", 200)

      tk2 =
        TopK.new(3)
        |> TopK.add("c", 150)
        |> TopK.add("d", 50)

      {:ok, merged_ab} = TopK.merge(tk1, tk2)
      {:ok, merged_ba} = TopK.merge(tk2, tk1)

      elements_ab = merged_ab |> TopK.top() |> Enum.map(fn {e, _} -> e end) |> Enum.sort()
      elements_ba = merged_ba |> TopK.top() |> Enum.map(fn {e, _} -> e end) |> Enum.sort()

      assert elements_ab == elements_ba
    end

    test "merge with empty tracker returns equivalent of original" do
      tk =
        TopK.new(3)
        |> TopK.add("a", 10)
        |> TopK.add("b", 20)

      empty = TopK.new(3)

      {:ok, merged} = TopK.merge(tk, empty)

      top = TopK.top(merged)
      elements = Enum.map(top, fn {elem, _} -> elem end)
      assert "a" in elements
      assert "b" in elements
    end

    test "merge preserves k" do
      tk1 = TopK.new(2) |> TopK.add("a", 10) |> TopK.add("b", 20)
      tk2 = TopK.new(2) |> TopK.add("c", 30) |> TopK.add("d", 40)

      {:ok, merged} = TopK.merge(tk1, tk2)

      assert merged.k == 2
      assert length(TopK.top(merged)) <= 2
    end

    test "merge returns error when k values differ" do
      tk1 = TopK.new(3)
      tk2 = TopK.new(5)

      assert {:error, :incompatible_k} = TopK.merge(tk1, tk2)
    end

    test "merge returns error when CMS dimensions differ" do
      tk1 = TopK.new(3, epsilon: 0.01, delta: 0.01)
      tk2 = TopK.new(3, epsilon: 0.1, delta: 0.1)

      assert {:error, :dimension_mismatch} = TopK.merge(tk1, tk2)
    end

    test "merged top list is sorted descending by count" do
      tk1 =
        TopK.new(5)
        |> TopK.add("a", 10)
        |> TopK.add("b", 50)

      tk2 =
        TopK.new(5)
        |> TopK.add("c", 30)
        |> TopK.add("d", 70)

      {:ok, merged} = TopK.merge(tk1, tk2)

      counts = merged |> TopK.top() |> Enum.map(fn {_, count} -> count end)
      assert counts == Enum.sort(counts, :desc)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "k=1 tracks only the single most frequent element" do
      tk =
        TopK.new(1)
        |> TopK.add("a", 10)
        |> TopK.add("b", 100)
        |> TopK.add("c", 50)

      [{elem, _count}] = TopK.top(tk)
      assert elem == "b"
      assert length(TopK.top(tk)) == 1
    end

    test "single element added once" do
      tk = TopK.new(5) |> TopK.add("only")

      assert TopK.top(tk) == [{"only", TopK.count(tk, "only")}]
      assert TopK.member?(tk, "only")
    end

    test "adding same element many times updates count in list" do
      tk =
        Enum.reduce(1..100, TopK.new(3), fn _i, acc ->
          TopK.add(acc, "repeat")
        end)

      [{elem, count}] = TopK.top(tk)
      assert elem == "repeat"
      assert count >= 100
    end

    test "adding exactly k distinct elements fills the list" do
      tk =
        TopK.new(3)
        |> TopK.add("a", 10)
        |> TopK.add("b", 20)
        |> TopK.add("c", 30)

      assert length(TopK.top(tk)) == 3
    end

    test "adding more than k distinct elements keeps only top k" do
      tk =
        TopK.new(2)
        |> TopK.add("a", 100)
        |> TopK.add("b", 200)
        |> TopK.add("c", 300)
        |> TopK.add("d", 400)

      assert length(TopK.top(tk)) == 2
      elements = Enum.map(TopK.top(tk), fn {e, _} -> e end)
      assert "d" in elements
      assert "c" in elements
    end

    test "struct fields are accessible" do
      tk = TopK.new(5)
      assert %TopK{k: _, cms: _, items: _, hash_fn: _, min_elem: _, min_count: _} = tk
    end

    test "large k with few elements" do
      tk =
        TopK.new(1000)
        |> TopK.add("a", 10)
        |> TopK.add("b", 20)

      assert length(TopK.top(tk)) == 2
    end

    test "elements with equal counts are all tracked" do
      tk =
        TopK.new(5)
        |> TopK.add("a", 100)
        |> TopK.add("b", 100)
        |> TopK.add("c", 100)

      assert length(TopK.top(tk)) == 3
      elements = Enum.map(TopK.top(tk), fn {e, _} -> e end)
      assert "a" in elements
      assert "b" in elements
      assert "c" in elements
    end
  end

  # ---------------------------------------------------------------------------
  # Ordering guarantees
  # ---------------------------------------------------------------------------

  describe "ordering" do
    test "top list is always sorted descending after single adds" do
      tk =
        Enum.reduce(1..20, TopK.new(10), fn i, acc ->
          TopK.add(acc, "item_#{i}", :rand.uniform(1000))
        end)

      counts = tk |> TopK.top() |> Enum.map(fn {_, c} -> c end)
      assert counts == Enum.sort(counts, :desc)
    end

    test "top list is sorted descending after incremental adds" do
      tk = TopK.new(5)

      tk =
        Enum.reduce(1..100, tk, fn i, acc ->
          element = "item_#{rem(i, 7)}"
          TopK.add(acc, element, :rand.uniform(50))
        end)

      counts = tk |> TopK.top() |> Enum.map(fn {_, c} -> c end)
      assert counts == Enum.sort(counts, :desc)
    end

    test "highest count element is always first" do
      tk =
        TopK.new(5)
        |> TopK.add("small", 1)
        |> TopK.add("medium", 50)
        |> TopK.add("large", 100)
        |> TopK.add("huge", 10_000)

      [{first_elem, _} | _] = TopK.top(tk)
      assert first_elem == "huge"
    end
  end

  # ---------------------------------------------------------------------------
  # Heavy hitters accuracy
  # ---------------------------------------------------------------------------

  describe "heavy hitters accuracy" do
    test "correctly identifies top items with well-separated frequencies" do
      # Create a workload where the top-5 are clearly distinct from the rest
      heavy_hitters = for i <- 1..5, do: {"heavy_#{i}", 10_000 + i * 1_000}
      light_items = for i <- 1..50, do: {"light_#{i}", i}

      tk =
        Enum.reduce(heavy_hitters ++ light_items, TopK.new(5), fn {elem, freq}, acc ->
          TopK.add(acc, elem, freq)
        end)

      top = TopK.top(tk)
      top_elements = Enum.map(top, fn {elem, _} -> elem end)

      # All heavy hitters should be in the top-5
      for {elem, _freq} <- heavy_hitters do
        assert elem in top_elements,
               "Expected #{elem} to be in top-5, got: #{inspect(top_elements)}"
      end
    end

    test "top-k elements have counts >= their true frequencies" do
      elements = for i <- 1..20, do: {"item_#{i}", i * 100}

      tk =
        Enum.reduce(elements, TopK.new(5), fn {elem, freq}, acc ->
          TopK.add(acc, elem, freq)
        end)

      for {elem, count} <- TopK.top(tk) do
        # Find the true frequency
        {^elem, true_freq} = Enum.find(elements, fn {e, _} -> e == elem end)
        assert count >= true_freq
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Zipfian distribution recall
  # ---------------------------------------------------------------------------

  describe "zipfian distribution" do
    test "recalls all true top-10 elements from a Zipf-like stream" do
      # Element i (for i in 1..100) appears ceil(10_000 / i) times.
      # Element 1 appears 10_000 times, element 2 appears 5_000, etc.
      # The true top-10 are elements 1..10.
      k = 10
      tk = TopK.new(k)

      tk =
        Enum.reduce(1..100, tk, fn i, acc ->
          freq = ceil(10_000 / i)

          Enum.reduce(1..freq, acc, fn _j, inner_acc ->
            TopK.add(inner_acc, "element_#{i}")
          end)
        end)

      top = TopK.top(tk)
      top_elements = Enum.map(top, fn {elem, _count} -> elem end)

      # All true top-10 elements (those with the highest frequencies) must appear
      for i <- 1..10 do
        assert "element_#{i}" in top_elements,
               "Expected element_#{i} (freq=#{ceil(10_000 / i)}) in top-#{k}, got: #{inspect(top_elements)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Error bound verification
  # ---------------------------------------------------------------------------

  describe "error bounds" do
    test "estimated counts stay within epsilon * N of true counts for top items" do
      # Insert 1000 distinct elements: element_i appears i times.
      # Total stream length N = sum(1..1000) = 500_500.
      elements = for i <- 1..1000, do: {"element_#{i}", i}

      tk =
        Enum.reduce(elements, TopK.new(20), fn {elem, freq}, acc ->
          TopK.add(acc, elem, freq)
        end)

      total_stream_length = Enum.sum(1..1000)
      # Default epsilon is 0.001
      epsilon = 0.001

      for {elem, estimated_count} <- TopK.top(tk) do
        # Find the true count for this element
        {^elem, true_count} =
          Enum.find(elements, fn {e, _} -> e == elem end)

        error = estimated_count - true_count
        max_error = epsilon * total_stream_length

        assert error >= 0,
               "CMS must never undercount: #{elem} estimated=#{estimated_count}, true=#{true_count}"

        assert error <= max_error,
               "Error for #{elem} is #{error}, exceeds epsilon*N=#{max_error}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # High-cardinality eviction stress
  # ---------------------------------------------------------------------------

  describe "high cardinality eviction" do
    test "retains only high-frequency items after inserting 10_000 distinct items" do
      k = 20

      tk =
        Enum.reduce(1..10_000, TopK.new(k), fn i, acc ->
          TopK.add(acc, "item_#{i}", i)
        end)

      top = TopK.top(tk)

      # Exactly k items are retained
      assert length(top) == k

      # Extract the true index (= true frequency) for each tracked item
      true_frequencies =
        Enum.map(top, fn {elem, _count} ->
          "item_" <> index_str = elem
          String.to_integer(index_str)
        end)

      # Every item in the top-k list should have a true frequency in the upper
      # portion of the distribution. With CMS overestimation from 10k items,
      # some lower-frequency items may appear, but they should still be from
      # the high end (above the 80th percentile = index > 8000).
      for freq <- true_frequencies do
        assert freq > 8000,
               "Expected all top-#{k} items to have true freq > 8000, but found freq=#{freq}"
      end

      # The list must be sorted descending by estimated count
      counts = Enum.map(top, fn {_elem, count} -> count end)
      assert counts == Enum.sort(counts, :desc)
    end
  end

  # ---------------------------------------------------------------------------
  # Late-arriving heavy hitter
  # ---------------------------------------------------------------------------

  describe "late arriving heavy hitter" do
    test "a single late heavy hitter displaces earlier items" do
      tk = TopK.new(5)

      # Add 100 items each with count 10
      tk =
        Enum.reduce(1..100, tk, fn i, acc ->
          TopK.add(acc, "item_#{i}", 10)
        end)

      # Now add a single item with a very large count
      tk = TopK.add(tk, "latecomer", 10_000)

      top = TopK.top(tk)
      top_elements = Enum.map(top, fn {elem, _count} -> elem end)

      assert "latecomer" in top_elements,
             "Expected 'latecomer' in top-5, got: #{inspect(top_elements)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-way merge
  # ---------------------------------------------------------------------------

  describe "multi-way merge" do
    test "merging 5 partitioned TopK structs produces correct top-k" do
      # Create 5 TopK structs, each with a different partition of elements.
      # Partition 1: "a"=500, "f"=10
      # Partition 2: "b"=400, "g"=20
      # Partition 3: "c"=300, "h"=30
      # Partition 4: "d"=200, "i"=40
      # Partition 5: "e"=100, "j"=50

      partitions = [
        [{"a", 500}, {"f", 10}],
        [{"b", 400}, {"g", 20}],
        [{"c", 300}, {"h", 30}],
        [{"d", 200}, {"i", 40}],
        [{"e", 100}, {"j", 50}]
      ]

      trackers =
        Enum.map(partitions, fn items ->
          Enum.reduce(items, TopK.new(5), fn {elem, freq}, acc ->
            TopK.add(acc, elem, freq)
          end)
        end)

      # Merge all sequentially: ((((tk1 merge tk2) merge tk3) merge tk4) merge tk5)
      [first | rest] = trackers

      merged =
        Enum.reduce(rest, first, fn tk, acc ->
          {:ok, result} = TopK.merge(acc, tk)
          result
        end)

      top = TopK.top(merged)
      top_elements = Enum.map(top, fn {elem, _count} -> elem end)

      assert length(top) == 5

      # The top-5 by true frequency are: a=500, b=400, c=300, d=200, e=100
      for expected <- ["a", "b", "c", "d", "e"] do
        assert expected in top_elements,
               "Expected '#{expected}' in merged top-5, got: #{inspect(top_elements)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Tie-breaking determinism
  # ---------------------------------------------------------------------------

  describe "tie breaking" do
    test "top/1 returns identical order across multiple calls" do
      k = 5

      tk =
        Enum.reduce(1..k, TopK.new(k), fn i, acc ->
          TopK.add(acc, "item_#{i}", 100)
        end)

      # Call top/1 multiple times and verify order is stable
      first_call = TopK.top(tk)
      second_call = TopK.top(tk)
      third_call = TopK.top(tk)

      assert first_call == second_call
      assert second_call == third_call
    end
  end

  # ---------------------------------------------------------------------------
  # No duplicates in top list
  # ---------------------------------------------------------------------------

  describe "no duplicates" do
    test "top-k list never contains duplicate elements after many operations" do
      tk = TopK.new(5)

      # Phase 1: Add initial items to fill the list
      tk =
        Enum.reduce(1..5, tk, fn i, acc ->
          TopK.add(acc, "item_#{i}", i * 10)
        end)

      # Phase 2: Repeatedly add items that cause evictions
      tk =
        Enum.reduce(6..50, tk, fn i, acc ->
          TopK.add(acc, "item_#{i}", i * 10)
        end)

      # Phase 3: Re-add items that were previously tracked (and may have been evicted)
      tk =
        Enum.reduce(1..50, tk, fn i, acc ->
          TopK.add(acc, "item_#{i}", 1)
        end)

      # Phase 4: Add large counts to previously evicted items to force re-insertion
      tk =
        Enum.reduce(1..10, tk, fn i, acc ->
          TopK.add(acc, "item_#{i}", 100_000)
        end)

      top = TopK.top(tk)
      elements = Enum.map(top, fn {elem, _count} -> elem end)

      assert length(top) == length(Enum.uniq(elements)),
             "Duplicate elements found in top-k: #{inspect(top)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Re-insertion after eviction
  # ---------------------------------------------------------------------------

  describe "re-insertion after eviction" do
    test "evicted element re-enters top-k when added with high count" do
      tk =
        TopK.new(2)
        |> TopK.add("a", 100)
        |> TopK.add("b", 50)

      # Verify "b" is currently tracked
      assert TopK.member?(tk, "b")

      # Add "c" with count 200, which should evict "b" (the minimum at 50)
      tk = TopK.add(tk, "c", 200)

      refute TopK.member?(tk, "b"),
             "Expected 'b' to be evicted after adding 'c' with count 200"

      # Now add "b" with a very large count so it re-enters
      tk = TopK.add(tk, "b", 300)

      assert TopK.member?(tk, "b"),
             "Expected 'b' to re-enter top-k after adding count 300"
    end
  end

  # ---------------------------------------------------------------------------
  # Disjoint merge
  # ---------------------------------------------------------------------------

  describe "disjoint merge" do
    test "merging trackers with disjoint items reflects correct ranking" do
      tk1 =
        TopK.new(3)
        |> TopK.add("a", 100)
        |> TopK.add("b", 200)
        |> TopK.add("c", 50)

      tk2 =
        TopK.new(3)
        |> TopK.add("d", 300)
        |> TopK.add("e", 150)
        |> TopK.add("f", 25)

      {:ok, merged} = TopK.merge(tk1, tk2)

      top = TopK.top(merged)
      top_elements = Enum.map(top, fn {elem, _count} -> elem end)

      assert length(top) == 3

      # Top-3 by count: d=300, b=200, e=150
      assert "d" in top_elements
      assert "b" in top_elements
      assert "e" in top_elements

      # Verify ordering: d should be first (highest count)
      [{first_elem, _} | _] = top
      assert first_elem == "d"
    end
  end

  # ---------------------------------------------------------------------------
  # Merge changes ranking
  # ---------------------------------------------------------------------------

  describe "merge ranking" do
    test "merge changes relative ranking when second tracker shifts counts" do
      tk1 =
        TopK.new(5)
        |> TopK.add("a", 100)
        |> TopK.add("b", 50)

      tk2 =
        TopK.new(5)
        |> TopK.add("a", 10)
        |> TopK.add("b", 200)

      {:ok, merged} = TopK.merge(tk1, tk2)

      top = TopK.top(merged)
      top_elements = Enum.map(top, fn {elem, _count} -> elem end)

      assert "a" in top_elements
      assert "b" in top_elements

      # After merge: a ~ 110, b ~ 250. "b" should be ranked higher (first).
      [{first_elem, first_count}, {second_elem, second_count}] = top

      assert first_elem == "b",
             "Expected 'b' to be ranked first after merge, got '#{first_elem}'"

      assert second_elem == "a",
             "Expected 'a' to be ranked second after merge, got '#{second_elem}'"

      assert first_count >= 250, "Expected merged count for 'b' >= 250, got #{first_count}"
      assert second_count >= 110, "Expected merged count for 'a' >= 110, got #{second_count}"
    end
  end
end
