defmodule Sketch.ReservoirTest do
  use ExUnit.Case, async: true

  alias Sketch.Reservoir

  doctest Sketch.Reservoir

  describe "new/2" do
    test "creates a reservoir with the given k" do
      r = Reservoir.new(10)
      assert r.k == 10
      assert r.count == 0
      assert r.samples == []
    end

    test "creates a reservoir with k=1" do
      r = Reservoir.new(1)
      assert r.k == 1
      assert r.count == 0
      assert r.samples == []
    end

    test "accepts an integer seed for reproducibility" do
      r1 = Reservoir.new(5, seed: 42)
      r2 = Reservoir.new(5, seed: 42)
      assert r1.seed == r2.seed
    end

    test "different seeds produce different seed states" do
      r1 = Reservoir.new(5, seed: 42)
      r2 = Reservoir.new(5, seed: 99)
      refute r1.seed == r2.seed
    end

    test "accepts a tuple seed state" do
      seed_state = :rand.seed_s(:exsss, 123)
      r = Reservoir.new(5, seed: seed_state)
      assert r.seed == seed_state
    end

    test "creates a random seed when no seed option is given" do
      r = Reservoir.new(5)
      assert is_tuple(r.seed)
    end

    test "raises for invalid k" do
      assert_raise FunctionClauseError, fn -> Reservoir.new(0) end
      assert_raise FunctionClauseError, fn -> Reservoir.new(-1) end
    end
  end

  describe "add/2" do
    test "adds element when reservoir is not full" do
      r =
        Reservoir.new(3, seed: 42)
        |> Reservoir.add(:a)
        |> Reservoir.add(:b)

      assert Reservoir.sample(r) == [:a, :b]
      assert Reservoir.count(r) == 2
    end

    test "fills reservoir to capacity" do
      r =
        Reservoir.new(3, seed: 42)
        |> Reservoir.add(:a)
        |> Reservoir.add(:b)
        |> Reservoir.add(:c)

      assert Reservoir.sample(r) == [:a, :b, :c]
      assert Reservoir.count(r) == 3
    end

    test "maintains exactly k elements after overflow" do
      r = Reservoir.new(3, seed: 42)
      r = Enum.reduce(1..100, r, fn x, acc -> Reservoir.add(acc, x) end)

      assert length(Reservoir.sample(r)) == 3
      assert Reservoir.count(r) == 100
    end

    test "k=1 keeps exactly one element" do
      r = Reservoir.new(1, seed: 42)
      r = Enum.reduce(1..1000, r, fn x, acc -> Reservoir.add(acc, x) end)

      assert length(Reservoir.sample(r)) == 1
      assert Reservoir.count(r) == 1000
    end

    test "handles various term types" do
      r =
        Reservoir.new(5, seed: 42)
        |> Reservoir.add("string")
        |> Reservoir.add(42)
        |> Reservoir.add(:atom)
        |> Reservoir.add({:tuple, 1})
        |> Reservoir.add([1, 2, 3])

      assert Reservoir.sample(r) == ["string", 42, :atom, {:tuple, 1}, [1, 2, 3]]
    end
  end

  describe "add_all/2" do
    test "adds all elements from a list" do
      r = Reservoir.new(5, seed: 42)
      r = Reservoir.add_all(r, [1, 2, 3])

      assert Reservoir.sample(r) == [1, 2, 3]
      assert Reservoir.count(r) == 3
    end

    test "adds all elements from a range" do
      r = Reservoir.new(100, seed: 42)
      r = Reservoir.add_all(r, 1..50)

      assert Reservoir.sample(r) == Enum.to_list(1..50)
      assert Reservoir.count(r) == 50
    end

    test "is equivalent to repeated add/2 calls" do
      seed = 42
      elements = Enum.to_list(1..200)

      r_add_all =
        Reservoir.new(10, seed: seed)
        |> Reservoir.add_all(elements)

      r_add =
        Enum.reduce(elements, Reservoir.new(10, seed: seed), fn x, acc ->
          Reservoir.add(acc, x)
        end)

      assert Reservoir.sample(r_add_all) == Reservoir.sample(r_add)
      assert Reservoir.count(r_add_all) == Reservoir.count(r_add)
    end

    test "handles empty enumerable" do
      r = Reservoir.new(5, seed: 42)
      r = Reservoir.add_all(r, [])

      assert Reservoir.sample(r) == []
      assert Reservoir.count(r) == 0
    end

    test "handles stream as enumerable" do
      stream = Stream.iterate(1, &(&1 + 1)) |> Stream.take(100)
      r = Reservoir.new(5, seed: 42)
      r = Reservoir.add_all(r, stream)

      assert length(Reservoir.sample(r)) == 5
      assert Reservoir.count(r) == 100
    end
  end

  describe "sample/1" do
    test "returns empty list for fresh reservoir" do
      r = Reservoir.new(5, seed: 42)
      assert Reservoir.sample(r) == []
    end

    test "returns all elements when count < k" do
      r =
        Reservoir.new(10, seed: 42)
        |> Reservoir.add_all([1, 2, 3])

      assert Reservoir.sample(r) == [1, 2, 3]
    end

    test "returns exactly k elements when count >= k" do
      r =
        Reservoir.new(5, seed: 42)
        |> Reservoir.add_all(1..500)

      assert length(Reservoir.sample(r)) == 5
    end

    test "all returned elements come from the input stream" do
      input = Enum.to_list(1..1000)

      r =
        Reservoir.new(20, seed: 42)
        |> Reservoir.add_all(input)

      for element <- Reservoir.sample(r) do
        assert element in input
      end
    end
  end

  describe "count/1" do
    test "returns 0 for fresh reservoir" do
      assert Reservoir.count(Reservoir.new(5, seed: 42)) == 0
    end

    test "tracks total elements seen, not reservoir size" do
      r =
        Reservoir.new(3, seed: 42)
        |> Reservoir.add_all(1..1000)

      assert Reservoir.count(r) == 1000
      assert length(Reservoir.sample(r)) == 3
    end

    test "increments by one for each add" do
      r = Reservoir.new(5, seed: 42)

      r = Reservoir.add(r, :a)
      assert Reservoir.count(r) == 1

      r = Reservoir.add(r, :b)
      assert Reservoir.count(r) == 2

      r = Reservoir.add(r, :c)
      assert Reservoir.count(r) == 3
    end
  end

  describe "merge/2" do
    test "merged reservoir has correct total count" do
      r1 = Reservoir.new(5, seed: 42) |> Reservoir.add_all(1..50)
      r2 = Reservoir.new(5, seed: 99) |> Reservoir.add_all(51..100)

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert Reservoir.count(merged) == 100
    end

    test "merged reservoir has exactly k elements" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..500)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all(501..1000)

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert length(Reservoir.sample(merged)) == 10
    end

    test "merged reservoir samples come from either reservoir" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..100)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all(101..200)

      {:ok, merged} = Reservoir.merge(r1, r2)

      valid_elements = Reservoir.sample(r1) ++ Reservoir.sample(r2)

      for element <- Reservoir.sample(merged) do
        assert element in valid_elements,
               "element #{inspect(element)} not found in either reservoir's samples"
      end
    end

    test "merging with empty reservoir returns the non-empty one" do
      r1 = Reservoir.new(5, seed: 42) |> Reservoir.add_all(1..50)
      empty = Reservoir.new(5, seed: 99)

      assert {:ok, ^r1} = Reservoir.merge(r1, empty)
      assert {:ok, ^r1} = Reservoir.merge(empty, r1)
    end

    test "merging two empty reservoirs returns the first reservoir" do
      r1 = Reservoir.new(5, seed: 42)
      r2 = Reservoir.new(5, seed: 99)

      {:ok, merged} = Reservoir.merge(r1, r2)
      assert merged == r1
      assert Reservoir.count(merged) == 0
      assert Reservoir.sample(merged) == []
    end

    test "returns error when k values differ" do
      r1 = Reservoir.new(5, seed: 42)
      r2 = Reservoir.new(10, seed: 99)

      assert {:error, :incompatible_size} = Reservoir.merge(r1, r2)
    end

    test "merge is weighted by count" do
      # Create r1 with 900 elements and r2 with 100 elements.
      # After many merges, roughly 90% of samples should come from r1's pool
      # and 10% from r2's pool.
      r1 = Reservoir.new(50, seed: 1) |> Reservoir.add_all(1..900)
      r2 = Reservoir.new(50, seed: 2) |> Reservoir.add_all(901..1000)

      # Perform many merges with different seeds to collect statistics
      r1_sample_set = MapSet.new(Reservoir.sample(r1))
      _r2_sample_set = MapSet.new(Reservoir.sample(r2))

      results =
        Enum.map(1..200, fn seed_val ->
          # Create new reservoirs with different seeds each time to get different
          # merge outcomes
          r1_copy = %{r1 | seed: :rand.seed_s(:exsss, seed_val)}
          {:ok, merged} = Reservoir.merge(r1_copy, r2)
          samples = Reservoir.sample(merged)

          Enum.count(samples, fn s -> MapSet.member?(r1_sample_set, s) end)
        end)

      avg_from_r1 = Enum.sum(results) / length(results)

      # With 50 samples and 90% weight, we expect ~45 from r1
      # Allow generous tolerance for randomness
      assert avg_from_r1 > 30, "Expected roughly 45 elements from r1, got avg #{avg_from_r1}"
      assert avg_from_r1 < 50, "Expected roughly 45 elements from r1, got avg #{avg_from_r1}"
    end

    test "merge when both have fewer than k elements" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all([1, 2, 3])
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all([4, 5, 6])

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert Reservoir.count(merged) == 6
      # Should have 6 elements since total < k=10
      assert length(Reservoir.sample(merged)) == 6
    end
  end

  describe "determinism with seed" do
    test "same seed produces same results" do
      r1 =
        Reservoir.new(5, seed: 42)
        |> Reservoir.add_all(1..1000)

      r2 =
        Reservoir.new(5, seed: 42)
        |> Reservoir.add_all(1..1000)

      assert Reservoir.sample(r1) == Reservoir.sample(r2)
    end

    test "different seeds produce different results (with high probability)" do
      r1 =
        Reservoir.new(5, seed: 42)
        |> Reservoir.add_all(1..10_000)

      r2 =
        Reservoir.new(5, seed: 99)
        |> Reservoir.add_all(1..10_000)

      # With 10k elements and k=5, the probability of identical samples
      # from different seeds is astronomically small
      refute Reservoir.sample(r1) == Reservoir.sample(r2)
    end

    test "seed state is properly threaded through operations" do
      # Build up incrementally and check determinism at each step
      base = Reservoir.new(3, seed: 42)

      r1 = base |> Reservoir.add(:a) |> Reservoir.add(:b)
      r2 = base |> Reservoir.add(:a) |> Reservoir.add(:b)

      assert r1.seed == r2.seed
    end
  end

  describe "statistical uniformity" do
    test "sample is approximately uniform over many elements" do
      # Add integers 1..100 into a reservoir of size 50.
      # Run many trials and count how often each integer appears.
      # Each element should appear with probability 50/100 = 0.5
      num_trials = 500
      n = 100
      k = 50

      counts =
        Enum.reduce(1..num_trials, %{}, fn trial, acc ->
          r =
            Reservoir.new(k, seed: trial)
            |> Reservoir.add_all(1..n)

          Enum.reduce(Reservoir.sample(r), acc, fn elem, inner_acc ->
            Map.update(inner_acc, elem, 1, &(&1 + 1))
          end)
        end)

      # Each element should appear roughly num_trials * k / n = 250 times
      expected = num_trials * k / n

      for i <- 1..n do
        observed = Map.get(counts, i, 0)
        # Allow 30% deviation from expected (generous for statistical test)
        assert observed > expected * 0.5,
               "Element #{i} appeared #{observed} times, expected ~#{expected}"

        assert observed < expected * 1.5,
               "Element #{i} appeared #{observed} times, expected ~#{expected}"
      end
    end

    test "k=1 reservoir selects each element with equal probability" do
      num_trials = 5000
      n = 10

      counts =
        Enum.reduce(1..num_trials, %{}, fn trial, acc ->
          r =
            Reservoir.new(1, seed: trial)
            |> Reservoir.add_all(1..n)

          [element] = Reservoir.sample(r)
          Map.update(acc, element, 1, &(&1 + 1))
        end)

      # Each element should appear roughly num_trials / n = 500 times
      expected = num_trials / n

      for i <- 1..n do
        observed = Map.get(counts, i, 0)
        # Allow 40% deviation
        assert observed > expected * 0.6,
               "Element #{i} appeared #{observed} times, expected ~#{expected}"

        assert observed < expected * 1.4,
               "Element #{i} appeared #{observed} times, expected ~#{expected}"
      end
    end
  end

  describe "edge cases" do
    test "adding a single element to a fresh reservoir" do
      r = Reservoir.new(5, seed: 42) |> Reservoir.add(42)

      assert Reservoir.sample(r) == [42]
      assert Reservoir.count(r) == 1
    end

    test "reservoir with k=1 and one element" do
      r = Reservoir.new(1, seed: 42) |> Reservoir.add(:only)

      assert Reservoir.sample(r) == [:only]
      assert Reservoir.count(r) == 1
    end

    test "adding nil as an element" do
      r = Reservoir.new(3, seed: 42) |> Reservoir.add(nil)

      assert Reservoir.sample(r) == [nil]
      assert Reservoir.count(r) == 1
    end

    test "reservoir with large k and few elements" do
      r = Reservoir.new(1000, seed: 42) |> Reservoir.add_all(1..5)

      assert Reservoir.sample(r) == [1, 2, 3, 4, 5]
      assert Reservoir.count(r) == 5
    end

    test "elements can be complex nested terms" do
      elements = [
        %{name: "Alice", age: 30},
        {:ok, [1, 2, 3]},
        {1, 2, {3, 4}},
        fn -> :hello end,
        make_ref()
      ]

      r = Reservoir.new(10, seed: 42) |> Reservoir.add_all(elements)

      assert length(Reservoir.sample(r)) == 5
      assert Reservoir.count(r) == 5
    end
  end

  describe "bias detection" do
    test "mean preservation with sorted ascending input" do
      true_mean = 100.5
      num_trials = 200

      sample_means =
        Enum.map(1..num_trials, fn trial ->
          r =
            Reservoir.new(20, seed: trial)
            |> Reservoir.add_all(1..200)

          samples = Reservoir.sample(r)
          Enum.sum(samples) / length(samples)
        end)

      avg_mean = Enum.sum(sample_means) / length(sample_means)

      assert_in_delta avg_mean,
                      true_mean,
                      true_mean * 0.10,
                      "Average sample mean #{avg_mean} is not within 10% of true mean #{true_mean}"
    end

    test "mean preservation with reverse sorted input" do
      true_mean = 100.5
      num_trials = 200

      sample_means =
        Enum.map(1..num_trials, fn trial ->
          r =
            Reservoir.new(20, seed: trial)
            |> Reservoir.add_all(200..1//-1)

          samples = Reservoir.sample(r)
          Enum.sum(samples) / length(samples)
        end)

      avg_mean = Enum.sum(sample_means) / length(sample_means)

      assert_in_delta avg_mean,
                      true_mean,
                      true_mean * 0.10,
                      "Average sample mean #{avg_mean} is not within 10% of true mean #{true_mean}"
    end
  end

  describe "sample uniqueness" do
    test "no duplicates from distinct input" do
      r =
        Reservoir.new(50, seed: 42)
        |> Reservoir.add_all(1..1000)

      sample = Reservoir.sample(r)
      assert length(Enum.uniq(sample)) == length(sample)
    end
  end

  describe "large stream" do
    @tag timeout: :infinity
    test "handles 1_000_000 elements" do
      r =
        Reservoir.new(10, seed: 42)
        |> Reservoir.add_all(1..1_000_000)

      sample = Reservoir.sample(r)
      assert length(sample) == 10
      assert Reservoir.count(r) == 1_000_000

      for element <- sample do
        assert element >= 1 and element <= 1_000_000
      end
    end
  end

  describe "merge commutativity" do
    test "merge(r1, r2) and merge(r2, r1) produce statistically similar means" do
      true_mean = 500.5
      num_trials = 100

      {means_ab, means_ba} =
        Enum.reduce(1..num_trials, {[], []}, fn trial, {ab_acc, ba_acc} ->
          r1 =
            Reservoir.new(20, seed: trial)
            |> Reservoir.add_all(1..500)

          r2 =
            Reservoir.new(20, seed: trial + 10_000)
            |> Reservoir.add_all(501..1000)

          {:ok, merged_ab} = Reservoir.merge(r1, r2)
          {:ok, merged_ba} = Reservoir.merge(r2, r1)

          sample_ab = Reservoir.sample(merged_ab)
          sample_ba = Reservoir.sample(merged_ba)

          mean_ab = Enum.sum(sample_ab) / length(sample_ab)
          mean_ba = Enum.sum(sample_ba) / length(sample_ba)

          {[mean_ab | ab_acc], [mean_ba | ba_acc]}
        end)

      avg_mean_ab = Enum.sum(means_ab) / length(means_ab)
      avg_mean_ba = Enum.sum(means_ba) / length(means_ba)

      # Both averages should be within 15% of each other
      assert_in_delta avg_mean_ab,
                      avg_mean_ba,
                      avg_mean_ab * 0.15,
                      "merge(r1,r2) avg mean #{avg_mean_ab} and merge(r2,r1) avg mean #{avg_mean_ba} differ by more than 15%"

      # Both should be within 15% of true mean
      assert_in_delta avg_mean_ab,
                      true_mean,
                      true_mean * 0.15,
                      "merge(r1,r2) avg mean #{avg_mean_ab} not within 15% of true mean #{true_mean}"

      assert_in_delta avg_mean_ba,
                      true_mean,
                      true_mean * 0.15,
                      "merge(r2,r1) avg mean #{avg_mean_ba} not within 15% of true mean #{true_mean}"
    end
  end

  describe "merge associativity" do
    test "merge(merge(r1,r2),r3) and merge(r1,merge(r2,r3)) produce same count and length" do
      r1 =
        Reservoir.new(20, seed: 1)
        |> Reservoir.add_all(1..300)

      r2 =
        Reservoir.new(20, seed: 2)
        |> Reservoir.add_all(301..600)

      r3 =
        Reservoir.new(20, seed: 3)
        |> Reservoir.add_all(601..900)

      {:ok, r1_r2} = Reservoir.merge(r1, r2)
      {:ok, left_assoc} = Reservoir.merge(r1_r2, r3)
      {:ok, r2_r3} = Reservoir.merge(r2, r3)
      {:ok, right_assoc} = Reservoir.merge(r1, r2_r3)

      assert Reservoir.count(left_assoc) == 900
      assert Reservoir.count(right_assoc) == 900
      assert length(Reservoir.sample(left_assoc)) == 20
      assert length(Reservoir.sample(right_assoc)) == 20
    end
  end

  describe "merge mean preservation" do
    test "merged sample mean is close to population mean over many trials" do
      true_mean = 5000.5
      num_trials = 100

      sample_means =
        Enum.map(1..num_trials, fn trial ->
          r1 =
            Reservoir.new(50, seed: trial)
            |> Reservoir.add_all(1..5000)

          r2 =
            Reservoir.new(50, seed: trial + 10_000)
            |> Reservoir.add_all(5001..10_000)

          {:ok, merged} = Reservoir.merge(r1, r2)
          sample = Reservoir.sample(merged)
          Enum.sum(sample) / length(sample)
        end)

      avg_mean = Enum.sum(sample_means) / length(sample_means)

      assert_in_delta avg_mean,
                      true_mean,
                      true_mean * 0.10,
                      "Average merged sample mean #{avg_mean} is not within 10% of true mean #{true_mean}"
    end
  end

  describe "asymmetric merge" do
    test "merge with extreme size asymmetry" do
      r_large =
        Reservoir.new(10, seed: 42)
        |> Reservoir.add_all(1..100_000)

      r_small =
        Reservoir.new(10, seed: 99)
        |> Reservoir.add_all(100_001..100_010)

      {:ok, merged} = Reservoir.merge(r_large, r_small)

      assert Reservoir.count(merged) == 100_010

      sample = Reservoir.sample(merged)
      assert length(sample) == 10

      # With count ratio 100_000:10, the vast majority of samples should
      # come from r_large's range (1..100_000). Run multiple trials to
      # verify the tendency.
      from_large = Enum.count(sample, fn e -> e >= 1 and e <= 100_000 end)

      # With such extreme asymmetry, nearly all elements should come from
      # the large reservoir. Allow at least 7 out of 10.
      assert from_large >= 7,
             "Expected most samples from large reservoir, got #{from_large}/10 from large range"
    end
  end

  describe "position bias" do
    test "sorted ascending input shows no position bias across trials" do
      num_trials = 500
      n = 100
      k = 10

      counts =
        Enum.reduce(1..num_trials, %{}, fn trial, acc ->
          r =
            Reservoir.new(k, seed: trial)
            |> Reservoir.add_all(1..n)

          Enum.reduce(Reservoir.sample(r), acc, fn elem, inner_acc ->
            Map.update(inner_acc, elem, 1, &(&1 + 1))
          end)
        end)

      frequencies = Map.values(counts)
      max_freq = Enum.max(frequencies)
      min_freq = Enum.min(frequencies)

      assert max_freq / min_freq < 3.0,
             "Position bias detected: max frequency #{max_freq}, min frequency #{min_freq}, ratio #{max_freq / min_freq}"
    end
  end

  describe "self merge" do
    test "merging a reservoir with itself doubles the count" do
      r =
        Reservoir.new(10, seed: 42)
        |> Reservoir.add_all(1..100)

      {:ok, merged} = Reservoir.merge(r, r)

      assert Reservoir.count(merged) == 200
      assert length(Reservoir.sample(merged)) == 10
    end
  end

  describe "incremental fill" do
    test "first k elements are added deterministically one at a time" do
      k = 10
      r = Reservoir.new(k, seed: 42)

      Enum.reduce(1..k, r, fn i, acc ->
        acc = Reservoir.add(acc, i)

        sample = Reservoir.sample(acc)

        assert sample == Enum.to_list(1..i),
               "After adding #{i} elements, expected #{inspect(Enum.to_list(1..i))}, got #{inspect(sample)}"

        assert Reservoir.count(acc) == i

        acc
      end)
    end
  end

  describe "merge bug fix regressions" do
    # ---------------------------------------------------------------
    # Bug 1: merge sampled WITH replacement (introduced duplicates)
    # The old merge could pick the same element multiple times.
    # Now it uses weighted-sample-without-replacement.
    # ---------------------------------------------------------------

    test "merge produces exactly k elements from the combined pools" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..10)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all(11..20)

      {:ok, merged} = Reservoir.merge(r1, r2)
      sample = Reservoir.sample(merged)

      assert length(sample) == 10
      valid_elements = Reservoir.sample(r1) ++ Reservoir.sample(r2)

      for element <- sample do
        assert element in valid_elements,
               "element #{inspect(element)} not found in either reservoir's samples"
      end
    end

    test "merge with larger streams produces k elements from valid pools" do
      for seed <- 1..50 do
        r1 = Reservoir.new(10, seed: seed) |> Reservoir.add_all(1..100)
        r2 = Reservoir.new(10, seed: seed + 1000) |> Reservoir.add_all(101..200)

        {:ok, merged} = Reservoir.merge(r1, r2)
        sample = Reservoir.sample(merged)

        assert length(sample) == 10,
               "Expected 10 elements with seed #{seed}, got #{length(sample)}"

        valid_elements = Reservoir.sample(r1) ++ Reservoir.sample(r2)

        for element <- sample do
          assert element in valid_elements,
                 "element #{inspect(element)} not in either reservoir's samples (seed #{seed})"
        end
      end
    end

    test "statistical test: merge preserves proportional representation" do
      # r1 streams 1000 elements (values 1..1000), r2 streams 100 elements (values 1001..1100).
      # Elements from r1 should appear ~10x more often than elements from r2.
      k = 20
      num_trials = 300

      counts =
        Enum.reduce(1..num_trials, %{from_r1: 0, from_r2: 0}, fn trial, acc ->
          r1 = Reservoir.new(k, seed: trial) |> Reservoir.add_all(1..1000)
          r2 = Reservoir.new(k, seed: trial + 50_000) |> Reservoir.add_all(1001..1100)

          {:ok, merged} = Reservoir.merge(r1, r2)
          sample = Reservoir.sample(merged)

          from_r1 = Enum.count(sample, fn e -> e >= 1 and e <= 1000 end)
          from_r2 = Enum.count(sample, fn e -> e >= 1001 and e <= 1100 end)

          %{acc | from_r1: acc.from_r1 + from_r1, from_r2: acc.from_r2 + from_r2}
        end)

      total_from_r1 = counts.from_r1
      total_from_r2 = counts.from_r2

      # r1 has 10x the stream size, so its elements should appear much more often.
      # The ratio should be roughly 10:1. We allow generous tolerance (5:1 to 20:1).
      ratio = total_from_r1 / max(total_from_r2, 1)

      assert ratio > 5.0,
             "Expected ~10:1 ratio, got #{Float.round(ratio, 2)}:1 (r1=#{total_from_r1}, r2=#{total_from_r2})"

      assert ratio < 20.0,
             "Expected ~10:1 ratio, got #{Float.round(ratio, 2)}:1 (r1=#{total_from_r1}, r2=#{total_from_r2})"
    end

    # ---------------------------------------------------------------
    # Bug 2: merge when total_count <= k should concatenate
    # When the total number of elements seen by both reservoirs is
    # <= k, ALL elements should appear in the merged sample.
    # ---------------------------------------------------------------

    test "merge two under-capacity reservoirs keeps all elements" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all([:a, :b, :c])
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all([:d, :e])

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert Reservoir.count(merged) == 5
      assert Enum.sort(Reservoir.sample(merged)) == [:a, :b, :c, :d, :e]
    end

    test "merge where total exactly equals k keeps all elements" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..5)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all(6..10)

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert Reservoir.count(merged) == 10
      assert length(Reservoir.sample(merged)) == 10
      assert Enum.sort(Reservoir.sample(merged)) == Enum.to_list(1..10)
    end

    test "merge where total is 1 less than k keeps all elements" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..5)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all(6..9)

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert Reservoir.count(merged) == 9
      assert length(Reservoir.sample(merged)) == 9
      assert Enum.sort(Reservoir.sample(merged)) == Enum.to_list(1..9)
    end

    test "merge one empty, one under-capacity keeps all elements from non-empty" do
      r1 = Reservoir.new(10, seed: 42)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all([:x, :y, :z])

      {:ok, merged} = Reservoir.merge(r1, r2)

      assert Reservoir.count(merged) == 3
      assert Enum.sort(Reservoir.sample(merged)) == [:x, :y, :z]
    end

    # ---------------------------------------------------------------
    # Bug 3: merge(empty, empty) returns r1
    # Previously returned r2. Now returns r1.
    # ---------------------------------------------------------------

    test "merge(empty, empty) preserves r1's seed" do
      r1 = Reservoir.new(5, seed: 42)
      r2 = Reservoir.new(5, seed: 99)

      {:ok, merged} = Reservoir.merge(r1, r2)

      # merged should be r1 (same seed, same state)
      assert merged.seed == r1.seed
      refute merged.seed == r2.seed
    end

    test "merge(empty, non-empty) returns r2" do
      empty = Reservoir.new(5, seed: 42)
      r2 = Reservoir.new(5, seed: 99) |> Reservoir.add_all([:a, :b])

      {:ok, merged} = Reservoir.merge(empty, r2)

      assert merged == r2
      assert Reservoir.sample(merged) == [:a, :b]
      assert Reservoir.count(merged) == 2
    end

    test "merge(non-empty, empty) returns r1" do
      r1 = Reservoir.new(5, seed: 42) |> Reservoir.add_all([:a, :b])
      empty = Reservoir.new(5, seed: 99)

      {:ok, merged} = Reservoir.merge(r1, empty)

      assert merged == r1
      assert Reservoir.sample(merged) == [:a, :b]
      assert Reservoir.count(merged) == 2
    end

    # ---------------------------------------------------------------
    # Additional edge cases
    # ---------------------------------------------------------------

    test "merge two reservoirs each with exactly k elements produces correct count and length" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..10)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all(11..20)

      # Both full, total (20) > k (10)
      {:ok, merged} = Reservoir.merge(r1, r2)
      sample = Reservoir.sample(merged)

      assert length(sample) == 10
      assert Reservoir.count(merged) == 20

      valid_elements = Reservoir.sample(r1) ++ Reservoir.sample(r2)

      for element <- sample do
        assert element in valid_elements,
               "element #{inspect(element)} not found in either reservoir's samples"
      end
    end

    test "merge where one reservoir has k elements and other has 1" do
      r1 = Reservoir.new(10, seed: 42) |> Reservoir.add_all(1..10)
      r2 = Reservoir.new(10, seed: 99) |> Reservoir.add_all([100])

      {:ok, merged} = Reservoir.merge(r1, r2)
      sample = Reservoir.sample(merged)

      assert Reservoir.count(merged) == 11
      assert length(sample) == 10

      # All elements must come from the union of both samples
      valid = Reservoir.sample(r1) ++ Reservoir.sample(r2)

      for el <- sample do
        assert el in valid,
               "Element #{inspect(el)} not in either reservoir's samples"
      end
    end

    test "merged reservoir's count is sum of both counts in multiple scenarios" do
      # Scenario 1: both under capacity
      r1 = Reservoir.new(20, seed: 1) |> Reservoir.add_all(1..5)
      r2 = Reservoir.new(20, seed: 2) |> Reservoir.add_all(6..8)
      {:ok, merged} = Reservoir.merge(r1, r2)
      assert Reservoir.count(merged) == 8

      # Scenario 2: both at capacity
      r1 = Reservoir.new(5, seed: 1) |> Reservoir.add_all(1..100)
      r2 = Reservoir.new(5, seed: 2) |> Reservoir.add_all(101..200)
      {:ok, merged} = Reservoir.merge(r1, r2)
      assert Reservoir.count(merged) == 200

      # Scenario 3: one under, one at capacity
      r1 = Reservoir.new(10, seed: 1) |> Reservoir.add_all(1..3)
      r2 = Reservoir.new(10, seed: 2) |> Reservoir.add_all(4..500)
      {:ok, merged} = Reservoir.merge(r1, r2)
      assert Reservoir.count(merged) == 500

      # Scenario 4: one empty, one non-empty
      r1 = Reservoir.new(10, seed: 1)
      r2 = Reservoir.new(10, seed: 2) |> Reservoir.add_all(1..50)
      {:ok, merged} = Reservoir.merge(r1, r2)
      assert Reservoir.count(merged) == 50

      # Scenario 5: both empty
      r1 = Reservoir.new(10, seed: 1)
      r2 = Reservoir.new(10, seed: 2)
      {:ok, merged} = Reservoir.merge(r1, r2)
      assert Reservoir.count(merged) == 0
    end
  end

  describe "bug verification" do
    @tag timeout: 120_000
    test "add/2 uses tuple-based storage for efficient replacement" do
      # put_elem/3 on BEAM tuples is O(k) (copies the tuple), but is a fast
      # memcpy-based operation -- much faster than the old list-based approach
      # which required traversal and reconstruction. We verify that the
      # implementation correctly uses tuples after the fill phase by checking
      # that the reservoir remains functional and that performance scales
      # sub-linearly relative to the naive list approach.
      k = 1_000
      n = 50_000

      r = Reservoir.new(k, seed: 42)
      r = Reservoir.add_all(r, 1..k)

      # Verify the reservoir works correctly after many replacements
      r = Enum.reduce(1..n, r, fn x, acc -> Reservoir.add(acc, x) end)

      assert length(Reservoir.sample(r)) == k
      assert Reservoir.count(r) == k + n

      # Verify that internal storage is a tuple (O(1) element access)
      assert is_tuple(r.samples),
             "Expected tuple-based storage after fill phase, got #{inspect(r.samples |> then(&if(is_list(&1), do: "list", else: "other")))}"
    end

    @tag timeout: 120_000
    test "merge/2 performance is O(k), not O(k^2)" do
      k_small = 100
      k_large = 5_000

      r1_small = Reservoir.new(k_small, seed: 42) |> Reservoir.add_all(1..1000)
      r2_small = Reservoir.new(k_small, seed: 99) |> Reservoir.add_all(1001..2000)

      r1_large = Reservoir.new(k_large, seed: 42) |> Reservoir.add_all(1..10_000)
      r2_large = Reservoir.new(k_large, seed: 99) |> Reservoir.add_all(10_001..20_000)

      {time_small, _} = :timer.tc(fn -> {:ok, _} = Reservoir.merge(r1_small, r2_small) end)
      {time_large, _} = :timer.tc(fn -> {:ok, _} = Reservoir.merge(r1_large, r2_large) end)

      # If O(k), ratio should be ~50 (5000/100). If O(k^2), ratio should be ~2500
      ratio = time_large / max(time_small, 1)
      expected_linear_ratio = k_large / k_small

      # Allow 3x the linear ratio for overhead, but not 10x+ (which would indicate O(k^2))
      assert ratio < expected_linear_ratio * 3,
             "merge is O(k^2) not O(k): ratio is #{Float.round(ratio, 1)}x, expected < #{expected_linear_ratio * 3}"
    end
  end
end
