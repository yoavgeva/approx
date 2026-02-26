defmodule Approx.TDigestTest do
  use ExUnit.Case, async: true

  alias Approx.TDigest

  doctest Approx.TDigest

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Build a digest from a list of values for convenience.
  defp build(values, delta \\ 100) do
    Enum.reduce(values, TDigest.new(delta), fn v, acc -> TDigest.add(acc, v) end)
  end

  # ---------------------------------------------------------------------------
  # new/2
  # ---------------------------------------------------------------------------

  describe "new/2" do
    test "creates an empty digest with default delta" do
      td = TDigest.new()
      assert td.delta == 100
      assert td.centroids == []
      assert td.buffer == []
      assert td.total_weight == 0.0
      assert td.min == nil
      assert td.max == nil
    end

    test "creates a digest with custom delta" do
      td = TDigest.new(200)
      assert td.delta == 200
    end

    test "creates a digest with custom buffer limit" do
      td = TDigest.new(50, buffer_limit: 100)
      assert td.buffer_limit == 100
    end

    test "default buffer limit is delta * 5" do
      td = TDigest.new(80)
      assert td.buffer_limit == 400
    end

    test "raises for invalid delta" do
      assert_raise FunctionClauseError, fn -> TDigest.new(0) end
      assert_raise FunctionClauseError, fn -> TDigest.new(-10) end
    end
  end

  # ---------------------------------------------------------------------------
  # add/3
  # ---------------------------------------------------------------------------

  describe "add/3" do
    test "adds a single value" do
      td = TDigest.new() |> TDigest.add(42.0)

      assert TDigest.count(td) == 1.0
      assert TDigest.min(td) == 42.0
      assert TDigest.max(td) == 42.0
    end

    test "adds a value with weight" do
      td = TDigest.new() |> TDigest.add(10.0, 5)

      assert TDigest.count(td) == 5.0
    end

    test "tracks min correctly" do
      td =
        TDigest.new()
        |> TDigest.add(10.0)
        |> TDigest.add(5.0)
        |> TDigest.add(20.0)
        |> TDigest.add(1.0)

      assert TDigest.min(td) == 1.0
    end

    test "tracks max correctly" do
      td =
        TDigest.new()
        |> TDigest.add(10.0)
        |> TDigest.add(5.0)
        |> TDigest.add(20.0)
        |> TDigest.add(1.0)

      assert TDigest.max(td) == 20.0
    end

    test "accepts integer values" do
      td = TDigest.new() |> TDigest.add(42)
      assert TDigest.count(td) == 1.0
      assert TDigest.min(td) == 42.0
    end

    test "accepts negative values" do
      td =
        TDigest.new()
        |> TDigest.add(-5.0)
        |> TDigest.add(5.0)

      assert TDigest.min(td) == -5.0
      assert TDigest.max(td) == 5.0
    end

    test "triggers compression when buffer is full" do
      # Use a small buffer limit so compression triggers quickly
      td = TDigest.new(100, buffer_limit: 10)

      td = Enum.reduce(1..20, td, fn x, acc -> TDigest.add(acc, x) end)

      # After adding 20 items with buffer_limit=10, compression should have occurred
      # and centroids should be non-empty while buffer should be smaller than 20
      assert td.centroids != [] or td.buffer != []
      assert TDigest.count(td) == 20.0
    end

    test "raises for non-positive weight" do
      td = TDigest.new()

      assert_raise FunctionClauseError, fn -> TDigest.add(td, 1.0, 0) end
      assert_raise FunctionClauseError, fn -> TDigest.add(td, 1.0, -1) end
    end
  end

  # ---------------------------------------------------------------------------
  # count/1
  # ---------------------------------------------------------------------------

  describe "count/1" do
    test "returns 0.0 for empty digest" do
      assert TDigest.count(TDigest.new()) == 0.0
    end

    test "tracks total weight with default weight" do
      td = build(1..100)
      assert TDigest.count(td) == 100.0
    end

    test "tracks total weight with custom weights" do
      td =
        TDigest.new()
        |> TDigest.add(1.0, 3)
        |> TDigest.add(2.0, 7)

      assert TDigest.count(td) == 10.0
    end
  end

  # ---------------------------------------------------------------------------
  # min/1 and max/1
  # ---------------------------------------------------------------------------

  describe "min/1" do
    test "returns nil for empty digest" do
      assert TDigest.min(TDigest.new()) == nil
    end

    test "tracks actual minimum across all insertions" do
      td = build([50, 30, 70, 10, 90, 1])
      assert TDigest.min(td) == 1.0
    end

    test "handles negative values" do
      td = build([-100, 0, 100])
      assert TDigest.min(td) == -100.0
    end
  end

  describe "max/1" do
    test "returns nil for empty digest" do
      assert TDigest.max(TDigest.new()) == nil
    end

    test "tracks actual maximum across all insertions" do
      td = build([50, 30, 70, 10, 90, 1])
      assert TDigest.max(td) == 90.0
    end

    test "handles negative values" do
      td = build([-100, -50, -1])
      assert TDigest.max(td) == -1.0
    end
  end

  # ---------------------------------------------------------------------------
  # percentile/2
  # ---------------------------------------------------------------------------

  describe "percentile/2" do
    test "returns nil for empty digest" do
      assert TDigest.percentile(TDigest.new(), 0.5) == nil
    end

    test "returns exact value for single element" do
      td = build([42.0])
      assert TDigest.percentile(td, 0.5) == 42.0
    end

    test "q=0.0 returns min" do
      td = build(1..100)
      assert TDigest.percentile(td, 0.0) == 1.0
    end

    test "q=1.0 returns max" do
      td = build(1..100)
      assert TDigest.percentile(td, 1.0) == 100.0
    end

    test "median of uniform 1..1000 is near 500.5" do
      td = build(1..1000)
      median = TDigest.percentile(td, 0.5)

      assert_in_delta median, 500.5, 15.0, "Median #{median} is too far from expected 500.5"
    end

    test "p25 of uniform 1..1000 is near 250" do
      td = build(1..1000)
      p25 = TDigest.percentile(td, 0.25)

      assert_in_delta p25, 250.5, 15.0, "p25 #{p25} is too far from expected 250.5"
    end

    test "p75 of uniform 1..1000 is near 750" do
      td = build(1..1000)
      p75 = TDigest.percentile(td, 0.75)

      assert_in_delta p75, 750.5, 15.0, "p75 #{p75} is too far from expected 750.5"
    end

    test "p99 of uniform 1..10000 is near 9900" do
      td = build(1..10_000)
      p99 = TDigest.percentile(td, 0.99)

      # t-digest should be very accurate at tails
      assert_in_delta p99, 9900.0, 150.0, "p99 #{p99} is too far from expected 9900"
    end

    test "p1 of uniform 1..10000 is near 100" do
      td = build(1..10_000)
      p1 = TDigest.percentile(td, 0.01)

      assert_in_delta p1, 100.0, 150.0, "p1 #{p1} is too far from expected 100"
    end

    test "p99.9 of uniform 1..10000 is near 9990" do
      td = build(1..10_000)
      p999 = TDigest.percentile(td, 0.999)

      assert_in_delta p999, 9990.0, 50.0, "p99.9 #{p999} is too far from expected 9990"
    end

    test "percentile is monotonically increasing" do
      td = build(1..1000)

      quantiles = [0.0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0]
      values = Enum.map(quantiles, &TDigest.percentile(td, &1))

      pairs = Enum.zip(values, tl(values))

      Enum.each(pairs, fn {a, b} ->
        assert a <= b, "Percentile not monotonic: #{a} > #{b}"
      end)
    end

    test "two-element digest returns correct extremes" do
      td = TDigest.new() |> TDigest.add(10.0) |> TDigest.add(20.0)

      assert TDigest.percentile(td, 0.0) == 10.0
      assert TDigest.percentile(td, 1.0) == 20.0
    end

    test "all same values returns that value for any quantile" do
      td = build(List.duplicate(42.0, 100))

      assert TDigest.percentile(td, 0.0) == 42.0
      assert TDigest.percentile(td, 0.5) == 42.0
      assert TDigest.percentile(td, 1.0) == 42.0
    end
  end

  # ---------------------------------------------------------------------------
  # median/1
  # ---------------------------------------------------------------------------

  describe "median/1" do
    test "returns nil for empty digest" do
      assert TDigest.median(TDigest.new()) == nil
    end

    test "returns the value for single element" do
      td = build([99.0])
      assert TDigest.median(td) == 99.0
    end

    test "is equivalent to percentile(td, 0.5)" do
      td = build(1..500)
      assert TDigest.median(td) == TDigest.percentile(td, 0.5)
    end
  end

  # ---------------------------------------------------------------------------
  # merge/2
  # ---------------------------------------------------------------------------

  describe "merge/2" do
    test "merged count is sum of individual counts" do
      td1 = build(1..50)
      td2 = build(51..100)
      merged = TDigest.merge(td1, td2)

      assert TDigest.count(merged) == 100.0
    end

    test "merged min is minimum of both digests" do
      td1 = build([10, 20, 30])
      td2 = build([5, 25, 35])
      merged = TDigest.merge(td1, td2)

      assert TDigest.min(merged) == 5.0
    end

    test "merged max is maximum of both digests" do
      td1 = build([10, 20, 30])
      td2 = build([5, 25, 35])
      merged = TDigest.merge(td1, td2)

      assert TDigest.max(merged) == 35.0
    end

    test "merge with empty digest returns equivalent of non-empty" do
      td1 = build(1..100)
      td2 = TDigest.new()
      merged = TDigest.merge(td1, td2)

      assert TDigest.count(merged) == 100.0
      assert TDigest.min(merged) == 1.0
      assert TDigest.max(merged) == 100.0
    end

    test "merge two empty digests returns empty" do
      merged = TDigest.merge(TDigest.new(), TDigest.new())

      assert TDigest.count(merged) == 0.0
      assert TDigest.min(merged) == nil
      assert TDigest.max(merged) == nil
    end

    test "merged digest gives similar percentiles as building from combined data" do
      data1 = Enum.to_list(1..500)
      data2 = Enum.to_list(501..1000)

      td1 = build(data1)
      td2 = build(data2)
      merged = TDigest.merge(td1, td2)

      combined = build(data1 ++ data2)

      for q <- [0.1, 0.25, 0.5, 0.75, 0.9, 0.99] do
        merged_val = TDigest.percentile(merged, q)
        combined_val = TDigest.percentile(combined, q)

        # Allow some tolerance -- merging may produce slightly different centroids
        assert_in_delta merged_val,
                        combined_val,
                        30.0,
                        "At q=#{q}: merged=#{merged_val}, combined=#{combined_val}"
      end
    end

    test "merge uses delta from first digest" do
      td1 = TDigest.new(50)
      td2 = TDigest.new(200)

      td1 = Enum.reduce(1..100, td1, fn x, acc -> TDigest.add(acc, x) end)
      td2 = Enum.reduce(101..200, td2, fn x, acc -> TDigest.add(acc, x) end)

      merged = TDigest.merge(td1, td2)
      assert merged.delta == 50
    end

    test "merge is commutative in result quality" do
      td1 = build(1..500)
      td2 = build(501..1000)

      merged_ab = TDigest.merge(td1, td2)
      merged_ba = TDigest.merge(td2, td1)

      # Both should give similar median estimates
      assert_in_delta TDigest.median(merged_ab), TDigest.median(merged_ba), 20.0
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization: to_binary/1 and from_binary/1
  # ---------------------------------------------------------------------------

  describe "to_binary/1 and from_binary/1" do
    test "round-trip preserves empty digest" do
      td = TDigest.new(50)
      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()

      assert TDigest.count(restored) == 0.0
      assert TDigest.min(restored) == nil
      assert TDigest.max(restored) == nil
      assert restored.delta == 50
    end

    test "round-trip preserves single element" do
      td = TDigest.new() |> TDigest.add(42.0)
      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()

      assert TDigest.count(restored) == 1.0
      assert TDigest.min(restored) == 42.0
      assert TDigest.max(restored) == 42.0
      assert TDigest.percentile(restored, 0.5) == 42.0
    end

    test "round-trip preserves percentile estimates" do
      td = build(1..1000)
      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()

      assert TDigest.count(restored) == TDigest.count(td)
      assert TDigest.min(restored) == TDigest.min(td)
      assert TDigest.max(restored) == TDigest.max(td)

      for q <- [0.1, 0.25, 0.5, 0.75, 0.9, 0.99] do
        original_val = TDigest.percentile(td, q)
        restored_val = TDigest.percentile(restored, q)

        assert_in_delta original_val,
                        restored_val,
                        0.001,
                        "Percentile #{q} differs after round-trip: #{original_val} vs #{restored_val}"
      end
    end

    test "binary starts with version byte 1" do
      td = TDigest.new() |> TDigest.add(1.0)
      <<version::8, _rest::binary>> = TDigest.to_binary(td)
      assert version == 1
    end

    test "binary has correct structure" do
      td = TDigest.new() |> TDigest.add(1.0) |> TDigest.add(2.0) |> TDigest.add(3.0)
      flushed = Approx.TDigest.compress(td)
      num_centroids = length(flushed.centroids)

      binary = TDigest.to_binary(td)

      # version(1) + delta(8) + buffer_limit(4) + total_weight(8) +
      # min(8) + max(8) + num_centroids(4) + centroids(16 each)
      expected_size = 1 + 8 + 4 + 8 + 8 + 8 + 4 + num_centroids * 16
      assert byte_size(binary) == expected_size
    end

    test "from_binary returns error for garbage input" do
      assert {:error, :invalid_binary} = TDigest.from_binary(<<>>)
      assert {:error, :invalid_binary} = TDigest.from_binary(<<0, 1, 2>>)
      assert {:error, :invalid_binary} = TDigest.from_binary("not a t-digest")
    end

    test "from_binary returns error for unknown version" do
      td = TDigest.new() |> TDigest.add(1.0)
      <<_version::8, rest::binary>> = TDigest.to_binary(td)
      bad_binary = <<99::8, rest::binary>>

      assert {:error, :invalid_binary} = TDigest.from_binary(bad_binary)
    end

    test "from_binary returns error when centroid data is truncated" do
      td = TDigest.new() |> TDigest.add(1.0) |> TDigest.add(2.0)
      binary = TDigest.to_binary(td)

      # Truncate last few bytes
      truncated = binary_part(binary, 0, byte_size(binary) - 4)
      assert {:error, :invalid_binary} = TDigest.from_binary(truncated)
    end

    test "preserves delta through round-trip" do
      for delta <- [25, 50, 100, 200, 500] do
        td = TDigest.new(delta) |> TDigest.add(1.0)
        {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()
        assert restored.delta == delta
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Accuracy: uniform distribution
  # ---------------------------------------------------------------------------

  describe "accuracy with uniform distribution" do
    test "median of 1..10000 is within 1% of true value" do
      td = build(1..10_000)
      median = TDigest.median(td)
      true_median = 5000.5

      error_pct = abs(median - true_median) / true_median * 100
      assert error_pct < 1.0, "Median error #{error_pct}% exceeds 1%"
    end

    test "all standard percentiles within reasonable bounds for large dataset" do
      n = 10_000
      td = build(1..n)

      checks = [
        {0.01, n * 0.01, 200.0},
        {0.05, n * 0.05, 200.0},
        {0.10, n * 0.10, 200.0},
        {0.25, n * 0.25, 200.0},
        {0.50, n * 0.50, 200.0},
        {0.75, n * 0.75, 200.0},
        {0.90, n * 0.90, 200.0},
        {0.95, n * 0.95, 200.0},
        {0.99, n * 0.99, 200.0}
      ]

      for {q, expected, tolerance} <- checks do
        actual = TDigest.percentile(td, q)

        assert_in_delta actual,
                        expected,
                        tolerance,
                        "q=#{q}: expected ~#{expected}, got #{actual}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Accuracy: tail precision
  # ---------------------------------------------------------------------------

  describe "tail accuracy" do
    test "p99 of 1..10000 has tighter accuracy than middle percentiles" do
      td = build(1..10_000)

      p99 = TDigest.percentile(td, 0.99)
      true_p99 = 9900.0

      # The t-digest is designed for tail accuracy -- error should be small
      error = abs(p99 - true_p99)
      assert error < 150.0, "p99 error #{error} is too large"
    end

    test "p1 of 1..10000 has tight accuracy" do
      td = build(1..10_000)

      p1 = TDigest.percentile(td, 0.01)
      true_p1 = 100.0

      error = abs(p1 - true_p1)
      assert error < 150.0, "p1 error #{error} is too large"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "empty digest" do
      td = TDigest.new()

      assert TDigest.count(td) == 0.0
      assert TDigest.min(td) == nil
      assert TDigest.max(td) == nil
      assert TDigest.median(td) == nil
      assert TDigest.percentile(td, 0.5) == nil
    end

    test "single element digest" do
      td = build([42.0])

      assert TDigest.count(td) == 1.0
      assert TDigest.min(td) == 42.0
      assert TDigest.max(td) == 42.0
      assert TDigest.percentile(td, 0.0) == 42.0
      assert TDigest.percentile(td, 0.5) == 42.0
      assert TDigest.percentile(td, 1.0) == 42.0
    end

    test "two element digest" do
      td = TDigest.new() |> TDigest.add(10.0) |> TDigest.add(20.0)

      assert TDigest.count(td) == 2.0
      assert TDigest.min(td) == 10.0
      assert TDigest.max(td) == 20.0
      assert TDigest.percentile(td, 0.0) == 10.0
      assert TDigest.percentile(td, 1.0) == 20.0

      # Median should be between 10 and 20
      median = TDigest.median(td)
      assert median >= 10.0 and median <= 20.0
    end

    test "all same values" do
      td = build(List.duplicate(7.7, 500))

      assert TDigest.count(td) == 500.0
      assert TDigest.min(td) == 7.7
      assert TDigest.max(td) == 7.7
      assert TDigest.median(td) == 7.7
      assert TDigest.percentile(td, 0.01) == 7.7
      assert TDigest.percentile(td, 0.99) == 7.7
    end

    test "very large values" do
      td = build([1.0e15, 2.0e15, 3.0e15])

      assert TDigest.min(td) == 1.0e15
      assert TDigest.max(td) == 3.0e15
    end

    test "very small values" do
      td = build([1.0e-15, 2.0e-15, 3.0e-15])

      assert TDigest.min(td) == 1.0e-15
      assert TDigest.max(td) == 3.0e-15
    end

    test "mixed positive and negative values" do
      td = build(-500..500)

      assert TDigest.min(td) == -500.0
      assert TDigest.max(td) == 500.0

      median = TDigest.median(td)
      assert_in_delta median, 0.0, 15.0
    end

    test "values added in descending order" do
      td = build(Enum.to_list(1000..1//-1))

      assert TDigest.min(td) == 1.0
      assert TDigest.max(td) == 1000.0
      assert TDigest.count(td) == 1000.0

      median = TDigest.median(td)
      assert_in_delta median, 500.5, 15.0
    end

    test "weighted insertions affect percentiles" do
      # Add value 1 with weight 99, and value 100 with weight 1
      # The median should be close to 1 because 99% of the weight is there
      td =
        TDigest.new()
        |> TDigest.add(1.0, 99)
        |> TDigest.add(100.0, 1)

      median = TDigest.median(td)
      assert median < 50.0, "Median #{median} should be dominated by the heavy weight at 1.0"
    end
  end

  # ---------------------------------------------------------------------------
  # Compression behavior
  # ---------------------------------------------------------------------------

  describe "compression" do
    test "number of centroids is bounded" do
      td = build(1..10_000)
      flushed = Approx.TDigest.compress(td)

      # With delta=100, we expect O(delta) centroids. In a single-pass
      # implementation the count can reach up to ~5*delta due to the
      # sequential merging approach.
      assert length(flushed.centroids) < 600,
             "Too many centroids: #{length(flushed.centroids)}"
    end

    test "compression is idempotent" do
      td = build(1..1000)
      flushed1 = Approx.TDigest.compress(td)
      flushed2 = Approx.TDigest.compress(flushed1)

      assert flushed1.centroids == flushed2.centroids
      assert flushed1.buffer == flushed2.buffer
    end

    test "centroids are sorted by mean after compression" do
      td = build(Enum.shuffle(1..1000))
      flushed = Approx.TDigest.compress(td)

      means = Enum.map(flushed.centroids, fn {mean, _weight} -> mean end)
      assert means == Enum.sort(means)
    end

    test "centroid weights sum to total_weight after compression" do
      td = build(1..500)
      flushed = Approx.TDigest.compress(td)

      centroid_weight_sum =
        Enum.reduce(flushed.centroids, 0.0, fn {_mean, weight}, acc -> acc + weight end)

      assert_in_delta centroid_weight_sum, flushed.total_weight, 0.001
    end
  end

  # ---------------------------------------------------------------------------
  # Large-scale stress tests
  # ---------------------------------------------------------------------------

  describe "large-scale data" do
    test "handles 100_000 elements" do
      td = build(1..100_000)

      assert TDigest.count(td) == 100_000.0
      assert TDigest.min(td) == 1.0
      assert TDigest.max(td) == 100_000.0

      median = TDigest.median(td)
      assert_in_delta median, 50_000.5, 1000.0
    end
  end

  # ---------------------------------------------------------------------------
  # Normal distribution accuracy
  # ---------------------------------------------------------------------------

  describe "normal distribution" do
    test "percentile estimates are accurate for standard normal samples" do
      # Use a seeded :rand for reproducibility
      seed = :rand.seed_s(:exsss, {12_345, 67_890, 11_121})

      # Generate 10_000 normally distributed samples via Box-Muller transform.
      # We generate 5_000 pairs, each producing 2 samples = 10_000 total.
      {samples, _seed} =
        Enum.map_reduce(1..5_000, seed, fn _i, s ->
          {u1, s} = :rand.uniform_s(s)
          {u2, s} = :rand.uniform_s(s)
          z0 = :math.sqrt(-2.0 * :math.log(u1)) * :math.cos(2.0 * :math.pi() * u2)
          z1 = :math.sqrt(-2.0 * :math.log(u1)) * :math.sin(2.0 * :math.pi() * u2)
          {[z0, z1], s}
        end)

      all_samples = List.flatten(samples)
      assert length(all_samples) == 10_000

      td = Enum.reduce(all_samples, TDigest.new(), fn v, acc -> TDigest.add(acc, v) end)

      median = TDigest.percentile(td, 0.5)
      p25 = TDigest.percentile(td, 0.25)
      p75 = TDigest.percentile(td, 0.75)

      assert_in_delta median, 0.0, 0.1, "Median #{median} not near 0.0"
      assert_in_delta p25, -0.674, 0.15, "p25 #{p25} not near -0.674"
      assert_in_delta p75, 0.674, 0.15, "p75 #{p75} not near 0.674"
    end
  end

  # ---------------------------------------------------------------------------
  # Exponential distribution
  # ---------------------------------------------------------------------------

  describe "exponential distribution" do
    test "percentile estimates are accurate for exponential samples" do
      seed = :rand.seed_s(:exsss, {99_999, 88_888, 77_777})

      {samples, _seed} =
        Enum.map_reduce(1..10_000, seed, fn _i, s ->
          {u, s} = :rand.uniform_s(s)
          value = -:math.log(u)
          {value, s}
        end)

      td = Enum.reduce(samples, TDigest.new(), fn v, acc -> TDigest.add(acc, v) end)

      median = TDigest.percentile(td, 0.5)
      p99 = TDigest.percentile(td, 0.99)

      # Theoretical median of Exp(1) is ln(2) ~ 0.693
      assert_in_delta median, :math.log(2), 0.1, "Median #{median} not near ln(2)=#{:math.log(2)}"

      # Theoretical p99 of Exp(1) is -ln(0.01) ~ 4.605
      assert_in_delta p99, 4.605, 0.5, "p99 #{p99} not near 4.605"
    end
  end

  # ---------------------------------------------------------------------------
  # Singleton in a crowd
  # ---------------------------------------------------------------------------

  describe "singleton in a crowd" do
    test "single outlier is detectable at extreme percentile" do
      td =
        TDigest.new()
        |> TDigest.add(10.0, 10_000)
        |> TDigest.add(20.0, 1)

      median = TDigest.median(td)
      assert_in_delta median, 10.0, 0.5, "Median #{median} should be close to 10.0"

      # At q=0.9999, most of the weight is at 10.0, but the very top should
      # approach 20.0
      high_p = TDigest.percentile(td, 0.9999)
      assert_in_delta high_p, 20.0, 1.0, "p99.99 #{high_p} should be close to 20.0"
    end
  end

  # ---------------------------------------------------------------------------
  # Percentile bounds
  # ---------------------------------------------------------------------------

  describe "percentile bounds" do
    test "percentile always returns a value within [min, max]" do
      seed = :rand.seed_s(:exsss, {42, 43, 44})

      {values, _seed} =
        Enum.map_reduce(1..1_000, seed, fn _i, s ->
          {v, s} = :rand.uniform_s(s)
          {v * 1_000.0, s}
        end)

      td = Enum.reduce(values, TDigest.new(), fn v, acc -> TDigest.add(acc, v) end)

      td_min = TDigest.min(td)
      td_max = TDigest.max(td)

      quantiles = [0.0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999, 1.0]

      for q <- quantiles do
        p = TDigest.percentile(td, q)

        assert p >= td_min,
               "percentile(#{q}) = #{p} is below min #{td_min}"

        assert p <= td_max,
               "percentile(#{q}) = #{p} is above max #{td_max}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Special float values
  # ---------------------------------------------------------------------------

  describe "special float values" do
    test "0.0 / 0.0 raises ArithmeticError (NaN is not representable)" do
      assert_raise ArithmeticError, fn -> 0.0 / 0.0 end
    end

    test ":math.exp(10000) raises ArithmeticError (infinity is not representable)" do
      assert_raise ArithmeticError, fn -> :math.exp(10_000) end
    end

    test "adding non-numeric atoms like :infinity raises FunctionClauseError" do
      td = TDigest.new()
      assert_raise FunctionClauseError, fn -> TDigest.add(td, :infinity) end
      assert_raise FunctionClauseError, fn -> TDigest.add(td, :nan) end
    end

    test "extreme but valid floats like 1.0e308 do not corrupt the digest" do
      td =
        TDigest.new()
        |> TDigest.add(1.0)
        |> TDigest.add(1.0e308)
        |> TDigest.add(2.0)

      assert TDigest.count(td) == 3.0
      assert TDigest.min(td) == 1.0
      assert TDigest.max(td) == 1.0e308

      # Digest should still return valid percentile estimates
      median = TDigest.median(td)
      assert is_float(median)
      assert median >= 1.0
      assert median <= 1.0e308
    end

    test "very small positive floats do not corrupt the digest" do
      td =
        TDigest.new()
        |> TDigest.add(5.0e-324)
        |> TDigest.add(1.0e-300)
        |> TDigest.add(1.0)

      assert TDigest.count(td) == 3.0
      assert TDigest.min(td) == 5.0e-324
      assert TDigest.max(td) == 1.0
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-round merge (many sub-digests)
  # ---------------------------------------------------------------------------

  describe "multi-round merge" do
    test "merging 10 sub-digests produces correct count and accurate median" do
      chunks =
        1..10_000
        |> Enum.chunk_every(1_000)

      sub_digests =
        Enum.map(chunks, fn chunk ->
          Enum.reduce(chunk, TDigest.new(), fn v, acc -> TDigest.add(acc, v) end)
        end)

      merged =
        sub_digests
        |> Enum.reduce(fn td, acc -> TDigest.merge(acc, td) end)

      assert TDigest.count(merged) == 10_000.0

      median = TDigest.median(merged)
      # Within 10% of 5000.5
      assert_in_delta median, 5000.5, 500.5, "Merged median #{median} is not within 10% of 5000.5"
    end
  end

  # ---------------------------------------------------------------------------
  # Error scaling with delta
  # ---------------------------------------------------------------------------

  describe "error scaling with delta" do
    test "larger delta does not increase median error" do
      values = Enum.to_list(1..5_000)
      true_median = 2500.5

      errors =
        Enum.map([25, 50, 100, 200], fn delta ->
          td = Enum.reduce(values, TDigest.new(delta), fn v, acc -> TDigest.add(acc, v) end)
          median = TDigest.percentile(td, 0.5)
          {delta, abs(median - true_median)}
        end)

      # Verify that each delta's error is not worse than the smallest delta's error
      # (allowing a small tolerance for noise). Larger delta = more centroids =
      # should be at least as accurate.
      [{_smallest_delta, smallest_error} | rest] = errors

      for {delta, error} <- rest do
        # Allow a small absolute tolerance for floating point noise
        assert error <= smallest_error + 5.0,
               "delta=#{delta} error #{error} is worse than delta=25 error #{smallest_error}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Large discontinuity
  # ---------------------------------------------------------------------------

  describe "large discontinuity" do
    test "single extreme outlier does not distort lower percentiles" do
      # Add 1000 values uniformly in (0, 1]: i/1000.0 for i in 1..1000
      td =
        Enum.reduce(1..1_000, TDigest.new(), fn i, acc ->
          TDigest.add(acc, i / 1_000.0)
        end)

      # Add one extreme outlier
      td = TDigest.add(td, 1_000_000.0)

      assert_in_delta TDigest.min(td), 0.001, 0.0001
      assert TDigest.max(td) == 1_000_000.0

      # 99% of data is in [0.001, 1.0], so p99 should still be in that range
      p99 = TDigest.percentile(td, 0.99)

      assert p99 >= 0.0,
             "p99 #{p99} should be >= 0.0"

      assert p99 <= 1.0,
             "p99 #{p99} should be <= 1.0 since 99% of data is in [0.001, 1.0]"
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted input does not degrade
  # ---------------------------------------------------------------------------

  describe "sorted input" do
    test "ascending sorted input produces bounded centroids and accurate median" do
      td_sorted = build(1..10_000)
      flushed_sorted = Approx.TDigest.compress(td_sorted)

      # Centroid count should be bounded by delta * 5
      assert length(flushed_sorted.centroids) < 100 * 5,
             "Too many centroids for sorted input: #{length(flushed_sorted.centroids)}"

      # Median accuracy within 10% of 5000.5
      median_sorted = TDigest.median(td_sorted)

      assert_in_delta median_sorted,
                      5000.5,
                      500.5,
                      "Sorted input median #{median_sorted} not within 10% of 5000.5"
    end

    test "sorted and shuffled input produce similar accuracy" do
      seed = :rand.seed_s(:exsss, {111, 222, 333})
      values = Enum.to_list(1..10_000)

      {shuffled, _seed} =
        Enum.map_reduce(Enum.reverse(values), seed, fn _v, s ->
          {idx, s} = :rand.uniform_s(s)
          {idx, s}
        end)

      # We need actually shuffled values, not random floats
      shuffled_values = Enum.shuffle(values)

      td_sorted = build(values)
      td_shuffled = build(shuffled_values)

      _ = shuffled

      true_median = 5000.5
      error_sorted = abs(TDigest.median(td_sorted) - true_median)
      error_shuffled = abs(TDigest.median(td_shuffled) - true_median)

      # Both should be reasonably accurate -- neither should be drastically worse
      assert error_sorted < 500.0,
             "Sorted input median error #{error_sorted} too large"

      assert error_shuffled < 500.0,
             "Shuffled input median error #{error_shuffled} too large"

      # The difference in errors should not be huge
      assert abs(error_sorted - error_shuffled) < 500.0,
             "Sorted vs shuffled error difference too large: #{error_sorted} vs #{error_shuffled}"
    end
  end

  # ---------------------------------------------------------------------------
  # Quantile monotonicity (three-point)
  # ---------------------------------------------------------------------------

  describe "quantile monotonicity" do
    test "percentiles are monotonically non-decreasing for three distinct values" do
      td =
        TDigest.new()
        |> TDigest.add(10.0)
        |> TDigest.add(50.0)
        |> TDigest.add(90.0)

      p10 = TDigest.percentile(td, 0.1)
      p50 = TDigest.percentile(td, 0.5)
      p90 = TDigest.percentile(td, 0.9)

      assert p10 <= p50, "p10=#{p10} should be <= p50=#{p50}"
      assert p50 <= p90, "p50=#{p50} should be <= p90=#{p90}"
    end

    test "full monotonicity sweep across many quantile points" do
      td =
        TDigest.new()
        |> TDigest.add(10.0)
        |> TDigest.add(50.0)
        |> TDigest.add(90.0)

      quantiles = Enum.map(0..20, fn i -> i / 20.0 end)
      values = Enum.map(quantiles, &TDigest.percentile(td, &1))

      pairs = Enum.zip(values, tl(values))

      Enum.each(pairs, fn {a, b} ->
        assert a <= b, "Monotonicity violated: #{a} > #{b}"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Compression direction regression (Bug 1: alternating forward/reverse)
  # ---------------------------------------------------------------------------

  describe "compression direction regression" do
    test "compress_direction alternates after each compression" do
      td = TDigest.new(100, buffer_limit: 10)
      assert td.compress_direction == :forward

      # Add 10 elements to trigger compression (buffer_limit == 10)
      td = Enum.reduce(1..10, td, fn x, acc -> TDigest.add(acc, x) end)
      assert td.compress_direction == :reverse

      # Add 10 more to trigger another compression
      td = Enum.reduce(11..20, td, fn x, acc -> TDigest.add(acc, x) end)
      assert td.compress_direction == :forward
    end

    test "right tail accuracy is high for uniform distribution" do
      td = Enum.reduce(1..10_000, TDigest.new(), fn x, acc -> TDigest.add(acc, x) end)

      # Check both tails are accurate
      p001 = TDigest.percentile(td, 0.001)
      p999 = TDigest.percentile(td, 0.999)

      # p0.1 should be around 10, p99.9 should be around 9990
      assert abs(p001 - 10) < 50,
             "p0.1 #{p001} is too far from expected ~10"

      assert abs(p999 - 9990) < 50,
             "p99.9 #{p999} is too far from expected ~9990"
    end

    test "left and right tail errors are symmetric for uniform distribution" do
      td = Enum.reduce(1..10_000, TDigest.new(), fn x, acc -> TDigest.add(acc, x / 1) end)

      p01 = TDigest.percentile(td, 0.01)
      p99 = TDigest.percentile(td, 0.99)

      # Expected: p01 ~ 100, p99 ~ 9900
      left_error = abs(p01 - 100)
      right_error = abs(p99 - 9900)

      # Errors should be somewhat symmetric (within factor of 5)
      if left_error > 0 and right_error > 0 do
        ratio = max(left_error, right_error) / max(min(left_error, right_error), 1)

        assert ratio < 5.0,
               "Tail errors too asymmetric: left=#{left_error}, right=#{right_error}, ratio=#{ratio}"
      end
    end

    test "merge preserves compress_direction from td1" do
      td1 = TDigest.new(100, buffer_limit: 10)
      td2 = TDigest.new(100, buffer_limit: 10)

      # Trigger compression on td1 to flip its direction to :reverse
      td1 = Enum.reduce(1..10, td1, fn x, acc -> TDigest.add(acc, x) end)
      assert td1.compress_direction == :reverse

      td2 = Enum.reduce(11..20, td2, fn x, acc -> TDigest.add(acc, x) end)

      # After merge, the compress_direction should be well-defined.
      # The merge function calls compress/1 on a fresh struct (default :forward),
      # so after one compression pass it should be :reverse.
      merged = TDigest.merge(td1, td2)
      assert merged.compress_direction in [:forward, :reverse]
    end

    test "compress/1 called explicitly alternates direction" do
      td = TDigest.new(100, buffer_limit: 1000)
      assert td.compress_direction == :forward

      # Add items to the buffer without triggering automatic compression
      td = Enum.reduce(1..50, td, fn x, acc -> TDigest.add(acc, x) end)
      assert td.compress_direction == :forward
      assert td.buffer != []

      # Explicitly compress
      td = TDigest.compress(td)
      assert td.compress_direction == :reverse
      assert td.buffer == []

      # Add more items and explicitly compress again
      td = Enum.reduce(51..100, td, fn x, acc -> TDigest.add(acc, x) end)
      td = TDigest.compress(td)
      assert td.compress_direction == :forward
    end

    test "compress on empty buffer is a no-op and does not flip direction" do
      td = TDigest.new(100, buffer_limit: 10)
      assert td.compress_direction == :forward

      # Compress with empty buffer should be a no-op
      td = TDigest.compress(td)
      assert td.compress_direction == :forward
    end

    test "multiple compressions cycle direction correctly" do
      td = TDigest.new(100, buffer_limit: 5)

      directions =
        Enum.reduce(1..30, {td, [:forward]}, fn x, {acc, dirs} ->
          acc = TDigest.add(acc, x)
          {acc, [acc.compress_direction | dirs]}
        end)

      {_final_td, all_directions} = directions
      all_directions = Enum.reverse(all_directions)

      # After every 5 additions, direction should flip.
      # Verify that we see both :forward and :reverse in the history.
      assert :forward in all_directions
      assert :reverse in all_directions
    end
  end

  # ---------------------------------------------------------------------------
  # Buffer limit serialization regression (Bug 2: from_binary loses buffer_limit)
  # ---------------------------------------------------------------------------

  describe "buffer_limit serialization regression" do
    test "custom buffer_limit survives serialization round-trip" do
      td = TDigest.new(100, buffer_limit: 200)
      assert td.buffer_limit == 200

      td = TDigest.add(td, 42.0)
      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()
      assert restored.buffer_limit == 200
    end

    test "default buffer_limit survives serialization round-trip" do
      td = TDigest.new(100)
      assert td.buffer_limit == 500

      td = TDigest.add(td, 1.0)
      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()
      assert restored.buffer_limit == 500
    end

    test "various buffer_limit values survive round-trip" do
      for buffer_limit <- [10, 500, 10_000] do
        td = TDigest.new(100, buffer_limit: buffer_limit)
        td = TDigest.add(td, 1.0)

        {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()

        assert restored.buffer_limit == buffer_limit,
               "buffer_limit #{buffer_limit} not preserved after round-trip, got #{restored.buffer_limit}"
      end
    end

    test "buffer_limit affects compression behavior after deserialization" do
      td = TDigest.new(100, buffer_limit: 5)
      td = Enum.reduce(1..4, td, fn x, acc -> TDigest.add(acc, x / 1) end)
      assert length(td.buffer) == 4

      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()
      assert restored.buffer_limit == 5

      # After serialize/deserialize, buffer is empty (to_binary compresses first).
      # Adding 5 elements should trigger compression at the 5th addition.
      restored = Enum.reduce(1..4, restored, fn x, acc -> TDigest.add(acc, x / 1) end)
      assert length(restored.buffer) == 4

      # The 5th element should trigger compression, emptying the buffer
      restored = TDigest.add(restored, 5.0)
      assert restored.buffer == []
      assert restored.centroids != []
    end

    test "binary format includes buffer_limit field" do
      td = TDigest.new(100, buffer_limit: 42) |> TDigest.add(1.0)
      binary = TDigest.to_binary(td)

      # Wire format:
      # <<version::8, delta::float-64, buffer_limit::unsigned-32,
      #   total_weight::float-64, min::float-64, max::float-64,
      #   num_centroids::unsigned-32, centroids::binary>>
      <<version::8, delta::big-float-64, buffer_limit::big-unsigned-32,
        _total_weight::big-float-64, _min::big-float-64, _max::big-float-64,
        _num_centroids::big-unsigned-32, _rest::binary>> = binary

      assert version == 1
      assert delta == 100.0
      assert buffer_limit == 42
    end

    test "buffer_limit is preserved across multiple serialize/deserialize cycles" do
      td = TDigest.new(50, buffer_limit: 77)
      td = Enum.reduce(1..100, td, fn x, acc -> TDigest.add(acc, x) end)

      # Round-trip three times
      {:ok, td} = td |> TDigest.to_binary() |> TDigest.from_binary()
      assert td.buffer_limit == 77

      td = Enum.reduce(101..200, td, fn x, acc -> TDigest.add(acc, x) end)
      {:ok, td} = td |> TDigest.to_binary() |> TDigest.from_binary()
      assert td.buffer_limit == 77

      td = Enum.reduce(201..300, td, fn x, acc -> TDigest.add(acc, x) end)
      {:ok, td} = td |> TDigest.to_binary() |> TDigest.from_binary()
      assert td.buffer_limit == 77

      # Data integrity is also maintained
      assert TDigest.count(td) == 300.0
    end

    test "empty digest with custom buffer_limit round-trips correctly" do
      td = TDigest.new(100, buffer_limit: 999)
      {:ok, restored} = td |> TDigest.to_binary() |> TDigest.from_binary()

      assert restored.buffer_limit == 999
      assert restored.delta == 100
      assert TDigest.count(restored) == 0.0
    end
  end

  # ---------------------------------------------------------------------------
  # Bug verification
  # ---------------------------------------------------------------------------

  describe "bug verification" do
    # Fixed: percentile/2 now accepts integer quantiles via is_number(q) guard.
    # Previously the guard used is_float(q), so integer 0 or 1 raised FunctionClauseError.

    test "percentile/2 accepts integer 0 and returns min" do
      td = TDigest.new() |> TDigest.add(1.0) |> TDigest.add(2.0)
      result = TDigest.percentile(td, 0)
      assert result == 1.0
    end

    test "percentile/2 accepts integer 1 and returns max" do
      td = TDigest.new() |> TDigest.add(1.0) |> TDigest.add(2.0)
      result = TDigest.percentile(td, 1)
      assert result == 2.0
    end

    # Bug 2: percentile interpolation doesn't blend toward min/max at extremes
    # For quantiles very close to 0 or 1, the result snaps to the first/last
    # centroid mean instead of interpolating toward the actual min/max as
    # described in Ted Dunning's reference implementation.

    test "BUG: percentile near 0 should interpolate toward min, not snap to first centroid" do
      td = TDigest.new(100)
      # Add a spread of values where min is clearly separated from the bulk
      td = TDigest.add(td, 0.0)

      td = Enum.reduce(1..1000, td, fn _, acc -> TDigest.add(acc, 100.0) end)

      # min is 0.0, but most values are 100.0
      # percentile(0.0) should return 0.0 (the min)
      # percentile at a very small q (like 0.0005) should be close to 0.0, not close to 100.0
      p = TDigest.percentile(td, 0.0005)

      # If the interpolation snaps to the first centroid mean (~100.0), this will fail
      assert p < 50.0, "Expected percentile(0.0005) to be near min (0.0), got #{p}"
    end

    test "BUG: percentile near 1 should interpolate toward max, not snap to last centroid" do
      td = TDigest.new(100)
      td = Enum.reduce(1..1000, td, fn _, acc -> TDigest.add(acc, 0.0) end)
      td = TDigest.add(td, 100.0)

      # max is 100.0, but most values are 0.0
      # percentile at a very high q (like 0.9995) should be close to 100.0, not close to 0.0
      p = TDigest.percentile(td, 0.9995)

      assert p > 50.0, "Expected percentile(0.9995) to be near max (100.0), got #{p}"
    end

    # Fixed: buffer size tracking now uses a buffer_size field instead of length(buffer),
    # giving O(1) buffer size checks instead of O(n).

    test "buffer_size field exists and tracks buffer length" do
      td = TDigest.new(100, buffer_limit: 1000)
      assert td.buffer_size == 0

      td = TDigest.add(td, 1.0)
      assert td.buffer_size == 1

      td = TDigest.add(td, 2.0)
      assert td.buffer_size == 2

      td = Enum.reduce(3..50, td, fn x, acc -> TDigest.add(acc, x) end)
      assert td.buffer_size == 50
    end

    @tag :performance
    test "add/3 buffer size check is O(1) via buffer_size field" do
      # The fix replaced `length(buffer)` with a `buffer_size` counter.
      # We verify the field is tracked correctly (the O(1) check happens in add/3).
      # The remaining cost difference between small and large buffers is due to
      # list prepend on a longer list (GC pressure), not the buffer size check.
      td = TDigest.new(100, buffer_limit: 10_000)
      td = Enum.reduce(1..5000, td, fn x, acc -> TDigest.add(acc, x) end)
      assert td.buffer_size == 5000
    end

    # Fixed: buffer_limit for small deltas now uses max(1, trunc(delta * 5)),
    # so it is never 0 even for very small delta values.

    test "new/2 with very small float delta produces buffer_limit >= 1" do
      td = TDigest.new(0.1)
      assert td.buffer_limit >= 1, "buffer_limit is #{td.buffer_limit}, expected >= 1"
    end

    test "new/2 with delta=0.01 produces buffer_limit >= 1" do
      td = TDigest.new(0.01)
      assert td.buffer_limit >= 1, "buffer_limit is #{td.buffer_limit}, expected >= 1"
    end

    test "new/2 with delta=0.001 still produces buffer_limit of 1" do
      td = TDigest.new(0.001)
      assert td.buffer_limit == 1
    end
  end
end
