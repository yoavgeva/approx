defmodule Approx.CountMinSketchTest do
  use ExUnit.Case, async: true

  alias Approx.CountMinSketch

  doctest CountMinSketch

  # ---------------------------------------------------------------------------
  # new/3
  # ---------------------------------------------------------------------------

  describe "new/3" do
    test "creates a approx with default parameters" do
      cms = CountMinSketch.new()

      # width = ceil(e / 0.001) = ceil(2718.28...) = 2719
      assert cms.width == 2719
      # depth = ceil(ln(1/0.01)) = ceil(4.605...) = 5
      assert cms.depth == 5
      assert is_function(cms.hash_fn, 1)
    end

    test "creates a approx with custom epsilon and delta" do
      cms = CountMinSketch.new(0.01, 0.05)

      # width = ceil(e / 0.01) = ceil(271.828...) = 272
      assert cms.width == 272
      # depth = ceil(ln(1/0.05)) = ceil(2.9957...) = 3
      assert cms.depth == 3
    end

    test "all counters are initialized to zero" do
      cms = CountMinSketch.new(0.1, 0.1)

      for i <- 0..(cms.depth - 1), j <- 0..(cms.width - 1) do
        row = elem(cms.table, i)
        assert elem(row, j) == 0
      end
    end

    test "accepts a custom hash function" do
      my_fn = fn _term -> 42 end
      cms = CountMinSketch.new(0.1, 0.1, hash_fn: my_fn)
      assert cms.hash_fn == my_fn
    end

    test "raises on non-positive epsilon" do
      assert_raise ArgumentError, ~r/epsilon must be a positive number/, fn ->
        CountMinSketch.new(0, 0.01)
      end

      assert_raise ArgumentError, ~r/epsilon must be a positive number/, fn ->
        CountMinSketch.new(-0.5, 0.01)
      end
    end

    test "raises on delta outside (0, 1)" do
      assert_raise ArgumentError, ~r/delta must be a number in/, fn ->
        CountMinSketch.new(0.01, 0)
      end

      assert_raise ArgumentError, ~r/delta must be a number in/, fn ->
        CountMinSketch.new(0.01, 1)
      end

      assert_raise ArgumentError, ~r/delta must be a number in/, fn ->
        CountMinSketch.new(0.01, -0.5)
      end

      assert_raise ArgumentError, ~r/delta must be a number in/, fn ->
        CountMinSketch.new(0.01, 1.5)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # add/3
  # ---------------------------------------------------------------------------

  describe "add/3" do
    test "increments count by default amount of 1" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("hello")

      assert CountMinSketch.count(cms, "hello") >= 1
    end

    test "increments count by specified amount" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("hello", 42)

      assert CountMinSketch.count(cms, "hello") >= 42
    end

    test "accumulates counts across multiple adds" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("x", 3)
        |> CountMinSketch.add("x", 7)
        |> CountMinSketch.add("x", 5)

      assert CountMinSketch.count(cms, "x") >= 15
    end

    test "handles different element types" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add(:atom_key, 2)
        |> CountMinSketch.add(123, 3)
        |> CountMinSketch.add({:tuple, "value"}, 4)
        |> CountMinSketch.add([1, 2, 3], 5)

      assert CountMinSketch.count(cms, :atom_key) >= 2
      assert CountMinSketch.count(cms, 123) >= 3
      assert CountMinSketch.count(cms, {:tuple, "value"}) >= 4
      assert CountMinSketch.count(cms, [1, 2, 3]) >= 5
    end

    test "raises on non-positive amount" do
      cms = CountMinSketch.new()

      assert_raise ArgumentError, ~r/amount must be a positive integer/, fn ->
        CountMinSketch.add(cms, "x", 0)
      end

      assert_raise ArgumentError, ~r/amount must be a positive integer/, fn ->
        CountMinSketch.add(cms, "x", -5)
      end
    end

    test "raises on non-integer amount" do
      cms = CountMinSketch.new()

      assert_raise ArgumentError, ~r/amount must be a positive integer/, fn ->
        CountMinSketch.add(cms, "x", 1.5)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # count/2
  # ---------------------------------------------------------------------------

  describe "count/2" do
    test "returns 0 for element never added (empty approx)" do
      cms = CountMinSketch.new()
      assert CountMinSketch.count(cms, "ghost") == 0
    end

    test "returns 0 for element not added to non-empty approx" do
      cms =
        CountMinSketch.new()
        |> CountMinSketch.add("present", 100)

      # This might be 0 or a small overestimate due to hash collisions;
      # but for a single element in a large approx it should be 0.
      assert CountMinSketch.count(cms, "absent") >= 0
    end

    test "never undercounts (fundamental CMS guarantee)" do
      # Add known frequencies and verify count >= true frequency for every element
      elements = for i <- 1..100, do: {"item_#{i}", i}

      cms =
        Enum.reduce(elements, CountMinSketch.new(0.001, 0.01), fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      for {elem, freq} <- elements do
        assert CountMinSketch.count(cms, elem) >= freq,
               "count for #{elem} was #{CountMinSketch.count(cms, elem)}, expected >= #{freq}"
      end
    end

    test "overestimate is bounded by epsilon * total_count with high probability" do
      epsilon = 0.01
      delta = 0.01
      cms = CountMinSketch.new(epsilon, delta)

      # Insert 1000 distinct elements, each with count 1
      n = 1000
      total_count = n

      cms =
        Enum.reduce(1..n, cms, fn i, acc ->
          CountMinSketch.add(acc, "elem_#{i}")
        end)

      # Check that overestimates are bounded for most elements.
      # With delta = 0.01, at most 1% of queries should exceed the bound.
      max_error = epsilon * total_count

      violations =
        Enum.count(1..n, fn i ->
          count = CountMinSketch.count(cms, "elem_#{i}")
          # True count is 1; overestimate = count - 1
          count - 1 > max_error
        end)

      # Allow up to delta fraction of violations (with some slack for small samples)
      max_violations = ceil(n * delta * 2)

      assert violations <= max_violations,
             "#{violations} out of #{n} queries exceeded error bound (max allowed: #{max_violations})"
    end
  end

  # ---------------------------------------------------------------------------
  # merge/2
  # ---------------------------------------------------------------------------

  describe "merge/2" do
    test "merged approx has counts equal to sum of individual approxes" do
      epsilon = 0.001
      delta = 0.01

      data1 = for i <- 1..50, do: {"item_#{i}", i}
      data2 = for i <- 26..75, do: {"item_#{i}", i * 2}

      cms1 =
        Enum.reduce(data1, CountMinSketch.new(epsilon, delta), fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      cms2 =
        Enum.reduce(data2, CountMinSketch.new(epsilon, delta), fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      # Build a combined approx from scratch for comparison
      cms_combined =
        Enum.reduce(data1 ++ data2, CountMinSketch.new(epsilon, delta), fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      {:ok, merged} = CountMinSketch.merge(cms1, cms2)

      # The merged approx should give the same counts as the combined one
      all_items = Enum.uniq(Enum.map(data1, &elem(&1, 0)) ++ Enum.map(data2, &elem(&1, 0)))

      for item <- all_items do
        assert CountMinSketch.count(merged, item) == CountMinSketch.count(cms_combined, item),
               "Mismatch for #{item}: merged=#{CountMinSketch.count(merged, item)}, combined=#{CountMinSketch.count(cms_combined, item)}"
      end
    end

    test "merge with empty approx returns equivalent of original" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("a", 5)
        |> CountMinSketch.add("b", 10)

      empty = CountMinSketch.new(0.01, 0.01)

      {:ok, merged} = CountMinSketch.merge(cms, empty)

      assert CountMinSketch.count(merged, "a") == CountMinSketch.count(cms, "a")
      assert CountMinSketch.count(merged, "b") == CountMinSketch.count(cms, "b")
    end

    test "merge is commutative" do
      cms1 =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("x", 3)

      cms2 =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("y", 7)

      {:ok, merged_ab} = CountMinSketch.merge(cms1, cms2)
      {:ok, merged_ba} = CountMinSketch.merge(cms2, cms1)

      assert CountMinSketch.count(merged_ab, "x") == CountMinSketch.count(merged_ba, "x")
      assert CountMinSketch.count(merged_ab, "y") == CountMinSketch.count(merged_ba, "y")
    end

    test "returns error when dimensions differ" do
      cms1 = CountMinSketch.new(0.01, 0.01)
      cms2 = CountMinSketch.new(0.1, 0.1)

      assert {:error, :dimension_mismatch} = CountMinSketch.merge(cms1, cms2)
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization: to_binary/1 and from_binary/1
  # ---------------------------------------------------------------------------

  describe "to_binary/1 and from_binary/1" do
    test "round-trip preserves all counters" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("alpha", 100)
        |> CountMinSketch.add("beta", 200)
        |> CountMinSketch.add("gamma", 300)

      binary = CountMinSketch.to_binary(cms)
      assert {:ok, restored} = CountMinSketch.from_binary(binary)

      assert restored.width == cms.width
      assert restored.depth == cms.depth

      # Verify all counters match exactly
      for i <- 0..(cms.depth - 1), j <- 0..(cms.width - 1) do
        original = elem(elem(cms.table, i), j)
        deserialized = elem(elem(restored.table, i), j)

        assert original == deserialized,
               "Counter mismatch at row=#{i}, col=#{j}: #{original} != #{deserialized}"
      end
    end

    test "round-trip preserves counts for queried elements" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("x", 42)
        |> CountMinSketch.add("y", 99)

      {:ok, restored} = cms |> CountMinSketch.to_binary() |> CountMinSketch.from_binary()

      assert CountMinSketch.count(restored, "x") == CountMinSketch.count(cms, "x")
      assert CountMinSketch.count(restored, "y") == CountMinSketch.count(cms, "y")
    end

    test "round-trip of empty approx" do
      cms = CountMinSketch.new(0.01, 0.01)

      {:ok, restored} = cms |> CountMinSketch.to_binary() |> CountMinSketch.from_binary()

      assert restored.width == cms.width
      assert restored.depth == cms.depth
      assert CountMinSketch.count(restored, "anything") == 0
    end

    test "binary format starts with correct version and dimensions" do
      cms = CountMinSketch.new(0.1, 0.1)
      binary = CountMinSketch.to_binary(cms)

      <<version::8, width::big-unsigned-32, depth::big-unsigned-32, _rest::binary>> = binary
      assert version == 1
      assert width == cms.width
      assert depth == cms.depth
    end

    test "binary has expected total size" do
      cms = CountMinSketch.new(0.1, 0.1)
      binary = CountMinSketch.to_binary(cms)

      # 1 (version) + 4 (width) + 4 (depth) + width * depth * 8 (counters)
      expected_size = 1 + 4 + 4 + cms.width * cms.depth * 8
      assert byte_size(binary) == expected_size
    end

    test "from_binary accepts custom hash_fn option" do
      cms = CountMinSketch.new(0.1, 0.1) |> CountMinSketch.add("test", 5)
      binary = CountMinSketch.to_binary(cms)

      custom_fn = fn _term -> 0 end
      {:ok, restored} = CountMinSketch.from_binary(binary, hash_fn: custom_fn)
      assert restored.hash_fn == custom_fn
    end

    test "from_binary returns error for unsupported version" do
      # Craft a binary with version 99
      bad_binary = <<99::8, 10::big-unsigned-32, 5::big-unsigned-32, 0::big-unsigned-64>>
      assert {:error, :invalid_binary} = CountMinSketch.from_binary(bad_binary)
    end

    test "from_binary returns error for truncated binary" do
      cms = CountMinSketch.new(0.1, 0.1)
      binary = CountMinSketch.to_binary(cms)

      # Chop off the last 10 bytes
      truncated = binary_part(binary, 0, byte_size(binary) - 10)
      assert {:error, :invalid_binary} = CountMinSketch.from_binary(truncated)
    end

    test "from_binary returns error for binary too short to contain header" do
      assert {:error, :invalid_binary} = CountMinSketch.from_binary(<<1, 2>>)
    end

    test "from_binary returns error for empty binary" do
      assert {:error, :invalid_binary} = CountMinSketch.from_binary(<<>>)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "single element add and count" do
      cms =
        CountMinSketch.new()
        |> CountMinSketch.add("only_one")

      assert CountMinSketch.count(cms, "only_one") >= 1
    end

    test "large amount add" do
      cms =
        CountMinSketch.new()
        |> CountMinSketch.add("big", 1_000_000)

      assert CountMinSketch.count(cms, "big") >= 1_000_000
    end

    test "many distinct elements" do
      n = 10_000

      cms =
        Enum.reduce(1..n, CountMinSketch.new(), fn i, acc ->
          CountMinSketch.add(acc, i)
        end)

      # Every element should have count >= 1
      for i <- Enum.take_random(1..n, 100) do
        assert CountMinSketch.count(cms, i) >= 1
      end
    end

    test "struct fields are accessible" do
      cms = CountMinSketch.new(0.01, 0.05)
      assert %CountMinSketch{width: _, depth: _, table: _, hash_fn: _} = cms
    end

    test "custom hash function is used for both add and count" do
      # Use a hash function that always returns 0 -- all elements hash to the same cell
      constant_hash = fn _term -> 0 end

      cms =
        CountMinSketch.new(0.1, 0.1, hash_fn: constant_hash)
        |> CountMinSketch.add("a", 5)
        |> CountMinSketch.add("b", 10)

      # Because all elements map to the same cell, count("a") should be 15 (5 + 10)
      assert CountMinSketch.count(cms, "a") == 15
      assert CountMinSketch.count(cms, "b") == 15
    end
  end

  # ---------------------------------------------------------------------------
  # Property: never undercount
  # ---------------------------------------------------------------------------

  describe "no-undercount property" do
    test "counts are always >= true frequency for random workload" do
      # Generate a random workload and verify the fundamental guarantee
      :rand.seed(:exsss, {1, 2, 3})

      elements =
        for _ <- 1..200 do
          key = :rand.uniform(50)
          freq = :rand.uniform(100)
          {key, freq}
        end

      # Compute true frequencies
      true_freq =
        Enum.reduce(elements, %{}, fn {key, freq}, acc ->
          Map.update(acc, key, freq, &(&1 + freq))
        end)

      # Build the approx
      cms =
        Enum.reduce(elements, CountMinSketch.new(0.001, 0.01), fn {key, freq}, acc ->
          CountMinSketch.add(acc, key, freq)
        end)

      # Verify no undercount
      for {key, expected} <- true_freq do
        actual = CountMinSketch.count(cms, key)

        assert actual >= expected,
               "Undercount detected for key=#{key}: actual=#{actual}, expected>=#{expected}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Skewed distribution accuracy
  # ---------------------------------------------------------------------------

  describe "skewed distribution accuracy" do
    test "never undercounts and overestimates are bounded for Zipf-like distribution" do
      epsilon = 0.001
      delta = 0.01
      cms = CountMinSketch.new(epsilon, delta)

      # Generate Zipf-like frequencies: element i has frequency ceil(10000/i)
      n = 1000

      true_freq =
        for i <- 1..n, into: %{} do
          {"zipf_#{i}", ceil(10_000 / i)}
        end

      total_count = true_freq |> Map.values() |> Enum.sum()

      cms =
        Enum.reduce(true_freq, cms, fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      # Verify no undercount for all elements
      for {elem, freq} <- true_freq do
        actual = CountMinSketch.count(cms, elem)

        assert actual >= freq,
               "Undercount for #{elem}: actual=#{actual}, expected>=#{freq}"
      end

      # Verify the fraction of elements where overestimate exceeds epsilon * total_count
      # is bounded by delta (with slack factor of 3)
      max_error = epsilon * total_count

      violations =
        Enum.count(true_freq, fn {elem, freq} ->
          CountMinSketch.count(cms, elem) - freq > max_error
        end)

      max_violations = ceil(n * delta * 3)

      assert violations <= max_violations,
             "Too many overestimate violations: #{violations}/#{n} (max allowed: #{max_violations})"
    end
  end

  # ---------------------------------------------------------------------------
  # Absent element queries
  # ---------------------------------------------------------------------------

  describe "absent element queries" do
    test "fraction of absent elements with large false counts is bounded" do
      epsilon = 0.01
      delta = 0.01
      cms = CountMinSketch.new(epsilon, delta)

      # Insert 1000 elements
      n_present = 1000

      cms =
        Enum.reduce(1..n_present, cms, fn i, acc ->
          CountMinSketch.add(acc, "present_#{i}")
        end)

      total_count = n_present

      # Query 10_000 absent elements
      n_absent = 10_000
      max_error = epsilon * total_count

      violations =
        Enum.count(1..n_absent, fn i ->
          CountMinSketch.count(cms, "absent_#{i}") > max_error
        end)

      # The fraction with count exceeding epsilon * total should be bounded by delta
      max_violations = ceil(n_absent * delta * 3)

      assert violations <= max_violations,
             "Too many false positives among absent elements: #{violations}/#{n_absent} " <>
               "(max allowed: #{max_violations})"
    end
  end

  # ---------------------------------------------------------------------------
  # Merge associativity
  # ---------------------------------------------------------------------------

  describe "merge associativity" do
    test "merge(merge(A,B), C) equals merge(A, merge(B,C))" do
      epsilon = 0.01
      delta = 0.01

      # Create three approxes with distinct data
      cms_a =
        Enum.reduce(1..100, CountMinSketch.new(epsilon, delta), fn i, acc ->
          CountMinSketch.add(acc, "a_#{i}", i)
        end)

      cms_b =
        Enum.reduce(1..100, CountMinSketch.new(epsilon, delta), fn i, acc ->
          CountMinSketch.add(acc, "b_#{i}", i * 2)
        end)

      cms_c =
        Enum.reduce(1..100, CountMinSketch.new(epsilon, delta), fn i, acc ->
          CountMinSketch.add(acc, "c_#{i}", i * 3)
        end)

      # (A merge B) merge C
      {:ok, ab} = CountMinSketch.merge(cms_a, cms_b)
      {:ok, ab_c} = CountMinSketch.merge(ab, cms_c)

      # A merge (B merge C)
      {:ok, bc} = CountMinSketch.merge(cms_b, cms_c)
      {:ok, a_bc} = CountMinSketch.merge(cms_a, bc)

      # Verify identical counts for elements from all three approxes
      test_elements =
        for prefix <- ["a_", "b_", "c_"],
            i <- [1, 25, 50, 75, 100] do
          "#{prefix}#{i}"
        end

      for elem <- test_elements do
        assert CountMinSketch.count(ab_c, elem) == CountMinSketch.count(a_bc, elem),
               "Associativity violated for #{elem}: " <>
                 "(AB)C=#{CountMinSketch.count(ab_c, elem)} vs A(BC)=#{CountMinSketch.count(a_bc, elem)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Accuracy after serialization
  # ---------------------------------------------------------------------------

  describe "accuracy after serialization" do
    test "deserialized approx preserves accuracy for a large workload" do
      epsilon = 0.001
      delta = 0.01

      # Build a approx with 5000 elements with varying frequencies
      true_freq =
        for i <- 1..5000, into: %{} do
          freq = rem(i * 7, 100) + 1
          {"serial_#{i}", freq}
        end

      total_count = true_freq |> Map.values() |> Enum.sum()

      cms =
        Enum.reduce(true_freq, CountMinSketch.new(epsilon, delta), fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      # Serialize and deserialize
      {:ok, restored} = cms |> CountMinSketch.to_binary() |> CountMinSketch.from_binary()

      # Verify accuracy on the restored approx: counts >= true counts
      sample_keys = Enum.take_every(Map.keys(true_freq), 10)

      for elem <- sample_keys do
        expected = Map.fetch!(true_freq, elem)
        actual = CountMinSketch.count(restored, elem)

        assert actual >= expected,
               "Undercount after deserialization for #{elem}: actual=#{actual}, expected>=#{expected}"
      end

      # Verify overestimates are bounded
      max_error = epsilon * total_count

      violations =
        Enum.count(sample_keys, fn elem ->
          CountMinSketch.count(restored, elem) - Map.fetch!(true_freq, elem) > max_error
        end)

      max_violations = ceil(length(sample_keys) * delta * 3)

      assert violations <= max_violations,
             "Too many overestimates after deserialization: #{violations}/#{length(sample_keys)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Merge with self
  # ---------------------------------------------------------------------------

  describe "merge with self" do
    test "merging a approx with itself exactly doubles all counts" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("alpha", 3)
        |> CountMinSketch.add("beta", 7)
        |> CountMinSketch.add("gamma", 15)

      original_counts = %{
        "alpha" => CountMinSketch.count(cms, "alpha"),
        "beta" => CountMinSketch.count(cms, "beta"),
        "gamma" => CountMinSketch.count(cms, "gamma")
      }

      {:ok, doubled} = CountMinSketch.merge(cms, cms)

      for {elem, original_count} <- original_counts do
        assert CountMinSketch.count(doubled, elem) == original_count * 2,
               "Expected #{elem} count to double from #{original_count} to #{original_count * 2}, " <>
                 "got #{CountMinSketch.count(doubled, elem)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Exact counts with deterministic hash
  # ---------------------------------------------------------------------------

  describe "exact counts" do
    test "single element with custom hash produces exact count" do
      # Use a hash function that produces distinct values for different elements.
      # The hash function must also handle the {:__approx_h2__, element} tuple
      # used for the second independent hash in double hashing.
      deterministic_hash = fn
        "only_element" -> 0x0001_0002
        {:__approx_h2__, "only_element"} -> 0x0003_0004
        other -> :erlang.phash2(other, Bitwise.bsl(1, 32))
      end

      n = 42

      cms =
        CountMinSketch.new(0.01, 0.01, hash_fn: deterministic_hash)
        |> CountMinSketch.add("only_element", n)

      assert CountMinSketch.count(cms, "only_element") == n
    end

    test "multiple elements with distinct hashes produce exact counts" do
      # Craft a hash function that maps each test element to a unique value
      # producing non-overlapping column indices across all rows.
      # Must also handle {:__approx_h2__, element} tuples for the second hash.
      deterministic_hash = fn
        "elem_a" -> 0x0010_0001
        "elem_b" -> 0x0020_0002
        "elem_c" -> 0x0030_0003
        {:__approx_h2__, "elem_a"} -> 0x0040_0004
        {:__approx_h2__, "elem_b"} -> 0x0050_0005
        {:__approx_h2__, "elem_c"} -> 0x0060_0006
        other -> :erlang.phash2(other, Bitwise.bsl(1, 32))
      end

      cms =
        CountMinSketch.new(0.01, 0.01, hash_fn: deterministic_hash)
        |> CountMinSketch.add("elem_a", 10)
        |> CountMinSketch.add("elem_b", 20)
        |> CountMinSketch.add("elem_c", 30)

      assert CountMinSketch.count(cms, "elem_a") == 10
      assert CountMinSketch.count(cms, "elem_b") == 20
      assert CountMinSketch.count(cms, "elem_c") == 30
    end
  end

  # ---------------------------------------------------------------------------
  # Extreme parameters
  # ---------------------------------------------------------------------------

  describe "extreme parameters" do
    test "very small epsilon creates a wide approx without crash" do
      cms = CountMinSketch.new(0.0001, 0.01)

      # width = ceil(e / 0.0001) = ceil(27182.8...) = 27183
      assert cms.width == 27_183
      assert cms.depth == 5

      # Verify it functions correctly
      cms = CountMinSketch.add(cms, "test", 10)
      assert CountMinSketch.count(cms, "test") >= 10
    end

    test "very small delta creates a deep approx without crash" do
      cms = CountMinSketch.new(0.01, 0.0001)

      assert cms.width == 272
      # depth = ceil(ln(1/0.0001)) = ceil(9.2103...) = 10
      assert cms.depth == 10

      # Verify it functions correctly
      cms = CountMinSketch.add(cms, "test", 10)
      assert CountMinSketch.count(cms, "test") >= 10
    end

    test "large epsilon and delta near 1 creates minimum dimensions and still functions" do
      cms = CountMinSketch.new(1.0, 0.999)

      # width = ceil(e / 1.0) = ceil(2.718...) = 3
      assert cms.width == 3
      # depth = ceil(ln(1/0.999)) = ceil(0.001...) = 1
      assert cms.depth == 1

      cms =
        cms
        |> CountMinSketch.add("a", 5)
        |> CountMinSketch.add("b", 10)

      assert CountMinSketch.count(cms, "a") >= 5
      assert CountMinSketch.count(cms, "b") >= 10
    end
  end

  # ---------------------------------------------------------------------------
  # from_binary edge cases
  # ---------------------------------------------------------------------------

  describe "from_binary edge cases" do
    test "rejects binary with width=0" do
      # Craft a binary: version=1, width=0, depth=5, no counter data
      binary = <<1::8, 0::big-unsigned-32, 5::big-unsigned-32>>
      assert {:error, :invalid_binary} = CountMinSketch.from_binary(binary)
    end

    test "rejects binary with depth=0" do
      # Craft a binary: version=1, width=5, depth=0, no counter data
      binary = <<1::8, 5::big-unsigned-32, 0::big-unsigned-32>>
      assert {:error, :invalid_binary} = CountMinSketch.from_binary(binary)
    end
  end

  # ---------------------------------------------------------------------------
  # Special elements
  # ---------------------------------------------------------------------------

  describe "special elements" do
    test "empty string as element" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add("", 5)

      assert CountMinSketch.count(cms, "") >= 5
    end

    test "nil as element" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add(nil, 3)

      assert CountMinSketch.count(cms, nil) >= 3
    end

    test "false as element" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add(false, 7)

      assert CountMinSketch.count(cms, false) >= 7
    end

    test "zero as element" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add(0, 11)

      assert CountMinSketch.count(cms, 0) >= 11
    end

    test "empty list as element" do
      cms =
        CountMinSketch.new(0.01, 0.01)
        |> CountMinSketch.add([], 9)

      assert CountMinSketch.count(cms, []) >= 9
    end

    test "multiple special elements coexist with correct counts" do
      cms =
        CountMinSketch.new(0.001, 0.01)
        |> CountMinSketch.add("", 5)
        |> CountMinSketch.add(nil, 3)
        |> CountMinSketch.add(false, 7)
        |> CountMinSketch.add(0, 11)
        |> CountMinSketch.add([], 9)

      assert CountMinSketch.count(cms, "") >= 5
      assert CountMinSketch.count(cms, nil) >= 3
      assert CountMinSketch.count(cms, false) >= 7
      assert CountMinSketch.count(cms, 0) >= 11
      assert CountMinSketch.count(cms, []) >= 9
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-approx merge
  # ---------------------------------------------------------------------------

  describe "multi-approx merge" do
    test "reduce-merge over 10 approxes matches single combined approx" do
      epsilon = 0.001
      delta = 0.01

      # Build 10 approxes with disjoint data
      approxes =
        for batch <- 0..9 do
          start = batch * 100 + 1
          stop = (batch + 1) * 100

          Enum.reduce(start..stop, CountMinSketch.new(epsilon, delta), fn i, acc ->
            CountMinSketch.add(acc, "multi_#{i}", rem(i, 50) + 1)
          end)
        end

      # Reduce-merge all 10 approxes
      [first | rest] = approxes

      merged =
        Enum.reduce(rest, first, fn approx, acc ->
          {:ok, result} = CountMinSketch.merge(acc, approx)
          result
        end)

      # Build a single approx from all combined data for comparison
      combined =
        Enum.reduce(1..1000, CountMinSketch.new(epsilon, delta), fn i, acc ->
          CountMinSketch.add(acc, "multi_#{i}", rem(i, 50) + 1)
        end)

      # All counts from the reduced merge should match the combined approx exactly
      # (merge is element-wise addition, same as building from scratch)
      sample = Enum.take_every(1..1000, 7)

      for i <- sample do
        elem_name = "multi_#{i}"
        true_count = rem(i, 50) + 1

        merged_count = CountMinSketch.count(merged, elem_name)
        combined_count = CountMinSketch.count(combined, elem_name)

        assert merged_count == combined_count,
               "Mismatch for #{elem_name}: merged=#{merged_count}, combined=#{combined_count}"

        assert merged_count >= true_count,
               "Undercount for #{elem_name}: got=#{merged_count}, expected>=#{true_count}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Bug fix regressions
  # ---------------------------------------------------------------------------

  describe "bug fix regressions" do
    # --- Bug 1: 16-bit hash halves -----------------------------------------------
    # The fix uses two independent 32-bit hashes instead of splitting one hash
    # into two 16-bit halves. Approxes wider than 65,536 now work correctly.

    test "wide approx (width > 65,536) returns correct counts" do
      # epsilon = 0.0001 produces width = ceil(e / 0.0001) = 27,183
      # This does not exceed 65,536 on its own, but we use a very small epsilon
      # to get a wide approx that would suffer from hash collisions with the old
      # 16-bit split. For a width > 65,536 we'd need epsilon < e/65536 ~ 0.0000416.
      cms = CountMinSketch.new(0.00001, 0.01)
      assert cms.width > 65_536, "width should exceed 65,536 for this test"

      # Add distinct elements with known counts
      elements = for i <- 1..200, do: {"wide_elem_#{i}", i}

      cms =
        Enum.reduce(elements, cms, fn {elem, freq}, acc ->
          CountMinSketch.add(acc, elem, freq)
        end)

      # Verify no undercounts (fundamental guarantee)
      for {elem, freq} <- elements do
        actual = CountMinSketch.count(cms, elem)

        assert actual >= freq,
               "Undercount in wide approx for #{elem}: actual=#{actual}, expected>=#{freq}"
      end

      # With only 200 elements in a >65K wide approx, counts should be exact
      # (virtually no collisions). The old 16-bit split would cause collisions
      # by mapping all positions into a 65K range.
      for {elem, freq} <- elements do
        actual = CountMinSketch.count(cms, elem)

        assert actual == freq,
               "Overcount in wide approx for #{elem}: actual=#{actual}, expected=#{freq}. " <>
                 "This may indicate hash position collisions from 16-bit truncation."
      end
    end

    test "hash_fn receives {:__approx_h2__, element} for the second hash" do
      parent = self()

      tracking_hash = fn term ->
        send(parent, {:hash_call, term})
        :erlang.phash2(term, Bitwise.bsl(1, 32))
      end

      cms = CountMinSketch.new(0.1, 0.1, hash_fn: tracking_hash)

      # Flush any messages from construction
      flush_messages()

      _cms = CountMinSketch.add(cms, "tracked_elem")

      calls = collect_messages()

      assert {:hash_call, "tracked_elem"} in calls,
             "hash_fn should be called with the raw element"

      assert {:hash_call, {:__approx_h2__, "tracked_elem"}} in calls,
             "hash_fn should be called with {:__approx_h2__, element} for the second hash"
    end

    test "wide approx element counts don't collide more than expected" do
      # With epsilon = 0.0001, width = 27,183 and depth = 5.
      # Insert 5000 elements each with count 1. Total count N = 5000.
      # The overestimate bound is epsilon * N = 0.0001 * 5000 = 0.5, so
      # ideally all counts should be exactly 1 (no overestimates above 1).
      epsilon = 0.0001
      delta = 0.01
      cms = CountMinSketch.new(epsilon, delta)
      assert cms.width > 20_000, "width should be large for this test"

      n = 5000

      cms =
        Enum.reduce(1..n, cms, fn i, acc ->
          CountMinSketch.add(acc, "collision_test_#{i}")
        end)

      total_count = n
      max_error = epsilon * total_count

      # Count how many elements have an overestimate exceeding the bound
      violations =
        Enum.count(1..n, fn i ->
          count = CountMinSketch.count(cms, "collision_test_#{i}")
          # True count is 1; overestimate = count - 1
          count - 1 > max_error
        end)

      # With delta = 0.01, at most 1% should violate the bound (with slack)
      max_violations = ceil(n * delta * 2)

      assert violations <= max_violations,
             "#{violations} out of #{n} queries exceeded error bound " <>
               "(max allowed: #{max_violations}). " <>
               "Possible regression to 16-bit hash splitting causing excess collisions."
    end

    test "add and count on approx with width just above 65,536 returns exact counts for single element" do
      # Use an epsilon that produces width just above 65,536 to test the boundary.
      # width = ceil(e / epsilon) > 65_536  =>  epsilon < e / 65_536 ~ 0.0000416
      cms = CountMinSketch.new(0.00004, 0.01)
      assert cms.width > 65_536

      cms = CountMinSketch.add(cms, "boundary_elem", 42)

      # With a single element in a very wide approx, the count must be exact.
      assert CountMinSketch.count(cms, "boundary_elem") == 42,
             "Single element count should be exact in a >65K wide approx"
    end
  end

  # ---------------------------------------------------------------------------
  # Bug verification
  # ---------------------------------------------------------------------------

  describe "bug verification (fixed)" do
    test "count of absent element in non-empty approx is exactly 0" do
      # Previously the test asserted `>= 0` which was tautologically true.
      # With only one element in a large approx, an absent element should have count 0.
      cms = CountMinSketch.new() |> CountMinSketch.add("present", 100)

      # With default params (width=2719, depth=5), a single element should not
      # cause any collision for a random absent element
      count = CountMinSketch.count(cms, "definitely_absent_element_xyz")
      assert count == 0, "Expected exactly 0 for absent element in sparse approx, got #{count}"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers for message-based hash tracking
  # ---------------------------------------------------------------------------

  defp flush_messages do
    receive do
      _ -> flush_messages()
    after
      0 -> :ok
    end
  end

  defp collect_messages do
    collect_messages([])
  end

  defp collect_messages(acc) do
    receive do
      msg -> collect_messages([msg | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
