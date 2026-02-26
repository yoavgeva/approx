defmodule Approx.BloomFilterTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Approx.BloomFilter

  doctest Approx.BloomFilter

  @max_32 bsl(1, 32)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # A trivially predictable hash function for deterministic tests.
  # Returns the element itself when it is already an integer, otherwise
  # falls back to :erlang.phash2 so strings still get numeric hashes.
  defp deterministic_hash(term) when is_integer(term), do: abs(term)
  defp deterministic_hash(term), do: :erlang.phash2(term, @max_32)

  defp new_filter(capacity), do: BloomFilter.new(capacity, 0.01)
  defp new_filter(capacity, fpp), do: BloomFilter.new(capacity, fpp)
  defp new_filter(capacity, fpp, opts), do: BloomFilter.new(capacity, fpp, opts)

  # ---------------------------------------------------------------------------
  # new/3
  # ---------------------------------------------------------------------------

  describe "new/3" do
    test "creates a filter with correct struct fields" do
      bf = new_filter(500, 0.05)

      assert %BloomFilter{} = bf
      assert is_binary(bf.bits)
      assert bf.size > 0
      assert bf.hash_count > 0
      assert is_function(bf.hash_fn, 1)
    end

    test "bit array is initially all zeros" do
      bf = new_filter(100)
      assert bf.bits == <<0::size(byte_size(bf.bits) * 8)>>
    end

    test "larger capacity or stricter fpp produces a larger bit array" do
      small = new_filter(100, 0.01)
      large = new_filter(10_000, 0.01)
      assert large.size > small.size

      relaxed = new_filter(1_000, 0.1)
      strict = new_filter(1_000, 0.001)
      assert strict.size > relaxed.size
    end

    test "accepts a custom :hash_fn option" do
      custom_fn = fn _term -> 42 end
      bf = new_filter(100, 0.01, hash_fn: custom_fn)
      assert bf.hash_fn == custom_fn
    end

    test "uses default hash function when no option is given" do
      bf = new_filter(100)
      # The default should be Approx.Hash.hash32/1; just verify it works.
      assert is_function(bf.hash_fn, 1)
      assert is_integer(bf.hash_fn.("test"))
    end

    test "computes optimal parameters (sanity check against formulas)" do
      capacity = 1_000
      fpp = 0.01
      bf = new_filter(capacity, fpp)

      # Expected m ~ 9_586, k ~ 7 for n=1000, p=0.01
      assert bf.size >= 9_000
      assert bf.size <= 10_000
      assert bf.hash_count >= 6
      assert bf.hash_count <= 8
    end
  end

  # ---------------------------------------------------------------------------
  # add/2 and member?/2
  # ---------------------------------------------------------------------------

  describe "add/2 and member?/2" do
    test "empty filter returns false for any element" do
      bf = new_filter(100)

      refute BloomFilter.member?(bf, "anything")
      refute BloomFilter.member?(bf, 42)
      refute BloomFilter.member?(bf, :atom)
    end

    test "added element is always found (no false negatives)" do
      bf = new_filter(1_000)

      elements = Enum.map(1..200, &"element_#{&1}")

      bf = Enum.reduce(elements, bf, fn elem, acc -> BloomFilter.add(acc, elem) end)

      Enum.each(elements, fn elem ->
        assert BloomFilter.member?(bf, elem),
               "Expected #{inspect(elem)} to be a member after insertion"
      end)
    end

    test "single element add and query" do
      bf = new_filter(100)
      bf = BloomFilter.add(bf, "solo")

      assert BloomFilter.member?(bf, "solo")
    end

    test "works with various term types" do
      bf = new_filter(100)

      terms = [
        42,
        3.14,
        :an_atom,
        "a string",
        {"a", "tuple"},
        [1, 2, 3],
        %{key: "value"},
        <<0, 1, 2>>
      ]

      bf = Enum.reduce(terms, bf, fn t, acc -> BloomFilter.add(acc, t) end)

      Enum.each(terms, fn t ->
        assert BloomFilter.member?(bf, t)
      end)
    end

    test "add is idempotent — adding the same element twice doesn't change the filter" do
      bf = new_filter(100)
      bf1 = BloomFilter.add(bf, "dup")
      bf2 = BloomFilter.add(bf1, "dup")

      assert bf1.bits == bf2.bits
    end

    test "works with injectable deterministic hash function" do
      bf = BloomFilter.new(100, 0.01, hash_fn: &deterministic_hash/1)
      bf = BloomFilter.add(bf, "test_element")

      assert BloomFilter.member?(bf, "test_element")
      refute BloomFilter.member?(bf, "other_element")
    end
  end

  # ---------------------------------------------------------------------------
  # False positive rate
  # ---------------------------------------------------------------------------

  describe "false positive rate" do
    test "observed FPP is within 2x of theoretical for a reasonably filled filter" do
      capacity = 10_000
      fpp = 0.01
      bf = new_filter(capacity, fpp)

      # Insert `capacity` elements.
      bf =
        Enum.reduce(1..capacity, bf, fn i, acc ->
          BloomFilter.add(acc, {:inserted, i})
        end)

      # Test 50_000 elements that were NOT inserted.
      test_count = 50_000

      false_positives =
        Enum.count(1..test_count, fn i ->
          BloomFilter.member?(bf, {:not_inserted, i})
        end)

      observed_fpp = false_positives / test_count

      # Allow up to 2x the theoretical rate to account for statistical variance.
      assert observed_fpp < fpp * 2,
             "Observed FPP #{Float.round(observed_fpp, 5)} exceeds 2x theoretical #{fpp}"
    end
  end

  # ---------------------------------------------------------------------------
  # merge/2 and union/2
  # ---------------------------------------------------------------------------

  describe "merge/2" do
    test "merged filter contains members from both input filters" do
      bf1 = new_filter(500) |> BloomFilter.add("alpha") |> BloomFilter.add("beta")
      bf2 = new_filter(500) |> BloomFilter.add("gamma") |> BloomFilter.add("delta")

      {:ok, merged} = BloomFilter.merge(bf1, bf2)

      assert BloomFilter.member?(merged, "alpha")
      assert BloomFilter.member?(merged, "beta")
      assert BloomFilter.member?(merged, "gamma")
      assert BloomFilter.member?(merged, "delta")
    end

    test "merge of two empty filters is still empty" do
      bf1 = new_filter(100)
      bf2 = new_filter(100)

      {:ok, merged} = BloomFilter.merge(bf1, bf2)
      assert merged.bits == bf1.bits
    end

    test "merge with self is idempotent" do
      bf = new_filter(100) |> BloomFilter.add("x")
      {:ok, merged} = BloomFilter.merge(bf, bf)
      assert merged.bits == bf.bits
    end

    test "returns error for filters with different sizes" do
      bf1 = new_filter(100, 0.01)
      bf2 = new_filter(100, 0.001)

      # Different fpp leads to different size
      if bf1.size != bf2.size do
        assert {:error, :incompatible_filters} = BloomFilter.merge(bf1, bf2)
      end
    end

    test "returns error for filters with different hash counts" do
      # Force incompatible filters by using very different parameters.
      bf1 = new_filter(100, 0.01)
      bf2 = new_filter(5_000, 0.0001)

      assert {:error, :incompatible_filters} = BloomFilter.merge(bf1, bf2)
    end

    test "merge is equivalent to adding all elements to a single filter" do
      elements_a = Enum.map(1..50, &"a_#{&1}")
      elements_b = Enum.map(1..50, &"b_#{&1}")

      bf_a = Enum.reduce(elements_a, new_filter(200), &BloomFilter.add(&2, &1))
      bf_b = Enum.reduce(elements_b, new_filter(200), &BloomFilter.add(&2, &1))

      {:ok, merged} = BloomFilter.merge(bf_a, bf_b)

      bf_combined =
        Enum.reduce(
          elements_a ++ elements_b,
          new_filter(200),
          &BloomFilter.add(&2, &1)
        )

      # Every element in the combined filter must be in the merged filter.
      Enum.each(elements_a ++ elements_b, fn elem ->
        assert BloomFilter.member?(merged, elem)
        assert BloomFilter.member?(bf_combined, elem)
      end)
    end
  end

  describe "union/2" do
    test "union is an alias for merge" do
      bf1 = new_filter(100) |> BloomFilter.add("u1")
      bf2 = new_filter(100) |> BloomFilter.add("u2")

      assert BloomFilter.union(bf1, bf2) == BloomFilter.merge(bf1, bf2)
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  describe "to_binary/1 and from_binary/1" do
    test "round-trip preserves membership results" do
      bf = new_filter(500)

      elements = Enum.map(1..100, &"ser_#{&1}")
      bf = Enum.reduce(elements, bf, fn e, acc -> BloomFilter.add(acc, e) end)

      binary = BloomFilter.to_binary(bf)
      {:ok, restored} = BloomFilter.from_binary(binary)

      Enum.each(elements, fn e ->
        assert BloomFilter.member?(restored, e),
               "Element #{inspect(e)} lost after round-trip"
      end)

      assert restored.size == bf.size
      assert restored.hash_count == bf.hash_count
      assert restored.bits == bf.bits
    end

    test "round-trip on empty filter" do
      bf = new_filter(100)
      binary = BloomFilter.to_binary(bf)
      {:ok, restored} = BloomFilter.from_binary(binary)

      assert restored.bits == bf.bits
      assert restored.size == bf.size
      assert restored.hash_count == bf.hash_count
    end

    test "binary starts with version byte 1" do
      bf = new_filter(100)
      <<version::8, _rest::binary>> = BloomFilter.to_binary(bf)
      assert version == 1
    end

    test "binary encodes size and hash_count correctly" do
      bf = new_filter(100)

      <<1::8, size::big-unsigned-64, hash_count::big-unsigned-16, _bits::binary>> =
        BloomFilter.to_binary(bf)

      assert size == bf.size
      assert hash_count == bf.hash_count
    end

    test "from_binary returns error for garbage input" do
      assert {:error, :invalid_binary} = BloomFilter.from_binary(<<>>)
      assert {:error, :invalid_binary} = BloomFilter.from_binary(<<0, 1, 2>>)
      assert {:error, :invalid_binary} = BloomFilter.from_binary("not a bloom filter")
    end

    test "from_binary returns error for unknown version" do
      bf = new_filter(100)
      <<_version::8, rest::binary>> = BloomFilter.to_binary(bf)
      bad_binary = <<99::8, rest::binary>>

      assert {:error, :invalid_binary} = BloomFilter.from_binary(bad_binary)
    end

    test "from_binary returns error when bits length doesn't match size" do
      bf = new_filter(100)

      <<version::8, size::big-unsigned-64, hash_count::big-unsigned-16, _bits::binary>> =
        BloomFilter.to_binary(bf)

      # Provide too few bits.
      truncated = <<version::8, size::big-unsigned-64, hash_count::big-unsigned-16, 0::8>>
      assert {:error, :invalid_binary} = BloomFilter.from_binary(truncated)
    end

    test "from_binary accepts custom :hash_fn option" do
      custom_fn = fn term -> :erlang.phash2(term, @max_32) end
      bf = new_filter(100, 0.01, hash_fn: custom_fn)
      bf = BloomFilter.add(bf, "custom")

      binary = BloomFilter.to_binary(bf)
      {:ok, restored} = BloomFilter.from_binary(binary, hash_fn: custom_fn)

      assert BloomFilter.member?(restored, "custom")
      assert restored.hash_fn == custom_fn
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "works with capacity of 1" do
      bf = BloomFilter.new(1, 0.01)
      bf = BloomFilter.add(bf, "only")
      assert BloomFilter.member?(bf, "only")
    end

    test "very low false positive probability creates more hash functions" do
      loose = new_filter(1_000, 0.5)
      strict = new_filter(1_000, 0.0001)

      assert strict.hash_count > loose.hash_count
    end

    test "filter with injectable hash producing collisions" do
      # A hash function that always returns the same value — extreme collision scenario.
      constant_hash = fn _term -> 12_345 end
      bf = BloomFilter.new(100, 0.01, hash_fn: constant_hash)

      bf = BloomFilter.add(bf, "a")

      # Because all elements hash to the same positions, everything looks like a member.
      assert BloomFilter.member?(bf, "a")
      assert BloomFilter.member?(bf, "b")
      assert BloomFilter.member?(bf, "anything")
    end

    test "large number of elements beyond capacity still has no false negatives" do
      capacity = 100
      bf = new_filter(capacity)

      # Insert 3x the capacity — FPP will be higher, but no false negatives.
      elements = Enum.map(1..300, &"over_#{&1}")
      bf = Enum.reduce(elements, bf, fn e, acc -> BloomFilter.add(acc, e) end)

      Enum.each(elements, fn e ->
        assert BloomFilter.member?(bf, e),
               "False negative for #{inspect(e)} even when over capacity"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Struct inspection (no leaking of internals)
  # ---------------------------------------------------------------------------

  describe "struct" do
    test "enforces required keys" do
      assert_raise ArgumentError, fn ->
        struct!(BloomFilter, [])
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Input validation
  # ---------------------------------------------------------------------------

  describe "input validation" do
    test "new/3 with capacity of 0 raises FunctionClauseError" do
      assert_raise FunctionClauseError, fn ->
        BloomFilter.new(0, 0.01)
      end
    end

    test "new/3 with negative capacity raises FunctionClauseError" do
      assert_raise FunctionClauseError, fn ->
        BloomFilter.new(-10, 0.01)
      end
    end

    test "new/3 with fpp of 0.0 raises FunctionClauseError" do
      assert_raise FunctionClauseError, fn ->
        BloomFilter.new(100, 0.0)
      end
    end

    test "new/3 with fpp of 1.0 raises FunctionClauseError" do
      assert_raise FunctionClauseError, fn ->
        BloomFilter.new(100, 1.0)
      end
    end

    test "new/3 with non-integer capacity raises FunctionClauseError" do
      assert_raise FunctionClauseError, fn ->
        BloomFilter.new(10.5, 0.01)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Large filter / overflow protection
  # ---------------------------------------------------------------------------

  describe "large filter" do
    test "capacity 1_000_000 and fpp 0.01 creates successfully" do
      bf = BloomFilter.new(1_000_000, 0.01)

      assert %BloomFilter{} = bf
      assert bf.size > 0
      assert byte_size(bf.bits) == div(bf.size + 7, 8)
    end

    test "capacity 10_000_000 and fpp 0.0000001 creates without overflow" do
      bf = BloomFilter.new(10_000_000, 0.0000001)

      assert %BloomFilter{} = bf
      assert bf.size > 0
      assert bf.hash_count > 0
    end
  end

  # ---------------------------------------------------------------------------
  # Optimal parameters
  # ---------------------------------------------------------------------------

  describe "optimal parameters" do
    test "known values for n=1000, p=0.01" do
      bf = BloomFilter.new(1000, 0.01)

      # m = ceil(-1000 * ln(0.01) / (ln(2))^2) = ceil(9585.06) = 9586
      expected_m = ceil(-1000 * :math.log(0.01) / (:math.log(2) * :math.log(2)))
      assert bf.size == expected_m

      # k = ceil(m/n * ln(2)) = ceil(9586/1000 * 0.693147) = 7
      expected_k = ceil(expected_m / 1000 * :math.log(2))
      assert bf.hash_count == expected_k
    end

    test "optimal_bit_count is always >= 8 for a range of configurations" do
      configurations = [
        {1, 0.99},
        {1, 0.5},
        {10, 0.1},
        {100, 0.01},
        {1000, 0.001},
        {10_000, 0.0001}
      ]

      for {capacity, fpp} <- configurations do
        m = BloomFilter.optimal_bit_count(capacity, fpp)

        assert m >= 8,
               "optimal_bit_count(#{capacity}, #{fpp}) = #{m}, expected >= 8"
      end
    end

    test "optimal_hash_count is always >= 1 for a range of configurations" do
      configurations = [
        {8, 1},
        {8, 100},
        {100, 10},
        {1000, 500},
        {10_000, 10_000},
        {1_000_000, 100_000}
      ]

      for {bit_count, capacity} <- configurations do
        k = BloomFilter.optimal_hash_count(bit_count, capacity)

        assert k >= 1,
               "optimal_hash_count(#{bit_count}, #{capacity}) = #{k}, expected >= 1"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # False positive rate across configurations
  # ---------------------------------------------------------------------------

  describe "false positive rate across configurations" do
    @fpp_configs [
      {100, 0.1},
      {1000, 0.01},
      {5000, 0.001}
    ]

    for {capacity, fpp} <- @fpp_configs do
      @tag :fpp_parameterized
      test "observed FPP < 2x theoretical for capacity=#{capacity}, fpp=#{fpp}" do
        capacity = unquote(capacity)
        fpp = unquote(fpp)

        bf = BloomFilter.new(capacity, fpp)

        bf =
          Enum.reduce(1..capacity, bf, fn i, acc ->
            BloomFilter.add(acc, {:member, i})
          end)

        test_count = 10_000

        false_positives =
          Enum.count(1..test_count, fn i ->
            BloomFilter.member?(bf, {:non_member, i})
          end)

        observed_fpp = false_positives / test_count

        assert observed_fpp < fpp * 2,
               "For capacity=#{capacity}, fpp=#{fpp}: observed #{Float.round(observed_fpp, 6)} exceeds 2x theoretical"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization size
  # ---------------------------------------------------------------------------

  describe "serialization size" do
    @size_configs [
      {100, 0.01},
      {500, 0.05},
      {1000, 0.001},
      {200, 0.1}
    ]

    for {capacity, fpp} <- @size_configs do
      test "to_binary output size equals header + bits for capacity=#{capacity}, fpp=#{fpp}" do
        bf = BloomFilter.new(unquote(capacity), unquote(fpp))
        binary = BloomFilter.to_binary(bf)

        # Format: <<version::8, size::big-64, hash_count::big-16, bits::binary>>
        # Header = 1 + 8 + 2 = 11 bytes
        expected_size = 1 + 8 + 2 + ceil(bf.size / 8)
        assert byte_size(binary) == expected_size
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Special elements (nil, empty string, etc.)
  # ---------------------------------------------------------------------------

  describe "special elements" do
    test "adding and querying empty string works" do
      bf = BloomFilter.new(100, 0.01)
      bf = BloomFilter.add(bf, "")
      assert BloomFilter.member?(bf, "")
    end

    test "adding and querying nil works" do
      bf = BloomFilter.new(100, 0.01)
      bf = BloomFilter.add(bf, nil)
      assert BloomFilter.member?(bf, nil)
    end

    test "adding and querying 0, false, and [] works" do
      bf = BloomFilter.new(100, 0.01)

      bf = BloomFilter.add(bf, 0)
      bf = BloomFilter.add(bf, false)
      bf = BloomFilter.add(bf, [])

      assert BloomFilter.member?(bf, 0)
      assert BloomFilter.member?(bf, false)
      assert BloomFilter.member?(bf, [])
    end

    test "special elements are distinguishable from each other" do
      # Add only one element and verify the others are not necessarily members.
      # We test each special element in isolation. Since false positives are
      # possible, we use a strict FPP to make collisions extremely unlikely.
      special_values = ["", nil, 0, false, []]

      for value <- special_values do
        bf = BloomFilter.new(100, 0.001)
        bf = BloomFilter.add(bf, value)

        # The added element must be found.
        assert BloomFilter.member?(bf, value),
               "Expected #{inspect(value)} to be a member after insertion"

        # At least some of the other special values should NOT be reported
        # as members. With a very low FPP and only 1 element inserted, the
        # probability that ALL others are false positives is negligible.
        others = Enum.reject(special_values, &(&1 === value))

        other_matches =
          Enum.count(others, fn other -> BloomFilter.member?(bf, other) end)

        assert other_matches < length(others),
               "After inserting only #{inspect(value)}, all other special values " <>
                 "were reported as members — they should be distinguishable"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Merge commutativity and associativity
  # ---------------------------------------------------------------------------

  describe "merge properties" do
    test "merge is commutative" do
      bf1 = BloomFilter.new(200, 0.01) |> BloomFilter.add("a") |> BloomFilter.add("b")
      bf2 = BloomFilter.new(200, 0.01) |> BloomFilter.add("c") |> BloomFilter.add("d")

      {:ok, merged_1_2} = BloomFilter.merge(bf1, bf2)
      {:ok, merged_2_1} = BloomFilter.merge(bf2, bf1)

      assert merged_1_2.bits == merged_2_1.bits
    end

    test "merge is associative" do
      bf1 = BloomFilter.new(200, 0.01) |> BloomFilter.add("x")
      bf2 = BloomFilter.new(200, 0.01) |> BloomFilter.add("y")
      bf3 = BloomFilter.new(200, 0.01) |> BloomFilter.add("z")

      {:ok, merged_12} = BloomFilter.merge(bf1, bf2)
      {:ok, left_assoc} = BloomFilter.merge(merged_12, bf3)

      {:ok, merged_23} = BloomFilter.merge(bf2, bf3)
      {:ok, right_assoc} = BloomFilter.merge(bf1, merged_23)

      assert left_assoc.bits == right_assoc.bits
    end
  end

  # ---------------------------------------------------------------------------
  # Immutability
  # ---------------------------------------------------------------------------

  describe "immutability" do
    test "adding to a filter does not mutate the original" do
      bf = BloomFilter.new(100, 0.01)
      bf1 = BloomFilter.add(bf, "x")
      bf2 = BloomFilter.add(bf1, "y")

      # bf1 should still NOT contain "y"
      assert BloomFilter.member?(bf1, "x")

      refute BloomFilter.member?(bf1, "y"),
             "bf1 should not contain 'y' — adding to bf1 should return a new filter"

      # bf2 should contain both
      assert BloomFilter.member?(bf2, "x")
      assert BloomFilter.member?(bf2, "y")

      # original bf should contain neither
      refute BloomFilter.member?(bf, "x")
      refute BloomFilter.member?(bf, "y")
    end
  end

  # ---------------------------------------------------------------------------
  # Bug fix regressions
  # ---------------------------------------------------------------------------

  describe "bug fix regressions" do
    # --- Bug 1: 16-bit hash halves -----------------------------------------------
    # The fix uses two independent 32-bit hashes instead of splitting one hash
    # into two 16-bit halves. Filters wider than 65,536 bits now work correctly.

    test "large filter (>65,536 bits) inserts and finds elements correctly" do
      # new(100_000, 0.001) produces ~1.4M bits, well above the 65,536 boundary
      bf = BloomFilter.new(100_000, 0.001)
      assert bf.size > 65_536, "filter size should exceed 65,536 bits"

      elements = Enum.map(1..500, &"large_filter_elem_#{&1}")

      bf = Enum.reduce(elements, bf, fn e, acc -> BloomFilter.add(acc, e) end)

      # Every inserted element must be found (no false negatives).
      Enum.each(elements, fn e ->
        assert BloomFilter.member?(bf, e),
               "Element #{inspect(e)} not found in large (>65K bit) filter"
      end)
    end

    test "hash_fn receives {:__approx_h2__, element} for the second hash" do
      parent = self()

      tracking_hash = fn term ->
        send(parent, {:hash_call, term})
        :erlang.phash2(term, bsl(1, 32))
      end

      bf = BloomFilter.new(100, 0.01, hash_fn: tracking_hash)

      # Flush any messages from construction
      flush_messages()

      _bf = BloomFilter.add(bf, "test_elem")

      calls = collect_messages()

      assert {:hash_call, "test_elem"} in calls,
             "hash_fn should be called with the raw element"

      assert {:hash_call, {:__approx_h2__, "test_elem"}} in calls,
             "hash_fn should be called with {:__approx_h2__, element} for the second hash"
    end

    test "large filter FPP is close to theoretical, not inflated by 16-bit hash collisions" do
      capacity = 100_000
      fpp = 0.01
      bf = BloomFilter.new(capacity, fpp)
      assert bf.size > 65_536

      bf =
        Enum.reduce(1..capacity, bf, fn i, acc ->
          BloomFilter.add(acc, {:member, i})
        end)

      test_count = 50_000

      false_positives =
        Enum.count(1..test_count, fn i ->
          BloomFilter.member?(bf, {:non_member, i})
        end)

      observed_fpp = false_positives / test_count

      # With the old 16-bit split, the effective bit space was capped at 65,536,
      # causing a much higher FPP. With the fix, observed FPP should be within
      # 2x of the theoretical rate.
      assert observed_fpp < fpp * 2,
             "Observed FPP #{Float.round(observed_fpp, 5)} exceeds 2x theoretical #{fpp} — " <>
               "possible regression to 16-bit hash splitting"
    end

    # --- Bug 2: serialization hash_count now 16-bit --------------------------------
    # The fix encodes hash_count as a 16-bit unsigned integer instead of 8-bit,
    # so hash_count values > 255 serialize and deserialize correctly.

    test "serialized binary matches format: <<version::8, size::big-64, hash_count::big-16, bits::binary>>" do
      bf = BloomFilter.new(500, 0.01)
      binary = BloomFilter.to_binary(bf)

      # Parse with the expected format
      <<version::8, size::big-unsigned-64, hash_count::big-unsigned-16, bits::binary>> = binary

      assert version == 1
      assert size == bf.size
      assert hash_count == bf.hash_count
      assert bits == bf.bits

      # Verify total size: 1 (version) + 8 (size) + 2 (hash_count) + byte_size(bits)
      assert byte_size(binary) == 1 + 8 + 2 + byte_size(bf.bits)
    end

    test "filter with hash_count > 255 serializes and deserializes correctly" do
      # Manually construct a struct with hash_count > 255 to test serialization.
      # We use a small bit array and a custom hash_fn for simplicity.
      custom_hash = fn term -> :erlang.phash2(term, bsl(1, 32)) end
      hash_count = 300

      bf = %BloomFilter{
        bits: <<0::size(1024)>>,
        size: 1024,
        hash_count: hash_count,
        hash_fn: custom_hash
      }

      binary = BloomFilter.to_binary(bf)

      # Parse and verify the hash_count field
      <<1::8, _size::big-unsigned-64, serialized_hash_count::big-unsigned-16, _bits::binary>> =
        binary

      assert serialized_hash_count == 300,
             "hash_count #{serialized_hash_count} should be 300 (would be 44 with 8-bit encoding)"

      # Round-trip deserialization
      {:ok, restored} = BloomFilter.from_binary(binary, hash_fn: custom_hash)
      assert restored.hash_count == 300
      assert restored.size == 1024
      assert restored.bits == bf.bits
    end

    # --- Bug 3: serialization size now 64-bit --------------------------------------
    # The fix encodes size as a 64-bit unsigned integer instead of 32-bit,
    # so very large filters (size > 2^32) can be serialized.

    test "large capacity filter serializes and deserializes with correct 64-bit size" do
      bf = BloomFilter.new(1_000_000, 0.01)

      binary = BloomFilter.to_binary(bf)
      {:ok, restored} = BloomFilter.from_binary(binary)

      assert restored.size == bf.size
      assert restored.hash_count == bf.hash_count
      assert restored.bits == bf.bits
    end

    test "from_binary can parse a size value larger than 2^32" do
      # Construct a binary manually with size > 2^32.
      # We cannot easily allocate a filter that large, but we can verify the
      # parser reads 64-bit size correctly and fails gracefully on the bits
      # length check (since we provide minimal bits data).
      large_size = bsl(1, 33) + 42
      hash_count = 7
      # Provide a single byte of bits — won't match expected_bytes so
      # from_binary should return {:error, :invalid_binary} because the
      # bits length doesn't match, but it proves the parser handled the
      # 64-bit size field without crashing.
      binary = <<1::8, large_size::big-unsigned-64, hash_count::big-unsigned-16, 0::8>>

      # With a 32-bit size field the parser would either crash or read
      # a garbled value. With 64-bit it correctly reads the large size
      # and then rejects based on bits length.
      assert {:error, :invalid_binary} = BloomFilter.from_binary(binary)
    end

    test "from_binary correctly round-trips a filter with size fitting in 64 bits" do
      # Use a moderate-large capacity that still fits in memory but whose size
      # value exercises the full 64-bit field (value > 2^16).
      bf = BloomFilter.new(100_000, 0.001)
      assert bf.size > 65_536

      binary = BloomFilter.to_binary(bf)

      # Verify the size field is read as 64-bit
      <<1::8, size_from_binary::big-unsigned-64, _rest::binary>> = binary
      assert size_from_binary == bf.size

      {:ok, restored} = BloomFilter.from_binary(binary)
      assert restored.size == bf.size
    end

    # --- Bug 4: from_binary rejects size=0 and hash_count=0 -----------------------

    test "from_binary rejects binary with size=0 and valid hash_count" do
      # Construct: version=1, size=0, hash_count=7, empty bits
      binary = <<1::8, 0::big-unsigned-64, 7::big-unsigned-16>>
      assert {:error, :invalid_binary} = BloomFilter.from_binary(binary)
    end

    test "from_binary rejects binary with valid size and hash_count=0" do
      # Construct: version=1, size=64, hash_count=0, 8 bytes of bits (64/8=8)
      bits = <<0::size(64)>>
      binary = <<1::8, 64::big-unsigned-64, 0::big-unsigned-16, bits::binary>>
      assert {:error, :invalid_binary} = BloomFilter.from_binary(binary)
    end

    test "from_binary rejects binary with both size=0 and hash_count=0" do
      binary = <<1::8, 0::big-unsigned-64, 0::big-unsigned-16>>
      assert {:error, :invalid_binary} = BloomFilter.from_binary(binary)
    end
  end

  # ---------------------------------------------------------------------------
  # Bug verification
  # ---------------------------------------------------------------------------

  describe "bug verification" do
    test "BUG VERIFICATION: different fpp values produce different filter sizes" do
      # The existing merge test wraps the assertion in `if bf1.size != bf2.size`
      # which could be vacuously true. Verify the precondition holds.
      bf1 = BloomFilter.new(100, 0.01)
      bf2 = BloomFilter.new(100, 0.001)

      # These should definitely have different sizes
      assert bf1.size != bf2.size,
             "Precondition violated: bf1.size=#{bf1.size} == bf2.size=#{bf2.size}. " <>
               "The conditional merge test would never assert!"

      # Now verify the merge correctly rejects them
      assert {:error, :incompatible_filters} = BloomFilter.merge(bf1, bf2)
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
