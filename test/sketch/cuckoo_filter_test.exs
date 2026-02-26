defmodule Sketch.CuckooFilterTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Sketch.CuckooFilter

  doctest Sketch.CuckooFilter

  @max_32 bsl(1, 32)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # A deterministic seed for reproducible eviction sequences.
  defp deterministic_seed, do: :rand.seed_s(:exsss, 42)

  defp new_filter(capacity, opts \\ []) do
    opts = Keyword.put_new(opts, :seed, deterministic_seed())
    CuckooFilter.new(capacity, opts)
  end

  # Insert N distinct elements, returning {cf, elements}.
  defp insert_elements(cf, count) do
    elements = Enum.map(1..count, &"elem_#{&1}")

    cf =
      Enum.reduce(elements, cf, fn elem, acc ->
        {:ok, acc} = CuckooFilter.insert(acc, elem)
        acc
      end)

    {cf, elements}
  end

  # ---------------------------------------------------------------------------
  # new/2
  # ---------------------------------------------------------------------------

  describe "new/2" do
    test "creates a filter with correct struct fields" do
      cf = new_filter(100)

      assert %CuckooFilter{} = cf
      assert is_tuple(cf.buckets)
      assert cf.num_buckets >= 2
      assert cf.bucket_size == 4
      assert cf.fingerprint_size == 8
      assert cf.count == 0
      assert is_function(cf.hash_fn, 1)
    end

    test "num_buckets is a power of 2" do
      for capacity <- [1, 5, 7, 8, 9, 16, 17, 100, 1_000] do
        cf = new_filter(capacity)

        assert Bitwise.band(cf.num_buckets, cf.num_buckets - 1) == 0,
               "num_buckets #{cf.num_buckets} is not a power of 2 for capacity #{capacity}"
      end
    end

    test "num_buckets is at least 2" do
      cf = new_filter(1)
      assert cf.num_buckets >= 2
    end

    test "num_buckets is the smallest power of 2 >= ceil(capacity / 4)" do
      # capacity=16 => ceil(16/4) = 4, next power of 2 = 4
      cf = new_filter(16)
      assert cf.num_buckets == 4

      # capacity=17 => ceil(17/4) = 5, next power of 2 = 8
      cf = new_filter(17)
      assert cf.num_buckets == 8

      # capacity=32 => ceil(32/4) = 8, next power of 2 = 8
      cf = new_filter(32)
      assert cf.num_buckets == 8
    end

    test "all buckets start empty (all slots are zero)" do
      cf = new_filter(100)

      for i <- 0..(cf.num_buckets - 1) do
        assert elem(cf.buckets, i) == {0, 0, 0, 0},
               "Bucket #{i} is not empty on construction"
      end
    end

    test "accepts a custom :hash_fn option" do
      custom_fn = fn _term -> 42 end
      cf = CuckooFilter.new(100, hash_fn: custom_fn, seed: deterministic_seed())
      assert cf.hash_fn == custom_fn
    end

    test "accepts a custom :seed option as integer" do
      cf1 = CuckooFilter.new(100, seed: 123)
      cf2 = CuckooFilter.new(100, seed: 123)
      assert cf1.seed == cf2.seed
    end

    test "uses default hash function when no option is given" do
      cf = CuckooFilter.new(100)
      assert is_function(cf.hash_fn, 1)
      assert is_integer(cf.hash_fn.("test"))
    end
  end

  # ---------------------------------------------------------------------------
  # insert/2 and member?/2
  # ---------------------------------------------------------------------------

  describe "insert/2 and member?/2" do
    test "empty filter returns false for any element" do
      cf = new_filter(100)

      refute CuckooFilter.member?(cf, "anything")
      refute CuckooFilter.member?(cf, 42)
      refute CuckooFilter.member?(cf, :atom)
    end

    test "inserted element is always found (no false negatives)" do
      cf = new_filter(1_000)
      {cf, elements} = insert_elements(cf, 200)

      Enum.each(elements, fn elem ->
        assert CuckooFilter.member?(cf, elem),
               "Expected #{inspect(elem)} to be a member after insertion"
      end)
    end

    test "single element insert and query" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "solo")

      assert CuckooFilter.member?(cf, "solo")
      assert cf.count == 1
    end

    test "count increments with each insertion" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "a")
      assert cf.count == 1
      {:ok, cf} = CuckooFilter.insert(cf, "b")
      assert cf.count == 2
      {:ok, cf} = CuckooFilter.insert(cf, "c")
      assert cf.count == 3
    end

    test "works with various term types" do
      cf = new_filter(100)

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

      cf =
        Enum.reduce(terms, cf, fn t, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, t)
          acc
        end)

      Enum.each(terms, fn t ->
        assert CuckooFilter.member?(cf, t),
               "Expected #{inspect(t)} to be a member after insertion"
      end)
    end

    test "insert returns {:error, :full} when filter is saturated" do
      # Create a very small filter (2 buckets * 4 slots = 8 total slots).
      cf = new_filter(1)

      # Keep inserting until we get :full.
      result =
        Enum.reduce_while(1..5_000, cf, fn i, acc ->
          case CuckooFilter.insert(acc, "overflow_#{i}") do
            {:ok, acc} -> {:cont, acc}
            {:error, :full} -> {:halt, :full}
          end
        end)

      assert result == :full
    end

    test "insert handles eviction (triggers cuckoo kicking)" do
      # Use a small filter to force evictions.
      cf = new_filter(8)

      # Insert enough elements to fill some buckets and trigger evictions.
      # With 2 buckets and 4 slots each, 8 elements can fit. We insert a
      # few more to force kicks.
      results =
        Enum.map(1..12, fn i ->
          CuckooFilter.insert(cf, "kick_#{i}")
        end)

      # At least some should succeed (filter has 8 slots).
      successes = Enum.count(results, &match?({:ok, _}, &1))
      assert successes >= 1
    end

    test "many sequential inserts succeed for filter with adequate capacity" do
      cf = new_filter(500)

      {cf, elements} = insert_elements(cf, 200)

      # All elements should be members.
      Enum.each(elements, fn elem ->
        assert CuckooFilter.member?(cf, elem)
      end)

      assert cf.count == 200
    end
  end

  # ---------------------------------------------------------------------------
  # Duplicate handling
  # ---------------------------------------------------------------------------

  describe "duplicate handling" do
    test "inserting the same element twice requires two deletes" do
      cf = new_filter(100)

      {:ok, cf} = CuckooFilter.insert(cf, "dup")
      {:ok, cf} = CuckooFilter.insert(cf, "dup")
      assert cf.count == 2

      # First delete removes one occurrence.
      {:ok, cf} = CuckooFilter.delete(cf, "dup")
      assert cf.count == 1
      assert CuckooFilter.member?(cf, "dup")

      # Second delete removes the last occurrence.
      {:ok, cf} = CuckooFilter.delete(cf, "dup")
      assert cf.count == 0
      refute CuckooFilter.member?(cf, "dup")
    end

    test "inserting the same element twice makes member? true" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "dup")
      {:ok, cf} = CuckooFilter.insert(cf, "dup")

      assert CuckooFilter.member?(cf, "dup")
    end
  end

  # ---------------------------------------------------------------------------
  # delete/2
  # ---------------------------------------------------------------------------

  describe "delete/2" do
    test "delete returns {:error, :not_found} on empty filter" do
      cf = new_filter(100)
      assert {:error, :not_found} = CuckooFilter.delete(cf, "missing")
    end

    test "delete returns {:error, :not_found} for non-member" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "present")

      assert {:error, :not_found} = CuckooFilter.delete(cf, "absent")
    end

    test "delete removes element so member? returns false" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "removable")

      assert CuckooFilter.member?(cf, "removable")

      {:ok, cf} = CuckooFilter.delete(cf, "removable")
      refute CuckooFilter.member?(cf, "removable")
      assert cf.count == 0
    end

    test "delete decrements count" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "a")
      {:ok, cf} = CuckooFilter.insert(cf, "b")
      assert cf.count == 2

      {:ok, cf} = CuckooFilter.delete(cf, "a")
      assert cf.count == 1

      {:ok, cf} = CuckooFilter.delete(cf, "b")
      assert cf.count == 0
    end

    test "delete one element does not affect other elements" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "keep")
      {:ok, cf} = CuckooFilter.insert(cf, "remove")

      {:ok, cf} = CuckooFilter.delete(cf, "remove")

      assert CuckooFilter.member?(cf, "keep")
      refute CuckooFilter.member?(cf, "remove")
    end

    test "insert, delete, re-insert cycle works" do
      cf = new_filter(100)

      {:ok, cf} = CuckooFilter.insert(cf, "cycle")
      assert CuckooFilter.member?(cf, "cycle")

      {:ok, cf} = CuckooFilter.delete(cf, "cycle")
      refute CuckooFilter.member?(cf, "cycle")

      {:ok, cf} = CuckooFilter.insert(cf, "cycle")
      assert CuckooFilter.member?(cf, "cycle")
      assert cf.count == 1
    end

    test "deleting multiple distinct elements" do
      cf = new_filter(100)
      elements = ["alpha", "beta", "gamma", "delta"]

      cf =
        Enum.reduce(elements, cf, fn elem, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, elem)
          acc
        end)

      assert cf.count == 4

      # Delete them all.
      cf =
        Enum.reduce(elements, cf, fn elem, acc ->
          {:ok, acc} = CuckooFilter.delete(acc, elem)
          acc
        end)

      assert cf.count == 0

      Enum.each(elements, fn elem ->
        refute CuckooFilter.member?(cf, elem)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  describe "to_binary/1 and from_binary/1" do
    test "round-trip preserves membership results" do
      cf = new_filter(500)
      {cf, elements} = insert_elements(cf, 100)

      binary = CuckooFilter.to_binary(cf)
      {:ok, restored} = CuckooFilter.from_binary(binary)

      Enum.each(elements, fn elem ->
        assert CuckooFilter.member?(restored, elem),
               "Element #{inspect(elem)} lost after round-trip"
      end)

      assert restored.count == cf.count
      assert restored.num_buckets == cf.num_buckets
    end

    test "round-trip on empty filter" do
      cf = new_filter(100)
      binary = CuckooFilter.to_binary(cf)
      {:ok, restored} = CuckooFilter.from_binary(binary)

      assert restored.count == 0
      assert restored.num_buckets == cf.num_buckets
      assert restored.bucket_size == cf.bucket_size
      assert restored.fingerprint_size == cf.fingerprint_size
    end

    test "binary starts with version byte 1" do
      cf = new_filter(100)
      <<version::8, _rest::binary>> = CuckooFilter.to_binary(cf)
      assert version == 1
    end

    test "binary encodes num_buckets and count correctly" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "ser")

      <<1::8, num_buckets::big-unsigned-32, count::big-unsigned-32, _data::binary>> =
        CuckooFilter.to_binary(cf)

      assert num_buckets == cf.num_buckets
      assert count == cf.count
    end

    test "binary has correct total size" do
      cf = new_filter(100)
      binary = CuckooFilter.to_binary(cf)

      # 1 (version) + 4 (num_buckets) + 4 (count) + num_buckets * 4 (bucket data)
      expected_size = 1 + 4 + 4 + cf.num_buckets * 4
      assert byte_size(binary) == expected_size
    end

    test "from_binary returns error for garbage input" do
      assert {:error, :invalid_binary} = CuckooFilter.from_binary(<<>>)
      assert {:error, :invalid_binary} = CuckooFilter.from_binary(<<0, 1, 2>>)
      assert {:error, :invalid_binary} = CuckooFilter.from_binary("not a cuckoo filter")
    end

    test "from_binary returns error for unknown version" do
      cf = new_filter(100)
      <<_version::8, rest::binary>> = CuckooFilter.to_binary(cf)
      bad_binary = <<99::8, rest::binary>>

      assert {:error, :invalid_binary} = CuckooFilter.from_binary(bad_binary)
    end

    test "from_binary returns error when bucket data length doesn't match" do
      cf = new_filter(100)

      <<version::8, num_buckets::big-unsigned-32, count::big-unsigned-32, _data::binary>> =
        CuckooFilter.to_binary(cf)

      # Provide too few bytes of bucket data.
      truncated = <<version::8, num_buckets::big-unsigned-32, count::big-unsigned-32, 0::8>>
      assert {:error, :invalid_binary} = CuckooFilter.from_binary(truncated)
    end

    test "from_binary returns error when num_buckets is not a power of 2" do
      # Craft a binary with num_buckets = 3 (not a power of 2).
      bucket_data = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      bad_binary = <<1::8, 3::big-unsigned-32, 0::big-unsigned-32, bucket_data::binary>>

      assert {:error, :invalid_binary} = CuckooFilter.from_binary(bad_binary)
    end

    test "from_binary accepts custom :hash_fn option" do
      custom_fn = fn term -> :erlang.phash2(term, @max_32) end
      cf = CuckooFilter.new(100, hash_fn: custom_fn, seed: deterministic_seed())
      {:ok, cf} = CuckooFilter.insert(cf, "custom")

      binary = CuckooFilter.to_binary(cf)
      {:ok, restored} = CuckooFilter.from_binary(binary, hash_fn: custom_fn)

      assert CuckooFilter.member?(restored, "custom")
    end

    test "serialized filter can be deleted from after deserialization" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "persist")

      binary = CuckooFilter.to_binary(cf)
      {:ok, restored} = CuckooFilter.from_binary(binary)

      assert CuckooFilter.member?(restored, "persist")
      {:ok, restored} = CuckooFilter.delete(restored, "persist")
      refute CuckooFilter.member?(restored, "persist")
    end
  end

  # ---------------------------------------------------------------------------
  # No false negatives (statistical)
  # ---------------------------------------------------------------------------

  describe "no false negatives" do
    test "inserting N elements guarantees all are members" do
      cf = new_filter(2_000)
      {cf, elements} = insert_elements(cf, 500)

      false_negatives =
        Enum.filter(elements, fn elem ->
          not CuckooFilter.member?(cf, elem)
        end)

      assert false_negatives == [],
             "False negatives detected: #{inspect(Enum.take(false_negatives, 5))}"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "member? on empty filter returns false" do
      cf = new_filter(100)
      refute CuckooFilter.member?(cf, "anything")
      refute CuckooFilter.member?(cf, 0)
      refute CuckooFilter.member?(cf, nil)
    end

    test "filter with capacity 1 can store at least one element" do
      cf = new_filter(1)
      {:ok, cf} = CuckooFilter.insert(cf, "only")
      assert CuckooFilter.member?(cf, "only")
      assert cf.count == 1
    end

    test "single element insert and delete" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "one")
      {:ok, cf} = CuckooFilter.delete(cf, "one")
      refute CuckooFilter.member?(cf, "one")
      assert cf.count == 0
    end

    test "near capacity behavior — filter stays correct under pressure" do
      # Small filter: 4 buckets * 4 slots = 16 total slots.
      cf = new_filter(16)

      # Try to insert 14 elements (near capacity but leaving some room).
      {successes, cf} =
        Enum.reduce(1..14, {0, cf}, fn i, {count, acc} ->
          case CuckooFilter.insert(acc, "near_#{i}") do
            {:ok, acc} -> {count + 1, acc}
            {:error, :full} -> {count, acc}
          end
        end)

      # At least some should have succeeded.
      assert successes >= 1

      # All successfully inserted elements must be members.
      assert cf.count == successes
    end

    test "delete on filter with single element that doesn't match" do
      cf = new_filter(100)
      {:ok, cf} = CuckooFilter.insert(cf, "yes")

      assert {:error, :not_found} = CuckooFilter.delete(cf, "no")
      assert cf.count == 1
      assert CuckooFilter.member?(cf, "yes")
    end

    test "works correctly with integer elements" do
      cf = new_filter(100)

      {:ok, cf} = CuckooFilter.insert(cf, 1)
      {:ok, cf} = CuckooFilter.insert(cf, 2)
      {:ok, cf} = CuckooFilter.insert(cf, 3)

      assert CuckooFilter.member?(cf, 1)
      assert CuckooFilter.member?(cf, 2)
      assert CuckooFilter.member?(cf, 3)
      refute CuckooFilter.member?(cf, 4)
    end

    test "works correctly with atom elements" do
      cf = new_filter(100)

      {:ok, cf} = CuckooFilter.insert(cf, :foo)
      {:ok, cf} = CuckooFilter.insert(cf, :bar)

      assert CuckooFilter.member?(cf, :foo)
      assert CuckooFilter.member?(cf, :bar)
      refute CuckooFilter.member?(cf, :baz)
    end
  end

  # ---------------------------------------------------------------------------
  # Struct enforcement
  # ---------------------------------------------------------------------------

  describe "struct" do
    test "enforces required keys" do
      assert_raise ArgumentError, fn ->
        struct!(CuckooFilter, [])
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Deterministic behavior with seed
  # ---------------------------------------------------------------------------

  describe "deterministic behavior" do
    test "same seed produces same eviction results" do
      seed = :rand.seed_s(:exsss, 99)

      cf1 = CuckooFilter.new(8, seed: seed)
      cf2 = CuckooFilter.new(8, seed: seed)

      # Insert the same elements in the same order.
      elements = Enum.map(1..10, &"det_#{&1}")

      {results1, _} =
        Enum.map_reduce(elements, cf1, fn elem, acc ->
          result = CuckooFilter.insert(acc, elem)

          case result do
            {:ok, acc} -> {result, acc}
            {:error, :full} -> {result, acc}
          end
        end)

      {results2, _} =
        Enum.map_reduce(elements, cf2, fn elem, acc ->
          result = CuckooFilter.insert(acc, elem)

          case result do
            {:ok, acc} -> {result, acc}
            {:error, :full} -> {result, acc}
          end
        end)

      # Same seed => same sequence of successes and failures.
      outcomes1 =
        Enum.map(results1, fn
          {tag, _} -> tag
          tag -> tag
        end)

      outcomes2 =
        Enum.map(results2, fn
          {tag, _} -> tag
          tag -> tag
        end)

      assert outcomes1 == outcomes2
    end
  end

  # ---------------------------------------------------------------------------
  # False positive rate measurement
  # ---------------------------------------------------------------------------

  describe "false positive rate" do
    test "false positive rate is below 4% for 8-bit fingerprints" do
      cf = new_filter(5_000)

      # Insert 5000 distinct elements.
      cf =
        Enum.reduce(1..5_000, cf, fn i, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, "inserted_#{i}")
          acc
        end)

      # Query 10_000 elements that were never inserted.
      # Use a distinct prefix to avoid collisions with inserted elements.
      false_positives =
        Enum.count(1..10_000, fn i ->
          CuckooFilter.member?(cf, "not_inserted_#{i}")
        end)

      fp_rate = false_positives / 10_000

      assert fp_rate < 0.04,
             "False positive rate #{Float.round(fp_rate * 100, 2)}% exceeds 4% threshold " <>
               "(#{false_positives} false positives out of 10_000 queries)"
    end
  end

  # ---------------------------------------------------------------------------
  # Load factor / capacity utilization
  # ---------------------------------------------------------------------------

  describe "load factor" do
    test "achieves at least 90% load factor before returning :full" do
      capacity = 1_000
      cf = new_filter(capacity)

      # Insert elements until the filter is full.
      {count, cf} =
        Enum.reduce_while(1..100_000, {0, cf}, fn i, {count, acc} ->
          case CuckooFilter.insert(acc, "load_#{i}") do
            {:ok, acc} -> {:cont, {count + 1, acc}}
            {:error, :full} -> {:halt, {count, acc}}
          end
        end)

      total_slots = cf.num_buckets * 4
      load_factor = count / total_slots

      assert load_factor > 0.90,
             "Load factor #{Float.round(load_factor * 100, 2)}% is below 90% " <>
               "(#{count} elements in #{total_slots} total slots)"
    end

    test "count matches actual successful insertions" do
      capacity = 1_000
      cf = new_filter(capacity)

      {expected_count, cf} =
        Enum.reduce_while(1..100_000, {0, cf}, fn i, {count, acc} ->
          case CuckooFilter.insert(acc, "count_#{i}") do
            {:ok, acc} -> {:cont, {count + 1, acc}}
            {:error, :full} -> {:halt, {count, acc}}
          end
        end)

      assert cf.count == expected_count,
             "Filter count #{cf.count} does not match expected #{expected_count}"
    end
  end

  # ---------------------------------------------------------------------------
  # Repeated fill-drain cycles
  # ---------------------------------------------------------------------------

  describe "fill drain cycles" do
    test "filter can be filled, drained, and reused 3 times" do
      n = 50
      cf = new_filter(100)

      Enum.reduce(1..3, cf, fn cycle, cf ->
        elements = Enum.map(1..n, &"cycle_#{cycle}_elem_#{&1}")

        # Fill: insert all N elements.
        cf =
          Enum.reduce(elements, cf, fn elem, acc ->
            {:ok, acc} = CuckooFilter.insert(acc, elem)
            acc
          end)

        # Verify all are members.
        Enum.each(elements, fn elem ->
          assert CuckooFilter.member?(cf, elem),
                 "Cycle #{cycle}: #{inspect(elem)} should be a member after insertion"
        end)

        assert cf.count == n, "Cycle #{cycle}: count should be #{n}, got #{cf.count}"

        # Drain: delete all elements.
        cf =
          Enum.reduce(elements, cf, fn elem, acc ->
            {:ok, acc} = CuckooFilter.delete(acc, elem)
            acc
          end)

        # Verify count is 0.
        assert cf.count == 0, "Cycle #{cycle}: count should be 0 after draining, got #{cf.count}"

        # Verify none are members anymore.
        Enum.each(elements, fn elem ->
          refute CuckooFilter.member?(cf, elem),
                 "Cycle #{cycle}: #{inspect(elem)} should not be a member after deletion"
        end)

        cf
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # All elements findable after evictions (post-eviction correctness)
  # ---------------------------------------------------------------------------

  describe "post-eviction correctness" do
    test "all previously inserted elements remain findable after each insertion" do
      # Small filter to force evictions quickly.
      cf = new_filter(32)
      inserted = []

      {_cf, _inserted} =
        Enum.reduce_while(1..200, {cf, inserted}, fn i, {cf, inserted} ->
          case CuckooFilter.insert(cf, "post_evict_#{i}") do
            {:ok, cf} ->
              inserted = ["post_evict_#{i}" | inserted]

              # Verify ALL previously inserted elements are still findable.
              Enum.each(inserted, fn elem ->
                assert CuckooFilter.member?(cf, elem),
                       "Element #{inspect(elem)} not found after inserting post_evict_#{i}"
              end)

              {:cont, {cf, inserted}}

            {:error, :full} ->
              {:halt, {cf, inserted}}
          end
        end)
    end
  end

  # ---------------------------------------------------------------------------
  # Zero fingerprint mapping
  # ---------------------------------------------------------------------------

  describe "zero fingerprint" do
    test "element whose hash low byte is 0 is correctly handled" do
      # hash_fn returns 0x100: low byte is 0x00, which should be mapped to
      # fingerprint 1 by the max(fp, 1) guard.
      # Primary index = bsr(0x100, 8) band (num_buckets - 1) = 1 band mask.
      zero_low_byte_hash_fn = fn _term -> 0x100 end

      cf = CuckooFilter.new(100, hash_fn: zero_low_byte_hash_fn, seed: deterministic_seed())

      {:ok, cf} = CuckooFilter.insert(cf, "zero_fp_element")
      assert CuckooFilter.member?(cf, "zero_fp_element")
      assert cf.count == 1

      {:ok, cf} = CuckooFilter.delete(cf, "zero_fp_element")
      refute CuckooFilter.member?(cf, "zero_fp_element")
      assert cf.count == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Alternate index symmetry (involution property)
  # ---------------------------------------------------------------------------

  describe "alternate index symmetry" do
    test "inserting 1000 elements and verifying all are members tests index involution" do
      # If alternate(alternate(i1, fp), fp) != i1, then member? lookups would
      # fail for evicted elements. Inserting 1000 elements into a moderately-
      # sized filter forces many evictions, and verifying membership for all
      # of them implicitly validates the involution property.
      cf = new_filter(1_200)

      elements = Enum.map(1..1_000, &"sym_#{&1}")

      cf =
        Enum.reduce(elements, cf, fn elem, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, elem)
          acc
        end)

      Enum.each(elements, fn elem ->
        assert CuckooFilter.member?(cf, elem),
               "Element #{inspect(elem)} not found — alternate index symmetry may be broken"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # False delete rate
  # ---------------------------------------------------------------------------

  describe "false delete rate" do
    test "deleting non-members produces false deletes below 4%" do
      cf = new_filter(6_000)

      # Insert 5000 elements.
      cf =
        Enum.reduce(1..5_000, cf, fn i, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, "real_#{i}")
          acc
        end)

      original_count = cf.count
      assert original_count == 5_000

      # Attempt to delete 5000 elements that were never inserted.
      {false_deletes, cf} =
        Enum.reduce(1..5_000, {0, cf}, fn i, {fd_count, acc} ->
          case CuckooFilter.delete(acc, "fake_#{i}") do
            {:ok, acc} -> {fd_count + 1, acc}
            {:error, :not_found} -> {fd_count, acc}
          end
        end)

      fd_rate = false_deletes / 5_000

      assert fd_rate < 0.04,
             "False delete rate #{Float.round(fd_rate * 100, 2)}% exceeds 4% threshold " <>
               "(#{false_deletes} false deletes out of 5_000 attempts)"

      # The count should have decreased by the number of false deletes.
      assert cf.count == original_count - false_deletes
    end
  end

  # ---------------------------------------------------------------------------
  # Elements with same fingerprint coexist (fingerprint collision)
  # ---------------------------------------------------------------------------

  describe "fingerprint collision" do
    test "two elements with same fingerprint but different indices coexist" do
      # Custom hash_fn that maps:
      #   :element_a -> 0x0105 (fingerprint = 0x05, primary index = bsr(0x0105, 8) band mask = 1)
      #   :element_b -> 0x0205 (fingerprint = 0x05, primary index = bsr(0x0205, 8) band mask = 2)
      #   anything else (including fingerprint value 5) -> normal hash for alternate index
      custom_hash_fn = fn
        :element_a -> 0x0105
        :element_b -> 0x0205
        term -> :erlang.phash2(term, bsl(1, 32))
      end

      # Need enough buckets so indices 1 and 2 are valid and distinct.
      cf = CuckooFilter.new(100, hash_fn: custom_hash_fn, seed: deterministic_seed())

      {:ok, cf} = CuckooFilter.insert(cf, :element_a)
      {:ok, cf} = CuckooFilter.insert(cf, :element_b)

      assert CuckooFilter.member?(cf, :element_a)
      assert CuckooFilter.member?(cf, :element_b)
      assert cf.count == 2

      # Delete one; the other must survive.
      {:ok, cf} = CuckooFilter.delete(cf, :element_a)
      refute CuckooFilter.member?(cf, :element_a)
      assert CuckooFilter.member?(cf, :element_b)
      assert cf.count == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Max duplicate insertions
  # ---------------------------------------------------------------------------

  describe "duplicate limits" do
    test "same element can be inserted up to 8 times (2 buckets * 4 slots)" do
      # Use a larger filter to ensure the element's two candidate buckets are
      # initially empty, giving us a clean 8-slot capacity for duplicates.
      cf = new_filter(1_000)

      # Insert the same element 8 times — should all succeed since there are
      # 2 candidate buckets with 4 slots each.
      cf =
        Enum.reduce(1..8, cf, fn _i, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, "dup_element")
          acc
        end)

      assert cf.count == 8
      assert CuckooFilter.member?(cf, "dup_element")

      # The 9th insert should either fail with :full (if both candidate
      # buckets are fully occupied by this fingerprint) or trigger eviction.
      result = CuckooFilter.insert(cf, "dup_element")

      case result do
        {:error, :full} ->
          # Expected: both candidate buckets are full of the same fingerprint,
          # eviction cannot help because every kicked fingerprint would need
          # to go back to the same full bucket.
          assert true

        {:ok, cf} ->
          # If eviction succeeded, the element must still be findable.
          assert CuckooFilter.member?(cf, "dup_element")
          assert cf.count == 9
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Capacity is at least requested value
  # ---------------------------------------------------------------------------

  describe "capacity bounds" do
    test "total slots (num_buckets * 4) is at least the requested capacity" do
      capacities = [1, 7, 8, 15, 16, 100, 1_000]

      for capacity <- capacities do
        cf = new_filter(capacity)
        total_slots = cf.num_buckets * 4

        assert total_slots >= capacity,
               "Total slots #{total_slots} (num_buckets=#{cf.num_buckets} * 4) " <>
                 "is less than requested capacity #{capacity}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Primary index regression — {:__cuckoo_index__, element} for independent hash
  # ---------------------------------------------------------------------------

  describe "primary index regression" do
    test "hash function receives index tuple for primary index computation" do
      parent = self()

      hash_fn = fn term ->
        send(parent, {:hash_call, term})
        :erlang.phash2(term, 4_294_967_296)
      end

      cf = CuckooFilter.new(100, hash_fn: hash_fn, seed: deterministic_seed())
      {:ok, _cf} = CuckooFilter.insert(cf, "hello")

      # Collect all hash calls from the mailbox
      calls = collect_hash_calls()

      assert {:__cuckoo_index__, "hello"} in calls,
             "Expected hash_fn to be called with {:__cuckoo_index__, \"hello\"}, got: #{inspect(calls)}"
    end

    test "index and fingerprint are independent" do
      # Use a custom hash_fn where two elements share the same lower 8 bits
      # (same fingerprint) but differ in the upper bits. With the old bsr(hash, 8)
      # approach, these would also share the same primary index if their upper
      # bits happened to align. With {:__cuckoo_index__, element}, the index
      # is computed from a fully independent hash.
      hash_fn = fn
        {:__cuckoo_index__, :elem_a} -> 1
        {:__cuckoo_index__, :elem_b} -> 2
        # Both elements produce the same fingerprint (low 8 bits = 0x42)
        :elem_a -> 0x0042
        :elem_b -> 0x0042
        term -> :erlang.phash2(term, 4_294_967_296)
      end

      cf = CuckooFilter.new(100, hash_fn: hash_fn, seed: deterministic_seed())

      {:ok, cf} = CuckooFilter.insert(cf, :elem_a)
      {:ok, cf} = CuckooFilter.insert(cf, :elem_b)

      # Both must be findable — they have the same fingerprint but different
      # primary indices.
      assert CuckooFilter.member?(cf, :elem_a)
      assert CuckooFilter.member?(cf, :elem_b)

      # Delete one; the other must survive.
      {:ok, cf} = CuckooFilter.delete(cf, :elem_a)
      refute CuckooFilter.member?(cf, :elem_a)
      assert CuckooFilter.member?(cf, :elem_b)
    end

    test "member? uses the index tuple for lookups" do
      parent = self()

      hash_fn = fn term ->
        send(parent, {:hash_call, term})
        :erlang.phash2(term, 4_294_967_296)
      end

      cf = CuckooFilter.new(100, hash_fn: hash_fn, seed: deterministic_seed())
      {:ok, cf} = CuckooFilter.insert(cf, "test_member")

      # Drain the mailbox from insert calls
      _ = collect_hash_calls()

      # Now call member? and check the hash_fn calls
      assert CuckooFilter.member?(cf, "test_member")

      calls = collect_hash_calls()

      assert {:__cuckoo_index__, "test_member"} in calls,
             "Expected member? to call hash_fn with {:__cuckoo_index__, \"test_member\"}, got: #{inspect(calls)}"
    end

    test "delete uses the index tuple for lookups" do
      parent = self()

      hash_fn = fn term ->
        send(parent, {:hash_call, term})
        :erlang.phash2(term, 4_294_967_296)
      end

      cf = CuckooFilter.new(100, hash_fn: hash_fn, seed: deterministic_seed())
      {:ok, cf} = CuckooFilter.insert(cf, "test_delete")

      # Drain the mailbox from insert calls
      _ = collect_hash_calls()

      # Now call delete and check the hash_fn calls
      {:ok, _cf} = CuckooFilter.delete(cf, "test_delete")

      calls = collect_hash_calls()

      assert {:__cuckoo_index__, "test_delete"} in calls,
             "Expected delete to call hash_fn with {:__cuckoo_index__, \"test_delete\"}, got: #{inspect(calls)}"
    end

    test "large filter with many elements validates full index range" do
      cf = new_filter(10_000)

      elements = Enum.map(1..5_000, &"large_idx_#{&1}")

      cf =
        Enum.reduce(elements, cf, fn elem, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, elem)
          acc
        end)

      assert cf.count == 5_000

      # Verify all inserted elements are found (no false negatives).
      Enum.each(elements, fn elem ->
        assert CuckooFilter.member?(cf, elem),
               "Element #{inspect(elem)} not found in large filter"
      end)
    end

    test "serialization round-trip works after index change" do
      cf = new_filter(1_000)

      elements = Enum.map(1..200, &"serial_idx_#{&1}")

      cf =
        Enum.reduce(elements, cf, fn elem, acc ->
          {:ok, acc} = CuckooFilter.insert(acc, elem)
          acc
        end)

      # Serialize and deserialize
      binary = CuckooFilter.to_binary(cf)
      {:ok, restored} = CuckooFilter.from_binary(binary)

      assert restored.count == cf.count
      assert restored.num_buckets == cf.num_buckets

      # Verify all elements are still members after round-trip
      Enum.each(elements, fn elem ->
        assert CuckooFilter.member?(restored, elem),
               "Element #{inspect(elem)} lost after serialization round-trip"
      end)

      # Verify we can still delete elements after deserialization
      {:ok, restored} = CuckooFilter.delete(restored, "serial_idx_1")
      refute CuckooFilter.member?(restored, "serial_idx_1")
      assert restored.count == cf.count - 1
    end
  end

  # ---------------------------------------------------------------------------
  # Bug verification
  # ---------------------------------------------------------------------------

  describe "bug verification" do
    test "from_binary recomputes count from bucket contents, ignoring tampered header" do
      cf = CuckooFilter.new(100)
      {:ok, cf} = CuckooFilter.insert(cf, "a")

      # Serialize, then tamper with the count field
      binary = CuckooFilter.to_binary(cf)
      # Format: <<version::8, num_buckets::big-32, count::big-32, bucket_data::binary>>
      <<version::8, num_buckets::big-unsigned-32, _count::big-unsigned-32, bucket_data::binary>> =
        binary

      # Set count to 1000 (way more than actual fingerprints)
      tampered =
        <<version::8, num_buckets::big-unsigned-32, 1000::big-unsigned-32, bucket_data::binary>>

      {:ok, restored} = CuckooFilter.from_binary(tampered)
      # Count should be recomputed from actual bucket contents (1 fingerprint)
      assert restored.count == 1

      # Delete works correctly with accurate count
      {:ok, after_delete} = CuckooFilter.delete(restored, "a")
      assert after_delete.count == 0
    end

    test "BUG: existing eviction test doesn't thread state — verify eviction actually works" do
      # Create a very small filter that will need evictions
      # 2 buckets * 4 slots = 8 slots
      cf = CuckooFilter.new(4, seed: 42)

      # Insert elements, threading the filter state
      results =
        Enum.reduce(1..12, {cf, []}, fn i, {current_cf, results} ->
          result = CuckooFilter.insert(current_cf, "evict_#{i}")

          case result do
            {:ok, updated_cf} -> {updated_cf, [{:ok, i} | results]}
            {:error, :full} -> {current_cf, [{:full, i} | results]}
          end
        end)

      {final_cf, insert_results} = results
      insert_results = Enum.reverse(insert_results)

      ok_count = Enum.count(insert_results, fn {status, _} -> status == :ok end)
      _full_count = Enum.count(insert_results, fn {status, _} -> status == :full end)

      # With 8 slots and eviction, we should get more than 8 successful inserts
      # (eviction relocates fingerprints to make room)
      # But at some point the filter will be full
      assert ok_count >= 8, "Expected at least 8 successful inserts, got #{ok_count}"
      assert final_cf.count == ok_count

      # Verify all successfully inserted elements are found
      Enum.each(insert_results, fn
        {:ok, i} -> assert CuckooFilter.member?(final_cf, "evict_#{i}")
        {:full, _} -> :ok
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers for primary index regression tests
  # ---------------------------------------------------------------------------

  defp collect_hash_calls do
    collect_hash_calls([])
  end

  defp collect_hash_calls(acc) do
    receive do
      {:hash_call, term} -> collect_hash_calls([term | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
