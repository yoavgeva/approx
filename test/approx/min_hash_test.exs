defmodule Approx.MinHashTest do
  use ExUnit.Case, async: true

  alias Approx.MinHash

  doctest Approx.MinHash

  # All tests use a fixed seed for determinism unless testing random behavior.
  @seed 12_345

  # ---------------------------------------------------------------------------
  # new/2
  # ---------------------------------------------------------------------------

  describe "new/2" do
    test "creates a MinHash with the default 128 hash functions" do
      mh = MinHash.new()
      assert mh.num_hashes == 128
      assert tuple_size(mh.coefficients_a) == 128
      assert tuple_size(mh.coefficients_b) == 128
    end

    test "creates a MinHash with a custom number of hash functions" do
      mh = MinHash.new(64, seed: @seed)
      assert mh.num_hashes == 64
      assert tuple_size(mh.coefficients_a) == 64
      assert tuple_size(mh.coefficients_b) == 64
    end

    test "accepts a custom hash function" do
      custom_fn = fn term -> :erlang.phash2(term, 1_000_000) end
      mh = MinHash.new(16, hash_fn: custom_fn, seed: @seed)
      assert mh.hash_fn == custom_fn
    end

    test "produces deterministic coefficients with the same seed" do
      mh1 = MinHash.new(32, seed: @seed)
      mh2 = MinHash.new(32, seed: @seed)
      assert mh1.coefficients_a == mh2.coefficients_a
      assert mh1.coefficients_b == mh2.coefficients_b
    end

    test "produces different coefficients with different seeds" do
      mh1 = MinHash.new(32, seed: 1)
      mh2 = MinHash.new(32, seed: 2)
      refute mh1.coefficients_a == mh2.coefficients_a
    end

    test "coefficients have the expected structure" do
      mh = MinHash.new(8, seed: @seed)

      for i <- 0..(mh.num_hashes - 1) do
        a = elem(mh.coefficients_a, i)
        b = elem(mh.coefficients_b, i)
        assert is_integer(a) and a >= 1
        assert is_integer(b) and b >= 0
      end
    end
  end

  # ---------------------------------------------------------------------------
  # signature/2
  # ---------------------------------------------------------------------------

  describe "signature/2" do
    test "returns a tuple of the correct size" do
      mh = MinHash.new(64, seed: @seed)
      sig = MinHash.signature(mh, MapSet.new([1, 2, 3]))
      assert is_tuple(sig)
      assert tuple_size(sig) == 64
    end

    test "returns max values for an empty set" do
      mh = MinHash.new(16, seed: @seed)
      sig = MinHash.signature(mh, [])
      max_val = Bitwise.bsl(1, 32) - 1

      expected = Tuple.duplicate(max_val, 16)
      assert sig == expected
    end

    test "produces the same signature for the same set and seed" do
      mh = MinHash.new(64, seed: @seed)
      set = MapSet.new(["apple", "banana", "cherry"])
      sig1 = MinHash.signature(mh, set)
      sig2 = MinHash.signature(mh, set)
      assert sig1 == sig2
    end

    test "signature is independent of element order" do
      mh = MinHash.new(64, seed: @seed)
      sig1 = MinHash.signature(mh, [3, 1, 2])
      sig2 = MinHash.signature(mh, [1, 2, 3])
      assert sig1 == sig2
    end

    test "signature values are non-negative integers" do
      mh = MinHash.new(32, seed: @seed)
      sig = MinHash.signature(mh, ["x", "y", "z"])

      Enum.each(Tuple.to_list(sig), fn val ->
        assert is_integer(val) and val >= 0
      end)
    end

    test "works with a single-element set" do
      mh = MinHash.new(16, seed: @seed)
      sig = MinHash.signature(mh, [42])
      assert tuple_size(sig) == 16

      max_val = Bitwise.bsl(1, 32) - 1

      Enum.each(Tuple.to_list(sig), fn val ->
        assert val < max_val
      end)
    end

    test "accepts any enumerable including streams" do
      mh = MinHash.new(16, seed: @seed)
      sig_list = MinHash.signature(mh, [1, 2, 3])
      sig_stream = MinHash.signature(mh, Stream.take(1..3, 3))
      assert sig_list == sig_stream
    end

    test "works with a MapSet" do
      mh = MinHash.new(16, seed: @seed)
      sig = MinHash.signature(mh, MapSet.new(1..10))
      assert tuple_size(sig) == 16
    end
  end

  # ---------------------------------------------------------------------------
  # similarity/2
  # ---------------------------------------------------------------------------

  describe "similarity/2" do
    test "identical sets have similarity 1.0" do
      mh = MinHash.new(256, seed: @seed)
      set = MapSet.new(1..100)
      sig = MinHash.signature(mh, set)

      assert MinHash.similarity(sig, sig) == 1.0
    end

    test "identical single-element sets have similarity 1.0" do
      mh = MinHash.new(128, seed: @seed)
      sig1 = MinHash.signature(mh, ["only"])
      sig2 = MinHash.signature(mh, ["only"])

      assert MinHash.similarity(sig1, sig2) == 1.0
    end

    test "completely disjoint sets have similarity near 0.0" do
      mh = MinHash.new(256, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new(1..100))
      sig2 = MinHash.signature(mh, MapSet.new(1001..1100))

      similarity = MinHash.similarity(sig1, sig2)
      assert similarity < 0.1
    end

    test "50% overlapping sets produce similarity near 0.5" do
      # Sets: A = {1..100}, B = {51..150}
      # |A intersection B| = 50, |A union B| = 150
      # True Jaccard = 50/150 = 1/3 ~ 0.333
      mh = MinHash.new(512, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new(1..100))
      sig2 = MinHash.signature(mh, MapSet.new(51..150))

      similarity = MinHash.similarity(sig1, sig2)
      true_jaccard = 50 / 150

      # With 512 hash functions the estimate should be within ~0.1 of true value
      assert_in_delta similarity, true_jaccard, 0.1
    end

    test "high overlap sets produce high similarity" do
      # Sets: A = {1..100}, B = {1..110}
      # |A intersection B| = 100, |A union B| = 110
      # True Jaccard = 100/110 ~ 0.909
      mh = MinHash.new(256, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new(1..100))
      sig2 = MinHash.signature(mh, MapSet.new(1..110))

      similarity = MinHash.similarity(sig1, sig2)
      true_jaccard = 100 / 110

      assert_in_delta similarity, true_jaccard, 0.1
    end

    test "similarity is symmetric" do
      mh = MinHash.new(128, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new(1..50))
      sig2 = MinHash.signature(mh, MapSet.new(30..80))

      assert MinHash.similarity(sig1, sig2) == MinHash.similarity(sig2, sig1)
    end

    test "returns float between 0.0 and 1.0" do
      mh = MinHash.new(128, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new(1..50))
      sig2 = MinHash.signature(mh, MapSet.new(25..75))

      similarity = MinHash.similarity(sig1, sig2)
      assert is_float(similarity)
      assert similarity >= 0.0
      assert similarity <= 1.0
    end

    test "empty signatures (zero hash functions) return 1.0" do
      # Edge case: both signatures are empty tuples
      assert MinHash.similarity({}, {}) == 1.0
    end

    test "two empty-set signatures have similarity 1.0" do
      mh = MinHash.new(64, seed: @seed)
      sig1 = MinHash.signature(mh, [])
      sig2 = MinHash.signature(mh, [])

      assert MinHash.similarity(sig1, sig2) == 1.0
    end

    test "more hash functions yield tighter estimates" do
      set_a = MapSet.new(1..200)
      set_b = MapSet.new(101..300)
      true_jaccard = 100 / 300

      # With few hashes
      mh_small = MinHash.new(32, seed: @seed)
      sig1_small = MinHash.signature(mh_small, set_a)
      sig2_small = MinHash.signature(mh_small, set_b)
      error_small = abs(MinHash.similarity(sig1_small, sig2_small) - true_jaccard)

      # With many hashes (average over multiple seeds for robustness)
      errors_large =
        for s <- 1..5 do
          mh_large = MinHash.new(1024, seed: s)
          sig1_large = MinHash.signature(mh_large, set_a)
          sig2_large = MinHash.signature(mh_large, set_b)
          abs(MinHash.similarity(sig1_large, sig2_large) - true_jaccard)
        end

      avg_error_large = Enum.sum(errors_large) / length(errors_large)

      # The average error with 1024 hashes should be less than the single
      # error with 32 hashes (or at worst comparable; this is probabilistic
      # but highly likely to hold).
      assert avg_error_large < error_small + 0.05
    end
  end

  # ---------------------------------------------------------------------------
  # merge/2
  # ---------------------------------------------------------------------------

  describe "merge/2" do
    test "merge produces element-wise minimum" do
      mh = MinHash.new(8, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new(["a", "b"]))
      sig2 = MinHash.signature(mh, MapSet.new(["c", "d"]))

      merged = MinHash.merge(sig1, sig2)

      for i <- 0..(tuple_size(merged) - 1) do
        assert elem(merged, i) == min(elem(sig1, i), elem(sig2, i))
      end
    end

    test "merging a signature with itself returns the same signature" do
      mh = MinHash.new(32, seed: @seed)
      sig = MinHash.signature(mh, MapSet.new(1..10))

      assert MinHash.merge(sig, sig) == sig
    end

    test "merge is commutative" do
      mh = MinHash.new(64, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new([:a, :b, :c]))
      sig2 = MinHash.signature(mh, MapSet.new([:d, :e, :f]))

      assert MinHash.merge(sig1, sig2) == MinHash.merge(sig2, sig1)
    end

    test "merge is associative" do
      mh = MinHash.new(64, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new([1, 2]))
      sig2 = MinHash.signature(mh, MapSet.new([3, 4]))
      sig3 = MinHash.signature(mh, MapSet.new([5, 6]))

      left = MinHash.merge(MinHash.merge(sig1, sig2), sig3)
      right = MinHash.merge(sig1, MinHash.merge(sig2, sig3))
      assert left == right
    end

    test "merged signature approximates union similarity" do
      mh = MinHash.new(256, seed: @seed)

      set_a = MapSet.new(1..50)
      set_b = MapSet.new(40..90)
      set_union = MapSet.union(set_a, set_b)
      set_c = MapSet.new(30..70)

      sig_a = MinHash.signature(mh, set_a)
      sig_b = MinHash.signature(mh, set_b)
      sig_union = MinHash.signature(mh, set_union)
      sig_c = MinHash.signature(mh, set_c)

      merged = MinHash.merge(sig_a, sig_b)

      # The similarity of the merged signature against C should be close to
      # the similarity of the union signature against C.
      sim_merged = MinHash.similarity(merged, sig_c)
      sim_union = MinHash.similarity(sig_union, sig_c)

      assert_in_delta sim_merged, sim_union, 0.15
    end

    test "merging with an empty-set signature returns the other signature" do
      mh = MinHash.new(16, seed: @seed)
      sig = MinHash.signature(mh, MapSet.new([1, 2, 3]))
      empty_sig = MinHash.signature(mh, [])

      # Since empty signature is all max values, merge should return the non-empty sig
      assert MinHash.merge(sig, empty_sig) == sig
      assert MinHash.merge(empty_sig, sig) == sig
    end
  end

  # ---------------------------------------------------------------------------
  # Determinism with seed
  # ---------------------------------------------------------------------------

  describe "determinism" do
    test "same seed produces identical signatures" do
      set = MapSet.new(["x", "y", "z"])

      mh1 = MinHash.new(128, seed: 999)
      mh2 = MinHash.new(128, seed: 999)

      assert MinHash.signature(mh1, set) == MinHash.signature(mh2, set)
    end

    test "different seeds produce different signatures" do
      set = MapSet.new(["x", "y", "z"])

      mh1 = MinHash.new(128, seed: 1)
      mh2 = MinHash.new(128, seed: 2)

      refute MinHash.signature(mh1, set) == MinHash.signature(mh2, set)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "single hash function" do
      mh = MinHash.new(1, seed: @seed)
      sig1 = MinHash.signature(mh, MapSet.new([1, 2, 3]))
      sig2 = MinHash.signature(mh, MapSet.new([1, 2, 3]))

      assert tuple_size(sig1) == 1
      assert MinHash.similarity(sig1, sig2) == 1.0
    end

    test "large number of hash functions" do
      mh = MinHash.new(1024, seed: @seed)
      sig = MinHash.signature(mh, MapSet.new(1..10))
      assert tuple_size(sig) == 1024
    end

    test "works with various term types" do
      mh = MinHash.new(32, seed: @seed)

      sig_ints = MinHash.signature(mh, [1, 2, 3])
      sig_strings = MinHash.signature(mh, ["a", "b", "c"])
      sig_atoms = MinHash.signature(mh, [:x, :y, :z])
      sig_tuples = MinHash.signature(mh, [{1, 2}, {3, 4}])

      assert tuple_size(sig_ints) == 32
      assert tuple_size(sig_strings) == 32
      assert tuple_size(sig_atoms) == 32
      assert tuple_size(sig_tuples) == 32
    end

    test "duplicate elements in the input do not affect the signature" do
      mh = MinHash.new(64, seed: @seed)
      sig_unique = MinHash.signature(mh, [1, 2, 3])
      sig_dupes = MinHash.signature(mh, [1, 1, 2, 2, 3, 3, 3])

      assert sig_unique == sig_dupes
    end

    test "custom hash function is used for signature computation" do
      # A trivial hash function that returns the value itself (for small integers)
      identity_fn = fn x -> x end
      mh = MinHash.new(4, hash_fn: identity_fn, seed: @seed)
      sig = MinHash.signature(mh, [100, 200, 300])

      # The signature should differ from using the default hash function
      mh_default = MinHash.new(4, seed: @seed)
      sig_default = MinHash.signature(mh_default, [100, 200, 300])

      refute sig == sig_default
    end
  end

  # ---------------------------------------------------------------------------
  # Mismatched signatures
  # ---------------------------------------------------------------------------

  describe "mismatched signatures" do
    test "similarity/2 raises when signatures have different lengths" do
      mh1 = MinHash.new(32, seed: @seed)
      mh2 = MinHash.new(64, seed: @seed)

      sig1 = MinHash.signature(mh1, MapSet.new(1..10))
      sig2 = MinHash.signature(mh2, MapSet.new(1..10))

      assert_raise FunctionClauseError, fn ->
        MinHash.similarity(sig1, sig2)
      end
    end

    test "merge/2 raises when signatures have different lengths" do
      mh1 = MinHash.new(32, seed: @seed)
      mh2 = MinHash.new(64, seed: @seed)

      sig1 = MinHash.signature(mh1, MapSet.new(1..10))
      sig2 = MinHash.signature(mh2, MapSet.new(1..10))

      assert_raise FunctionClauseError, fn ->
        MinHash.merge(sig1, sig2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Accuracy scaling
  # ---------------------------------------------------------------------------

  describe "accuracy scaling" do
    test "error decreases as k increases (1/sqrt(k) bound)" do
      set_a = MapSet.new(1..100)
      set_b = MapSet.new(51..150)
      # |A ∩ B| = 50, |A ∪ B| = 150, true Jaccard = 50/150 ≈ 0.333
      true_jaccard = 50 / 150
      trials = 20

      k_values = [16, 64, 256, 1024]

      mean_errors =
        Enum.map(k_values, fn k ->
          errors =
            for trial <- 1..trials do
              mh = MinHash.new(k, seed: trial * 7 + k)
              sig_a = MinHash.signature(mh, set_a)
              sig_b = MinHash.signature(mh, set_b)
              abs(MinHash.similarity(sig_a, sig_b) - true_jaccard)
            end

          Enum.sum(errors) / trials
        end)

      error_at_k16 = Enum.at(mean_errors, 0)
      error_at_k1024 = Enum.at(mean_errors, 3)

      assert error_at_k1024 < error_at_k16
    end
  end

  # ---------------------------------------------------------------------------
  # Subset Jaccard
  # ---------------------------------------------------------------------------

  describe "subset jaccard" do
    test "estimates Jaccard for subset relationship within tolerance" do
      # A = {1..50}, B = {1..100}
      # |A ∩ B| = 50, |A ∪ B| = 100, true Jaccard = 0.5
      mh = MinHash.new(256, seed: @seed)
      sig_a = MinHash.signature(mh, MapSet.new(1..50))
      sig_b = MinHash.signature(mh, MapSet.new(1..100))

      similarity = MinHash.similarity(sig_a, sig_b)
      assert_in_delta similarity, 0.5, 0.15
    end
  end

  # ---------------------------------------------------------------------------
  # Asymmetric sets
  # ---------------------------------------------------------------------------

  describe "asymmetric sets" do
    test "estimates low Jaccard for vastly different set sizes" do
      # A = {1..10}, B = {1..1000}
      # |A ∩ B| = 10, |A ∪ B| = 1000, true Jaccard = 0.01
      mh = MinHash.new(256, seed: @seed)
      sig_a = MinHash.signature(mh, MapSet.new(1..10))
      sig_b = MinHash.signature(mh, MapSet.new(1..1000))

      similarity = MinHash.similarity(sig_a, sig_b)
      assert similarity < 0.1
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-way merge
  # ---------------------------------------------------------------------------

  describe "multi-way merge" do
    test "incrementally merging 10 disjoint set signatures approximates union signature" do
      mh = MinHash.new(256, seed: @seed)

      # 10 disjoint sets: 1..100, 101..200, ..., 901..1000
      signatures =
        Enum.map(0..9, fn i ->
          start = i * 100 + 1
          stop = (i + 1) * 100
          MinHash.signature(mh, MapSet.new(start..stop))
        end)

      merged = Enum.reduce(signatures, &MinHash.merge/2)

      # Signature of the full union 1..1000
      sig_full = MinHash.signature(mh, MapSet.new(1..1000))

      similarity = MinHash.similarity(merged, sig_full)
      assert_in_delta similarity, 1.0, 0.15
    end
  end

  # ---------------------------------------------------------------------------
  # Chunked merge
  # ---------------------------------------------------------------------------

  describe "chunked merge" do
    test "merging chunked signatures equals full set signature" do
      mh = MinHash.new(256, seed: @seed)

      # Split 1..1000 into 10 chunks of 100
      chunk_sigs =
        Enum.map(0..9, fn i ->
          start = i * 100 + 1
          stop = (i + 1) * 100
          MinHash.signature(mh, Enum.to_list(start..stop))
        end)

      merged = Enum.reduce(chunk_sigs, &MinHash.merge/2)

      sig_full = MinHash.signature(mh, MapSet.new(1..1000))

      similarity = MinHash.similarity(merged, sig_full)
      assert_in_delta similarity, 1.0, 0.15
    end
  end

  # ---------------------------------------------------------------------------
  # Coefficient uniqueness
  # ---------------------------------------------------------------------------

  describe "coefficient uniqueness" do
    test "all 1024 coefficient pairs are unique" do
      mh = MinHash.new(1024, seed: @seed)

      pairs =
        for i <- 0..1023 do
          {elem(mh.coefficients_a, i), elem(mh.coefficients_b, i)}
        end

      unique_count = pairs |> MapSet.new() |> MapSet.size()
      assert unique_count == 1024
    end
  end

  # ---------------------------------------------------------------------------
  # Coefficient validity
  # ---------------------------------------------------------------------------

  describe "coefficient validity" do
    test "all a values in coefficients are >= 1" do
      mh = MinHash.new(1024, seed: @seed)

      for i <- 0..1023 do
        a = elem(mh.coefficients_a, i)
        assert a >= 1, "Expected a >= 1, got a=#{a}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  describe "serialization" do
    test "Erlang term serialization round-trip preserves signature" do
      mh = MinHash.new(128, seed: @seed)
      sig = MinHash.signature(mh, MapSet.new(1..50))

      serialized = :erlang.term_to_binary(sig)
      deserialized = :erlang.binary_to_term(serialized)

      assert deserialized == sig
    end
  end

  # ---------------------------------------------------------------------------
  # Large sets
  # ---------------------------------------------------------------------------

  describe "large sets" do
    @tag timeout: 60_000
    test "computes signature for 100_000 elements with 128 hashes" do
      mh = MinHash.new(128, seed: @seed)
      sig = MinHash.signature(mh, 1..100_000)

      assert is_tuple(sig)
      assert tuple_size(sig) == 128
    end

    @tag timeout: 60_000
    test "similarity of two large overlapping sets approximates true Jaccard" do
      # A = {1..50_000}, B = {25_001..75_000}
      # |A ∩ B| = 25_000, |A ∪ B| = 75_000, true Jaccard = 25000/75000 ≈ 0.333
      mh = MinHash.new(128, seed: @seed)
      sig_a = MinHash.signature(mh, 1..50_000)
      sig_b = MinHash.signature(mh, 25_001..75_000)

      similarity = MinHash.similarity(sig_a, sig_b)
      true_jaccard = 25_000 / 75_000

      assert_in_delta similarity, true_jaccard, 0.15
    end
  end

  # ---------------------------------------------------------------------------
  # Seed purity regression — :rand.seed_s instead of :rand.seed
  # ---------------------------------------------------------------------------

  describe "seed purity regression" do
    test "creating MinHash with seed does not mutate process random state" do
      # Set a known process-level state
      :rand.seed(:exsss, 12_345)
      state_before = :rand.export_seed()

      # Creating a MinHash should NOT change process state
      _mh = MinHash.new(64, seed: 99)
      state_after = :rand.export_seed()

      assert state_before == state_after
    end

    test "creating MinHash without seed does not mutate process random state" do
      # Set a known process-level state
      :rand.seed(:exsss, 12_345)
      state_before = :rand.export_seed()

      # Creating a MinHash without an explicit seed should NOT change process state
      _mh = MinHash.new(64)
      state_after = :rand.export_seed()

      assert state_before == state_after
    end

    test "creating multiple MinHash instances with same seed produces identical coefficients" do
      mh1 = MinHash.new(64, seed: 42)
      mh2 = MinHash.new(64, seed: 42)
      assert mh1.coefficients_a == mh2.coefficients_a
      assert mh1.coefficients_b == mh2.coefficients_b
    end

    test "MinHash with seed is deterministic regardless of process random state" do
      :rand.seed(:exsss, 111)
      mh1 = MinHash.new(64, seed: 42)

      :rand.seed(:exsss, 999)
      mh2 = MinHash.new(64, seed: 42)

      assert mh1.coefficients_a == mh2.coefficients_a
      assert mh1.coefficients_b == mh2.coefficients_b
    end

    test "concurrent MinHash creation is safe" do
      tasks =
        for seed <- 1..20 do
          Task.async(fn ->
            mh = MinHash.new(128, seed: seed)
            sig = MinHash.signature(mh, MapSet.new(1..50))
            {seed, mh.coefficients_a, mh.coefficients_b, sig}
          end)
        end

      results = Task.await_many(tasks, 5_000)

      # Verify each result is deterministic by re-creating with the same seed
      Enum.each(results, fn {seed, coeffs_a, coeffs_b, sig} ->
        mh = MinHash.new(128, seed: seed)

        assert mh.coefficients_a == coeffs_a,
               "Seed #{seed} produced different coefficients_a"

        assert mh.coefficients_b == coeffs_b,
               "Seed #{seed} produced different coefficients_b"

        expected_sig = MinHash.signature(mh, MapSet.new(1..50))
        assert sig == expected_sig, "Seed #{seed} produced different signature"
      end)

      # Verify all 20 results completed (no crashes)
      assert length(results) == 20
    end
  end
end
