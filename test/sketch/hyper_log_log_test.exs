defmodule Sketch.HyperLogLogTest do
  use ExUnit.Case, async: true

  alias Sketch.HyperLogLog

  doctest Sketch.HyperLogLog

  # ── new/2 ───────────────────────────────────────────────────────────────

  describe "new/2" do
    test "creates an estimator with default precision of 14" do
      hll = HyperLogLog.new()
      assert hll.precision == 14
      assert tuple_size(hll.registers) == 16_384
    end

    test "creates an estimator with custom precision" do
      for p <- 4..16 do
        hll = HyperLogLog.new(p)
        assert hll.precision == p
        assert tuple_size(hll.registers) == Integer.pow(2, p)
      end
    end

    test "all registers are initialized to zero" do
      hll = HyperLogLog.new(4)
      m = Integer.pow(2, 4)

      for i <- 0..(m - 1) do
        assert elem(hll.registers, i) == 0
      end
    end

    test "accepts a custom hash function" do
      custom_fn = fn term -> :erlang.phash2(term, Integer.pow(2, 32)) end
      hll = HyperLogLog.new(8, hash_fn: custom_fn)
      assert hll.hash_fn == custom_fn
    end

    test "raises ArgumentError for precision below minimum" do
      assert_raise ArgumentError, ~r/precision must be between 4 and 16/, fn ->
        HyperLogLog.new(3)
      end
    end

    test "raises ArgumentError for precision above maximum" do
      assert_raise ArgumentError, ~r/precision must be between 4 and 16/, fn ->
        HyperLogLog.new(17)
      end
    end
  end

  # ── add/2 ───────────────────────────────────────────────────────────────

  describe "add/2" do
    test "updates at least one register after adding an element" do
      hll = HyperLogLog.new(8)
      hll = HyperLogLog.add(hll, "hello")
      m = Integer.pow(2, 8)

      non_zero =
        Enum.count(0..(m - 1), fn i -> elem(hll.registers, i) > 0 end)

      assert non_zero >= 1
    end

    test "handles various term types" do
      hll = HyperLogLog.new(8)

      hll = HyperLogLog.add(hll, "string")
      hll = HyperLogLog.add(hll, :atom)
      hll = HyperLogLog.add(hll, 42)
      hll = HyperLogLog.add(hll, 3.14)
      hll = HyperLogLog.add(hll, {1, 2, 3})
      hll = HyperLogLog.add(hll, [1, 2, 3])
      hll = HyperLogLog.add(hll, %{key: "value"})

      assert HyperLogLog.count(hll) > 0
    end

    test "register values only increase (never decrease)" do
      hll = HyperLogLog.new(8)
      hll = HyperLogLog.add(hll, "test")
      registers_after_first = hll.registers

      hll = HyperLogLog.add(hll, "another")
      m = Integer.pow(2, 8)

      for i <- 0..(m - 1) do
        assert elem(hll.registers, i) >= elem(registers_after_first, i)
      end
    end
  end

  # ── count/1 ─────────────────────────────────────────────────────────────

  describe "count/1" do
    test "returns 0 for an empty estimator" do
      hll = HyperLogLog.new(14)
      assert HyperLogLog.count(hll) == 0
    end

    test "returns 0 for empty estimators at all precisions" do
      for p <- 4..16 do
        hll = HyperLogLog.new(p)
        assert HyperLogLog.count(hll) == 0, "expected 0 for precision #{p}"
      end
    end

    test "returns 1 after adding a single element" do
      hll = HyperLogLog.new(14)
      hll = HyperLogLog.add(hll, "only_one")
      assert HyperLogLog.count(hll) == 1
    end

    test "estimates cardinality within expected error for 1_000 elements at precision 14" do
      hll = HyperLogLog.new(14)
      n = 1_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      # Standard error is ~1.04/sqrt(m). At p=14, m=16384, SE ≈ 0.81%.
      # Use 10% as a generous threshold for test stability.
      error_rate = abs(count - n) / n

      assert error_rate < 0.10,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "estimates cardinality within expected error for 10_000 elements at precision 14" do
      hll = HyperLogLog.new(14)
      n = 10_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      error_rate = abs(count - n) / n

      assert error_rate < 0.10,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "estimates cardinality within expected error for 100_000 elements at precision 14" do
      hll = HyperLogLog.new(14)
      n = 100_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      error_rate = abs(count - n) / n

      assert error_rate < 0.10,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "estimates cardinality at precision 8" do
      hll = HyperLogLog.new(8)
      n = 5_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      # SE at p=8 is ~6.5%, use 20% threshold for test stability
      error_rate = abs(count - n) / n

      assert error_rate < 0.20,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "estimates cardinality at precision 10" do
      hll = HyperLogLog.new(10)
      n = 5_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      # SE at p=10 is ~3.25%, use 15% threshold
      error_rate = abs(count - n) / n

      assert error_rate < 0.15,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "estimates cardinality at precision 4" do
      hll = HyperLogLog.new(4)
      n = 1_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      # SE at p=4 is ~26%, use 50% threshold for low precision
      error_rate = abs(count - n) / n

      assert error_rate < 0.50,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end
  end

  # ── Idempotency ─────────────────────────────────────────────────────────

  describe "idempotency" do
    test "adding the same element multiple times does not change the count" do
      hll = HyperLogLog.new(14)
      hll = HyperLogLog.add(hll, "duplicate")
      count_after_one = HyperLogLog.count(hll)

      hll = HyperLogLog.add(hll, "duplicate")
      hll = HyperLogLog.add(hll, "duplicate")
      hll = HyperLogLog.add(hll, "duplicate")
      count_after_many = HyperLogLog.count(hll)

      assert count_after_one == count_after_many
    end

    test "registers remain unchanged when re-adding the same element" do
      hll = HyperLogLog.new(8)
      hll = HyperLogLog.add(hll, "same")
      registers_first = hll.registers

      hll = HyperLogLog.add(hll, "same")
      assert hll.registers == registers_first
    end
  end

  # ── merge/2 ─────────────────────────────────────────────────────────────

  describe "merge/2" do
    test "merged estimate approximates the union of both sets" do
      n1 = 1_000
      n2 = 1_000

      hll1 =
        Enum.reduce(1..n1, HyperLogLog.new(14), fn i, acc ->
          HyperLogLog.add(acc, {:set1, i})
        end)

      hll2 =
        Enum.reduce(1..n2, HyperLogLog.new(14), fn i, acc ->
          HyperLogLog.add(acc, {:set2, i})
        end)

      {:ok, merged} = HyperLogLog.merge(hll1, hll2)
      merged_count = HyperLogLog.count(merged)

      # Both sets are disjoint, so expected total is 2000
      expected = n1 + n2
      error_rate = abs(merged_count - expected) / expected
      assert error_rate < 0.10, "merged count=#{merged_count}, expected ~#{expected}"
    end

    test "merge is equivalent to building from the union" do
      data1 = Enum.map(1..500, &"item_#{&1}")
      data2 = Enum.map(301..800, &"item_#{&1}")

      hll1 = Enum.reduce(data1, HyperLogLog.new(12), &HyperLogLog.add(&2, &1))
      hll2 = Enum.reduce(data2, HyperLogLog.new(12), &HyperLogLog.add(&2, &1))

      {:ok, merged} = HyperLogLog.merge(hll1, hll2)
      merged_count = HyperLogLog.count(merged)

      # Build directly from the union
      union = Enum.uniq(data1 ++ data2)
      direct = Enum.reduce(union, HyperLogLog.new(12), &HyperLogLog.add(&2, &1))
      direct_count = HyperLogLog.count(direct)

      assert merged_count == direct_count
    end

    test "merge with an empty HLL returns the same estimate" do
      hll =
        Enum.reduce(1..100, HyperLogLog.new(10), fn i, acc ->
          HyperLogLog.add(acc, i)
        end)

      empty = HyperLogLog.new(10)
      {:ok, merged} = HyperLogLog.merge(hll, empty)

      assert HyperLogLog.count(merged) == HyperLogLog.count(hll)
    end

    test "merge of two empty HLLs returns 0 count" do
      {:ok, merged} = HyperLogLog.merge(HyperLogLog.new(8), HyperLogLog.new(8))
      assert HyperLogLog.count(merged) == 0
    end

    test "merge is commutative" do
      hll1 = Enum.reduce(1..100, HyperLogLog.new(10), &HyperLogLog.add(&2, &1))
      hll2 = Enum.reduce(101..200, HyperLogLog.new(10), &HyperLogLog.add(&2, &1))

      {:ok, merged_ab} = HyperLogLog.merge(hll1, hll2)
      {:ok, merged_ba} = HyperLogLog.merge(hll2, hll1)

      assert HyperLogLog.count(merged_ab) == HyperLogLog.count(merged_ba)
    end

    test "merge is associative" do
      hll1 = Enum.reduce(1..100, HyperLogLog.new(10), &HyperLogLog.add(&2, &1))
      hll2 = Enum.reduce(101..200, HyperLogLog.new(10), &HyperLogLog.add(&2, &1))
      hll3 = Enum.reduce(201..300, HyperLogLog.new(10), &HyperLogLog.add(&2, &1))

      {:ok, m12} = HyperLogLog.merge(hll1, hll2)
      {:ok, left} = HyperLogLog.merge(m12, hll3)

      {:ok, m23} = HyperLogLog.merge(hll2, hll3)
      {:ok, right} = HyperLogLog.merge(hll1, m23)

      assert HyperLogLog.count(left) == HyperLogLog.count(right)
    end

    test "returns error when precisions differ" do
      hll1 = HyperLogLog.new(8)
      hll2 = HyperLogLog.new(10)

      assert {:error, :incompatible_precision} = HyperLogLog.merge(hll1, hll2)
    end
  end

  # ── Serialization ───────────────────────────────────────────────────────

  describe "to_binary/1 and from_binary/1" do
    test "round-trip preserves precision and count for empty HLL" do
      for p <- [4, 8, 12, 14, 16] do
        hll = HyperLogLog.new(p)
        {:ok, restored} = hll |> HyperLogLog.to_binary() |> HyperLogLog.from_binary()

        assert restored.precision == p
        assert HyperLogLog.count(restored) == 0
      end
    end

    test "round-trip preserves registers and count with data" do
      hll = HyperLogLog.new(10)
      hll = Enum.reduce(1..500, hll, fn i, acc -> HyperLogLog.add(acc, i) end)

      {:ok, restored} = hll |> HyperLogLog.to_binary() |> HyperLogLog.from_binary()

      assert restored.precision == hll.precision
      assert restored.registers == hll.registers
      assert HyperLogLog.count(restored) == HyperLogLog.count(hll)
    end

    test "binary has correct size" do
      for p <- [4, 8, 12, 14, 16] do
        hll = HyperLogLog.new(p)
        binary = HyperLogLog.to_binary(hll)
        m = Integer.pow(2, p)

        # 1 byte version + 1 byte precision + m bytes registers
        expected_size = 2 + m
        assert byte_size(binary) == expected_size
      end
    end

    test "binary starts with version 1 and correct precision byte" do
      hll = HyperLogLog.new(10)
      <<version::8, precision::8, _rest::binary>> = HyperLogLog.to_binary(hll)

      assert version == 1
      assert precision == 10
    end

    test "restored HLL uses default hash function" do
      hll = HyperLogLog.new(8)
      {:ok, restored} = hll |> HyperLogLog.to_binary() |> HyperLogLog.from_binary()

      # Verify we can still add elements (hash function works)
      restored = HyperLogLog.add(restored, "after_restore")
      assert HyperLogLog.count(restored) >= 1
    end

    test "returns error on invalid version byte" do
      # Version 99 is not supported
      binary = <<99, 8>> <> :binary.copy(<<0>>, 256)

      assert {:error, :invalid_binary} = HyperLogLog.from_binary(binary)
    end

    test "returns error on truncated binary" do
      assert {:error, :invalid_binary} = HyperLogLog.from_binary(<<1, 8, 0, 0>>)
    end

    test "returns error on empty binary" do
      assert {:error, :invalid_binary} = HyperLogLog.from_binary(<<>>)
    end

    test "returns error on single byte binary" do
      assert {:error, :invalid_binary} = HyperLogLog.from_binary(<<1>>)
    end
  end

  # ── Custom hash function ────────────────────────────────────────────────

  describe "custom hash function" do
    test "deterministic hash function produces predictable results" do
      # A hash function that always returns a known value
      # For p=4: first 4 bits = 0b0011 = index 3, remaining 28 bits = 0b0001...0 = leading 27 zeros + 1 = rho 28
      # Hash value: 0b0011_0000...001 = (3 << 28) | 1
      import Bitwise
      hash_value = bsl(3, 28) ||| 1
      deterministic_fn = fn _term -> hash_value end

      hll = HyperLogLog.new(4, hash_fn: deterministic_fn)
      hll = HyperLogLog.add(hll, "anything")

      # Register at index 3 should have rho = 28 (27 leading zeros + 1)
      assert elem(hll.registers, 3) == 28

      # All other registers should be 0
      for i <- 0..15, i != 3 do
        assert elem(hll.registers, i) == 0
      end
    end

    test "hash function producing zero results in max rho" do
      # Hash = 0: index = 0 (first p bits all zero), remaining bits all zero
      # Leading zeros in 28 bits = 28, rho = 29
      deterministic_fn = fn _term -> 0 end

      hll = HyperLogLog.new(4, hash_fn: deterministic_fn)
      hll = HyperLogLog.add(hll, "anything")

      # Register at index 0 should have rho = 28 + 1 = 29
      assert elem(hll.registers, 0) == 29
    end

    test "hash function producing max value results in rho = 1" do
      import Bitwise
      # Hash = 0xFFFFFFFF: all bits 1
      # For p=4: index = 0b1111 = 15, remaining 28 bits = 0b1111...1111
      # Leading zeros = 0, rho = 1
      hash_value = bsl(1, 32) - 1
      deterministic_fn = fn _term -> hash_value end

      hll = HyperLogLog.new(4, hash_fn: deterministic_fn)
      hll = HyperLogLog.add(hll, "anything")

      assert elem(hll.registers, 15) == 1
    end
  end

  # ── Edge cases ──────────────────────────────────────────────────────────

  describe "edge cases" do
    test "adding a very large number of distinct elements" do
      hll = HyperLogLog.new(12)
      n = 50_000

      hll = Enum.reduce(1..n, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      error_rate = abs(count - n) / n
      assert error_rate < 0.10, "count=#{count}, expected ~#{n}"
    end

    test "count returns a non-negative integer" do
      hll = HyperLogLog.new(8)
      hll = Enum.reduce(1..100, hll, fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      assert is_integer(count)
      assert count >= 0
    end

    test "adding nil as an element" do
      hll = HyperLogLog.new(8)
      hll = HyperLogLog.add(hll, nil)
      assert HyperLogLog.count(hll) >= 1
    end

    test "adding an empty string as an element" do
      hll = HyperLogLog.new(8)
      hll = HyperLogLog.add(hll, "")
      assert HyperLogLog.count(hll) >= 1
    end
  end

  # ── Small cardinality / linear counting ────────────────────────────────

  describe "small cardinality" do
    test "100 unique elements at p=14 estimated within 15%" do
      n = 100
      hll = Enum.reduce(1..n, HyperLogLog.new(14), fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      error_rate = abs(count - n) / n

      assert error_rate < 0.15,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "50 unique elements at p=10 (m=1024) estimated within 20%" do
      n = 50
      hll = Enum.reduce(1..n, HyperLogLog.new(10), fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      error_rate = abs(count - n) / n

      assert error_rate < 0.20,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end
  end

  # ── Large cardinality ─────────────────────────────────────────────────

  describe "large cardinality" do
    @tag timeout: :infinity
    test "1_000_000 unique elements at p=14 has error rate below 5%" do
      n = 1_000_000

      hll = Enum.reduce(1..n, HyperLogLog.new(14), fn i, acc -> HyperLogLog.add(acc, i) end)
      count = HyperLogLog.count(hll)

      error_rate = abs(count - n) / n

      assert error_rate < 0.05,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end
  end

  # ── Standard error verification across trials ─────────────────────────

  describe "standard error verification" do
    @tag timeout: :infinity
    test "mean absolute relative error across 20 trials is below 3 * theoretical SE at p=10" do
      p = 10
      m = :math.pow(2, p) |> round()
      theoretical_se = 1.04 / :math.sqrt(m)
      n = 5_000
      num_trials = 20

      errors =
        Enum.map(1..num_trials, fn trial ->
          hll =
            Enum.reduce(1..n, HyperLogLog.new(p), fn i, acc ->
              HyperLogLog.add(acc, {:trial, trial, i})
            end)

          count = HyperLogLog.count(hll)
          abs(count - n) / n
        end)

      mean_error = Enum.sum(errors) / num_trials

      assert mean_error < 3 * theoretical_se,
             "mean error=#{Float.round(mean_error * 100, 2)}%, " <>
               "threshold=#{Float.round(3 * theoretical_se * 100, 2)}%"
    end
  end

  # ── Accuracy across all precisions at fixed n ─────────────────────────

  describe "accuracy across precisions" do
    @tag timeout: :infinity
    test "error rate is below 3 * theoretical SE for each precision 4..16" do
      n = 10_000

      for p <- [4, 6, 8, 10, 12, 14, 16] do
        hll = Enum.reduce(1..n, HyperLogLog.new(p), fn i, acc -> HyperLogLog.add(acc, i) end)
        count = HyperLogLog.count(hll)

        m = :math.pow(2, p)
        threshold = 3 * 1.04 / :math.sqrt(m)
        error_rate = abs(count - n) / n

        assert error_rate < threshold,
               "p=#{p}: count=#{count}, expected ~#{n}, " <>
                 "error=#{Float.round(error_rate * 100, 2)}%, " <>
                 "threshold=#{Float.round(threshold * 100, 2)}%"
      end
    end
  end

  # ── Many-partition merge ──────────────────────────────────────────────

  describe "many-partition merge" do
    test "merging 10 disjoint HLLs approximates total count within 15%" do
      p = 12
      elements_per_partition = 1_000
      num_partitions = 10

      hlls =
        Enum.map(0..(num_partitions - 1), fn partition ->
          start = partition * elements_per_partition + 1
          stop = start + elements_per_partition - 1

          Enum.reduce(start..stop, HyperLogLog.new(p), fn i, acc ->
            HyperLogLog.add(acc, i)
          end)
        end)

      merged =
        Enum.reduce(hlls, fn hll, acc ->
          {:ok, m} = HyperLogLog.merge(acc, hll)
          m
        end)

      count = HyperLogLog.count(merged)

      expected = num_partitions * elements_per_partition
      error_rate = abs(count - expected) / expected

      assert error_rate < 0.15,
             "count=#{count}, expected ~#{expected}, error=#{Float.round(error_rate * 100, 2)}%"
    end

    test "merging 10 overlapping HLLs (all same data) approximates single set count within 15%" do
      p = 12
      n = 1_000
      num_partitions = 10

      hlls =
        Enum.map(1..num_partitions, fn _partition ->
          Enum.reduce(1..n, HyperLogLog.new(p), fn i, acc ->
            HyperLogLog.add(acc, i)
          end)
        end)

      merged =
        Enum.reduce(hlls, fn hll, acc ->
          {:ok, m} = HyperLogLog.merge(acc, hll)
          m
        end)

      count = HyperLogLog.count(merged)

      error_rate = abs(count - n) / n

      assert error_rate < 0.15,
             "count=#{count}, expected ~#{n}, error=#{Float.round(error_rate * 100, 2)}%"
    end
  end

  # ── Insertion order independence ──────────────────────────────────────

  describe "insertion order independence" do
    test "forward and reverse insertion produce identical registers" do
      n = 5_000
      p = 14

      hll_forward =
        Enum.reduce(1..n, HyperLogLog.new(p), fn i, acc -> HyperLogLog.add(acc, i) end)

      hll_reverse =
        Enum.reduce(n..1//-1, HyperLogLog.new(p), fn i, acc -> HyperLogLog.add(acc, i) end)

      assert hll_forward.registers == hll_reverse.registers
    end
  end

  # ── from_binary with invalid precision ────────────────────────────────

  describe "from_binary validation" do
    test "returns error on binary with precision below minimum (p=3)" do
      # version=1, precision=3, then 2^3=8 register bytes
      register_bytes = :binary.copy(<<0>>, 8)
      binary = <<1::8, 3::8, register_bytes::binary>>

      assert {:error, :invalid_binary} = HyperLogLog.from_binary(binary)
    end

    test "returns error on binary with precision above maximum (p=17)" do
      # version=1, precision=17, then 2^17=131072 register bytes
      register_bytes = :binary.copy(<<0>>, 131_072)
      binary = <<1::8, 17::8, register_bytes::binary>>

      assert {:error, :invalid_binary} = HyperLogLog.from_binary(binary)
    end

    test "returns error on binary with precision zero" do
      # version=1, precision=0, then 2^0=1 register byte
      register_bytes = :binary.copy(<<0>>, 1)
      binary = <<1::8, 0::8, register_bytes::binary>>

      assert {:error, :invalid_binary} = HyperLogLog.from_binary(binary)
    end
  end

  # ── Repeated count/1 determinism ──────────────────────────────────────

  describe "count determinism" do
    test "count/1 returns the same value on repeated calls" do
      hll = Enum.reduce(1..5_000, HyperLogLog.new(14), fn i, acc -> HyperLogLog.add(acc, i) end)

      count1 = HyperLogLog.count(hll)
      count2 = HyperLogLog.count(hll)
      count3 = HyperLogLog.count(hll)

      assert count1 == count2
      assert count2 == count3
    end
  end

  # ── Self-merge ────────────────────────────────────────────────────────

  describe "self merge" do
    test "merging HLL with itself preserves count and registers" do
      hll = Enum.reduce(1..5_000, HyperLogLog.new(12), fn i, acc -> HyperLogLog.add(acc, i) end)

      original_count = HyperLogLog.count(hll)
      {:ok, merged} = HyperLogLog.merge(hll, hll)

      assert HyperLogLog.count(merged) == original_count
      assert merged.registers == hll.registers
    end
  end

  # ── Large range correction regression (raw_estimate >= 2^32) ─────────

  describe "large range correction regression" do
    test "extreme register values produce valid count without crashing" do
      # With p=4 (m=16), alpha=0.673, and all registers at 30:
      # raw_estimate = alpha * m * m / sum = 0.673 * 16 * 16 / (16 * 2^-30)
      #             = 0.673 * 16 * 2^30 ≈ 11.56 billion, which exceeds 2^32.
      # Before the fix, this would crash because log(1 - raw_estimate / 2^32)
      # would receive a non-positive argument.
      hll = HyperLogLog.new(4)
      registers = :erlang.make_tuple(16, 30)
      hll = %{hll | registers: registers}

      assert is_integer(HyperLogLog.count(hll))
      assert HyperLogLog.count(hll) > 0
    end

    test "all registers at theoretical maximum do not crash" do
      # For p=4, the maximum rho value is 32 - p = 28 (all remaining bits zero
      # plus 1). Setting every register to this value creates the largest
      # possible raw_estimate for this precision.
      hll = HyperLogLog.new(4)
      max_rho = 32 - 4
      registers = :erlang.make_tuple(16, max_rho)
      hll = %{hll | registers: registers}

      assert is_integer(HyperLogLog.count(hll))
      assert HyperLogLog.count(hll) > 0
    end

    test "registers at boundary: raw_estimate just below 2^32/30 (no large range correction)" do
      # For p=4, m=16, alpha=0.673:
      #   raw_estimate = 0.673 * 16 * 2^val
      # At val=23: raw_estimate ≈ 0.673 * 16 * 2^23 ≈ 90.3M < 2^32/30 ≈ 143.2M
      # This falls into the "no correction" branch.
      hll = HyperLogLog.new(4)
      registers = :erlang.make_tuple(16, 23)
      hll = %{hll | registers: registers}

      count = HyperLogLog.count(hll)
      assert is_integer(count)
      assert count > 0
    end

    test "registers at boundary: raw_estimate between 2^32/30 and 2^32 (large range correction applied)" do
      # For p=4, m=16, alpha=0.673:
      #   raw_estimate = 0.673 * 16 * 2^val
      # At val=24: raw_estimate ≈ 0.673 * 16 * 2^24 ≈ 180.7M
      # This is > 2^32/30 ≈ 143.2M and < 2^32 ≈ 4.295B, so the large range
      # correction branch is taken: -2^32 * log(1 - raw_estimate / 2^32).
      hll = HyperLogLog.new(4)
      registers = :erlang.make_tuple(16, 24)
      hll = %{hll | registers: registers}

      count = HyperLogLog.count(hll)
      assert is_integer(count)
      assert count > 0

      # The corrected estimate should be larger than the raw estimate because
      # the log correction inflates values in this range.
      # raw ≈ 180.7M, corrected ≈ -2^32 * log(1 - 180.7M/4295M) ≈ 184.6M
      assert count > 100_000_000
    end

    test "registers at boundary: raw_estimate just above 2^32 (skips large range correction)" do
      # For p=4, m=16, alpha=0.673:
      #   raw_estimate = 0.673 * 16 * 2^val
      # At val=28: raw_estimate ≈ 0.673 * 16 * 2^28 ≈ 2.89B (< 2^32)
      # At val=29: raw_estimate ≈ 0.673 * 16 * 2^29 ≈ 5.78B (> 2^32)
      # val=29 exceeds 2^32, so the fix causes it to fall through to the "no
      # correction" branch instead of crashing.
      hll = HyperLogLog.new(4)
      registers = :erlang.make_tuple(16, 29)
      hll = %{hll | registers: registers}

      count = HyperLogLog.count(hll)
      assert is_integer(count)
      assert count > 0
    end

    test "high precision (p=14) with saturated registers does not crash" do
      # With p=14, m=16384, alpha≈0.7213:
      #   raw_estimate = 0.7213 * 16384 * 2^val
      # Even at val=10: raw_estimate ≈ 0.7213 * 16384 * 1024 ≈ 12.1M
      # At val=20: raw_estimate ≈ 12.4 trillion — vastly exceeds 2^32.
      # This would crash before the fix.
      hll = HyperLogLog.new(14)
      registers = :erlang.make_tuple(16_384, 20)
      hll = %{hll | registers: registers}

      count = HyperLogLog.count(hll)
      assert is_integer(count)
      assert count > 0
    end

    test "count returns 0 for empty HLL (small range correction edge case)" do
      # With all registers at 0, raw_estimate is very small and all registers
      # are zero, so linear counting yields m * log(m/m) = m * log(1) = 0.
      for p <- [4, 8, 14] do
        hll = HyperLogLog.new(p)
        assert HyperLogLog.count(hll) == 0, "expected 0 for precision #{p}"
      end
    end

    test "count returns at least 1 after adding a single element" do
      for p <- [4, 8, 14] do
        hll = HyperLogLog.new(p)
        hll = HyperLogLog.add(hll, "single_element")
        count = HyperLogLog.count(hll)

        assert count >= 1,
               "expected count >= 1 after single add at precision #{p}, got #{count}"
      end
    end
  end

  # ── Skewed distribution ───────────────────────────────────────────────

  describe "skewed distribution" do
    test "Zipf-like distribution: estimate is within 15% of actual distinct count" do
      p = 12

      # Generate elements following a Zipf-like pattern:
      # element i is repeated ceil(1000/i) times, for i in 1..1000
      elements =
        Enum.flat_map(1..1_000, fn i ->
          repetitions = ceil(1_000 / i)
          List.duplicate(i, repetitions)
        end)

      actual_distinct = elements |> Enum.uniq() |> length()

      hll =
        Enum.reduce(elements, HyperLogLog.new(p), fn elem, acc ->
          HyperLogLog.add(acc, elem)
        end)

      count = HyperLogLog.count(hll)
      error_rate = abs(count - actual_distinct) / actual_distinct

      assert error_rate < 0.15,
             "count=#{count}, actual_distinct=#{actual_distinct}, " <>
               "error=#{Float.round(error_rate * 100, 2)}%"
    end
  end

  # ── Bug verification ─────────────────────────────────────────────────

  describe "bug verification" do
    test "BUG 22: to_binary performance should not degrade quadratically with precision" do
      # p=10 has 1024 registers, p=14 has 16384 registers (16x more)
      hll_small = Sketch.HyperLogLog.new(10)
      hll_small = Enum.reduce(1..100, hll_small, &Sketch.HyperLogLog.add(&2, &1))

      hll_large = Sketch.HyperLogLog.new(14)
      hll_large = Enum.reduce(1..100, hll_large, &Sketch.HyperLogLog.add(&2, &1))

      # Time serialization
      {time_small, _} =
        :timer.tc(fn ->
          for _ <- 1..100, do: Sketch.HyperLogLog.to_binary(hll_small)
        end)

      {time_large, _} =
        :timer.tc(fn ->
          for _ <- 1..100, do: Sketch.HyperLogLog.to_binary(hll_large)
        end)

      # If O(m), ratio should be ~16. If O(m^2), ratio should be ~256.
      ratio = time_large / max(time_small, 1)

      assert ratio < 50,
             "to_binary is O(m^2): p=14 is #{Float.round(ratio, 1)}x slower than p=10 (expected ~16x for O(m))"
    end

    test "BUG 23: merge performance should not degrade quadratically with precision" do
      hll1_small =
        Enum.reduce(1..1000, Sketch.HyperLogLog.new(10), &Sketch.HyperLogLog.add(&2, &1))

      hll2_small =
        Enum.reduce(1001..2000, Sketch.HyperLogLog.new(10), &Sketch.HyperLogLog.add(&2, &1))

      hll1_large =
        Enum.reduce(1..1000, Sketch.HyperLogLog.new(14), &Sketch.HyperLogLog.add(&2, &1))

      hll2_large =
        Enum.reduce(1001..2000, Sketch.HyperLogLog.new(14), &Sketch.HyperLogLog.add(&2, &1))

      {time_small, _} =
        :timer.tc(fn ->
          for _ <- 1..100 do
            {:ok, _} = Sketch.HyperLogLog.merge(hll1_small, hll2_small)
          end
        end)

      {time_large, _} =
        :timer.tc(fn ->
          for _ <- 1..100 do
            {:ok, _} = Sketch.HyperLogLog.merge(hll1_large, hll2_large)
          end
        end)

      ratio = time_large / max(time_small, 1)

      # Linear would be ~16x (16384/1024). Quadratic would be ~256x.
      # Use generous threshold (100x) since timing tests are inherently noisy.
      assert ratio < 100,
             "merge is O(m^2): p=14 is #{Float.round(ratio, 1)}x slower than p=10 (expected ~16x for O(m))"
    end

    test "BUG 24: phash2 provides reasonable distribution across full 32-bit range" do
      # Generate many hashes and check that values above 2^27 actually occur
      hashes = for i <- 1..10_000, do: Sketch.Hash.hash32("element_#{i}")

      max_hash = Enum.max(hashes)

      # 2^27 = 134_217_728
      # If phash2 only has 27 bits of quality, most values would be < 2^27
      above_27_bits = Enum.count(hashes, fn h -> h > 134_217_728 end)

      pct_above = above_27_bits / 10_000 * 100

      # If truly 32-bit distributed, ~96.9% should be above 2^27
      # (since 2^27 is 1/32 of the 2^32 range, 31/32 values should be above it)
      # If only 27-bit quality, this percentage would be much lower
      assert pct_above > 50,
             "Only #{Float.round(pct_above, 1)}% of hashes above 2^27 (max=#{max_hash}) — suggests limited bit quality"
    end
  end
end
