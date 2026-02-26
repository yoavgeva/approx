defmodule Sketch.ApiConsistencyTest do
  use ExUnit.Case, async: true

  # ===========================================================================
  # merge/2 return types — consistent API across modules
  #
  # API contract:
  #   - Modules that CAN have incompatible inputs return
  #     {:ok, t()} | {:error, reason} where reason is an atom
  #   - TDigest.merge always succeeds (no compatibility constraint) so bare t() is acceptable
  #   - MinHash.merge operates on signature tuples, not structs, so different API is acceptable
  #
  # Current behavior:
  #   - BloomFilter.merge/2      -> {:ok, t()} | {:error, :incompatible_filters}   (atom)
  #   - CountMinSketch.merge/2   -> {:ok, t()} | {:error, :dimension_mismatch}     (atom)
  #   - HyperLogLog.merge/2      -> {:ok, t()} | {:error, :incompatible_precision} (atom)
  #   - TopK.merge/2             -> {:ok, t()} | {:error, :incompatible_k}         (atom)
  #   - Reservoir.merge/2        -> {:ok, t()} | {:error, :incompatible_size}      (atom)
  #   - TDigest.merge/2          -> t()                                            (always succeeds)
  #   - MinHash.merge/2          -> signature()                                    (different domain)
  #   - CuckooFilter             -> no merge/2 at all
  # ===========================================================================

  describe "merge/2 return type consistency — success cases" do
    test "BloomFilter.merge returns {:ok, t()} on success" do
      bf1 = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("a")
      bf2 = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("b")
      assert {:ok, %Sketch.BloomFilter{}} = Sketch.BloomFilter.merge(bf1, bf2)
    end

    test "CountMinSketch.merge returns {:ok, t()} on success" do
      cms1 = Sketch.CountMinSketch.new() |> Sketch.CountMinSketch.add("a")
      cms2 = Sketch.CountMinSketch.new() |> Sketch.CountMinSketch.add("b")
      assert {:ok, %Sketch.CountMinSketch{}} = Sketch.CountMinSketch.merge(cms1, cms2)
    end

    test "TopK.merge returns {:ok, t()} on success" do
      tk1 = Sketch.TopK.new(5) |> Sketch.TopK.add("a", 10)
      tk2 = Sketch.TopK.new(5) |> Sketch.TopK.add("b", 20)
      assert {:ok, %Sketch.TopK{}} = Sketch.TopK.merge(tk1, tk2)
    end

    test "HyperLogLog.merge returns {:ok, t()} on success" do
      hll1 = Sketch.HyperLogLog.new(4) |> Sketch.HyperLogLog.add("a")
      hll2 = Sketch.HyperLogLog.new(4) |> Sketch.HyperLogLog.add("b")
      assert {:ok, %Sketch.HyperLogLog{}} = Sketch.HyperLogLog.merge(hll1, hll2)
    end

    test "Reservoir.merge returns {:ok, t()} on success" do
      r1 = Sketch.Reservoir.new(5, seed: 42) |> Sketch.Reservoir.add(:a)
      r2 = Sketch.Reservoir.new(5, seed: 99) |> Sketch.Reservoir.add(:b)
      assert {:ok, %Sketch.Reservoir{}} = Sketch.Reservoir.merge(r1, r2)
    end
  end

  describe "merge/2 return type consistency — error/incompatible cases" do
    test "BloomFilter.merge returns {:error, reason} on incompatible filters" do
      bf1 = Sketch.BloomFilter.new(100, 0.01)
      bf2 = Sketch.BloomFilter.new(100, 0.001)
      assert {:error, _reason} = Sketch.BloomFilter.merge(bf1, bf2)
    end

    test "CountMinSketch.merge returns {:error, reason} on incompatible sketches" do
      cms1 = Sketch.CountMinSketch.new(0.01, 0.01)
      cms2 = Sketch.CountMinSketch.new(0.001, 0.01)
      assert {:error, _reason} = Sketch.CountMinSketch.merge(cms1, cms2)
    end

    test "TopK.merge returns {:error, reason} on incompatible trackers (different k)" do
      tk1 = Sketch.TopK.new(3) |> Sketch.TopK.add("a", 10)
      tk2 = Sketch.TopK.new(5) |> Sketch.TopK.add("b", 20)
      assert {:error, _reason} = Sketch.TopK.merge(tk1, tk2)
    end

    test "HyperLogLog.merge returns {:error, :incompatible_precision} on incompatible" do
      hll1 = Sketch.HyperLogLog.new(4)
      hll2 = Sketch.HyperLogLog.new(8)
      assert {:error, :incompatible_precision} = Sketch.HyperLogLog.merge(hll1, hll2)
    end

    test "Reservoir.merge returns {:error, :incompatible_size} on incompatible" do
      r1 = Sketch.Reservoir.new(5, seed: 42)
      r2 = Sketch.Reservoir.new(10, seed: 42)
      assert {:error, :incompatible_size} = Sketch.Reservoir.merge(r1, r2)
    end
  end

  describe "merge/2 error reason type consistency" do
    test "BloomFilter and CountMinSketch merge error reasons are both atoms" do
      bf1 = Sketch.BloomFilter.new(100, 0.01)
      bf2 = Sketch.BloomFilter.new(100, 0.001)
      {:error, bf_reason} = Sketch.BloomFilter.merge(bf1, bf2)

      cms1 = Sketch.CountMinSketch.new(0.01, 0.01)
      cms2 = Sketch.CountMinSketch.new(0.001, 0.01)
      {:error, cms_reason} = Sketch.CountMinSketch.merge(cms1, cms2)

      bf_type = if is_atom(bf_reason), do: :atom, else: :string
      cms_type = if is_atom(cms_reason), do: :atom, else: :string

      # Both use atoms for error reasons
      assert bf_type == cms_type
    end

    test "TopK.merge error reason is an atom" do
      tk1 = Sketch.TopK.new(3) |> Sketch.TopK.add("a", 10)
      tk2 = Sketch.TopK.new(5) |> Sketch.TopK.add("b", 20)
      {:error, reason} = Sketch.TopK.merge(tk1, tk2)

      assert is_atom(reason)
    end
  end

  # ===========================================================================
  # from_binary return types — consistent API across modules
  #
  # API contract:
  #   - All from_binary functions return {:ok, t()} | {:error, reason}
  #     where reason is an atom
  #
  # Current behavior:
  #   - BloomFilter.from_binary/2    -> {:ok, t()} | {:error, :invalid_binary}    (atom)
  #   - CountMinSketch.from_binary/2 -> {:ok, t()} | {:error, :invalid_binary}    (atom)
  #   - HyperLogLog.from_binary/1    -> {:ok, t()} | {:error, :invalid_binary}    (atom)
  #   - TDigest.from_binary/1        -> {:ok, t()} | {:error, :invalid_binary}    (atom)
  #   - CuckooFilter.from_binary/2   -> {:ok, t()} | {:error, :invalid_binary}    (atom)
  #   - TopK, Reservoir, MinHash     -> no from_binary at all
  # ===========================================================================

  describe "from_binary/1 return type consistency — success round-trips" do
    test "BloomFilter round-trip returns {:ok, t()}" do
      bf = Sketch.BloomFilter.new(100) |> Sketch.BloomFilter.add("test")
      bin = Sketch.BloomFilter.to_binary(bf)
      assert {:ok, %Sketch.BloomFilter{}} = Sketch.BloomFilter.from_binary(bin)
    end

    test "CountMinSketch round-trip returns {:ok, t()}" do
      cms = Sketch.CountMinSketch.new() |> Sketch.CountMinSketch.add("test")
      bin = Sketch.CountMinSketch.to_binary(cms)
      assert {:ok, %Sketch.CountMinSketch{}} = Sketch.CountMinSketch.from_binary(bin)
    end

    test "TDigest round-trip returns {:ok, t()}" do
      td = Sketch.TDigest.new() |> Sketch.TDigest.add(42.0)
      bin = Sketch.TDigest.to_binary(td)
      assert {:ok, %Sketch.TDigest{}} = Sketch.TDigest.from_binary(bin)
    end

    test "CuckooFilter round-trip returns {:ok, t()}" do
      cf = Sketch.CuckooFilter.new(100)
      {:ok, cf} = Sketch.CuckooFilter.add(cf, "test")
      bin = Sketch.CuckooFilter.to_binary(cf)
      assert {:ok, %Sketch.CuckooFilter{}} = Sketch.CuckooFilter.from_binary(bin)
    end

    test "HyperLogLog round-trip returns {:ok, t()}" do
      hll = Sketch.HyperLogLog.new(4) |> Sketch.HyperLogLog.add("test")
      bin = Sketch.HyperLogLog.to_binary(hll)
      assert {:ok, %Sketch.HyperLogLog{}} = Sketch.HyperLogLog.from_binary(bin)
    end
  end

  describe "from_binary/1 return type consistency — invalid input" do
    test "BloomFilter.from_binary returns {:error, reason} on invalid input" do
      assert {:error, _reason} = Sketch.BloomFilter.from_binary(<<0, 0, 0>>)
    end

    test "CountMinSketch.from_binary returns {:error, reason} on invalid input" do
      assert {:error, _reason} = Sketch.CountMinSketch.from_binary(<<0, 0, 0>>)
    end

    test "TDigest.from_binary returns {:error, reason} on invalid input" do
      assert {:error, _reason} = Sketch.TDigest.from_binary(<<0, 0, 0>>)
    end

    test "CuckooFilter.from_binary returns {:error, reason} on invalid input" do
      assert {:error, _reason} = Sketch.CuckooFilter.from_binary(<<0, 0, 0>>)
    end

    test "HyperLogLog.from_binary returns {:error, :invalid_binary} on invalid input" do
      assert {:error, :invalid_binary} = Sketch.HyperLogLog.from_binary(<<0, 0, 0>>)
    end

    test "HyperLogLog.from_binary returns {:error, :invalid_binary} on truncated valid-version binary" do
      # A binary with the correct version byte (1) but truncated register data
      # to test the size-mismatch error path
      truncated = <<1, 4, 0, 0>>
      assert {:error, :invalid_binary} = Sketch.HyperLogLog.from_binary(truncated)
    end
  end

  describe "from_binary/1 error reason type consistency" do
    test "BloomFilter.from_binary error reason is an atom" do
      {:error, reason} = Sketch.BloomFilter.from_binary(<<0, 0, 0>>)
      assert is_atom(reason), "Expected atom, got: #{inspect(reason)}"
    end

    test "TDigest.from_binary error reason is an atom" do
      {:error, reason} = Sketch.TDigest.from_binary(<<0, 0, 0>>)
      assert is_atom(reason), "Expected atom, got: #{inspect(reason)}"
    end

    test "CuckooFilter.from_binary error reason is an atom" do
      {:error, reason} = Sketch.CuckooFilter.from_binary(<<0, 0, 0>>)
      assert is_atom(reason), "Expected atom, got: #{inspect(reason)}"
    end

    test "CountMinSketch.from_binary error reason is an atom" do
      {:error, reason} = Sketch.CountMinSketch.from_binary(<<0, 0, 0>>)
      assert is_atom(reason)
    end

    test "CountMinSketch.from_binary returns :invalid_binary for different error paths" do
      # Too short
      {:error, reason_short} = Sketch.CountMinSketch.from_binary(<<0, 0, 0>>)

      # Wrong version (version 99, but enough bytes for header)
      {:error, reason_version} =
        Sketch.CountMinSketch.from_binary(<<99, 0, 0, 0, 10, 0, 0, 0, 5>>)

      # Both error paths now return the same :invalid_binary atom
      assert is_atom(reason_short)
      assert is_atom(reason_version)
      assert reason_short == reason_version
    end
  end

  # ===========================================================================
  # Cross-cutting consistency: document which modules have merge, from_binary,
  # to_binary, and verify the API surface is consistent
  # ===========================================================================

  describe "API surface consistency — merge/2 availability" do
    # Modules with merge that return {:ok, t()} | {:error, reason}:
    # BloomFilter, CountMinSketch, TopK, HyperLogLog, Reservoir

    # Modules with merge on different types (signatures, not structs):
    # MinHash

    # Modules with always-succeeds merge (no compatibility check):
    # TDigest

    # Modules without merge at all:
    # CuckooFilter

    test "TDigest.merge always succeeds (returns bare struct, no error case)" do
      td1 = Sketch.TDigest.new(50) |> Sketch.TDigest.add(1.0)
      td2 = Sketch.TDigest.new(200) |> Sketch.TDigest.add(2.0)

      # TDigest merge always succeeds even with different deltas
      result = Sketch.TDigest.merge(td1, td2)
      assert %Sketch.TDigest{} = result
    end

    test "MinHash.merge operates on bare signature tuples, not structs" do
      mh = Sketch.MinHash.new(4, seed: 42)
      sig1 = Sketch.MinHash.signature(mh, MapSet.new(["a", "b"]))
      sig2 = Sketch.MinHash.signature(mh, MapSet.new(["c", "d"]))

      # MinHash merge takes and returns tuples — different domain than other modules
      result = Sketch.MinHash.merge(sig1, sig2)
      assert is_tuple(result)
      assert tuple_size(result) == 4
    end
  end

  # ===========================================================================
  # new/1 availability — every module exposes a constructor
  #
  # API contract:
  #   - All sketch modules expose new/1 (or new/0 with defaults) returning t()
  #
  # Current behavior:
  #   - BloomFilter.new/3   -> t()
  #   - CountMinSketch.new/3 -> t()
  #   - CuckooFilter.new/2  -> t()
  #   - HyperLogLog.new/2   -> t()
  #   - MinHash.new/2       -> t()
  #   - Reservoir.new/2     -> t()
  #   - TDigest.new/2       -> t()
  #   - TopK.new/2          -> t()
  # ===========================================================================

  describe "new/1 availability — all modules have a constructor" do
    test "BloomFilter.new returns a struct" do
      assert %Sketch.BloomFilter{} = Sketch.BloomFilter.new(100)
    end

    test "CountMinSketch.new returns a struct" do
      assert %Sketch.CountMinSketch{} = Sketch.CountMinSketch.new()
    end

    test "CuckooFilter.new returns a struct" do
      assert %Sketch.CuckooFilter{} = Sketch.CuckooFilter.new(100)
    end

    test "HyperLogLog.new returns a struct" do
      assert %Sketch.HyperLogLog{} = Sketch.HyperLogLog.new(4)
    end

    test "MinHash.new returns a struct" do
      assert %Sketch.MinHash{} = Sketch.MinHash.new(4)
    end

    test "Reservoir.new returns a struct" do
      assert %Sketch.Reservoir{} = Sketch.Reservoir.new(10)
    end

    test "TDigest.new returns a struct" do
      assert %Sketch.TDigest{} = Sketch.TDigest.new()
    end

    test "TopK.new returns a struct" do
      assert %Sketch.TopK{} = Sketch.TopK.new(5)
    end
  end

  # ===========================================================================
  # add availability — consistent element-insertion API across modules
  #
  # API contract:
  #   - Modules that accumulate individual elements expose add/2 (or add/3)
  #   - MinHash does not have add — it computes signatures over sets
  #
  # Current behavior:
  #   - BloomFilter.add/2      -> t()                        (pure, always succeeds)
  #   - CountMinSketch.add/3   -> t()                        (pure, always succeeds)
  #   - CuckooFilter.add/2     -> {:ok, t()} | {:error, :full}  (alias for insert/2)
  #   - HyperLogLog.add/2      -> t()                        (pure, always succeeds)
  #   - Reservoir.add/2        -> t()                        (pure, always succeeds)
  #   - TDigest.add/3          -> t()                        (pure, always succeeds)
  #   - TopK.add/3             -> t()                        (pure, always succeeds)
  #   - MinHash                -> no add (uses signature/2 over sets)
  # ===========================================================================

  describe "add availability — all element-accumulating modules have add" do
    test "BloomFilter.add returns updated struct" do
      bf = Sketch.BloomFilter.new(100)
      assert %Sketch.BloomFilter{} = Sketch.BloomFilter.add(bf, "x")
    end

    test "CountMinSketch.add returns updated struct" do
      cms = Sketch.CountMinSketch.new()
      assert %Sketch.CountMinSketch{} = Sketch.CountMinSketch.add(cms, "x")
    end

    test "CuckooFilter.add returns {:ok, t()} on success" do
      cf = Sketch.CuckooFilter.new(100)
      assert {:ok, %Sketch.CuckooFilter{}} = Sketch.CuckooFilter.add(cf, "x")
    end

    test "CuckooFilter.add is equivalent to insert" do
      cf = Sketch.CuckooFilter.new(100)
      {:ok, cf_add} = Sketch.CuckooFilter.add(cf, "test_element")
      {:ok, cf_insert} = Sketch.CuckooFilter.insert(cf, "test_element")

      assert Sketch.CuckooFilter.member?(cf_add, "test_element")
      assert Sketch.CuckooFilter.member?(cf_insert, "test_element")
      assert cf_add.count == cf_insert.count
    end

    test "HyperLogLog.add returns updated struct" do
      hll = Sketch.HyperLogLog.new(4)
      assert %Sketch.HyperLogLog{} = Sketch.HyperLogLog.add(hll, "x")
    end

    test "Reservoir.add returns updated struct" do
      r = Sketch.Reservoir.new(10)
      assert %Sketch.Reservoir{} = Sketch.Reservoir.add(r, "x")
    end

    test "TDigest.add returns updated struct" do
      td = Sketch.TDigest.new()
      assert %Sketch.TDigest{} = Sketch.TDigest.add(td, 42.0)
    end

    test "TopK.add returns updated struct" do
      tk = Sketch.TopK.new(5)
      assert %Sketch.TopK{} = Sketch.TopK.add(tk, "x", 10)
    end
  end
end
