defmodule Sketch.Hash do
  @moduledoc false

  # Shared hash helpers for probabilistic data structures.
  #
  # Uses `:erlang.phash2/2` as the default hash function. All modules accept
  # a `:hash_fn` option to override for testing or custom hashing.

  import Bitwise

  @max_32 bsl(1, 32)

  @doc false
  @spec hash32(term()) :: non_neg_integer()
  def hash32(term) do
    # phash2 with range 2^32 gives a 32-bit hash
    :erlang.phash2(term, @max_32)
  end

  @doc false
  @spec double_hash(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def double_hash(h1, h2, i) do
    # Kirsch-Mitzenmacher: h1 + i * h2
    h1 + i * h2
  end
end
