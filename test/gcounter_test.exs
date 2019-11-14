defmodule GCounterTest do
  use ExUnit.Case

  alias DeltaCrdt.GCounter

  test "inc and read a value" do
    assert 4 = GCounter.inc(4, :crypto.rand_uniform(0,18446744073709551616), GCounter.new()) |> GCounter.read()
  end

  test "join two inc same pid" do
    s1 = GCounter.inc(5, 42, GCounter.new())
    assert 5 = GCounter.inc(42, GCounter.new()) |> GCounter.join(s1) |> GCounter.read()
  end

end
