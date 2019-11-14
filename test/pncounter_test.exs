defmodule PNCounterTest do
  use ExUnit.Case

  alias DeltaCrdt.PNCounter

  test "inc and read a value" do
    assert 4 = PNCounter.inc(4, :crypto.rand_uniform(0,18446744073709551616), PNCounter.new()) |> PNCounter.read()
  end

  test "dec and read a value" do
    assert -4 = PNCounter.dec(4, :crypto.rand_uniform(0,18446744073709551616), PNCounter.new()) |> PNCounter.read()
  end

  test "join inc and dec" do
    s1 = PNCounter.inc(10, :crypto.rand_uniform(0,18446744073709551616), PNCounter.new())
    assert 9 = PNCounter.dec(:crypto.rand_uniform(0,18446744073709551616), PNCounter.new()) |> PNCounter.join(s1) |> PNCounter.read()
  end


end
