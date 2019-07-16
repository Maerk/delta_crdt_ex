defmodule PNCounterTest do
  use ExUnit.Case

  alias DeltaCrdt.PNCounter

  test "inc and read a value" do
    assert 4 = PNCounter.inc(4, PNCounter.new(self())) |> PNCounter.read()
  end

  test "dec and read a value" do
    assert -4 = PNCounter.dec(4, PNCounter.new(self())) |> PNCounter.read()
  end

  test "join inc and dec" do
    s1 = PNCounter.inc(10, PNCounter.new(self()))
    assert 9 = PNCounter.dec(PNCounter.new(self())) |> PNCounter.join(s1) |> PNCounter.read()
  end


end
