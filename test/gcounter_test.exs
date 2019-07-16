defmodule GCounterTest do
  use ExUnit.Case

  alias DeltaCrdt.GCounter

  test "inc and read a value" do
    assert 4 = GCounter.inc(4, GCounter.new(self())) |> GCounter.read()
  end

  test "join two inc same pid" do
    s1 = GCounter.inc(5, GCounter.new(self()))
    assert 5 = GCounter.inc(GCounter.new(self())) |> GCounter.join(s1) |> GCounter.read()
  end

end
