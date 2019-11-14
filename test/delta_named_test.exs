defmodule DeltaNamedTest do
  use ExUnit.Case, async: true

  alias DeltaCrdt.NamedCrdt
  alias DeltaCrdt.GCounter
  alias DeltaCrdt.PNCounter

  test "receives deltas updates" do
    test_pid = self()

    {:ok, c1} =
      DeltaCrdt.start_link(NamedCrdt,
        PNCounter,
        sync_interval: 50,
        on_diffs: fn diffs -> send(test_pid, {:diff, diffs}) end
      )

    :ok = DeltaCrdt.mutate(c1, :inc, [5])
    assert_received({:diff, [{:inc, _node, 5}]})
    :ok = DeltaCrdt.mutate(c1, :dec, [6])
    assert_received({:diff, [{:dec, _node, 6}]})

    {:ok, c2} =
      DeltaCrdt.start_link(NamedCrdt,
        GCounter,
        sync_interval: 50,
        on_diffs: fn diffs -> send(test_pid, {:diff, diffs}) end
      )

    :ok = DeltaCrdt.mutate(c2, :inc, [5])
    assert_received({:diff, [{:inc, c2, 5}]})
  end

end
