defmodule NamedCrdtTest do
  use ExUnit.Case, async: true

  alias DeltaCrdt.GCounter
  alias DeltaCrdt.PNCounter
  alias DeltaCrdt.NamedCrdt

  test "basic gcounter test case" do
    {:ok, g1} = DeltaCrdt.start_link(NamedCrdt, GCounter, sync_interval: 50)
    DeltaCrdt.mutate_async(g1, :inc, [10])
    assert 10 = DeltaCrdt.read(g1)
  end

  test "basic pncounter test case" do
    {:ok, pn1} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)
    DeltaCrdt.mutate_async(pn1, :inc, [10])
    DeltaCrdt.mutate_async(pn1, :dec, [5])
    assert 5 = DeltaCrdt.read(pn1)
  end


  test "synchronization is directional, diffs are sent TO neighbours" do
    {:ok, c1} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)
    {:ok, c2} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)
    DeltaCrdt.set_neighbours(c1, [c2])
    DeltaCrdt.mutate(c1, :inc, [7])
    DeltaCrdt.mutate(c2, :dec, [4])
    Process.sleep(100)
    assert 7 = DeltaCrdt.read(c1)
    assert 3 = DeltaCrdt.read(c2)
  end

  test "can sync to neighbours specified by name" do
    {:ok, c1} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50, name: :neighbour_name_1PN)
    {:ok, c2} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50, name: :neighbour_name_2PN)
    DeltaCrdt.set_neighbours(c1, [:neighbour_name_2PN])
    DeltaCrdt.set_neighbours(c2, [{:neighbour_name_1PN, node()}])
    DeltaCrdt.mutate(c1, :dec, [10])
    DeltaCrdt.mutate(c2, :inc, [4])
    Process.sleep(100)
    assert -6 = DeltaCrdt.read(c1)
    assert -6 = DeltaCrdt.read(c2)
  end

  test "storage backend can store and retrieve state" do
    DeltaCrdt.start_link(NamedCrdt, PNCounter, storage_module: MemoryStorage, name: :storage_testPNstor)
    DeltaCrdt.mutate(:storage_testPNstor, :inc, [50])
    assert 50 = DeltaCrdt.read(:storage_testPNstor)
  end

  test "storage backend is used to rehydrate state after a crash" do
    task =
      Task.async(fn ->
        DeltaCrdt.start_link(NamedCrdt, PNCounter, storage_module: MemoryStorage, name: :storage_testPN)
        DeltaCrdt.mutate(:storage_testPN, :inc, [105])
      end)

    Task.await(task)

    # time for the previous process to deregister itself
    Process.sleep(10)

    {:ok, _} = DeltaCrdt.start_link(NamedCrdt, PNCounter, storage_module: MemoryStorage, name: :storage_testPN)

    assert 105 = DeltaCrdt.read(:storage_testPN)
  end

  test "syncs after adding neighbour" do
    {:ok, c1} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)
    {:ok, c2} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)
    DeltaCrdt.mutate(c1, :inc, [100])
    DeltaCrdt.mutate(c2, :dec, [3])
    DeltaCrdt.set_neighbours(c1, [c2])
    Process.sleep(100)
    assert 97 = DeltaCrdt.read(c2)
  end

  test "can sync after network partition" do
    {:ok, c1} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)
    {:ok, c2} = DeltaCrdt.start_link(NamedCrdt, PNCounter, sync_interval: 50)

    DeltaCrdt.set_neighbours(c1, [c2])
    DeltaCrdt.set_neighbours(c2, [c1])

    DeltaCrdt.mutate(c1, :inc, [20])
    DeltaCrdt.mutate(c2, :inc, [12])

    Process.sleep(200)

    assert 32 = DeltaCrdt.read(c1)
    assert 32 = DeltaCrdt.read(c2)

    # uncouple them
    DeltaCrdt.set_neighbours(c1, [])
    DeltaCrdt.set_neighbours(c2, [])

    DeltaCrdt.mutate(c1, :inc, [30])
    DeltaCrdt.mutate(c2, :dec, [30])

    Process.sleep(200)

    assert 62 = DeltaCrdt.read(c1)
    assert 2 = DeltaCrdt.read(c2)

    # make them neighbours again
    DeltaCrdt.set_neighbours(c1, [c2])
    DeltaCrdt.set_neighbours(c2, [c1])

    Process.sleep(200)

    assert 32 = DeltaCrdt.read(c1)
    assert 32 = DeltaCrdt.read(c2)
  end


end
