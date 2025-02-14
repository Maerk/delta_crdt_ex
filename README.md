# DeltaCrdt

[![Hex pm](http://img.shields.io/hexpm/v/delta_crdt.svg?style=flat)](https://hex.pm/packages/delta_crdt) [![CircleCI badge](https://circleci.com/gh/Maerk/delta_crdt_ex.png?circle-token=:circle-token)](https://circleci.com/gh/Maerk/delta_crdt_ex)

DeltaCrdt implements a key/value store using concepts from Delta CRDTs, and relies on [`MerkleMap`](https://github.com/derekkraan/merkle_map) for efficient synchronization.
It implements also GCounter and PNCounter

There is a (slightly out of date) [introductory blog post](https://medium.com/@derek.kraan2/dc838c383ad5) and the (very much up to date) official documentation on [hexdocs.pm](https://hexdocs.pm/delta_crdt) is also very good.

The following papers have been used to implement this library:
- [`Delta State Replicated Data Types – Almeida et al. 2016`](https://arxiv.org/pdf/1603.01529.pdf)
- [`Efficient Synchronization of State-based CRDTs – Enes et al. 2018`](https://arxiv.org/pdf/1803.02750.pdf)

## Usage

Documentation can be found on [hexdocs.pm](https://hexdocs.pm/delta_crdt).

Here's a short example to illustrate adding an entry to a map:

```elixir
# start 2 Delta CRDTs
{:ok, crdt1} = DeltaCrdt.start_link(DeltaCrdt.Causal, DeltaCrdt.AWLWWMap)
{:ok, crdt2} = DeltaCrdt.start_link(DeltaCrdt.Causal, DeltaCrdt.AWLWWMap)

# make them aware of each other
DeltaCrdt.set_neighbours(crdt1, [crdt2])

# show the initial value
DeltaCrdt.read(crdt1)
%{}

# add a key/value in crdt1
DeltaCrdt.mutate(crdt1, :add, ["CRDT", "is magic!"])

# read it after it has been replicated to crdt2
DeltaCrdt.read(crdt2)
%{"CRDT" => "is magic!"}

#start 2 PNCounter
{:ok, crdt3} = DeltaCrdt.start_link(DeltaCrdt.NamedCrdt,DeltaCrdt.PNCounter)
{:ok, crdt4} = DeltaCrdt.start_link(DeltaCrdt.NamedCrdt,DeltaCrdt.PNCounter)

DeltaCrdt.set_neighbours(crdt3, [crdt4])

DeltaCrdt.read(crdt4)
0

DeltaCrdt.mutate(crdt3, :inc, [5])

DeltaCrdt.read(crdt4)
5

DeltaCrdt.mutate(crdt3, :dec, [3])

DeltaCrdt.read(crdt4)
2

#start 2 GCounter
{:ok, crdt5} = DeltaCrdt.start_link(DeltaCrdt.NamedCrdt,DeltaCrdt.GCounter)
{:ok, crdt6} = DeltaCrdt.start_link(DeltaCrdt.NamedCrdt,DeltaCrdt.GCounter)

DeltaCrdt.set_neighbours(crdt5, [crdt6])

DeltaCrdt.read(crdt6)
0

DeltaCrdt.mutate(crdt5, :inc, [5])

DeltaCrdt.read(crdt6)
5
```

## Telemetry metrics

DeltaCrdt publishes the metric `[:delta_crdt, :sync, :done]`.

## Installation

The package can be installed by adding `delta_crdt` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delta_crdt, "~> 0.5.0"}
  ]
end
```
