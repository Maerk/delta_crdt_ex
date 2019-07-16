defmodule DeltaCrdt.PNCounter do
  defstruct p: DeltaCrdt.GCounter,
            n: DeltaCrdt.GCounter

  @doc false
  def new(id), do: %__MODULE__{p: DeltaCrdt.GCounter.new(id), n: DeltaCrdt.GCounter.new(id)}

  def inc(value \\ 1, state) do
    %{__MODULE__.new(state.p.id) | p: DeltaCrdt.GCounter.inc(value, state.p)}
  end

  def dec(value \\ 1, state) do
    %{__MODULE__.new(state.n.id) | n: DeltaCrdt.GCounter.inc(value, state.n)}
  end

  def join(delta1, delta2) do
    %{
      %{delta1 | p: DeltaCrdt.GCounter.join(delta1.p, delta2.p)}
      | n: DeltaCrdt.GCounter.join(delta1.n,delta2.n)
    }
  end

  def read(%{p: pos, n: neg}) do
    DeltaCrdt.GCounter.read(pos) - DeltaCrdt.GCounter.read(neg)
  end

  def get_m(%{p: pos, n: neg}) do
    DeltaCrdt.GCounter.get_m(pos) ++ DeltaCrdt.GCounter.get_m(neg)
  end

end
