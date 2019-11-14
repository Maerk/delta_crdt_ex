defmodule DeltaCrdt.GCounter do
  require Logger
  defstruct m: Map.new()

  @doc false
  def new(), do: %__MODULE__{}

  #return only delta
  def inc(value \\ 1, i, state) do
    if value <= 0 do
      Logger.warn "value must be positive"
    else
      %{__MODULE__.new() | m: Map.put(Map.new(), i, Map.get(state.m, i, 0) + value)}
    end
  end

  @doc false
  def join(delta1, delta2) do
    %{delta1 | m: Map.merge(delta1.m, delta2.m,  fn _k, v1, v2 -> max(v1, v2) end)}
  end

  def read(%{m: values}) do
    Map.values(values) |> List.foldl(0, fn x, acc -> x+acc end)
  end

  def get_m(%{m: values}) do
    [values]
  end
end
