defmodule DeltaCrdt.GCounter do
  require Logger
  defstruct m: Map.new(),
            id: nil

  @doc false
  def new(id), do: %__MODULE__{id: id}

  #return only delta
  def inc(value \\ 1, state) do
    if value <= 0 do
      Logger.warn "value must be positive"
    else
    %{__MODULE__.new(state.id) | m: Map.put(state.m, state.id, Map.get(state.m, state.id, 0) + value)}
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
