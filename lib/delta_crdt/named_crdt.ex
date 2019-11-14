defmodule DeltaCrdt.NamedCrdt do
  use GenServer

  require Logger

  #require BenchmarkHelper

  #BenchmarkHelper.inject_in_dev()

  @moduledoc false

  defstruct node_id: nil,
            name: nil,
            on_diffs: nil,
            storage_module: nil,
            crdt_module: nil,
            crdt_state: nil,
            crdt_deltas: nil,
            sequence_number: 0,
            neighbours: MapSet.new(),
            neighbour_monitors: %{},
            outstanding_syncs: %{},
            sync_interval: nil,
            max_sync_size: nil

  defmodule(Diff, do: defstruct(deltas: nil, originator: nil, from: nil, to: nil))

  defmacrop strip_continue(tuple) do
    #erlang version > 20
    if System.otp_release() |> String.to_integer() > 20 do
      tuple
    else
      quote do
        case unquote(tuple) do
          {tup1, tup2, {:continue, _}} -> {tup1, tup2}
        end
      end
    end
  end

  ### GenServer callbacks

  def init(opts) do
    send(self(), :sync)

    Process.flag(:trap_exit, true)

    crdt_module = Keyword.get(opts, :crdt_module)

    max_sync_size =
      case Keyword.get(opts, :max_sync_size) do
        :infinite ->
          :infinite

        size when is_integer(size) and size > 0 ->
          size

        invalid_size ->
          raise ArgumentError, "#{inspect(invalid_size)} is not a valid max_sync_size"
      end

    initial_state = %__MODULE__{
      node_id: :rand.uniform(18446744073709551616),
      name: Keyword.get(opts, :name),
      on_diffs: Keyword.get(opts, :on_diffs, fn _diffs -> nil end),
      storage_module: Keyword.get(opts, :storage_module),
      sync_interval: Keyword.get(opts, :sync_interval),
      max_sync_size: max_sync_size,
      crdt_module: crdt_module,
      crdt_state: crdt_module.new(),
      crdt_deltas: crdt_module.new()
    }

    strip_continue({:ok, initial_state, {:continue, :read_storage}})
  end

  def handle_continue(:read_storage, state) do
    {:noreply, read_from_storage(state)}
  end

  def handle_info({:ack_diff, to}, state) do
    {:noreply, %{state | outstanding_syncs: Map.delete(state.outstanding_syncs, to)}}
  end

  def handle_info({:diff, diff}, state) do
    state =
      %{
        %{state | crdt_state: state.crdt_module.join(state.crdt_state, diff.deltas)}
        | crdt_deltas: state.crdt_module.join(state.crdt_deltas, diff.deltas)
      }

    diff = reverse_diff(diff)
    ack_diff(diff)
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _object, _reason}, state) do
    {neighbour, _ref} =
      Enum.find(state.neighbour_monitors, fn
        {_neighbour, ^ref} -> true
        _ -> false
      end)

    new_neighbour_monitors = Map.delete(state.neighbour_monitors, neighbour)

    new_outstanding_syncs = Map.delete(state.outstanding_syncs, neighbour)

    new_state = %{
      state
      | neighbour_monitors: new_neighbour_monitors,
        outstanding_syncs: new_outstanding_syncs
    }

    {:noreply, new_state}
  end

  def handle_info({:set_neighbours, neighbours}, state) do
    state = %{state | neighbours: MapSet.new(neighbours)}

    new_outstanding_syncs =
      Enum.filter(state.outstanding_syncs, fn {neighbour, 1} ->
        MapSet.member?(state.neighbours, neighbour)
      end)
      |> Map.new()

    state = %{state | outstanding_syncs: new_outstanding_syncs}

    {:noreply, sync_interval_or_state_to_all(false, state)}
  end

  def handle_info(:sync, state) do
    state = sync_interval_or_state_to_all(state)

    Process.send_after(self(), :sync, state.sync_interval)

    {:noreply, state}
  end

  def handle_call(:read, _from, state), do: {:reply, state.crdt_module.read(state.crdt_state), state}

  def handle_call({:operation, operation}, _from, state) do
    {:reply, :ok, handle_operation(operation, state)}
  end

  def handle_cast({:operation, operation}, state) do
    {:noreply, handle_operation(operation, state)}
  end

  # TODO this won't sync everything anymore, since syncing is now a 2-step process.
  # Figure out how to do this properly. Maybe with a `receive` block.
  def terminate(_reason, state) do
    sync_interval_or_state_to_all(state)
  end

  defp read_from_storage(%{storage_module: nil} = state) do
    state
  end

  defp read_from_storage(state) do
    case state.storage_module.read(state.name) do
      nil ->
        state

      {node_id, sequence_number, crdt_state} ->
        Map.put(state, :sequence_number, sequence_number)
        |> Map.put(:crdt_state, crdt_state)
        |> Map.put(:node_id, node_id)
    end
  end

  defp write_to_storage(%{storage_module: nil} = state) do
    state
  end

  defp write_to_storage(state) do
    :ok =
      state.storage_module.write(
        state.name,
        {state.node_id, state.sequence_number, state.crdt_state}
      )

    state
  end

  defp sync_interval_or_state_to_all(choose_delta \\ true, state) do
    state = monitor_neighbours(state)

    diff = %Diff{
      deltas: (if choose_delta, do: state.crdt_deltas, else: state.crdt_state),
      from: self(),
      originator: self()
    }
    new_outstanding_syncs =
      Enum.filter(state.neighbours, &process_alive?/1)
      |> Enum.reject(fn pid -> self() == pid end)
      |> Enum.reduce(state.outstanding_syncs, fn neighbour, outstanding_syncs ->
        Map.put_new_lazy(outstanding_syncs, neighbour, fn ->
          send(neighbour, {:diff, %Diff{diff | to: neighbour}})
          1
        end)
      end)

    Map.put(state, :outstanding_syncs, new_outstanding_syncs)
    |> Map.put(:crdt_deltas, state.crdt_module.new())
  end

  defp monitor_neighbours(state) do
    new_neighbour_monitors =
      Enum.reduce(state.neighbours, state.neighbour_monitors, fn neighbour, monitors ->
        #add neighbour to monitor if is't there
        Map.put_new_lazy(monitors, neighbour, fn -> Process.monitor(neighbour) end)
      end)

    Map.put(state, :neighbour_monitors, new_neighbour_monitors)
  end

  defp reverse_diff(diff) do
    %Diff{diff | from: diff.to, to: diff.from}
  end

  defp handle_operation({function, [value]}, state) when is_integer(value) do
    delta = apply(state.crdt_module, function, [value, state.node_id, state.crdt_state])
    update_state_with_delta(state, delta)
  end

  defp diff_val([old | tlo], [new | tln], neg \\ false) do
    ls =
      Enum.flat_map(Map.keys(new), fn key ->
        case {Map.get(old, key, 0), Map.get(new, key, 0)} do
          {old, old} -> []
          {old, new} when neg -> [{:dec, key, new-old}]
          {old, new} -> [{:inc, key, new-old}]
        end
      end)
    if tlo != [] && tln != [] do
      ls ++ diff_val(tlo,tln, true)
    else
      ls
    end
  end

  defp diff(old_state, new_state) do
    oldl = old_state.crdt_module.get_m(old_state.crdt_state)
    newl = old_state.crdt_module.get_m(new_state.crdt_state)
    diff_val(oldl, newl)
  end

  defp update_state_with_delta(state, delta) do
    new_crdt_state = state.crdt_module.join(state.crdt_state, delta)
    new_crdt_delta = state.crdt_module.join(state.crdt_deltas, delta)
    diffs = diff(state, Map.put(state, :crdt_state, new_crdt_state))

    case diffs do
      [] -> nil
      diffs -> state.on_diffs.(diffs)
    end

    Map.put(state, :crdt_state, new_crdt_state)
    |> Map.put(:crdt_deltas, new_crdt_delta)
    |> write_to_storage()
  end

  defp process_alive?({name, n}) when n == node(), do: Process.whereis(name) != nil

  defp process_alive?({name, n}) do
    Enum.member?(Node.list(), n) && :rpc.call(n, Process, :whereis, [name]) != nil
  end

  defp process_alive?(name) when is_atom(name) do
    with pid when is_pid(pid) <- Process.whereis(name) do
      Process.alive?(pid)
    end
  end

  defp process_alive?(pid) when node(pid) == node(), do: Process.alive?(pid)

  defp process_alive?(pid) do
    Enum.member?(Node.list(), node(pid)) && :rpc.call(node(pid), Process, :alive?, [pid])
  end

  defp ack_diff(%{originator: originator, from: originator, to: to}) do
    send(originator, {:ack_diff, to})
  end

  defp ack_diff(%{originator: originator, from: from, to: originator}) do
    send(originator, {:ack_diff, from})
  end

end
