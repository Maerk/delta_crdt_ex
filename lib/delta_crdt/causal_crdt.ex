defmodule DeltaCrdt.CausalCrdt do
  use GenServer

  require Logger

  require BenchmarkHelper

  BenchmarkHelper.inject_in_dev()

  @type delta :: {k :: integer(), delta :: any()}
  @type delta_interval :: {a :: integer(), b :: integer(), delta :: delta()}

  @moduledoc false

  defstruct node_id: nil,
            name: nil,
            on_diffs: nil,
            storage_module: nil,
            crdt_module: nil,
            crdt_state: nil,
            merkle_map: MerkleMap.new(),
            sequence_number: 0,
            neighbours: MapSet.new(),
            neighbour_monitors: %{},
            outstanding_syncs: %{},
            sync_interval: nil,
            max_sync_size: nil

  defmodule(Diff, do: defstruct(continuation: nil, dots: nil, originator: nil, from: nil, to: nil))

  defmacrop strip_continue(tuple) do
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

    initial_state = %__MODULE__{
      node_id: :rand.uniform(1_000_000_000),
      name: Keyword.get(opts, :name),
      on_diffs: Keyword.get(opts, :on_diffs, fn _diffs -> nil end),
      storage_module: Keyword.get(opts, :storage_module),
      sync_interval: Keyword.get(opts, :sync_interval),
      max_sync_size: Keyword.get(opts, :max_sync_size),
      crdt_module: crdt_module,
      crdt_state: crdt_module.new() |> crdt_module.compress_dots()
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
    diff = reverse_diff(diff)

    case MerkleMap.diff_keys(diff.continuation, state.merkle_map, 8) do
      {:continue, continuation} ->
        %Diff{diff | continuation: MerkleMap.truncate_diff(continuation, state.max_sync_size)}
        |> send_diff_continue()

      {:ok, []} ->
        ack_diff(diff)

      {:ok, keys} ->
        send_diff(diff, Enum.take(keys, state.max_sync_size), state)
        ack_diff(diff)
    end

    {:noreply, state}
  end

  def handle_info({:get_diff, diff, keys}, state) do
    send(
      diff.to,
      {:diff,
       %{state.crdt_state | dots: diff.dots, value: Map.take(state.crdt_state.value, keys)}, keys}
    )

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

    new_neighbour_monitors =
      Enum.filter(state.neighbour_monitors, fn {neighbour, ref} ->
        if MapSet.member?(state.neighbours, neighbour) do
          true
        else
          Process.demonitor(ref)
          false
        end
      end)
      |> Map.new()

    new_outstanding_syncs =
      Enum.filter(state.outstanding_syncs, fn {neighbour, 1} ->
        MapSet.member?(state.neighbours, neighbour)
      end)
      |> Map.new()

    state = %{
      state
      | neighbour_monitors: new_neighbour_monitors,
        outstanding_syncs: new_outstanding_syncs
    }

    {:noreply, sync_interval_or_state_to_all(state)}
  end

  def handle_info({:diff, diff, keys}, state) do
    new_state = update_state_with_delta(state, diff, keys)
    {:noreply, new_state}
  end

  def handle_info(:sync, state) do
    state = sync_interval_or_state_to_all(state)

    Process.send_after(self(), :sync, state.sync_interval)

    {:noreply, state}
  end

  def handle_call(:read, _from, state), do: {:reply, Enum.into(state.merkle_map, %{}), state}

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

      {node_id, sequence_number, crdt_state, merkle_map} ->
        Map.put(state, :sequence_number, sequence_number)
        |> Map.put(:crdt_state, crdt_state)
        |> Map.put(:merkle_map, merkle_map)
        |> Map.put(:node_id, node_id)
        |> remove_crdt_state_keys()
    end
  end

  defp remove_crdt_state_keys(state) do
    %{state | crdt_state: Map.put(state.crdt_state, :keys, MapSet.new())}
  end

  defp write_to_storage(%{storage_module: nil} = state) do
    state
  end

  defp write_to_storage(state) do
    :ok =
      state.storage_module.write(
        state.name,
        {state.node_id, state.sequence_number, state.crdt_state, state.merkle_map}
      )

    state
  end

  defp sync_interval_or_state_to_all(state) do
    state = monitor_neighbours(state)
    {:continue, continuation} = MerkleMap.prepare_partial_diff(state.merkle_map, 8)

    diff = %Diff{
      continuation: continuation,
      dots: state.crdt_state.dots,
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
  end

  defp monitor_neighbours(state) do
    new_neighbour_monitors =
      Enum.reduce(state.neighbours, state.neighbour_monitors, fn neighbour, monitors ->
        Map.put_new_lazy(monitors, neighbour, fn -> Process.monitor(neighbour) end)
      end)

    Map.put(state, :neighbour_monitors, new_neighbour_monitors)
  end

  defp reverse_diff(diff) do
    %Diff{diff | from: diff.to, to: diff.from}
  end

  defp send_diff_continue(diff) do
    send(diff.to, {:diff, diff})
  end

  defp send_diff(diff, keys, state) do
    if diff.originator == diff.to do
      send(diff.from, {:get_diff, diff, keys})
    else
      send(
        diff.to,
        {:diff,
         %{state.crdt_state | dots: diff.dots, value: Map.take(state.crdt_state.value, keys)},
         keys}
      )
    end
  end

  defp handle_operation({function, [key | rest_args]}, state) do
    delta =
      apply(state.crdt_module, function, [key | rest_args] ++ [state.node_id, state.crdt_state])

    update_state_with_delta(state, delta, [key])
  end

  defp diff(old_state, new_state, keys) do
    old = old_state.crdt_module.read(old_state.crdt_state, keys)
    new = old_state.crdt_module.read(new_state.crdt_state, keys)

    Enum.flat_map(keys, fn key ->
      case {Map.get(old, key), Map.get(new, key)} do
        {old, old} -> []
        {_old, nil} -> [{:remove, key}]
        {_old, new} -> [{:add, key, new}]
      end
    end)
  end

  defp update_state_with_delta(state, delta, keys) do
    new_crdt_state = state.crdt_module.join(state.crdt_state, delta, keys)
    diffs = diff(state, Map.put(state, :crdt_state, new_crdt_state), keys)

    new_merkle_map =
      Enum.reduce(diffs, state.merkle_map, fn
        {:add, key, value}, mm -> MerkleMap.put(mm, key, value)
        {:remove, key}, mm -> MerkleMap.delete(mm, key)
      end)

    case diffs do
      [] -> nil
      diffs -> state.on_diffs.(diffs)
    end

    Map.put(state, :crdt_state, new_crdt_state)
    |> Map.put(:merkle_map, new_merkle_map)
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
