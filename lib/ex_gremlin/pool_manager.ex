defmodule ExGremlin.PoolManager do
	@moduledoc false

	use GenServer

	require Logger

	@pool_supervisor ExGremlin.PoolSupervisor
	@pool_worker ExGremlin.Client

	@monitors :pooler_monitors

	defmodule State do
		defstruct size: nil, overflow: 0, waitings: nil, max_overflow: nil, workers: [], worker_spec: nil
	end

###==========================================================================###
### Public API                                                               ###
###==========================================================================###
	def checkout do
		GenServer.call(__MODULE__, :checkout)
	end

	def checkout(isBlock, timeout \\ 5_000) do
		GenServer.call(__MODULE__, {:checkout, isBlock}, timeout)
	end

	def checkin(worker) do
		GenServer.cast(__MODULE__, {:checkin, worker})
	end

	def transaction(fun) do
		worker = ExGremlin.PoolManager.checkout(true)
		try do
			fun.(worker)
		after
			ExGremlin.PoolManager.checkin(worker)
		end
	end


###==========================================================================###
### API                                                                      ###
###==========================================================================###
	def start_link(args) do
		GenServer.start_link(__MODULE__, args, name: __MODULE__)
	end


###==========================================================================###
### Callbacks                                                                ###
###==========================================================================###
	@impl GenServer
	def init(args) do
		Process.flag(:trap_exit, true)

		pool = Map.get(args,:pool)
		gremlin = Map.get(args,:gremlin)

		size = Map.get(pool,:size)
		max_overflow = Map.get(pool,:max_overflow)

		:ets.new(@monitors,[:protected,:named_table])
		send(self(),:init_pool)
		{:ok,%State{size: size, waitings: :queue.new(), max_overflow: max_overflow, worker_spec: Supervisor.child_spec({@pool_worker, gremlin},restart: :temporary)}}
	end

###################
### handle_cast ###
###################
	@impl GenServer
	def handle_cast({:checkin, worker_pid}, %{waitings: waitings} =state) do
		case :queue.out(waitings) do
			{{:value,{from,ref}},left} ->
				true = :ets.insert(@monitors, {worker_pid,ref})
				GenServer.reply(from, worker_pid)
				{:noreply, %{state | waitings: left}}
			{:empty, empty} ->
				case :ets.lookup(@monitors, worker_pid) do
					[{pid,ref}] ->
						new_state = do_checkin(pid,ref,%{state | waitings: empty})
						{:noreply, new_state}
					[] ->
						{:noreply, %{state | waitings: empty}}
				end
		end
	end

###################
### handle_call ###
###################
	@impl GenServer
	def handle_call({:checkout, isBlock}, {from,_ref}, %{workers: pool_workers, worker_spec: worker_spec} = state) do
		case pool_workers do
			[worker | workers] ->
				monitor_worker(worker,from)
				{:reply, worker,%{state | workers: workers}}
			[]  ->
				%{overflow: overflow, max_overflow: max_overflow} = state
				if max_overflow > 0 and overflow < max_overflow do
					worker = new_worker(worker_spec)
					monitor_worker(worker,from)
					{:reply, worker,%{state | overflow: overflow+1}}
				else
					if isBlock == true do
						%{waitings: waitings} = state

						ref = Process.monitor(from)
						waitings = :queue.in({from,ref}, waitings)

						{:noreply, %{state | waitings: waitings}, :infinity}
					else
						{:reply, :full, state}
					end
				end
		end
	end

###################
### handle_info ###
###################
	@impl GenServer
	def handle_info(:init_pool,%{size: size,worker_spec: worker_spec} = state) do
		workers =
			for _ <- 1..size do
				new_worker(worker_spec)
			end
		{:noreply, %{state | workers: workers}}
	end

	@impl GenServer
	def handle_info({:DOWN, ref, :process, _, _}, state) do
		Logger.debug("[pool manager]worker down #{inspect :ets.match(@monitors, {:"$0",ref})}")

		case :ets.match(@monitors, {:"$0",ref}) do
			[[pid]] ->
				true = :ets.delete(@monitors,pid)
				{:noreply, state |> idle_worker(pid)}
			[] ->
				{:noreply, state}
		end
	end

	@impl GenServer
	def handle_info({:EXIT, pid, _reason}, %{workers: workers,worker_spec: worker_spec,waitings: waitings} = state) do
		case :ets.lookup(@monitors, pid) do
			[{pid, ref}] ->
				true = Process.demonitor(ref)
				true = :ets.delete(@monitors, pid)
			[] ->
				:ok
		end
		workers =
			if workers |> Enum.member?(pid) do
				workers
				|> Enum.reject( & &1 == pid)
			end

		worker = new_worker(worker_spec)
		case :queue.out(waitings) do
			{{:value,{from,ref}},left} ->
				monitor_worker(worker,ref)
				GenServer.reply(from, worker)

				{:noreply, %{state | waitings: left, workers: worker}}
			{:empty, empty} ->
				state = %{state | waitings: empty, workers: workers}
						|> exit_worker(worker)
				{:noreply, state}
		end
	end

	@impl GenServer
	def handle_info(msg, state) do
		Logger.debug("Pooler.Manager[handle_info][unhandled message] : #{inspect msg}")
		{:noreply, state}
	end
###==========================================================================###
### Private Functions                                                        ###
###==========================================================================###

	defp new_worker(worker_spec) do
		{:ok, pid} = ExGremlin.PoolSupervisor.start_worker(@pool_supervisor, worker_spec)
		true = Process.link(pid)
		pid
	end

	defp monitor_worker(worker_pid, client) do
		ref = Process.monitor(client)
		:ets.insert(@monitors,{worker_pid, ref})
		:ok
	end

	defp idle_worker(%{workers: workers} = state, worker_pid) do
		%{state | workers: [worker_pid | workers]}
	end

	defp do_checkin(worker_pid,ref,%{overflow: overflow} = state) do
		if overflow > 0 do
			Process.demonitor(ref)
			true = :ets.delete(@monitors, worker_pid)
			ExGremlin.PoolSupervisor.stop_worker(@pool_supervisor, worker_pid)
			%{state | overflow: overflow-1}
		else
			Process.demonitor(ref)
			true = :ets.delete(@monitors,worker_pid)
			state |> idle_worker(worker_pid)
		end
	end

	defp exit_worker(%{workers: workers,overflow: overflow} = state, worker_pid) do
		if overflow > 0 do
			%{state | overflow: overflow-1}
		else
			%{state | workers: [worker_pid | workers]}
		end
	end

end
