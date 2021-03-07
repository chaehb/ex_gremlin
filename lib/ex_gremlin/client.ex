defmodule ExGremlin.Client do
	@moduledoc """
	Gremlin Websocket Client by Gun.
	"""

	use GenServer, restart: :temporary

	alias ExGremlin.Request
	alias ExGremlin.PoolManager

	require Logger

	@type state :: %{
		pid: pid(),
		stream_ref: :gun.stream_ref(), 
		from: {pid(), tag :: term()},
		timer_ref: reference(),
		ping_delay: 0
	}

	@type response ::
		{:ok, list()}
		| {:error, :unauthorized, String.t()}
		| {:error, :malformed_request, String.t()}
		| {:error, :invalid_request_arguments, String.t()}
		| {:error, :server_error, String.t()}
		| {:error, :script_evaluation_error, String.t()}
		| {:error, :server_timeout, String.t()}
		| {:error, :server_serialization_error, String.t()}

	@open_options %{
		connect_timeout: 60_000,
		retry: 10,
		retry_timeout: 500
	}

	defmodule State do
		defstruct [:pid, :stream_ref, :from, :timer_ref, :ping_delay]
	end

###==========================================================================###
### Public API                                                               ###
###==========================================================================###
	@doc """
	Accepts a graph or a raw string query which it converts into a query and queries the gremlin database.

	Params:
	* query - A 'ExGremlin.Graph.t' or raw string query
	* timeout (Default: 5000ms) - Timeout in milliseconds to pass to GenServer
	"""
  @spec query(ExGremlin.Graph.t() | String.t(), number() | :infinity ) :: response
	def query(query, timeout \\ 5000) do
		payload =
			query
			|> Request.new()
			|> Jason.encode!()

		PoolManager.transaction(fn worker_pid -> 
			GenServer.call(worker_pid, {:query, payload, timeout},:infinity)
		end)
	end

###==========================================================================###
### API                                                                      ###
###==========================================================================###
  
  @spec start_link(%{host: String.t(), port: number(), path: String.t(), secure: boolean()}) :: pid()
	def start_link(%{host: _host, port: _port, path: _path, secure: _secure} = args) do
		GenServer.start_link(__MODULE__, args, [])
	end

	defdelegate stop(pid), to: GenServer

###==========================================================================###
### Callbacks                                                                ###
###==========================================================================###

	@impl GenServer
	def init(args) do
		{:ok, args,{:continue,:connect}}
	end

	@impl GenServer
	@spec handle_continue(:connect,%{host: String.t(), port: number(), path: String.t(), secure: boolean()}) :: {:noreply, state}
	def handle_continue(:connect, args) do
		connState = connect(args)
		ping_delay = Map.get(args,:ping_delay, 0)
		if ping_delay > 0 do
			Process.send_after(self(), :ping, ping_delay)
		end
		{:noreply, %{connState | ping_delay: ping_delay}}
	end
###################
### handle_cast ###
###################

###################
### handle_call ###
###################

	@impl GenServer
	@spec handle_call({:query, String.t(), number() | :infinity}, {pid(), tag :: term()} , state) :: {:noreply, state, :infinity}
	def handle_call({:query, query, timeout}, from, state) do
		timer = Process.send_after(self(),:query_timeout, timeout)

		:gun.ws_send(state.pid, state.stream_ref,{:binary, <<16, "application/json" <> query>>})

		{:noreply, %{state | from: from, timer_ref: timer}, :infinity}
	end

###################
### handle_info ###
###################

	##============================ Websocket responses
	@impl GenServer
	def handle_info({:gun_ws, _pid, _stream_ref, {:binary, data}}, %{from: from} = state) do
		if from != nil do
			if state.timer_ref != nil do
				Process.cancel_timer(state.timer_ref)
			end
			GenServer.reply(from, ExGremlin.Response.parse(data))
		else
			:gun.flush(state.pid)
		end
		
		{:noreply, %{state | from: nil, timer_ref: nil}}
	end

	@impl GenServer
	def handle_info({:gun_ws, _pid, _stream_ref, {:text, data}}, %{from: from} = state) do
		if from != nil do
			Process.cancel_timer(state.timer_ref)
			GenServer.reply(from, ExGremlin.Response.parse(data))
		else
			:gun.flush(state.pid)
		end
		
		{:noreply, %{state | from: nil, timer_ref: nil}}
	end

	@impl GenServer
	def handle_info(:query_timeout, %{from: from} = state) do
		:gun.flush(state.pid)

		GenServer.reply(from, {:error, {:server_timeout, "query timeout"}})
		{:noreply, %{state | from: nil, timer_ref: nil}}
	end

	@impl GenServer
	def handle_info(:ping, state) do
		Process.send_after(self(),:ping, state.ping_delay)
		:gun.ws_send(state.pid, state.stream_ref, {:pong, ""})
		{:noreply, state}
	end

	@impl GenServer
	def handle_info(msg, state) do
		Logger.debug("Pooler.Connection[handle_info][unhandled message] :\n#{inspect msg}")
		{:noreply, state}
	end

###==========================================================================###
### Private Functions                                                        ###
###==========================================================================###

  @spec connect(%{host: String.t(), port: number(), path: String.t(), secure: boolean()}) :: state | no_return
	defp connect(%{host: host, port: port, path: path, secure: secure}) do
		openOptions = 
			if secure do
				Map.put(@open_options, :transport, :tls)
			else
				@open_options
			end
		{:ok, pid} = :gun.open(String.to_charlist(host), port, openOptions)
		case :gun.await_up(pid) do
			{:ok, _protocol} ->
				upgrade_socket(pid, path)
			{:error, _error} ->
				exit({:shutdown, :connect_error})
		end
	end

	@spec upgrade_socket(pid(),String.t(),List.t()) :: state | no_return
	defp upgrade_socket(pid, path, headers \\ []) do
		streamRef = :gun.ws_upgrade(pid, path, headers)
		receive do
		  {:gun_upgrade, ^pid, ^streamRef, ["websocket"], _headers} ->
		    Logger.debug("[ExGremlin.Client] websocket upgrade success")
		    %State{pid: pid, stream_ref: streamRef}
		  {:gun_response, ^pid, _, _, status, headers} ->
		    Logger.debug("[ExGremlin.Client] websocket upgrade  error status : #{inspect status}")
		    exit({:ws_upgrade_failed, status, headers})
		  {:gun_error, _pid, _streamRef, reason} ->
		    Logger.debug("[ExGremlin.Client] websocket upgrade  error : #{inspect reason}")
		    exit({:ws_upgrade_failed, reason})
		  err ->
		    Logger.debug("[ExGremlin.Client] websocket upgrade error other : #{inspect err}")
		    exit({:ws_upgrade_failed, err})
		  # More clauses here as needed.
		after 5000 ->
		  Logger.debug("[ExGremlin.Client] websocket upgrade  error : timeout")
		  exit({:shutdown, :timeout})
		end
	end
end