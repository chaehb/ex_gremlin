defmodule ExGremlin.ExSupervisor do
	@moduledoc false

	use Supervisor

	def start_link(args) do
		Supervisor.start_link(__MODULE__, args, name: __MODULE__)
	end

	@impl Supervisor
	def init(args) do
		Process.flag(:trap_exit, true)

		children = [
			ExGremlin.PoolSupervisor,
			{ExGremlin.PoolManager, args}
		]

		opts = [strategy: :one_for_all]
		Supervisor.init(children, opts)
	end
end
