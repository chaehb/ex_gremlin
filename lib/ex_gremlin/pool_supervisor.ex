defmodule ExGremlin.PoolSupervisor do
	@moduledoc false

	use DynamicSupervisor

	def start_link(args) do
		DynamicSupervisor.start_link(__MODULE__, args, name: __MODULE__)
	end

	defdelegate start_worker(sup, spec), to: DynamicSupervisor, as: :start_child
	defdelegate stop_worker(sup, pid), to: DynamicSupervisor, as: :terminate_child

	@impl DynamicSupervisor
	def init(_arg) do
		DynamicSupervisor.init(strategy: :one_for_one)
	end

end
