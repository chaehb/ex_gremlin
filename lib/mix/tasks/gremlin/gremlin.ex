defmodule Mix.Tasks.Gremlin do
	use Mix.Task

	@shortdoc "Prints Gremlin help information"

	@moduledoc """
	Prints Gremlin tasks and their information.

	mix gremlin
	"""

	def run(args) do
		{_opts, args} = OptionParser.parse!(args, strict: [])

		case args do
			[] ->
				general()
			_ ->
				Mix.raise "Invalid arguments, expected mix gremlin"
		end
	end

	defp general() do
		# Application.ensure_all_started(:ex_gremlin)
		Mix.shell().info "\n=== ExGremlin v#{Application.spec(:ex_gremlin, :vsn)} ==="
		Mix.shell().info "--- An Elixir client for Gremlin (Apache TinkerPop™) ---"
		Mix.shell().info "\nAvailable tasks:\n"
		Mix.Tasks.Help.run(["--search","gremlin."])
	end
end