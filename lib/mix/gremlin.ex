defmodule Mix.Gremlin do
	@moduledoc """
	Conveniences for writing Gremlin schema related Mix tasks
	"""

	def default_schema do
		"priv/gremlin/schema.exs"
	end

	def app_name do
		Mix.Project.config()[:app]
	end
	def app_mod do
		Mix.Project.get() |> Module.split()
		|> List.first
	end

	def schema_dir(group) do
		Path.join(["priv","gremlin",Macro.underscore(group)])
	end
end