defmodule Mix.Tasks.Gremlin.Schema.Gen do
	use Mix.Task

	import Mix.Gremlin

	@shortdoc """
	Generate gremlin schema template file.
	"""

	require Logger

	def run(argv) do
		{parsed, _argv,_errors} = OptionParser.parse( argv, 
			strict: [group: :string, module: :string], 
			aliases: [g: :group, m: :module]
		)

		schema_group = Keyword.get(parsed,:group,"Schema")
		{schema_mod,schema_file} = case Keyword.get(parsed,:module) do
			nil ->
				Mix.raise """
				Invalid arguments, expected.
				You must propose a new schema's (module) name.
				ex) mix gremlin -m NewSchema [-g SchemaGroup ]
				"""
			mod ->
				{mod,Macro.underscore(mod)}
		end

		app = app_name()
		lib_app = app_mod()

		mod = Module.concat([lib_app,schema_group,schema_mod])
		mod_path = Path.join(schema_dir(schema_group),"#{schema_file}.ex")

		Mix.shell().info "Mix.Project.config()[:app] : #{inspect app}"
		Mix.shell().info "Mix.Project.get() : #{inspect mod}"
		Mix.shell().info "Mix.Project.get() : #{inspect mod_path}"
	end

end