defmodule ExGremlin.Utility do
	@moduledoc """
	Some utilities for ExGremlin
	"""
	def uuid do
		:uuid.get_v4() |> :uuid.uuid_to_string(:binary_standard)
	end
	def uuid_bin do
		:uuid.get_v4()
	end

end