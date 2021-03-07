defmodule ExGremlin.Utility do
	def uuid do
		:uuid.get_v4() |> :uuid.uuid_to_string(:binary_standard)
	end
	def uuid_bin do
		:uuid.get_v4()
	end

end