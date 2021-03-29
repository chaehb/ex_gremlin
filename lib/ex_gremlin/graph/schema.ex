defmodule ExGremlin.Schema do	
	defmacro __using__(_opts) do
		quote do
			require Logger

			defstruct vertices: [], edges: [], properties: [], indices: []
			
			def schema(), do: %__MODULE__{}

			def exec(schema, timeout \\ 5_000) do
				ExGremlin.Mgmt.open_management()
				|> ExGremlin.Mgmt.add_query(generate(schema))
				|> ExGremlin.Mgmt.commit()
				|> ExGremlin.Mgmt.tx_commit()
				|> ExGremlin.Mgmt.query(timeout)
			end

			def generate(schema) do
				Enum.reduce(schema.vertices,"",fn(vertex,query)-> 
						query <> make_vertex_label(vertex) <> "\n"
				   end)
				<> Enum.reduce(schema.edges,"",fn(edge,query)-> 
						query <> make_edge_label(edge) <> "\n"
				   end)
				<> Enum.reduce(schema.properties,"",fn(property,query)-> 
						query <> make_property_key(property) <> "\n"
				   end)
				<> Enum.reduce(schema.indices,"",fn(index,query)-> 
						query <> make_index(index) <> "\n"
				   end)
			end

			# vertex : %{label: label}
			def vertex_label(label) do
				%{label: label}
			end
			def add_vertex_label(schema, vertex) do
				case Map.get(vertex,:label,nil) do
					nil ->
						Logger.debug("no vertex label :")
						Logger.debug("#{inspect vertex}")
						schema
					_ ->
						%{schema | vertices: schema.vertices ++ [vertex]}
				end
			end

			# edge : %{label: label, opts: %{multiplicity: multiplicity}}
			def edge_label(label, multiplicity \\ :multi) do
				%{label: label, opts: %{multiplicity: multiplicity}}
			end

			def add_edge_label(schema, edge) do
				case Map.get(edge,:label,nil) do
					nil ->
						Logger.debug("no edge label :")
						Logger.debug("#{inspect edge}")
						schema
					_ ->
						%{schema | edges: schema.edges ++ [edge]}
				end
			end

			# property : %{key: key, opts: %{data_type: data_type, cardinality: cardinality}}
			def property_key(key, data_type, cardinality \\ :single) do
				%{key: key, opts: %{data_type: data_type, cardinality: cardinality}}
			end
			def add_property_key(schema,property) do
				case Map.get(property,:key,nil) do
					nil ->
						Logger.debug("no property key :")
						Logger.debug("#{inspect property}")
						schema
					_ ->
						%{schema | properties: schema.properties ++ [property]}
				end
			end

			## default do nothing to index_schema
			def add_index(schema, index_schema) do
				Logger.debug("graph index is not defined :")
				Logger.debug("#{inspect index_schema}")
				schema
			end

##========= query builders
			#---------- vertex management
			def get_vertex_label(vertex) when is_bitstring(vertex) do
				"mgmt.getVertexLabel('#{vertex}')"
			end
			def get_vertex_label(_) do
				Logger.debug("no vertex label")
				""
			end

			def make_vertex_label(vertex)do
				case Map.get(vertex,:label,nil) do
					nil ->
						Logger.debug("no vertex label")
						""
					label ->
						"mgmt.makeVertexLabel('#{label}')"
						<> Enum.reduce(Map.get(vertex,:opts,%{}),"",fn({k,v},query) -> 
								query <> vertex_label_option(k,v)
						   end)
						<> ".make()"
				end
			end
			defp vertex_label_option(_,_) do
				""
			end

			#---------- edge management
			def get_edge_label(edge) when is_bitstring(edge) do
				"mgmt.getEdgeLabel('#{edge}')"
			end
			def get_edge_label(_) do
				Logger.debug("no edge label")
				""
			end

			def make_edge_label(edge) do
				case Map.get(edge,:label,nil) do
					nil ->
						Logger.debug("no edge label")
						""
					label ->
						"mgmt.makeEdgeLabel('#{label}')"
						<> Enum.reduce(Map.get(edge,:opts,%{}),"",fn({k,v},query) -> 
								query <> edge_label_option(k,v)
							 end)
						<> ".make()"
				end
			end

			defp edge_label_option(:multiplicity,v) when v != nil do
				".multiplicity(#{_multiplicity(v)})"
			end
			defp edge_label_option(_,_) do
				""
			end

			#---------- property key management
			def get_property_key(property) when is_bitstring(property) do
				"mgmt.getPropertyKey('#{property}')"
			end
			def get_property_key(_) do
				Logger.debug("no property key")
				""
			end

			def make_property_key(property) do
				case Map.get(property,:key,nil) do
					nil ->
						Logger.debug("no property key")
						""
					key ->
						"mgmt.makePropertyKey('#{key}')"
						<> Enum.reduce(Map.get(property,:opts,%{}),"",fn({k,v},query) -> 
								query <> property_key_option(k,v)
							end)
						<> ".make()"
				end
			end

			defp property_key_option(:data_type,v) when v != nil do
				".dataType(#{_data_type(v)})"
			end
			defp property_key_option(:cardinality,v) when v != nil do
				".cardinality(#{_cardinality(v)})"
			end
			defp property_key_option(key, _) do
				Logger.debug("property option [#{key}] is not supported")
				""
			end

			def get_index(index) do
				Logger.debug("graph index is not supported")
				""
			end
			def make_index(_index) do
				Logger.debug("graph index is not supported")
				""
			end
			defp index_option(_,_) do
				Logger.debug("graph index is not supported")
				""
			end
##### -- private methods
			defp _multiplicity(multi) do
				case multi do
					:multi -> "MULTI"
					:simple -> "SIMPLE"
					:many_2_one -> "MANY2ONE"
					:one_2_many -> "ONE2MANY"
					:one_2_one -> "ONE2ONE"
					_ -> # default
						"MULTI"
				end
			end

			defp _data_type(type) do
				case type do
					:string -> "String.class"
					:character -> "Character.class"
					:boolean -> "Boolean.class"
					:byte -> "Byte.class"
					:short -> "Short.class"
					:integer -> "Integer.class"
					:long -> "Long.class"
					:float -> "Float.class"
					:double -> "Double.class"
					:date -> "Date.class"
					_ -> "String.class"
				end
			end

			defp _cardinality(cardinality) do
				case cardinality do
					:single -> "Cardinality.SINGLE"
					:list -> "Cardinality.LIST"
					:set -> "Cardinality.SET"
					_ -> "Cardinality.SINGLE"
				end
			end

			#---------------- graph query utilities
			def predicate(predicate, pattern) do
				[:predicate, predicate, pattern]
			end
			def predicate_raw(predicate, pattern) do
				[:predicate_raw, predicate, pattern]
			end

			defoverridable [
				generate: 1,

				add_vertex_label: 2,
				add_edge_label: 2,
				add_property_key: 2,
				add_index: 2,

				vertex_label: 1,
				get_vertex_label: 1,
				make_vertex_label: 1,
				vertex_label_option: 2,
				
				edge_label: 2,
				get_edge_label: 1,
				make_edge_label: 1,
				edge_label_option: 2,
				
				property_key: 3,
				get_property_key: 1,
				make_property_key: 1,
				property_key_option: 2,

				get_index: 1,
				make_index: 1,
				index_option: 2,
				
				#------- utility methods
				_multiplicity: 1,
				_data_type: 1,
				_cardinality: 1,
				#-------------------- graph query
				predicate: 2,
				predicate_raw: 2,
			]

			defimpl ExGremlin.Mgmt.Query do
				def get_property_key(data) do
					@for.get_property_key(data)
				end
				def make_property_key(data) do
					@for.make_property_key(data)
				end
				def get_vertex_label(data) do
					@for.get_vertex_label(data)
				end
				def make_vertex_label(data) do
					@for.make_vertex_label(data)
				end
				def get_edge_label(data) do
					@for.get_edge_label(data)
				end
				def make_edge_label(data) do
					@for.make_edge_label(data)
				end
			end 
		end
	end
end