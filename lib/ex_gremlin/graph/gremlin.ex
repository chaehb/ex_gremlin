defmodule ExGremlin.Gremlin do
	defmacro __using__(_opts) do
		quote do
			defstruct [:var, :args, :opts]
			
			def new(arg), do: %__MODULE__{arg: arg}
			def new(var,arg), do: %__MODULE__{var: var, arg: arg}
			def new(var,arg,opts), do: %__MODULE__{var: var, arg: arg, opts: opts}

			def get_property_key(%{var: var, args: key}) do
				"""
				#{var} = mgmt.getPropertyKey('#{key}');
				"""
			end

			def make_property_key(%{var: nil, args: key, opts: {data_type, cardinality}}) do
				"""
				mgmt.getPropertyKey('#{key}').dataType(#{_data_type(data_type)}).cardinality(#{_cardinality(cardinality)}).make();
				"""
			end
			def make_property_key(%{var: var, args: key, opts: {data_type, cardinality}}) do
				"""
				#{var} = mgmt.getPropertyKey('#{key}').dataType(#{_data_type(data_type)}).cardinality(#{_cardinality(cardinality)}).make();
				"""
			end
			#---------- vertex management
			def get_vertex_label(%{var: var, args: label}) do
				"""
				#{var} = mgmt.getVertexLabel('#{label}');
				"""
			end

			def make_vertex_label(%{var: nil, args: label, opts: :static})do
				"""
				mgmt.makeVertexLabel('#{label}').setStatic().make();
				"""
			end
			def make_vertex_label(%{var: nil, args: label})do
				"""
				mgmt.makeVertexLabel('#{label}').make();
				"""
			end
			def make_vertex_label(%{var: var, args: label, opts: :static}) do
				"""
				#{var} = mgmt.makeVertexLabel('#{label}').setStatic().make();
				"""
			end
			def make_vertex_label(%{var: var, args: label}) do
				"""
				#{var} = mgmt.makeVertexLabel('#{label}').make();
				"""
			end

			#---------- edge management
			def get_edge_label(%{var: var, args: label}) do
				"""
				#{var} = mgmt.getEdgeLabel('#{label}');
				"""
			end

			def make_edge_label(%{var: nil, args: label, opts: {multiplicity, :undirected}}) do
				"""
				mgmt.makeEdgeLabel('#{label}').multiplicity(#{_multiplicity(multiplicity)}).unidirected().make()
				"""
			end

			def make_edge_label(%{var: nil, args: label, opts: multiplicity}) do
				"""
				mgmt.makeEdgeLabel('#{label}').multiplicity(#{_multiplicity(multiplicity)}).make()
				"""
			end

			def make_edge_label(%{var: var, args: label, opts: {multiplicity, :undirected}}) do
				"""
				#{var} = mgmt.makeEdgeLabel('#{label}').multiplicity(#{_multiplicity(multiplicity)}).unidirected().make()
				"""
			end

			def make_edge_label(%{var: var, args: label, opts: multiplicity}) do
				"""
				#{var} = mgmt.makeEdgeLabel('#{label}').multiplicity(#{_multiplicity(multiplicity)}).make()
				"""
			end

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
					:instant -> "Instant.class"
					:geoshape -> "Geoshape.class"
					:uuid ->  "UUID.class"
					_ -> "String.class"
				end
			end

			defp _cardinality(cardinality) do
				case cardinality do
					:single -> "SINGLE"
					:list -> "LIST"
					:set -> "SET"
					_ -> "SINGLE"
				end
			end

			#---------------- graph query
			def predicate(predicate, pattern) do
				[:predicate, predicate, pattern]
			end
			def predicate_raw(predicate, pattern) do
				[:predicate_raw, predicate, pattern]
			end

			defoverridable [
				get_property_key: 1,
				make_property_key: 1,
				
				get_vertex_label: 1,
				make_vertex_label: 1,
				
				get_edge_label: 1,
				make_edge_label: 1,
				
				#------- private methods
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