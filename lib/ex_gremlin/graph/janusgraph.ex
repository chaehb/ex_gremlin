defmodule ExGremlin.Janusgraph do
	@moduledoc """
	Graph Management implementation for JanusGraph
	"""
	use ExGremlin.Schema

#================================================  Geoshape
	def geoshape(:point, {latitude,longitude}) do
		"Geoshape.point(#{latitude},#{longitude})"
	end
	# radius in km
	def geoshape(:circle, {latitude,longitude, radius}) do
		"Geoshape.circle(#{latitude},#{longitude},#{radius})"
	end
	# south_east lat,lng , north_west lat,lng
	def geoshape(:box, {sw_latitude,sw_longitude, ne_latitude, ne_longitude}) do
		"Geoshape.box(#{sw_latitude}, #{sw_longitude}, #{ne_latitude}, #{ne_longitude})"
	end
	# well-known text representation of geometry
	def geoshape(:wkt, wkt) do
		"Geoshape.fromWkt(\"#{wkt}\")"
	end

	# point : { longitude(X), latitude(Y) }
	def geoshape(:multi_point, points) do
		str = Enum.reduce(points,"",fn {x,y},str -> 
			str <> ".pointXY(#{x},#{y})"
		end)

		"Geoshape.geoshape(Geoshape.getShapeFactory().multiPoint()#{str}.build())"
	end
	# line : {{start_x,start_y}, {end_x,end_y}}
	def geoshape(:multi_line, lines) do
		str = Enum.reduce(lines,"",fn {{sX,sY},{eX,eY}},str -> 
			str <> ".add(Geoshape.getShapeFactory().lineString().pointXY(#{sX},#{sY}).pointXY(#{eX},#{eY}))"
		end)

		"Geoshape.geoshape(Geoshape.getShapeFactory().multiLineString()#{str}.build())"
	end
	# polygon: [{X,Y}]
	def geoshape(:multi_poligon,polygons) do
		str = Enum.reduce(polygons,"",fn polygon,str -> 
			polygon_str = Enum.reduce(polygon,"",fn {x,y},polygon_str -> 
				polygon_str <> ".pointXY(#{x},#{y})"
			end)
			str <> ".add(Geoshape.getShapeFactory().polygon()#{polygon_str})"
		end)

		"Geoshape.geoshape(Geoshape.getShapeFactory().multiPolygon()#{str}.build())"
	end
	def geoshape(:geometry_collection, collection) do
		collection_str = Enum.reduce(collection,"", fn
			{x,y},collection_str ->
				collection_str <> ".add(Geoshape.getShapeFactory().pointXY(#{x},#{y}))"
			[{sX,sY},{eX,eY}],collection_str ->
				collection_str <> ".add(Geoshape.getShapeFactory().lineString().pointXY(#{sX},#{sY}).pointXY(#{eX},#{eY}))"
			points,collection_str when length(points) > 2 ->
				points_str = Enum.reduce(points,"",fn {x,y},points_str -> 
					points_str <> ".pointXY(#{x},#{y})"
				end)
				collection_str <> ".add(Geoshape.getShapeFactory().polygon()#{points_str})"
			_, collection_str -> collection_str
		end)
		"Geoshape.geoshape(Geoshape.getGeometryCollectionBuilder()#{collection_str}.build())"
	end

#================================================  Search Predicates
	#----------- string search predicate
	def text_prefix(pattern) do
		predicate(:textPrefix, pattern)
	end
	
	def text_regex(pattern) do
		predicate(:textRegex, pattern)
	end
	
	def text_fuzzy(pattern) do
		predicate(:textFuzzy, pattern)
	end
	
	#----------- full text search predicate
	def text_contains(pattern) do
		predicate(:textContains, pattern)
	end

	def text_contains_prefix(pattern) do
		predicate(:textContainsPrefix, pattern)
	end

	def text_contains_regex(pattern) do
		predicate(:textContainsRegex, pattern)
	end

	def text_contains_fuzzy(pattern) do
		predicate(:textContainsFuzzy, pattern)
	end

	#----------- geo search predicate
	def geo_intersect(pattern) do
		predicate_raw(:geoIntersect, pattern)
	end
	def geo_within(pattern) do
		predicate_raw(:geoWithin, pattern)
	end
	def geo_disjoint(pattern) do
		predicate_raw(:geoDisjoint, pattern)
	end
	def geo_contains(pattern) do
		predicate_raw(:geoContains, pattern)
	end

#================================================  Vertex management
	def vertex_label(label, true) do
		%{label: label, opts: %{static: true}}
	end
	def vertex_label(label, _ ) do
		%{label: label}
	end

	defp vertex_label_option(:static, true) do
		".setStatic()"
	end
	defp vertex_label_option(_,_) do
		""
	end
#================================================  Edge management
	def edge_label(label, multiplicity, true) do
		%{label: label, opts: %{multiplicity: multiplicity, unidirected: true}}
	end
	def edge_label(label, multiplicity, _ ) do
		%{label: label, opts: %{multiplicity: multiplicity}}
	end

	defp edge_label_option(:multiplicity,v) when v != nil do
		".multiplicity(#{_multiplicity(v)})"
	end

	defp edge_label_option(:unidirected, true) do
		".unidirected()"
	end

	defp edge_label_option(_,_) do
		""
	end

#================================================  Property management
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

#================================================ index management
	# index: %{index: index_name, type: :composite|:mixed|:edge|:ttl, opts: %{element: :vertex|:edge |:property,keys: [property_key | {property_key, mapping [,mapping_parameters]}],direction: direction, order: order,unique: true|false}}
	def composite_index(name, element, keys, index_only \\ nil, unique \\ false) do
		%{name: name, type: :composite, opts: %{element: element,keys: keys, index_only: index_only, unique: unique}}
	end
	
	def mixed_index(name, element, keys, index_only \\ nil, search_index \\ "search") do
		%{name: name, type: :mixed, opts: %{element: element,keys: keys, index_only: index_only, search: search_index}}
	end
	
	def edge_index(name, edge, key, direction \\ :both, order \\ :asc) do
		%{name: name, type: :edge, opts: %{edge: edge, key: key, direction: direction, order: order}}
	end

	def ttl(key, element, duration, unit \\ :seconds) do
		%{name: key, type: :ttl, opts: %{element: element, duration: duration, unit: unit}}
	end
	
	def add_ttl(schema, ttl) do
		%{schema | indices: schema.indices ++ [ttl]}
	end

	def add_index(schema,index) do
		%{schema | indices: schema.indices ++ [index]}
	end

	def get_index(index) do
		case Map.get(index,:type,nil) do
			:composite ->
				get_graph_index(Map.get(index,:name,nil))
			:mixed ->
				get_graph_index(Map.get(index,:name,nil))
			:edge ->
				get_edge_index(Map.get(index,:name,nil),Map.get(index,:edge,nil))
			_ -> ""
		end
	end

	defp get_graph_index(name) when name != nil do
		"mgmt.getGraphIndex('#{name}')"
	end
	defp get_graph_index(_name) do
		""
	end

	defp get_edge_index(name, edge) when name != nil and edge != nil do
		"mgmt.getRelationIndex(mgmt.getEdgeLabel('#{edge}'),'#{name}')"
	end
	defp get_edge_index(_name,_edge) do
		""
	end

	def make_index(%{name: name, type: type, opts: opts} = _index) do
		case name do
			nil ->
				""
			_ ->
				case type do
					:composite -> make_composite_index(name,Map.get(opts,:element,nil),Map.get(opts,:keys,[]),Map.get(opts,:unique,false),Map.get(opts,:index_only,nil))
					:mixed -> make_mixed_index(name,Map.get(opts,:element,nil),Map.get(opts,:keys,[]),Map.get(opts,:index_only,nil),Map.get(opts,:search,"search"))
					:edge -> make_edge_index(name,Map.get(opts,:edge,nil),Map.get(opts,:key,nil),Map.get(opts,:direction,:both),Map.get(opts,:order,:asc))
					:ttl -> make_ttl(name,opts)
					_ -> ""
				end
		end
	end
	def make_index(_index) do
		""
	end

	defp make_composite_index(name,element,keys, unique, index_only) when keys != [] do
		"mgmt.buildIndex('#{name}',#{_index_element(element)})#{_index_keys(keys)}"
		<> _index_only(element,index_only)
		<> _is_unique(unique)
		<> ".buildCompositeIndex()"
	end
	defp make_composite_index(_,_,_,_,_) do
		""
	end

	defp make_mixed_index(name,element,keys,index_only, search_index) when keys != [] do
		"mgmt.buildIndex('#{name}',#{_index_element(element)})#{_index_keys(keys)}"
		<> _index_only(element,index_only)
		<> ".buildMixedIndex('#{search_index}')"
	end
	defp make_mixed_index(_,_,_,_,_) do
		""
	end

	defp make_edge_index(name,edge,keys,direction, order) when edge != nil and keys != [] do
		"mgmt.buildEdgeIndex(#{get_edge_label(edge)},'#{name}',#{_index_direction(direction)},#{_index_order(order)},#{_index_edge_keys(keys)})"
	end
	defp make_edge_index(_,_,_,_,_) do
		""
	end

	defp make_ttl(key, %{element: element, duration: duration, unit: unit} = _opts) when duration > 0 do
		"mgmt.setTTL("
		<> case element do
			:property ->
				get_property_key(key)
			:edge ->
				get_edge_label(key)
			_ -> # vertex
				get_vertex_label(key)
		end
		<> ","
		<> _ttl_duration(duration, unit)
		<> ")"
	end
	defp make_ttl(_,_) do
		""
	end
	
	defp _index_element(:edge), do: "Edge.class"
	defp _index_element(:vertex), do: "Vertex.class"
	defp _index_element(_any), do: "Vertex.class"

	defp _index_only(:edge, edge) when edge != nil do
		".indexOnly(#{get_edge_label(edge)})"
	end
	defp _index_only(:vertex, vertex) when vertex != nil do
		".indexOnly(#{get_vertex_label(vertex)})"
	end
	defp _index_only(_, _) do
		""
	end

	defp _is_unique(true), do: ".unique()"
	defp _is_unique(false), do: ""

	defp _index_key({key, mapping}) do
		".addKey(#{get_property_key(key)},#{_mapping(mapping)})"
	end
	defp _index_key({key, mapping, params}) do
		".addKey(#{get_property_key(key)},#{_mapping(mapping)}#{_mapping_parameters(params)})"
	end
	defp _index_key(key) when is_bitstring(key) do
		".addKey(#{get_property_key(key)})"
	end
	defp _index_key(_key) do
		""
	end

	defp _index_keys(key) when is_bitstring(key) do
		_index_key(key)
	end
	defp _index_keys(keys) do
		_index_keys(keys,"")
	end

	defp _index_keys([],query) do
		query
	end

	defp _index_keys([key|rest],query) do
		_index_keys(rest, query <> _index_key(key))
	end

	defp _index_edge_keys(key) when is_bitstring(key) do
			"," <> get_property_key(key)
	end

	defp _index_edge_keys(keys) do
		Enum.reduce(keys,"",fn(key,query)-> 
			query <> "," <> get_property_key(key)
		end)
	end

	defp _index_direction(direction) do
		case direction do
			:out ->
				"Direction.OUT"
			:in ->
				"Direction.IN"
			_ -> # :both
				"Direction.BOTH"
		end
	end
	defp _index_order(order) do
		case order do
			:desc ->
				"Order.desc"
			_ -> # :asc
				"Order.asc"
		end
	end

	defp _ttl_duration(duration, unit) do
		case unit do
			:days -> "Duration.ofDays(#{duration}))"
			:hours -> "Duration.ofHours(#{duration}))"
			:minutes -> "Duration.ofMinutes(#{duration}))"
			:millis -> "Duration.ofMillis(#{duration}))"
			:nanos -> "Duration.ofNanos(#{duration}))"
			:seconds -> "Duration.ofSeconds(#{duration}))"
			_ -> "Duration.ofSeconds(#{duration}))"
		end
	end

	defp _mapping(mapping) do
		case mapping do
			:string ->
				"Mapping.STRING.asParameter()"
			:prefix_tree -> # for geo mapping
				"Mapping.PREFIX_TREE.asParameter()"
			:text_string ->
				"Mapping.TEXTSTRING.asParameter()"
			_ -> # default :text
				"Mapping.TEXT.asParameter()"
		end
	end

	# for prefix_tree mapping
	defp _mapping_parameters([]) do
		""
	end
	defp _mapping_parameters(params) do
		 _mapping_parameters(params,"")
	end

	defp _mapping_parameters([],query) do
		query
	end

	defp _mapping_parameters([{field,value} | rest],query) do
		query = query <> ",Parameter.of(\"#{field}\",#{inspect value})"
		_mapping_parameters(rest, query)
	end

	def re_index(:map_reduce, index,property) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call()
		mgmt = graph.openManagement()
		mrd = new MapReduceIndexManagement(graph)
		mrd.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:reindex)}).get()
		mgmt.commit()
		mgmt = graph.openManagement()
		mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'), SchemaAction.ENABLE_INDEX).get()
		mgmt.commit()
		mgmt = graph.openManagement()
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call()
		mgmt.rollback()
		"""
	end

	def re_index(:janusgraph, index, property) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call()
		mgmt.graph.openManagement()
		mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:reindex)})
		mgmt.commit()
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call()
		graph.tx().rollback()
		"""
	end

	def re_index(:map_reduce, index) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call()
		mgmt = graph.openManagement()
		mrd = new MapReduceIndexManagement(graph)
		mrd.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:reindex)}).get()
		mgmt.commit()
		mgmt = graph.openManagement()
		mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),SchemaAction.ENABLE_INDEX).get()
		mgmt.commit()
		mgmt = graph.openManagement()
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call()
		mgmt.rollback()
		"""
	end

	def re_index(:janusgraph, index) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call()
		mgmt.graph.openManagement()
		mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:reindex)})
		m.commit()
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call()
		graph.tx().rollback()
		"""
	end

	# index removal job on MapReduce
	# for vertex-centric index
	def delete_index(:map_reduce, index, property) do
		"""
		mgmt = graph.openManagement()
		mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:disabled)}).get()
		mgmt.commit()
		graph.tx().commit()
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call()
		mrd = new MapReduceIndexManagement(graph)
		future = mrd.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:removed)})
		mgmt.commit()
		graph.tx().commit()
		future.get()
		"""
	end

	def delete_index(:janusgraph, index, property) do
		"""
		mgmt = graph.openManagement()
		mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:disable)}).get()
		mgmt.commit()
		graph.tx().commit()
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call()
		mgmt = graph.openManagement()
		future = mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:remove)})
		mgmt.commit()
		graph.tx().commit()
		future.get()
		"""
	end

	def delete_index(:map_reduce, index) do
		"""
		mgmt = graph.openManagement()
		mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:disabled)}).get()
		mgmt.commit()
		graph.tx().commit()
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call()
		mrd = new MapReduceIndexManagement(graph)
		future = mrd.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:removed)})
		mgmt.commit()
		graph.tx().commit()
		future.get()
		"""
	end

	def delete_index(:janusgraph, index) do
		"""
		mgmt = graph.openManagement()
		mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:disable)}).get()
		mgmt.commit()
		graph.tx().commit()
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call()
		mgmt = graph.openManagement()
		future = mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:remove)})
		mgmt.commit()
		graph.tx().commit()
		future.get()
		"""
	end

	defp _schema_action(schema_action) do
		case schema_action do
			:reindex -> "SchemaAction.REGISTER_INDEX"
			:register -> "SchemaAction.REINDEX"
			:enable -> "SchemaAction.ENABLE_INDEX"
			:disable -> "SchemaAction.DISABLE_INDEX"
			:remove -> "SchemaAction.REMOVE_INDEX"
		end
	end
	defp _schema_status(schema_status) do
		case schema_status do
			:installed -> "SchemaStatus.INSTALLED"
			:register -> "SchemaStatus.REGISTERED" # for wait status by action
			:registered -> "SchemaStatus.REGISTERED"
			:enable -> "SchemaStatus.ENABLED" # for wait status by action
			:enabled -> "SchemaStatus.ENABLED"
			:disable -> "SchemaStatus.DISABLED" # for wait status by action
			:disabled -> "SchemaStatus.DISABLED"
		end
	end

end
