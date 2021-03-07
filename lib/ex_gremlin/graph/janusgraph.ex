defmodule ExGremlin.Janusgraph do
	@moduledoc """
	Graph Management implementation for JanusGraph
	"""
	use ExGremlin.Gremlin

#================================================ graph
	#---------- Geoshape
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
	#---------- search predicate

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

#================================================ mgmt
	#---------- set TTL
	@spec set_ttl(String.t, number(), 
			:Days | :Hours | :Millis | :Minutes | :Nanos | :Seconds
	)	:: String.t
	def set_ttl(var, duration, duration_type \\ :Days) do
		"""
		mgmt.setTTL(#{var}, Duration.of#{duration_type}(#{duration}))
		"""
	end
	#---------- index management
	def get_index(var,edge,index) do
		"""
		#{var} = mgmt.getRelationIndex(#{edge},"#{index}");
		"""
	end
	def get_index(var,index) do
		"""
		#{var} = mgmt.getGraphIndex("#{index}")
		"""
	end

	def create_index(:composite, {name, element, keys}, var) do
		"""
		#{var} = mgmt.buildIndex('#{name}',#{_element(element)})#{_index_keys(keys)}.buildCompositeIndex();
		"""
	end

	def create_index(:composite, {name, element, keys, is_unique}, var) do
		"""
		#{var} = mgmt.buildIndex('#{name}',#{_element(element)})#{_index_keys(keys)}#{_is_unique(is_unique)}.buildCompositeIndex();
		"""
	end

	def create_index(:mixed, {name, element, keys, search_index}, var) do
		"""
		#{var} = mgmt.buildIndex('#{name}',#{_element(element)})#{_index_keys(keys)}.buildMixedIndex(\"#{search_index}\");
		"""
	end

	def create_index(:edge, {name, edge, property, direction, order},var) do
		"""
		#{var} = mgmt.buildEdgeIndex(#{edge},'#{name}',#{_index_direction(direction)},#{_index_order(order)},#{property});
		"""
	end

	def create_index(:composite, {name, element, keys}) do
		"""
		mgmt.buildIndex('#{name}',#{_element(element)})#{_index_keys(keys)}.buildCompositeIndex();
		"""
	end

	def create_index(:composite, {name, element, keys, is_unique}) do
		"""
		mgmt.buildIndex('#{name}',#{_element(element)})#{_index_keys(keys)}#{_is_unique(is_unique)}.buildCompositeIndex();
		"""
	end

	def create_index(:mixed, {name, element, keys,search_index}) do
		"""
		mgmt.buildIndex('#{name}',#{_element(element)})#{_index_keys(keys)}.buildMixedIndex(\"#{search_index}\");
		"""
	end

	def create_index(:edge, {name, edge, property, direction, order}) do
		"""
		mgmt.buildEdgeIndex(#{edge},'#{name}',#{_index_direction(direction)},#{_index_order(order)},#{property});
		"""
	end

	def re_index(:map_reduce, index,property) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call();
		mgmt = graph.openManagement();
		mrd = new MapReduceIndexManagement(graph);
		mrd.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:reindex)}).get();
		m.commit();
		mgmt = graph.openManagement();
		mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'), SchemaAction.ENABLE_INDEX).get();
		mgmt.commit();
		mgmt = graph.openManagement();
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call();
		mgmt.rollback();
		"""
	end

	def re_index(:janusgraph, index, property) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call();
		mgmt.graph.openManagement();
		mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:reindex)});
		m.commit();
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call()
		graph.tx().rollback();
		"""
	end

	def re_index(:map_reduce, index) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call();
		mgmt = graph.openManagement();
		mrd = new MapReduceIndexManagement(graph);
		mrd.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:reindex)}).get();
		m.commit();
		mgmt = graph.openManagement();
		mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),SchemaAction.ENABLE_INDEX).get();
		mgmt.commit();
		mgmt = graph.openManagement();
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call();
		mgmt.rollback();
		"""
	end

	def re_index(:janusgraph, index) do
		"""
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.REGISTERED).call();
		mgmt.graph.openManagement();
		mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:reindex)});
		m.commit();
		ManagementSystem.awaitGraphIndexStatus(graph, '#{index}').status(SchemaStatus.ENABLED).call()
		graph.tx().rollback();
		"""
	end

	# index removal job on MapReduce
	# for vertex-centric index
	def delete_index(:map_reduce, index, property) do
		"""
		mgmt = graph.openManagement();
		idx = mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}');
		mgmt.updateIndex(idx,#{_schema_action(:disabled)}).get();
		mgmt.commit();
		graph.tx().commit();
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call();
		mrd = new MapReduceIndexManagement(graph);
		future = mrd.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:removed)});
		mgmt.commit();
		graph.tx().commit();
		future.get();
		"""
	end

	def delete_index(:janusgraph, index, property) do
		"""
		mgmt = graph.openManagement();
		idx = mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}');
		mgmt.updateIndex(idx,#{_schema_action(:disable)}).get();
		mgmt.commit();
		graph.tx().commit();
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call();
		mgmt = graph.openManagement();
		future = mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType('#{property}'),'#{index}'),#{_schema_action(:remove)});
		mgmt.commit();
		graph.tx().commit();
		future.get();
		"""
	end

	def delete_index(:map_reduce, index) do
		"""
		mgmt = graph.openManagement();
		idx = mgmt.getGraphIndex('#{index}');
		mgmt.updateIndex(idx,#{_schema_action(:disabled)}).get();
		mgmt.commit();
		graph.tx().commit();
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call();
		mrd = new MapReduceIndexManagement(graph);
		future = mrd.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:removed)});
		mgmt.commit();
		graph.tx().commit();
		future.get();
		"""
	end

	def delete_index(:janusgraph, index) do
		"""
		mgmt = graph.openManagement();
		idx = mgmt.getGraphIndex('#{index}');
		mgmt.updateIndex(idx,#{_schema_action(:disable)}).get();
		mgmt.commit();
		graph.tx().commit();
		ManagementSystem.awaitGraphIndexStatus(graph,'#{index}').status(#{_schema_status(:disabled)}).call();
		mgmt = graph.openManagement();
		future = mgmt.updateIndex(mgmt.getGraphIndex('#{index}'),#{_schema_action(:remove)});
		mgmt.commit();
		graph.tx().commit();
		future.get();
		"""
	end

	defp _element(:edge), do: "Edge.class"
	defp _element(:vertex), do: "Vertex.class"
	defp _element(_any), do: "Vertex.class"

	defp _is_unique(true), do: ".unique()"
	defp _is_unique(false), do: ""

	defp _index_key({name, mapping}) do
		".addKey(#{name},#{_mapping(mapping)})"
	end
	defp _index_key({name, mapping, params}) do
		".addKey(#{name},#{_mapping(mapping)},#{_mapping_parameters(params)})"
	end
	defp _index_key(key) when is_bitstring(key) do
		".addKey(#{key})"
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

	defp _mapping(mapping) do
		case mapping do
			:text ->
				"Mapping.TEXT.asParameter()"
			:prefix_tree ->
				"Mapping.PREFIX_TREE.asParameter()"
			_ -> # default :string
				"Mapping.STRING.asParameter()"
		end
	end

	defp _mapping_parameters(params) do
		_mapping_parameters(params,"")
	end

	defp _mapping_parameters([],query) do
		query
	end

	defp _mapping_parameters([{field,value} | rest],query) do
		query = query <> ",Parameter.of(\"#{field}\", #{value})"
		_mapping_parameters(rest, query)
	end

end