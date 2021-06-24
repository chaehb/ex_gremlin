defmodule ExGremlin.Graph do
  @moduledoc """
  Functions for traversing and mutating the Graph.

  Graph operations are stored in a queue which can be created with `g/0`.
  Mosts functions return the queue so that they can be chained together
  similar to how Gremlin queries work.

  Example:
  ```
  g.V(1).values("name")
  ```
  Would translate to
  ```
  g |> v(1) |> values("name")
  ```

  Note: This module doesn't actually execute any queries, it just allows you to build one.
  For query execution see `ExGremlin.Client.query/1`

  originally made by Gremlex
  """
  alias :queue, as: Queue

  @type t :: {[], []}

  @default_namespace "ExGremlin"
  @default_namespace_property "namespace"



  @doc """
  Start of graph traversal. All graph operations are stored in a queue.
  """
  @spec g :: ExGremlin.Graph.t()
  def g, do: Queue.new()

  @spec anonymous :: ExGremlin.Graph.t()
  def anonymous do
    enqueue(Queue.new(), "__", [])
  end

  @doc """
  Appends an addV command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec add_v(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def add_v(graph, id) do
    enqueue(graph, "addV", [id])
  end

  @doc """
  Appends an addE command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec add_e(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def add_e(graph, edge) do
    enqueue(graph, "addE", [edge])
  end

  @doc """
  Appends an aggregate command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec aggregate(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def aggregate(graph, aggregate) do
    enqueue(graph, "aggregate", aggregate)
  end

  @spec barrier(ExGremlin.Graph.t(), non_neg_integer()) :: ExGremlin.Graph.t()
  def barrier(graph, max_barrier_size) do
    enqueue(graph, "barrier", max_barrier_size)
  end

  @spec barrier(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def barrier(graph) do
    enqueue(graph, "barrier", [])
  end

  @doc """
  Appends a coin command to the traversal. Takes in a graph and a probability
  modifier as parameters.
  Returns a graph to allow chaining.
  """
  @spec coin(ExGremlin.Graph.t(), Float.t()) :: ExGremlin.Graph.t()
  def coin(graph, probability) do
    enqueue(graph, "coin", probability)
  end

  @spec has_label(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def has_label(graph, label) do
    enqueue(graph, "hasLabel", [label])
  end

  @spec has(ExGremlin.Graph.t(), any(), any()) :: ExGremlin.Graph.t()
  def has(graph, key, value) do
    enqueue(graph, "has", [key, value])
  end

  @spec key(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def key(graph) do
    enqueue(graph, "key", [])
  end

  @doc """
  Appends property command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec property(ExGremlin.Graph.t(), String.t(), any()) :: ExGremlin.Graph.t()
  def property(graph, key, value) do
    enqueue(graph, "property", [key, value])
  end

  @spec property(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def property(graph, key) do
    enqueue(graph, "property", [key])
  end

  @spec property(ExGremlin.Graph.t(), atom(), String.t(), any()) :: ExGremlin.Graph.t()
  def property(graph, :single, key, value) do
    enqueue(graph, "property", [:single, key, value])
  end

  @spec property(ExGremlin.Graph.t(), atom(), String.t(), any()) :: ExGremlin.Graph.t()
  def property(graph, :list, key, value) do
    enqueue(graph, "property", [[:list, key, value]])
  end

  @spec property(ExGremlin.Graph.t(), atom(), String.t(), any()) :: ExGremlin.Graph.t()
  def property(graph, :set, key, value) do
    enqueue(graph, "property", [[:set, key, value]])
  end

  @spec property(ExGremlin.Graph.t(), atom(), String.t(), any()) :: ExGremlin.Graph.t()
  def property(graph, :raw, key, value) do
    enqueue(graph, "property", [[:raw, key, value]])
  end

  @doc """
  Appends properties command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec properties(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def properties(graph, key) do
    enqueue(graph, "properties", [key])
  end

  @spec properties(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def properties(graph) do
    enqueue(graph, "properties", [])
  end

  @doc """
  Appends the store command to the traversal. Takes in a graph and the name of
  the side effect key that will hold the aggregate.
  Returns a graph to allow chaining.
  """
  @spec store(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def store(graph, store) do
    enqueue(graph, "store", store)
  end

  @spec cap(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def cap(graph, cap) do
    enqueue(graph, "cap", cap)
  end

  @doc """
  Appends valueMap command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec value_map(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def value_map(graph) do
    enqueue(graph, "valueMap", [])
  end

  @spec value_map(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def value_map(graph, value) when is_binary(value) do
    enqueue(graph, "valueMap", [value])
  end

  @spec value_map(ExGremlin.Graph.t(), list(String.t())) :: ExGremlin.Graph.t()
  def value_map(graph, values) when is_list(values) do
    enqueue(graph, "valueMap", values)
  end

  @doc """
  Appends values command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec values(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def values(graph, key) do
    enqueue(graph, "values", [key])
  end

  @doc """
  Appends values the `V` command allowing you to select a vertex.
  Returns a graph to allow chaining.
  """
  @spec v(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def v({h, t} = graph) when is_list(h) and is_list(t) do
    enqueue(graph, "V", [])
  end

  @spec v(number()) :: ExGremlin.Vertex.t()
  def v(id) do
    %ExGremlin.Vertex{id: id, label: ""}
  end

  @spec v(ExGremlin.Graph.t(), ExGremlin.Vertex.t()) :: ExGremlin.Graph.t()
  def v(graph, %ExGremlin.Vertex{id: id}) do
    enqueue(graph, "V", [id])
  end

  @doc """
  Appends values the `V` command allowing you to select a vertex.
  Returns a graph to allow chaining.
  """
  @spec v(ExGremlin.Graph.t(), number()) :: ExGremlin.Graph.t()
  def v(graph, id) when is_number(id) or is_binary(id) do
    enqueue(graph, "V", [id])
  end

  @spec v(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def v(graph, id) do
    enqueue(graph, "V", id)
  end

  @spec in_e(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def in_e(graph) do
    enqueue(graph, "inE", [])
  end

  @spec in_e(ExGremlin.Graph.t(), String.t() | List.t()) :: ExGremlin.Graph.t()
  def in_e(graph, edges) do
    enqueue(graph, "inE", edges)
  end

  @spec out_e(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def out_e(graph) do
    enqueue(graph, "outE", [])
  end

  @spec out_e(ExGremlin.Graph.t(), String.t() | List.t()) :: ExGremlin.Graph.t()
  def out_e(graph, edges) do
    enqueue(graph, "outE", edges)
  end

  @spec out(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def out(graph) do
    enqueue(graph, "out", [])
  end

  @spec out(ExGremlin.Graph.t(), String.t() | List.t()) :: ExGremlin.Graph.t()
  def out(graph, labels) do
    enqueue(graph, "out", labels)
  end

  @spec in_(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def in_(graph, edge) do
    enqueue(graph, "in", [edge])
  end

  @spec in_(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def in_(graph) do
    enqueue(graph, "in", [])
  end

  @spec or_(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def or_(graph) do
    enqueue(graph, "or", [])
  end

  @spec and_(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def and_(graph) do
    enqueue(graph, "and", [])
  end

  @spec in_v(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def in_v(graph) do
    enqueue(graph, "inV", [])
  end

  @spec in_v(ExGremlin.Graph.t(), String.t() | List.t()) :: ExGremlin.Graph.t()
  def in_v(graph, labels) do
    enqueue(graph, "inV", labels)
  end

  @spec out_v(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def out_v(graph) do
    enqueue(graph, "outV", [])
  end

  @spec out_v(ExGremlin.Graph.t(), String.t() | List.t()) :: ExGremlin.Graph.t()
  def out_v(graph, labels) do
    enqueue(graph, "outV", labels)
  end

  @spec both(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def both(graph) do
    enqueue(graph, "both", [])
  end

  @spec both(ExGremlin.Graph.t(), List.t()) :: ExGremlin.Graph.t()
  def both(graph, labels) when is_list(labels) do
    enqueue(graph, "both", labels)
  end

  @spec both(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def both(graph, label) do
    enqueue(graph, "both", [label])
  end

  @spec both_e(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def both_e(graph) do
    enqueue(graph, "bothE", [])
  end

  @spec both_e(ExGremlin.Graph.t(), String.t() | List.t()) :: ExGremlin.Graph.t()
  def both_e(graph, labels) do
    enqueue(graph, "bothE", labels)
  end

  @spec both_v(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def both_v(graph) do
    enqueue(graph, "bothV", [])
  end

  @spec both_v(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def both_v(graph, labels) do
    enqueue(graph, "bothV", labels)
  end

  @spec dedup(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def dedup(graph) do
    enqueue(graph, "dedup", [])
  end

  @spec to(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def to(graph, target) do
    enqueue(graph, "to", [target])
  end

  @spec has_next(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def has_next(graph) do
    enqueue(graph, "hasNext", [])
  end

  @spec next(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def next(graph) do
    enqueue(graph, "next", [])
  end

  @spec next(ExGremlin.Graph.t(), number()) :: ExGremlin.Graph.t()
  def next(graph, numberOfResults) do
    enqueue(graph, "next", [numberOfResults])
  end

  @spec try_next(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def try_next(graph) do
    enqueue(graph, "tryNext", [])
  end

  @spec to_list(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def to_list(graph) do
    enqueue(graph, "toList", [])
  end

  @spec to_set(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def to_set(graph) do
    enqueue(graph, "toSet", [])
  end

  @spec to_bulk_set(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def to_bulk_set(graph) do
    enqueue(graph, "toBulkSet", [])
  end

  @spec drop(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def drop(graph) do
    enqueue(graph, "drop", [])
  end

  @spec iterate(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def iterate(graph) do
    enqueue(graph, "iterate", [])
  end

  @spec sum(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def sum(graph) do
    enqueue(graph, "sum", [])
  end

  @spec inject(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def inject(graph, target) do
    enqueue(graph, "inject", [target])
  end

  @spec tail(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def tail(graph) do
    enqueue(graph, "tail", [1])
  end

  @spec tail(ExGremlin.Graph.t(), non_neg_integer()) :: ExGremlin.Graph.t()
  def tail(graph, size) do
    enqueue(graph, "tail", [size])
  end

  @spec min(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def min(graph) do
    enqueue(graph, "min", [])
  end

  @spec max(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def max(graph) do
    enqueue(graph, "max", [])
  end

  @spec identity(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def identity(graph) do
    enqueue(graph, "identity", [])
  end

  @spec constant(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def constant(graph, constant) do
    enqueue(graph, "constant", constant)
  end

  @spec id(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def id(graph) do
    enqueue(graph, "id", [])
  end

  @spec cyclic_path(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def cyclic_path(graph) do
    enqueue(graph, "cyclicPath", [])
  end

  @spec count(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def count(graph) do
    enqueue(graph, "count", [])
  end

  @spec group(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def group(graph) do
    enqueue(graph, "group", [])
  end

  @spec group_count(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def group_count(graph) do
    enqueue(graph, "groupCount", [])
  end

  @doc """
  Appends groupCount command to the traversal. Takes in a graph and the name
  of the key that will hold the aggregated grouping.
  Returns a graph to allow chainig.
  """
  @spec group_count(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def group_count(graph, key) do
    enqueue(graph, "groupCount", key)
  end

  defp enqueue(graph, op, args) when is_list(args) do
    Queue.in({op, args}, graph)
  end

  defp enqueue(graph, op, args) do
    Queue.in({op, [args]}, graph)
  end

  @doc """
  Appends values the `E` command allowing you to select an edge.
  Returns a graph to allow chaining.
  """
  @spec e(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def e(graph) do
    enqueue(graph, "E", [])
  end

  @spec e(ExGremlin.Graph.t(), ExGremlin.Edge.t()) :: ExGremlin.Graph.t()
  def e(graph, %ExGremlin.Edge{id: id}) do
    enqueue(graph, "E", [id])
  end

  @spec e(ExGremlin.Graph.t(), number | String.t()) :: ExGremlin.Graph.t()
  def e(graph, id) when is_number(id) or is_binary(id) do
    enqueue(graph, "E", [id])
  end

  @doc """
  Adds a namespace as property
  """
  @spec add_namespace(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def add_namespace(graph) do
    add_namespace(graph, namespace())
  end

  @spec add_namespace(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def add_namespace(graph, ns) do
    graph |> property(namespace_property(), ns)
  end

  @spec has_namespace(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def has_namespace(graph) do
    has_namespace(graph, namespace())
  end

  @spec has_namespace(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def has_namespace(graph, ns) do
    graph |> has(namespace_property(), ns)
  end

  @spec has_id(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def has_id(graph, id) do
    enqueue(graph, "hasId", id)
  end

  @spec has_key(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def has_key(graph, key) do
    enqueue(graph, "hasKey", key)
  end

  @spec has_not(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def has_not(graph, key) do
    enqueue(graph, "hasNot", key)
  end

  @spec coalesce(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def coalesce(graph, traversals) do
    enqueue(graph, "coalesce", traversals)
  end

  @spec fold(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def fold(graph) do
    enqueue(graph, "fold", [])
  end

  @spec fold(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def fold(graph, traversal) do
    enqueue(graph, "fold", [traversal])
  end

  @spec unfold(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def unfold(graph) do
    enqueue(graph, "unfold", [])
  end

  @spec unfold(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def unfold(graph, traversal) do
    enqueue(graph, "unfold", [traversal])
  end

  @spec as(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def as(graph, name) do
    enqueue(graph, "as", name)
  end

  @spec select(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def select(graph, names) do
    enqueue(graph, "select", names)
  end

  @spec by(ExGremlin.Graph.t(), List.t() | String.t()) :: ExGremlin.Graph.t()
  def by(graph, value) do
    enqueue(graph, "by", value)
  end

  @spec path(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def path(graph) do
    enqueue(graph, "path", [])
  end

  @spec simple_path(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def simple_path(graph) do
    enqueue(graph, "simplePath", [])
  end

  @spec from(ExGremlin.Graph.t(), String.t()) :: ExGremlin.Graph.t()
  def from(graph, name) do
    enqueue(graph, "from", [name])
  end

  @spec repeat(ExGremlin.Graph.t(), ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def repeat(graph, traversal) do
    enqueue(graph, "repeat", [traversal])
  end

  @spec until(ExGremlin.Graph.t(), ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def until(graph, traversal) do
    enqueue(graph, "until", [traversal])
  end

  @spec loops(ExGremlin.Graph.t()) :: ExGremlin.Graph.t()
  def loops(graph) do
    enqueue(graph, "loops", [])
  end

  @spec is(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def is(graph, value) do
    enqueue(graph, "is", [value])
  end

  @spec eq(ExGremlin.Graph.t(), number()) :: ExGremlin.Graph.t()
  def eq(graph, number) do
    enqueue(graph, "eq", [number])
  end

  @spec where(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def where(graph, traversal) do
    enqueue(graph, "where", [traversal])
  end

  @spec not_(ExGremlin.Graph.t(), any()) :: ExGremlin.Graph.t()
  def not_(graph, traversal) do
    enqueue(graph, "not", [traversal])
  end

  @doc """
  Creates a `within` predicate that will match at least one of the values provided.
  Takes in a range or a list as the values.
  Examples:
  ```
  g.V().has('age', within(1..18))
  g.V().has('name', within(["some", "value"]))
  ```
  """
  def within(%Range{} = range) do
    enqueue(Queue.new(), "within", [range])
  end

  def within(values) do
    enqueue(Queue.new(), "within", values)
  end

  @doc """
  Creates a `without` predicate that will filter out values that match the values provided.
  Takes in a range or a list as the values.
  Examples:
  ```
  g.V().has('age', without(18..30))
  g.V().has('name', without(["any", "value"]))
  ```
  """
  def without(%Range{} = range) do
    enqueue(Queue.new(), "without", [range])
  end

  def without(values) do
    enqueue(Queue.new(), "without", values)
  end

  def eq(value) do
    enqueue(Queue.new(),"eq", value)
  end
  def neq(value) do
    enqueue(Queue.new(),"neq", value)
  end
  def gt(value) do
    enqueue(Queue.new(),"gt", value)
  end
  def gte(value) do
    enqueue(Queue.new(),"gte", value)
  end
  def lt(value) do
    enqueue(Queue.new(),"lt", value)
  end
  def lte(value) do
    enqueue(Queue.new(),"lte", value)
  end

  @doc """
  Compiles a graph into the Gremlin query.
  """
  @spec encode(ExGremlin.Graph.t()) :: String.t()
  def encode(graph) do
    encode(graph, "g")
  end

  defp encode({[], []}, acc), do: acc

  defp encode(graph, acc) do
    {{:value, {op, args}}, remainder} = :queue.out(graph)

    args =
      args
      |> Enum.map(fn
        nil ->
          "none"
        %ExGremlin.Vertex{id: id} when is_number(id) ->
          "V(#{id})"

        %ExGremlin.Vertex{id: id} when is_binary(id) ->
          "V('#{id}')"

        arg when is_number(arg) or is_atom(arg) ->
          "#{arg}"

        %Range{first: first, last: last} ->
          "#{first}..#{last}"
        [:predicate_raw, predicate, pattern] ->
          predicate_raw(predicate,pattern)
        [:predicate, predicate, pattern] ->
          predicate(predicate,pattern)
        [:list, key, values] ->
          "'#{key}',#{list_string values}"
        [:set, key,values] ->
          "'#{key}',#{list_string values}"
        [:raw, key, value] ->
          "'#{key}',#{value}"
        arg when is_tuple(arg) ->
          case :queue.is_queue(arg) and :queue.get(arg) do
            {"V", _} -> encode(arg, "g")
            _ -> encode(arg, "")
          end
        str ->
          "'#{escape(str)}'"
      end)
      |> Enum.join(", ")

    construct_fn_call(acc, op, args, remainder)
  end

  @spec construct_fn_call(String.t(), String.t(), String.t(), ExGremlin.Graph.t()) :: String.t()
  defp construct_fn_call("", "__", _, remainder), do: encode(remainder, "" <> "__")

  defp construct_fn_call(_, "__", _, _), do: raise("Not a valid traversal")

  defp construct_fn_call("", op, args, remainder), do: encode(remainder, "" <> "#{op}(#{args})")

  defp construct_fn_call(acc, op, args, remainder),
    do: encode(remainder, acc <> ".#{op}(#{args})")

  # @spec escape(String.t()) :: String.t()
  def escape(str) do
    # We escape single quote if it is not already escaped by an odd number of backslashes
    String.replace(str, ~r/((\A|[^\\])(\\\\)*)'/, "\\1\\'")
  end

  defp predicate(predicate, pattern) when is_bitstring(pattern) do
    "#{predicate}('#{escape pattern}')"
  end
  defp predicate(predicate, pattern) do
    "#{predicate}(#{pattern})"
  end
  defp predicate_raw(predicate, pattern) do
    "#{predicate}(#{pattern})"
  end

  defp list_string(list) do
    
    str = Enum.map(list, fn 
      el when is_bitstring(el) -> "'#{escape(el)}'"
      el -> "#{el}"
    end)
    |> Enum.join(",")
    "[#{str}]"
  end

  defp namespace_property do
    Application.get_env(:ex_gremlin, :namespace, %{})
    |> Map.get(:property, @default_namespace_property)
  end

  defp namespace do
    Application.get_env(:ex_gremlin, :namespace, %{})
    |> Map.get(:namespace, @default_namespace)
  end
end
