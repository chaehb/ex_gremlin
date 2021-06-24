defmodule ExGremlin.Edge do
  @moduledoc """
  originally made by Gremlex
  """
	alias ExGremlin.Deserializer

  @enforce_keys [:label, :id, :in_vertex, :out_vertex, :properties]
  @derive [Jason.Encoder]
  @type t :: %ExGremlin.Edge{
          label: String.t(),
          id: number(),
          properties: map(),
          in_vertex: ExGremlin.Vertex.t(),
          out_vertex: ExGremlin.Vertex.t()
        }
  defstruct [:label, :id, :in_vertex, :out_vertex, :properties]

  def new(
        id,
        label,
        in_vertex_id,
        in_vertex_label,
        out_vertex_id,
        out_vertex_label,
        properties \\ %{}
      ) do
    in_vertex = %ExGremlin.Vertex{id: in_vertex_id, label: in_vertex_label}
    out_vertex = %ExGremlin.Vertex{id: out_vertex_id, label: out_vertex_label}

    %ExGremlin.Edge{
      id: id,
      label: label,
      in_vertex: in_vertex,
      out_vertex: out_vertex,
      properties: properties
    }
  end

  def from_response(value) do
    %{
      "id" => edge_id,
      "inV" => in_v,
      "inVLabel" => in_v_label,
      "label" => label,
      "outV" => out_v,
      "outVLabel" => out_v_label
    } = value

    json_properties = Map.get(value, "properties", %{})
    id = Deserializer.deserialize(edge_id)
    in_v_id = Deserializer.deserialize(in_v)
    out_v_id = Deserializer.deserialize(out_v)

    properties =
      Enum.reduce(json_properties, %{}, fn {key, prop_value}, acc ->
        %{"@type" => type, "@value" => value} = prop_value
        value = Deserializer.deserialize(type, value)
        Map.put(acc, String.to_atom(key), value)
      end)

    ExGremlin.Edge.new(
      id,
      label,
      in_v_id,
      in_v_label,
      out_v_id,
      out_v_label,
      properties
    )
  end
	
end