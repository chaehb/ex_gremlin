defmodule ExGremlin.VertexProperty do
  @moduledoc """
  originally made by Gremlex
  """
  @type t :: %ExGremlin.VertexProperty{
          label: String.t(),
          id: number(),
          value: any(),
          vertex: number()
        }
  @enforce_keys [:label, :id, :value]
  @derive [Jason.Encoder]
  defstruct [:label, :id, :value, :vertex]

  def from_response(%{"id" => json_id, "value" => value, "vertex" => vertex, "label" => label}) do
    %{"@value" => id} = json_id
    %{"@value" => vertex_id} = vertex
    %ExGremlin.VertexProperty{label: label, id: id, vertex: vertex_id, value: value}
  end

  def from_response(%{"id" => id, "label" => label, "value" => value}) do
    %{"@value" => property_id} = id
    %ExGremlin.VertexProperty{label: label, id: property_id, value: value}
  end
end
