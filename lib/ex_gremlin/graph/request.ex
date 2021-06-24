defmodule ExGremlin.Request do
  # alias ExGremlin.Graph
  @moduledoc """
  originally made by Gremlex  
  """
  @derive [Jason.Encoder]
  @op "eval"
  @processor ""
  @enforce_keys [:op, :processor, :requestId, :args]
  defstruct [:op, :processor, :requestId, :args]

  @doc """
  Accepts plain query or a graph and returns a Request.
  """
  @spec new(String.t()) :: Request
  def new(query) when is_binary(query) do
    args = %{gremlin: query, language: "gremlin-groovy"}
    %ExGremlin.Request{requestId: ExGremlin.Utility.uuid(), args: args, op: @op, processor: @processor}
  end

  @spec new(ExGremlin.Graph.t()) :: Request
  def new(query) do
    new(ExGremlin.Graph.encode(query))
  end
end
