defmodule ExGremlin do
  @moduledoc false
  defmacro __using__(_) do
    quote do
      import ExGremlin.Graph
      import ExGremlin.Client
    end
  end
end
