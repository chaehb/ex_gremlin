defmodule ExGremlinTest do
  use ExUnit.Case
  doctest ExGremlin

  test "greets the world" do
    assert ExGremlin.hello() == :world
  end
end
