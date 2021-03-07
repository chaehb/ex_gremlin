defmodule ExGremlin.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @default_pool %{
      size: 8,
      max_overflow: 16
  }

  @default_gremlin_server %{
    host: "localhost",
    port: 8182,
    path: "/gremlin",
    secure: false,
    ping_delay: 0
  }

  @impl Application
  def start(_type, _args) do
    pool = Map.merge(@default_pool,Application.get_env(:ex_gremlin,:pool))
    gremlin = Map.merge(@default_gremlin_server,Application.get_env(:ex_gremlin,:gremlin))
    
    size = Map.get(pool,:size)
    max_overflow = Map.get(pool,:max_overflow) |> min(@default_pool.max_overflow)

    # children = [
    #   {ExGremlin.Supervisor,%{pool: %{size: size,max_overflow: max_overflow}, gremlin: gremlin}}
    # ]
    children = [
      ExGremlin.PoolSupervisor,
      {ExGremlin.PoolManager, %{pool: %{size: size,max_overflow: max_overflow}, gremlin: gremlin}}
    ]


    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_all, name: ExGremlin.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
