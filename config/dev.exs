config :ex_gremlin, 
  pool: %{
    size: 8,
    max_overflow: 16
  },
  gremlin: %{
    host: "localhost",
    port: 8182,
    path: "/gremlin",
    secure: false,
    ping_delay: 0
  }
