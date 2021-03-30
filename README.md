<p align="center"><img src="logo.png"></img></p>

# ExGremlin

## Current State
- [x] Basic Implementations
- [ ] Optimizing for Janusgraph
- [ ] Mix Commands

### Rewrite from [gremlex](https://github.com/Revmaker/gremlex)
- Use the most of the architecture of gremlex
- change websocket client library from [elixir-socket](https://github.com/meh/elixir-socket) to [gun](https://github.com/ninenines/gun)
- remove use of [poolboy](https://github.com/devinus/poolboy) and write own client pool
- change json library from [poison](https://github.com/devinus/poison) to [jason](https://github.com/michalmuskala/jason)
- change uuid library from [uuid v1.1 (rename elixir-uuid after v1.2)](https://github.com/zyro/elixir-uuid) to [uuid_erl](https://github.com/okeuday/uuid)