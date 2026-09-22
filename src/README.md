# Source map

- `discovery.rs` builds the common market set.
- `feeds/` contains venue-specific REST/WebSocket adapters.
- `engine.rs` computes and ranks directed routes.
- `collector.rs` persists normalized events.
- `ui.rs` renders the read-only desktop scanner.
- `config.rs` owns CLI flags and endpoint overrides.

The binaries under `bin/` are profiling, codec, transport, and connectivity tools rather than alternate production runtimes.
