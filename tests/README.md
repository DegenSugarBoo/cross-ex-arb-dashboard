# Tests

- `parsers.rs` covers exchange payload decoding.
- `symbols.rs` covers discovery filters and symbol normalization.
- `spread.rs` covers route arithmetic and ranking.
- `history.rs` covers the rolling route-history window.
- `collector.rs` covers bootstrap, envelopes, partitions, and codecs.
- `integration_mock.rs` covers multi-exchange engine behavior and staleness.

Run the full suite with `cargo test --locked`.
