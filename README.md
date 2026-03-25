# cross-ex-arb

`cross-ex-arb` is a Rust market-monitoring tool for cross-exchange perpetual futures arbitrage.

It watches multiple perp venues in real time, normalizes quote and funding data into one shared model, and surfaces the best two-leg buy/sell routes in a desktop dashboard. The same feed stack can also run headless as a raw market-data collector for replay, benchmarking, and downstream research.

The project is read-only. It does not place orders, manage positions, or execute trades.

## Overview

This repo is built for people who want to:

- monitor live perp spreads across exchanges from one screen
- compare quote, funding, and fee-adjusted net spread behavior
- collect normalized raw exchange events to disk for offline analysis
- benchmark parsers, websocket ingestion, and collector throughput

Out of the box, the project supports two runtime modes:

- Scanner mode: launches an `egui` desktop app that ranks live arbitrage routes.
- Collector mode: runs headless and writes normalized raw events to partitioned files.

## Why This Repo Exists

Cross-exchange arbitrage data is messy: every venue exposes different symbols, websocket payloads, funding formats, and fee rules. `cross-ex-arb` handles that normalization layer so you can focus on observing spreads and evaluating market behavior instead of stitching feeds together by hand.

At a high level, the pipeline is:

1. Discover eligible perpetual markets on each supported exchange.
2. Map symbols into a shared cross-venue market set.
3. Stream quote and funding updates from websocket and REST-backed feeds.
4. Normalize events into a common model.
5. Rank live directed routes by raw and fee-adjusted spread.
6. Optionally persist raw envelopes for replay and research.

## What It Does

- Discovers perpetual markets across supported exchanges and builds a common-symbol map.
- Ingests live quote and funding updates from websocket and REST-backed feeds.
- Normalizes market data into a shared event model.
- Computes directed buy/sell routes and retains a rolling 30-second history for each route.
- Renders the best opportunities in a desktop UI.
- Optionally writes raw collector envelopes to hourly JSONL partitions before any downstream aggregation.

## Current Exchange Support

| Exchange | Market Discovery | Quote Source | Funding Source | Taker Fee Used in Net Spread |
| --- | --- | --- | --- | --- |
| Lighter | REST `orderBooks` | WS `ticker/<market_id>` | REST `funding-rates` poller | Per-market `taker_fee` from discovery |
| Aster | REST `exchangeInfo` | WS `<symbol>@bookTicker` | REST `premiumIndex` poller | Base taker fee `0.04%` |
| Binance | REST `exchangeInfo` | WS `<symbol>@bookTicker` combined stream | WS `<symbol>@markPrice` combined stream | Base taker fee `0.04%` |
| Bybit | REST `v5/market/instruments-info?category=linear` | WS `tickers.<symbol>` | WS `tickers.<symbol>` | Base taker fee `0.04%` |
| Extended | REST `info/markets` with fallback symbol inference | WS orderbook stream | WS funding stream | Base taker fee `0.025%` |
| edgeX | REST `meta/getMetaData` | WS `depth.<contractId>.15` | WS `ticker.<contractId>` | `takerFeeRate` from metadata when present, otherwise `0.038%` |
| Hyperliquid | REST `POST /info` with `type=meta` | WS `bbo` | REST `POST /info` with `type=metaAndAssetCtxs` poller | Base taker fee `0.045%` |
| GRVT | REST `full/v1/instruments` | WS `v1.ticker.d` | WS `v1.ticker.d` | Base taker fee `0.045%` |
| ApeX | REST `/v3/symbols` | WS `orderBook200.H.<symbol>` with local book state | WS `instrumentInfo.H.<symbol>` | Base taker fee `0.05%` |

## Installation

Prerequisites:

- stable Rust toolchain
- network access to the supported exchange REST and websocket endpoints
- a desktop environment if you want to use scanner mode

Install Rust with `rustup` if you do not already have it, then clone and build the project:

```bash
git clone https://github.com/DegenSugarBoo/cross-ex-arb-dashboard.git
cd cross-ex-arb-dashboard
cargo build --release
```

No exchange API keys are required for the default public-market-data flows.

Quick start:

```bash
# launch the desktop scanner
cargo run --release

# or run the headless collector
cargo run --release -- --collect-mode
```

## Runtime Modes

### Scanner Mode

Scanner mode is the default:

```bash
cargo run --release
```

What happens in scanner mode:

1. Discovery builds the common market set across exchanges.
2. Feed tasks stream normalized `Quote` and `Funding` events into the engine.
3. The engine recomputes directed routes on each event and keeps a strict 30-second history window.
4. The UI refreshes a ranked table of the top opportunities.

Spread math:

```text
raw_bps = (sell_bid / buy_ask - 1.0) * 10_000
net_bps = raw_bps - 2 * (buy_taker_fee_pct * 100) - 2 * (sell_taker_fee_pct * 100)
```

The UI shows the top 20 rows by `net_bps`.

### Trading Fee Assumptions Behind `net_bps`

`net_bps` is intentionally conservative. The current implementation subtracts taker fees for both exchanges and applies a `2x` multiplier to each exchange leg:

- `buy_taker_fee_pct` is the buy venue's taker fee in percent units, for example `0.04` means `0.04%`
- `sell_taker_fee_pct` is the sell venue's taker fee in percent units
- the engine converts each percent fee into basis points by multiplying by `100`
- each side is then multiplied by `2`, so the formula currently assumes a round-trip style fee haircut on both the buy venue and the sell venue
- maker fees, rebates, VIP tiers, token discounts, borrow costs, transfer costs, slippage, and funding carry are not folded into `net_bps`

Current fee sources and defaults used by route construction:

| Exchange | Fee Assumption Used In `net_bps` |
| --- | --- |
| Lighter | Uses per-market `taker_fee` returned by discovery metadata |
| Aster | Fixed taker fee `0.04%` |
| Binance | Fixed taker fee `0.04%` |
| Bybit | Fixed taker fee `0.04%` |
| Extended | Fixed taker fee `0.025%` |
| edgeX | Uses metadata `takerFeeRate` when present, otherwise falls back to `0.038%` |
| Hyperliquid | Fixed taker fee `0.045%` |
| GRVT | Fixed taker fee `0.045%` |
| ApeX | Fixed taker fee `0.05%` |

If your real trading costs differ from these defaults, the displayed `net_bps` should be treated as an approximation rather than execution-ready PnL.

### Collector Mode

Collector mode runs the same discovery and feed stack without launching the desktop UI:

```bash
cargo run --release -- --collect-mode
```

Collector behavior:

- Buffers events until every discovered exchange has produced at least one event, or until `--collector-bootstrap-timeout-ms` elapses.
- Drains the bootstrap buffer once the gate opens.
- Writes normalized envelopes to hourly JSONL partitions.
- Emits one `trade_unsupported` marker per discovered `(symbol, exchange)` tuple in collector v1.
- Flushes periodically and shuts down cleanly on `Ctrl-C` or `SIGTERM`.

## UI Overview

- Exchange status cards show per-exchange liveness, quote rate, funding rate, and last-event age.
- The main table shows symbol, buy exchange, sell exchange, buy ask, sell bid, raw bps, net bps, funding, size, notional, age, and latency.
- Clicking a symbol opens a floating route detail pane for that directed route.
- The detail pane renders linked 30-second charts for `net_spread_bps`, `max_usd_notional`, and `age_ms`.

## Collector Output

Default collector root:

```text
data/
```

Partition layout:

- `data/<SYMBOL>/<exchange>/quote/<YYYY-MM-DD>/<HH>.jsonl`
- `data/<SYMBOL>/<exchange>/funding/<YYYY-MM-DD>/<HH>.jsonl`
- `data/<SYMBOL>/<exchange>/trade/<YYYY-MM-DD>/<HH>.jsonl`
- `data/<SYMBOL>/<exchange>/trade_unsupported/<YYYY-MM-DD>/<HH>.jsonl`

With compression enabled, the filename gets an extra suffix:

- `none` -> `.jsonl`
- `zstd` -> `.jsonl.zst`
- `lz4hc` -> `.jsonl.lz4`

The current default is `zstd`, based on the repo's codec scorecard that weights size at 60% and throughput at 40%.

Example partition path:

```text
data/BTC/aster/quote/2026-03-24/13.jsonl.zst
```

Path components are sanitized before writing. Exchange directory names are lowercased.

Each line is a `CollectorEnvelope` with metadata plus a payload:

```json
{
  "schema_version": 1,
  "record_type": "quote",
  "global_seq": 1,
  "exchange": "Aster",
  "symbol": "BTC",
  "symbol_base": "BTC",
  "exchange_symbol": "BTCUSDT",
  "exchange_ts_ms": 1700000000000,
  "recv_ts_ms": 1700000000001,
  "collected_at_ms": 1700000000002,
  "bid_px": 100.01,
  "bid_qty": 2.5,
  "ask_px": 100.02,
  "ask_qty": 1.7
}
```

Replay ordering key for deterministic offline processing:

1. `exchange`
2. `symbol`
3. `record_type`
4. `exchange_ts_ms`
5. `recv_ts_ms`
6. `global_seq`

## Requirements

- Stable Rust toolchain
- Network access to the configured exchange REST and websocket endpoints
- A desktop environment if you want to run scanner mode

## Common Commands

Show CLI help:

```bash
cargo run -- --help
```

Enable logs:

```bash
RUST_LOG=info cargo run --release
```

Headless collector smoke run:

```bash
cargo run --release -- --collect-mode --collector-compression=none --collector-data-root /tmp/cross-ex-arb-collector-smoke
```

## Useful Runtime Flags

Scanner and feed tuning:

- `--stale-ms` default `2500`: max quote age allowed per ranked row
- `--ui-fps` default `60`: UI repaint cap
- `--quote-channel-capacity` default `8192`: shared event channel depth
- `--funding-poll-secs` default `300`: poll cadence for REST-backed funding feeds
- `--http-timeout-secs` default `10`: REST timeout
- `--discovery-refresh-secs` default `600`: periodic discovery summary logging

Collector tuning:

- `--collect-mode`: disable the UI and run the raw collector
- `--collector-data-root` default `data`: collector output root
- `--collector-compression` default `zstd`: `none`, `zstd`, or `lz4hc`
- `--collector-bootstrap-timeout-ms` default `45000`: maximum wait before opening the bootstrap gate
- `--collector-write-buffer` default `16384`: bootstrap queue cap and per-writer buffer size
- `--collector-flush-interval-ms` default `1000`: periodic flush cadence
- `--collector-max-open-files` default `128`: configured writer handle budget before eviction

Endpoint overrides are available for every exchange feed via `--*-rest-url` and `--*-ws-url` flags, including `--binance-rest-url`, `--binance-ws-url`, `--bybit-rest-url`, `--bybit-ws-url`, `--extended-funding-ws-url`, and `--apex-depth-rest-url`.

## Performance Snapshot

Saved release-mode baselines live in `benchmarks/` and were captured on 2026-03-15 across 3 runs. Absolute numbers will vary by machine, but these snapshots are useful for understanding the current performance envelope and for catching regressions over time.

Core pipeline baseline from `benchmarks/perf_baseline.json`:

| Benchmark | Throughput | Cost |
| --- | --- | --- |
| ingest only | 2.80M ev/s | 356.6 ns/ev |
| run_engine end-to-end | 1.80M ev/s | 557.1 ns/ev |
| ingest + throttled snapshot | 976k ev/s | 1024.6 ns/ev |
| snapshot only | 91k snaps/s | 10969.5 ns/snap |
| aster markPrice parser | 6.47M msg/s | 154.6 ns/msg |
| aster bookTicker parser | 3.87M msg/s | 258.1 ns/msg |
| lighter funding parser | 4.32M msg/s | 231.4 ns/msg |
| hyperliquid bbo parser | 1.52M msg/s | 656.2 ns/msg |

Websocket ingest baseline from `benchmarks/ws_ingest_baseline_post_ws_rollout.json`:

| Benchmark | Throughput | Cost |
| --- | --- | --- |
| aster ws ingest markPrice | 1.33M msg/s | 753.8 ns/msg |
| aster ws ingest bookTicker | 1.08M msg/s | 929.3 ns/msg |
| lighter ws ingest marketStats | 1.08M msg/s | 924.0 ns/msg |
| extended ws ingest funding | 851k msg/s | 1175.3 ns/msg |
| hyperliquid ws ingest bbo | 723k msg/s | 1383.1 ns/msg |
| edgeX ws ingest depth | 671k msg/s | 1489.6 ns/msg |

## Development And Validation

Run tests:

```bash
cargo test
```

Run the synthetic codec benchmark directly:

```bash
cargo run --release --bin collector_codec_bench -- --events 200000 --write-buffer 16384
```

Score collector codecs across multiple runs:

```bash
python3 scripts/collector_codec_bench.py --runs 3
```

Run the parser and engine performance profile:

```bash
cargo run --release --bin perf_profile
```

Capture or refresh a performance baseline:

```bash
./scripts/perf_capture_baseline.py --runs 3 --output benchmarks/perf_baseline.json
```

Check current performance against the saved baseline:

```bash
./scripts/perf_regression_check.py --baseline benchmarks/perf_baseline.json --runs 3
```

Run websocket ingest benchmark scenarios:

```bash
cargo run --release --bin ws_exchange_ingest_bench --features ws-compare-tungstenite
```

Run websocket transport comparison benchmark:

```bash
cargo run --release --bin ws_transport_bench --features ws-compare-tungstenite
```

Check websocket ingest regressions against a saved baseline:

```bash
./scripts/ws_ingest_regression_check.py --baseline benchmarks/ws_ingest_baseline_post_ws_rollout.json --runs 3
```

## Project Layout

- `src/discovery.rs`: exchange market discovery, normalization, symbol intersection, and periodic discovery logging
- `src/feeds/`: exchange-specific websocket and REST adapters
- `src/engine.rs`: event ingestion, route history retention, spread computation, rankings, and feed health
- `src/collector.rs`: collector envelopes, bootstrap gating, partitioning, codecs, and file writer lifecycle
- `src/ui.rs`: `egui` application, table rendering, exchange cards, and route detail pane
- `src/config.rs`: CLI configuration
- `src/bin/`: standalone benchmark and profiling binaries
- `scripts/`: regression-check and benchmark helper scripts
- `tests/`: parser, spread, symbol, collector, and integration coverage

## Notes

- Extended REST discovery can return `403` in some environments; the discovery layer can still infer common `BASE-USD` and `BASE/USD` mappings from other exchanges.
- edgeX funding is consumed from websocket ticker events to avoid aggressive REST polling.
- GRVT quote and funding are both normalized from `v1.ticker.d`, including timestamp unit drift across `ns`, `us`, `ms`, and `s`.
- ApeX quote output is built from local snapshot-plus-delta order book state and resyncs through `/v3/depth` if sequence continuity breaks.
- Collector v1 persists quotes and funding events today; `trade_unsupported` markers make the absence of trade streams explicit in the dataset.
