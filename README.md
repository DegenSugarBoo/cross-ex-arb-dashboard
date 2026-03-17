# cross-ex-arb

`cross-ex-arb` is a latency-first Rust desktop scanner for cross-exchange perpetual futures arbitrage.

It ingests live top-of-book quotes and funding data, computes two-leg opportunities across exchanges, and displays the best direction per symbol in an `egui` table.
Each row now supports a clickable route detail pane with synchronized 30-second time-series charts.

This project is a read-only market data scanner. It does not place orders.

## Collector Mode (Raw Event Storage)

The scanner also supports a headless collector mode that writes raw parsed market events to disk before any downstream aggregation.

- CLI flag: `--collect-mode`
- Default root: `data/` (override with `--collector-data-root`)
- Compression: JSONL + codec layer (`none`, `zstd`, `lz4hc`)
- Current default codec: `zstd` (selected by codec benchmark score using 60% size + 40% throughput)

Collector output layout:

- `data/<SYMBOL>/<EXCHANGE>/quote/<YYYY-MM-DD>/<HH>.jsonl.<ext>`
- `data/<SYMBOL>/<EXCHANGE>/funding/<YYYY-MM-DD>/<HH>.jsonl.<ext>`
- `data/<SYMBOL>/<EXCHANGE>/trade/<YYYY-MM-DD>/<HH>.jsonl.<ext>`
- `data/<SYMBOL>/<EXCHANGE>/trade_unsupported/<YYYY-MM-DD>/<HH>.jsonl.<ext>`

`trade_unsupported` rows are emitted once per discovered `(symbol, exchange)` tuple in v1 until exchange trade parsers are integrated.

Replay ordering key for offline determinism:

1. `exchange`
2. `symbol`
3. `record_type`
4. `exchange_ts_ms`
5. `recv_ts_ms`
6. `global_seq`

## Current Exchange Support

| Exchange | Market Discovery | Quote Source | Funding Source | Taker Fee Used in Net Spread |
| --- | --- | --- | --- | --- |
| Lighter | REST `orderBooks` | WS `ticker/<market_id>` | REST `funding-rates` poller | Per-market `taker_fee` from discovery |
| Aster | REST `exchangeInfo` | WS `<symbol>@bookTicker` | REST `premiumIndex` poller | Base taker fee `0.04%` |
| Extended | REST `info/markets` (with fallback mapping if blocked) | WS orderbook stream | WS funding stream | Base taker fee `0.025%` |
| edgeX | REST `meta/getMetaData` | WS `depth.<contractId>.15` | WS `ticker.<contractId>` | `takerFeeRate` from metadata when present, otherwise `0.038%` |
| Hyperliquid | REST `POST /info` (`type=meta`) | WS `bbo` | REST `POST /info` (`type=metaAndAssetCtxs`) poller | Base taker fee `0.045%` |
| GRVT | REST `full/v1/instruments` | WS `v1.ticker.d` | WS `v1.ticker.d` | Base taker fee `0.045%` |
| ApeX | REST `/v3/symbols` | WS `orderBook200.H.<symbol>` + local book state | WS `instrumentInfo.H.<symbol>` | Base taker fee `0.05%` |

## How It Works

1. Discover active perp markets per exchange and build a common-symbol map.
2. Start one feed task per exchange and stream normalized quote/funding updates into a central channel.
3. Build and retain a strict in-memory rolling 30-second history for every directed route (`symbol + buy_exchange + sell_exchange`).
4. Recompute best-direction arbitrage rows on each market event.
5. Render a continuously refreshed table sorted by net spread.
6. Lazily materialize only the currently selected route detail snapshot at 10 Hz for the floating detail pane.

Spread math:

```text
raw_bps = (sell_bid / buy_ask - 1.0) * 10_000
net_bps = raw_bps - 2 * (buy_taker_fee_pct * 100) - 2 * (sell_taker_fee_pct * 100)
```

The top 20 rows by `net_bps` are shown.

## UI Overview

Top exchange status panel:

- Horizontally wrapped collapsible exchange cards (all collapsed on launch)
- Collapsed row shows exchange name plus health dot (`green` live / `red` stale)
- Expanded row shows realtime stats: quote rate (`Q x.x/s`), funding rate (`F x.x/s`), and last-event age (`Age ...ms`)

Main table columns:

- Symbol, buy exchange, sell exchange
- Buy ask, sell bid
- Raw bps, net bps
- Funding buy/sell
- Max base size, max USD notional
- Age ms, latency ms

Clickable route detail pane:

- Click any symbol cell to open a reusable floating detail window for that exact directed route.
- The pane renders 3 vertically stacked linked charts in a `2:1:1` ratio:
  - `net_spread_bps`
  - `max_usd_notional`
  - `age_ms`
- All charts use a shared x-axis over relative time `[-30s, 0s]` and synchronized cursor/hover behavior.
- Closing the pane clears route selection.

The table adapts column widths to current window size. Buy/sell prices are displayed without trailing zeros.

## Requirements

- Rust toolchain (stable)
- Network access to the configured exchange REST/WS endpoints

## Quick Start

```bash
cargo run --release
```

Headless collector mode:

```bash
cargo run --release -- --collect-mode
```

Useful options:

```bash
cargo run --release -- --help
```

Common runtime tuning:

- `--stale-ms` (default `2500`): max quote age allowed per row
- `--ui-fps` (default `60`): UI repaint cap
- `--funding-poll-secs` (default `300`): baseline polling interval for REST-based funding feeds
- `--http-timeout-secs` (default `10`): REST timeout
- `--discovery-refresh-secs` (default `600`): periodic discovery log refresh interval
- `--collect-mode`: run discovery + feeds + raw collector without launching UI
- `--collector-data-root` (default `data`): output root for collector partitions
- `--collector-compression` (default `zstd`): collector codec (`none|zstd|lz4hc`)
- `--collector-bootstrap-timeout-ms` (default `45000`): max wait for feed bootstrap gate before writing
- `--collector-write-buffer` (default `16384`): bootstrap queue capacity and writer buffer size
- `--collector-flush-interval-ms` (default `1000`): periodic flush cadence
- `--collector-max-open-files` (default `128`): open file handle cap before eviction
- `--grvt-rest-url` / `--grvt-ws-url`: override GRVT discovery + WS endpoints
- `--apex-rest-url` / `--apex-ws-url` / `--apex-depth-rest-url`: override ApeX discovery, WS, and depth snapshot resync endpoint

Logging:

```bash
RUST_LOG=info cargo run --release
```

## Tests

```bash
cargo test
```

Collector smoke run:

```bash
cargo run --release -- --collect-mode --collector-compression=none --collector-data-root /tmp/cross-ex-arb-collector-smoke
```

Codec benchmark and default selection check:

```bash
python3 scripts/collector_codec_bench.py --runs 3
```

Performance baseline and regression checks:

```bash
# Capture/update baseline (median of 3 runs)
./scripts/perf_capture_baseline.py --runs 3 --output benchmarks/perf_baseline.json

# Check current code against baseline (fails on >10% slowdown per metric)
./scripts/perf_regression_check.py --baseline benchmarks/perf_baseline.json --runs 3
```

## Project Layout

- `src/discovery.rs`: exchange market discovery, normalization, symbol intersection
- `src/feeds/`: exchange-specific WS/REST adapters
- `src/engine.rs`: event ingestion, directed-route history retention, spread computation, ranking, snapshot + feed health
- `src/ui.rs`: `egui` app, sortable table, exchange status panel, clickable detail pane with linked charts
- `src/config.rs`: CLI configuration
- `tests/`: parser, spread, symbol, and integration behavior tests

## Notes

- Extended REST discovery can return `403` in some environments. When that happens, the feed layer can still infer common `BASE-USD` / `BASE/USD` mappings from discovered symbols on other exchanges.
- edgeX funding is consumed from websocket ticker events to avoid aggressive REST polling and rate limits.
- GRVT quote and funding are both derived from `v1.ticker.d`; parser normalizes `et`/`nf` unit drift (ns/us/ms/s).
- ApeX orderbook quote output is built from local snapshot+delta state and auto-resyncs via `/v3/depth` when sequence continuity breaks.
