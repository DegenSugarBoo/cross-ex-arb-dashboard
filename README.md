# cross-ex-arb

`cross-ex-arb` is a latency-first Rust desktop scanner for cross-exchange perpetual futures arbitrage.

It ingests live top-of-book quotes and funding data, computes two-leg opportunities across exchanges, and displays the best direction per symbol in an `egui` table.

This project is a read-only market data scanner. It does not place orders.

## Current Exchange Support

| Exchange | Market Discovery | Quote Source | Funding Source | Taker Fee Used in Net Spread |
| --- | --- | --- | --- | --- |
| Lighter | REST `orderBooks` | WS `ticker/<market_id>` | REST `funding-rates` poller | Per-market `taker_fee` from discovery |
| Aster | REST `exchangeInfo` | WS `<symbol>@bookTicker` | REST `premiumIndex` poller | Base taker fee `0.04%` |
| Extended | REST `info/markets` (with fallback mapping if blocked) | WS orderbook stream | WS funding stream | Base taker fee `0.025%` |
| edgeX | REST `meta/getMetaData` | WS `depth.<contractId>.15` | WS `ticker.<contractId>` | `takerFeeRate` from metadata when present, otherwise `0.038%` |
| Hyperliquid | REST `POST /info` (`type=meta`) | WS `l2Book` | REST `POST /info` (`type=metaAndAssetCtxs`) poller | Base taker fee `0.045%` |

## How It Works

1. Discover active perp markets per exchange and build a common-symbol map.
2. Start one feed task per exchange and stream normalized quote/funding updates into a central channel.
3. Recompute best-direction arbitrage rows on each market event.
4. Render a continuously refreshed table sorted by net spread.

Spread math:

```text
raw_bps = (sell_bid / buy_ask - 1.0) * 10_000
net_bps = raw_bps - (buy_taker_fee_pct * 100) - (sell_taker_fee_pct * 100)
```

Only positive `net_bps` rows are shown.

## UI Overview

Top status strip:

- Per exchange health indicator (`green` active / `red` inactive)
- Quote message rate (`Q x.x/s`)
- Funding message rate (`F x.x/s`)
- Age since last event (`Age ...ms`)

Main table columns:

- Symbol, buy exchange, sell exchange
- Buy ask, sell bid
- Raw bps, net bps
- Funding buy/sell
- Max base size, max USD notional
- Age ms, latency ms

The table adapts column widths to current window size. Buy/sell prices are displayed without trailing zeros.

## Requirements

- Rust toolchain (stable)
- Network access to the configured exchange REST/WS endpoints

## Quick Start

```bash
cargo run --release
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

Logging:

```bash
RUST_LOG=info cargo run --release
```

## Tests

```bash
cargo test
```

## Project Layout

- `src/discovery.rs`: exchange market discovery, normalization, symbol intersection
- `src/feeds/`: exchange-specific WS/REST adapters
- `src/engine.rs`: event ingestion, spread computation, ranking, snapshot + feed health
- `src/ui.rs`: `egui` app, sortable table, exchange health strip
- `src/config.rs`: CLI configuration
- `tests/`: parser, spread, symbol, and integration behavior tests

## Notes

- Extended REST discovery can return `403` in some environments. When that happens, the feed layer can still infer common `BASE-USD` / `BASE/USD` mappings from discovered symbols on other exchanges.
- edgeX funding is consumed from websocket ticker events to avoid aggressive REST polling and rate limits.
