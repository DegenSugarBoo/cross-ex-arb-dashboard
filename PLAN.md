# One-Shot Implementation Plan: Latency-First Rust egui Arb Scanner (Lighter + Aster)

## 1) Objective
Build a new Rust desktop app (`eframe` + `egui`) that discovers common perpetual markets across Lighter and Aster, subscribes to live BBO updates over WebSockets, computes two-leg cross-exchange arb opportunities in real time, and shows exactly one best-direction row per symbol sorted by highest spread.

Primary optimization target: quote-to-screen latency.

## 2) Feasibility Check (validated on 2026-03-14)

### Live endpoint reachability
- Lighter discovery endpoint is reachable and returns active perps with fees:
  - `GET https://mainnet.zklighter.elliot.ai/api/v1/orderBooks`
  - Top-level shape: `{ "code": 200, "order_books": [...] }`
  - Per market includes: `symbol`, `market_id`, `market_type`, `status`, `taker_fee`, `maker_fee`.
- Aster discovery endpoint is reachable:
  - `GET https://fapi.asterdex.com/fapi/v1/exchangeInfo`
  - Top-level keys include `symbols`.
  - Per symbol includes: `symbol`, `baseAsset`, `quoteAsset`, `contractType`, `status`.

### Live overlap sanity
- Lighter active perps: `153`.
- Aster trading perpetual bases: `312`.
- Exact base-symbol overlap: `113`.
- Result: enough shared symbols for meaningful scanner output.

### Live WebSocket protocol validation
- Aster WS works with one shared socket + SUBSCRIBE payload:
  - Connect: `wss://fstream.asterdex.com/ws`
  - Subscribe request:
    - `{"method":"SUBSCRIBE","params":["btcusdt@bookTicker","ethusdt@bookTicker"],"id":1}`
  - Ack observed: `{"id":1,"result":null}`
  - Update observed:
    - `{"e":"bookTicker","s":"BTCUSDT","b":"...","B":"...","a":"...","A":"...","T":...,"E":...}`
- Lighter WS works with `type=subscribe` + `ticker/<market_id>` channel:
  - Connect: `wss://mainnet.zklighter.elliot.ai/stream?readonly=true`
  - Subscribe request:
    - `{"type":"subscribe","channel":"ticker/1"}`
  - Messages observed:
    - connected: `{"session_id":"...","type":"connected"}`
    - subscribed/update ticker:
      - `{"channel":"ticker:1","ticker":{"s":"BTC","a":{"price":"...","size":"..."},"b":{"price":"...","size":"..."}},"timestamp":...,"type":"update/ticker"}`

### Feasibility verdict
Feasible with no blocking API gap. Implementation can proceed immediately using tested payload shapes.

## 3) Locked v1 Decisions (no ambiguity)

### Exchanges and instruments
- Exchanges: exactly `Lighter` and `Aster`.
- Instruments: perpetuals only.
- Aster filter: `contractType == "PERPETUAL" && status == "TRADING" && quoteAsset == "USDT"`.
- Lighter filter: `market_type == "perp" && status == "active"`.

### Symbol matching
- Join key: uppercase base symbol.
- Lighter base: `order_books[].symbol`.
- Aster base: `symbols[].baseAsset`.
- Ignore symbols not present on both exchanges.

### Price source choice
- Lighter source for BBO: `ticker/<market_id>` channel (not full order_book channel in v1).
- Aster source for BBO: `<symbol_lower>@bookTicker`.
- Rationale: lower parsing cost, direct BBO fields, lower implementation risk.

### Fees and profitability metric
- Lighter fees: parse from discovery payload (`taker_fee`, `maker_fee`) per market.
- Aster fees: no taker/maker in `exchangeInfo`; use runtime-config defaults:
  - `aster_taker_fee_pct = 0.04`
  - `aster_maker_fee_pct = 0.005`
- v1 net metric uses taker+taker only:
  - `raw_spread_pct = (sell_bid / buy_ask - 1.0) * 100.0`
  - `net_spread_pct = raw_spread_pct - buy_taker_fee_pct - sell_taker_fee_pct`

### Staleness and ranking
- Keep latest quote per `(exchange, symbol_base)`.
- Row staleness:
  - `age_ms = max(now - buy_recv_ts, now - sell_recv_ts)`.
- Hide stale rows if either side age exceeds `stale_ms = 2500` (runtime configurable).
- Ranking:
  - primary: `net_spread_pct` descending,
  - tie-break: `raw_spread_pct` descending,
  - tie-break: `symbol` ascending.
- No minimum spread threshold in v1.

### UI behavior
- One row per symbol (best of both directions only).
- Default sort is highest spread first.
- UI redraw capped to 60 FPS.

## 4) Project Skeleton (to be created in this repo)

```
Cargo.toml
src/main.rs
src/config.rs
src/model.rs
src/discovery.rs
src/feeds/mod.rs
src/feeds/lighter.rs
src/feeds/aster.rs
src/engine.rs
src/ui.rs
tests/symbols.rs
tests/spread.rs
tests/parsers.rs
tests/integration_mock.rs
fixtures/lighter_ticker.json
fixtures/aster_bookticker.json
```

## 5) Data Contracts (final)

```rust
pub enum Exchange {
    Lighter,
    Aster,
}

pub struct MarketMeta {
    pub exchange: Exchange,
    pub symbol_base: String,
    pub exchange_symbol: String,
    pub market_id: Option<u32>,
    pub taker_fee_pct: f64,
    pub maker_fee_pct: f64,
}

pub struct QuoteUpdate {
    pub exchange: Exchange,
    pub symbol_base: String,
    pub bid_px: f64,
    pub bid_qty: f64,
    pub ask_px: f64,
    pub ask_qty: f64,
    pub exch_ts_ms: i64,
    pub recv_ts_ms: i64,
}

pub struct ArbRow {
    pub symbol: String,
    pub direction: String, // "BUY_LIGHTER_SELL_ASTER" or reverse
    pub buy_ex: Exchange,
    pub sell_ex: Exchange,
    pub buy_ask: f64,
    pub sell_bid: f64,
    pub raw_spread_pct: f64,
    pub net_spread_pct: f64,
    pub max_base_qty: f64,
    pub max_usd_notional: f64,
    pub age_ms: i64,
}
```

## 6) Runtime Architecture (final)

### Task graph
- `discovery_task` (startup + optional periodic refresh every 10 min):
  - fetch both exchange metadata,
  - compute intersection,
  - produce immutable market map for feeds + engine.
- `lighter_feed_task`:
  - single WS connection,
  - subscribe `ticker/<market_id>` for all matched symbols,
  - parse ticker updates into `QuoteUpdate` and send over channel.
- `aster_feed_task`:
  - single WS connection,
  - send `SUBSCRIBE` with `<symbol_lower>@bookTicker` for all matched symbols,
  - parse into `QuoteUpdate` and send over channel.
- `engine_task`:
  - consume quote stream,
  - maintain latest quote map,
  - recompute affected symbol row on each event,
  - publish ranked snapshot.
- `ui_task`:
  - read latest snapshot,
  - render table,
  - throttle redraw to 60 FPS.

### Concurrency primitives
- `tokio::sync::mpsc` for quote events.
- `Arc<RwLock<Vec<ArbRow>>>` (or `ArcSwap`) for latest ranked snapshot consumed by UI.
- `tokio::select!` for reconnect loops + shutdown.

### Reconnect policy
- On socket error/close, reconnect with exponential backoff:
  - 250 ms -> 500 ms -> 1 s -> 2 s -> 5 s (cap at 5 s).
- After reconnect, resubscribe full symbol list.

## 7) Implementation Phases With Exit Criteria

### Phase 1: Scaffold + models + config
Deliverables:
- Cargo project initialized.
- Compile-time complete models/config.
- CLI/runtime config for fees + staleness.
Exit criteria:
- `cargo check` passes.

### Phase 2: Discovery
Deliverables:
- Lighter and Aster REST clients.
- Symbol intersection output with fee-enriched `MarketMeta` map.
Exit criteria:
- Unit tests pass for normalization/intersection.
- Startup log prints counts: lighter, aster, common.

### Phase 3: WS adapters
Deliverables:
- Lighter ticker parser and adapter.
- Aster bookTicker parser and adapter.
- Reconnect/resubscribe loops.
Exit criteria:
- Parser tests pass from fixtures.
- Manual run shows continuous quote events from both feeds.

### Phase 4: Engine
Deliverables:
- Incremental recompute for both directions.
- Best-direction row selection per symbol.
- Ranking and stale-row filtering.
Exit criteria:
- Unit tests pass for formulas/sort/staleness.

### Phase 5: egui desktop table
Deliverables:
- Desktop app window with sortable table columns:
  - symbol, direction, buy/sell exchange, buy ask, sell bid,
  - raw spread %, net spread %, max base size, max USD notional, age ms.
- Auto refresh from shared snapshot.
Exit criteria:
- Visual table updates in real time and default order is descending spread.

### Phase 6: End-to-end tests + polish
Deliverables:
- Integration mock stream test for `tick -> engine -> snapshot` path.
- Basic tracing logs for reconnect and update rates.
Exit criteria:
- `cargo test` passes.
- `cargo clippy --all-targets -- -D warnings` passes.
- `cargo fmt --check` passes.

## 8) Test Matrix (must-have)
- `symbols.rs`
  - base normalization and exact intersection correctness.
- `spread.rs`
  - raw/net spread formulas,
  - max size/notional,
  - direction selection,
  - sorting tie-breaks.
- `parsers.rs`
  - Lighter ticker payload -> `QuoteUpdate`.
  - Aster bookTicker payload -> `QuoteUpdate`.
- `integration_mock.rs`
  - mixed quote stream generates expected ranked output,
  - stale-leg suppression,
  - reconnect state continuity (mocked).

## 9) Definition Of Done
- App starts, discovers markets, and subscribes only to shared symbols.
- Live rows appear for common symbols with one best-direction row each.
- Table is sorted by spread descending by default.
- Net spread reflects taker+taker fees with configured defaults.
- Reconnect works on either socket interruption.
- All quality gates pass (`fmt`, `clippy`, `test`).

## 10) Out Of Scope (v1)
- Automated trading/execution.
- Slippage and impact modeling.
- Funding-rate adjustments.
- Multi-exchange beyond Lighter + Aster.
