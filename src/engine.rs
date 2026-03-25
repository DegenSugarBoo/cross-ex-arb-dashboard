use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU32, Ordering as AtomicOrdering},
};
use std::time::Duration;

use compact_str::CompactString;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::discovery::SymbolMarkets;
use crate::model::{
    ArbRow, EXCHANGE_COUNT, Exchange, ExchangeFeedHealth, FundingUpdate, MarketEvent,
    NO_ROUTE_SELECTED, QuoteUpdate, RouteHistoryPoint, RouteHistorySnapshot, RouteKey, now_ms,
};

const HISTORY_WINDOW_MS: i64 = 30_000;
const HISTORY_COMPACT_THRESHOLD: usize = 2_048;
const TOP_ROWS_LIMIT: usize = 20;

#[derive(Debug, Clone)]
struct SymbolQuotes {
    by_exchange: [Option<QuoteUpdate>; EXCHANGE_COUNT],
}

impl Default for SymbolQuotes {
    fn default() -> Self {
        Self {
            by_exchange: [const { None }; EXCHANGE_COUNT],
        }
    }
}

#[derive(Debug, Clone)]
struct SymbolFunding {
    by_exchange: [Option<FundingUpdate>; EXCHANGE_COUNT],
}

impl Default for SymbolFunding {
    fn default() -> Self {
        Self {
            by_exchange: [const { None }; EXCHANGE_COUNT],
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouteMeta {
    pub key: RouteKey,
    pub buy_ex: Exchange,
    pub sell_ex: Exchange,
    pub buy_taker_fee_pct: f64,
    pub sell_taker_fee_pct: f64,
}

#[derive(Debug, Clone, Default)]
struct RouteHistoryColumns {
    ts_ms: Vec<i64>,
    net_spread_bps: Vec<f32>,
    max_usd_notional: Vec<f32>,
    age_ms: Vec<u32>,
    start_idx: usize,
}

impl RouteHistoryColumns {
    fn append(&mut self, ts_ms: i64, net_spread_bps: f64, max_usd_notional: f64, age_ms: i64) {
        self.ts_ms.push(ts_ms);
        self.net_spread_bps.push(net_spread_bps as f32);
        self.max_usd_notional.push(max_usd_notional as f32);
        self.age_ms.push(age_ms.clamp(0, u32::MAX as i64) as u32);
    }

    fn prune(&mut self, cutoff_ms: i64) {
        while self.start_idx < self.ts_ms.len() && self.ts_ms[self.start_idx] < cutoff_ms {
            self.start_idx += 1;
        }

        if self.start_idx >= HISTORY_COMPACT_THRESHOLD
            && self.start_idx.saturating_mul(2) >= self.ts_ms.len()
        {
            self.ts_ms.drain(0..self.start_idx);
            self.net_spread_bps.drain(0..self.start_idx);
            self.max_usd_notional.drain(0..self.start_idx);
            self.age_ms.drain(0..self.start_idx);
            self.start_idx = 0;
        }

        debug_assert_eq!(self.ts_ms.len(), self.net_spread_bps.len());
        debug_assert_eq!(self.ts_ms.len(), self.max_usd_notional.len());
        debug_assert_eq!(self.ts_ms.len(), self.age_ms.len());
    }

    fn visible_start_idx_for_cutoff(&self, cutoff_ms: i64) -> usize {
        let mut idx = self.start_idx.min(self.ts_ms.len());
        while idx < self.ts_ms.len() && self.ts_ms[idx] < cutoff_ms {
            idx += 1;
        }
        idx
    }

    fn visible_len(&self) -> usize {
        self.ts_ms.len().saturating_sub(self.start_idx)
    }
}

#[derive(Debug)]
pub struct EngineState {
    latest_quotes: HashMap<CompactString, SymbolQuotes>,
    latest_funding: HashMap<CompactString, SymbolFunding>,
    best_rows: HashMap<CompactString, ArbRow>,
    rows_by_symbol: HashMap<CompactString, Vec<ArbRow>>,
    route_catalog: Vec<RouteMeta>,
    routes_by_symbol: HashMap<CompactString, Vec<u32>>,
    route_history: Vec<RouteHistoryColumns>,
    exchange_enabled: [bool; EXCHANGE_COUNT],
    // Reusable buffer to avoid repeated allocations.
    snapshot_buf: Vec<ArbRow>,
    detail_revision: u64,
}

pub struct DirectionRowInput<'a> {
    pub route_id: u32,
    pub symbol: &'a str,
    pub buy_ex: Exchange,
    pub sell_ex: Exchange,
    pub buy_quote: &'a QuoteUpdate,
    pub sell_quote: &'a QuoteUpdate,
    pub buy_taker_fee_pct: f64,
    pub sell_taker_fee_pct: f64,
    pub now_ms: i64,
}

impl EngineState {
    pub fn new(markets: Arc<SymbolMarkets>) -> Self {
        let (route_catalog, routes_by_symbol) = build_route_catalog(&markets);
        let route_history = vec![RouteHistoryColumns::default(); route_catalog.len()];

        Self {
            latest_quotes: HashMap::new(),
            latest_funding: HashMap::new(),
            best_rows: HashMap::new(),
            rows_by_symbol: HashMap::new(),
            route_catalog,
            routes_by_symbol,
            route_history,
            exchange_enabled: [true; EXCHANGE_COUNT],
            snapshot_buf: Vec::new(),
            detail_revision: 0,
        }
    }

    pub fn route_catalog(&self) -> &[RouteMeta] {
        &self.route_catalog
    }

    pub fn route_ids_for_symbol(&self, symbol: &str) -> Option<&[u32]> {
        self.routes_by_symbol.get(symbol).map(Vec::as_slice)
    }

    pub fn history_column_lengths_for_route(
        &self,
        route_id: u32,
    ) -> Option<(usize, usize, usize, usize)> {
        let history = self.route_history.get(route_id as usize)?;
        Some((
            history.ts_ms.len(),
            history.net_spread_bps.len(),
            history.max_usd_notional.len(),
            history.age_ms.len(),
        ))
    }

    pub fn history_len_for_route(&self, route_id: u32) -> Option<usize> {
        self.route_history
            .get(route_id as usize)
            .map(RouteHistoryColumns::visible_len)
    }

    pub fn ingest_event(&mut self, event: MarketEvent, now_ms: i64) {
        match event {
            MarketEvent::Quote(quote) => {
                if self.exchange_is_enabled(quote.exchange) {
                    self.ingest_quote(quote, now_ms);
                }
            }
            MarketEvent::Funding(funding) => {
                if self.exchange_is_enabled(funding.exchange) {
                    self.ingest_funding(funding);
                }
            }
            MarketEvent::ExchangeEnabled(exchange) => self.set_exchange_enabled(exchange, true),
            MarketEvent::ExchangeDisabled(exchange) => {
                self.set_exchange_enabled(exchange, false);
                self.disable_exchange(exchange, now_ms);
            }
        }
    }

    pub fn set_exchange_enabled(&mut self, exchange: Exchange, enabled: bool) {
        self.exchange_enabled[exchange.index()] = enabled;
    }

    pub fn exchange_is_enabled(&self, exchange: Exchange) -> bool {
        self.exchange_enabled[exchange.index()]
    }

    fn ingest_quote(&mut self, quote: QuoteUpdate, now_ms: i64) {
        let ex_idx = quote.exchange.index();
        // Take ownership of symbol before moving quote into the slot.
        let symbol = quote.symbol_base.clone();
        let quote_slot = self.latest_quotes.entry(symbol.clone()).or_default();
        quote_slot.by_exchange[ex_idx] = Some(quote);
        self.recompute_symbol(&symbol, now_ms);
    }

    fn ingest_funding(&mut self, funding: FundingUpdate) {
        let ex_idx = funding.exchange.index();
        let funding_slot = self
            .latest_funding
            .entry(funding.symbol_base.clone())
            .or_default();
        funding_slot.by_exchange[ex_idx] = Some(funding);
    }

    pub fn disable_exchange(&mut self, exchange: Exchange, now_ms: i64) {
        let exchange_idx = exchange.index();
        let mut affected_symbols: HashSet<CompactString> = HashSet::new();

        for (symbol, quotes) in &mut self.latest_quotes {
            if quotes.by_exchange[exchange_idx].take().is_some() {
                affected_symbols.insert(symbol.clone());
            }
        }

        for (symbol, funding) in &mut self.latest_funding {
            if funding.by_exchange[exchange_idx].take().is_some() {
                affected_symbols.insert(symbol.clone());
            }
        }

        self.latest_quotes
            .retain(|_, quotes| quotes.by_exchange.iter().any(Option::is_some));
        self.latest_funding
            .retain(|_, funding| funding.by_exchange.iter().any(Option::is_some));

        self.clear_route_history_for_exchange(exchange);

        for symbol in affected_symbols {
            self.recompute_symbol(symbol.as_str(), now_ms);
        }
    }

    fn clear_route_history_for_exchange(&mut self, exchange: Exchange) {
        for (route_id, route_meta) in self.route_catalog.iter().enumerate() {
            if route_meta.buy_ex == exchange || route_meta.sell_ex == exchange {
                self.route_history[route_id] = RouteHistoryColumns::default();
            }
        }
    }

    fn recompute_symbol(&mut self, symbol: &str, now_ms: i64) {
        let Some(route_ids) = self.routes_by_symbol.get(symbol) else {
            self.best_rows.remove(symbol);
            self.rows_by_symbol.remove(symbol);
            return;
        };

        let Some(quotes) = self.latest_quotes.get(symbol) else {
            self.best_rows.remove(symbol);
            self.rows_by_symbol.remove(symbol);
            return;
        };

        let best_row: Option<ArbRow> = {
            let route_catalog = &self.route_catalog;
            let route_history = &mut self.route_history;
            let cutoff_ms = now_ms - HISTORY_WINDOW_MS;
            let mut best_row: Option<ArbRow> = None;
            let mut all_rows: Vec<ArbRow> = Vec::with_capacity(route_ids.len());

            for route_id in route_ids {
                let Some(route_meta) = route_catalog.get(*route_id as usize) else {
                    continue;
                };

                let Some(buy_quote) = &quotes.by_exchange[route_meta.buy_ex.index()] else {
                    continue;
                };
                let Some(sell_quote) = &quotes.by_exchange[route_meta.sell_ex.index()] else {
                    continue;
                };

                if let Some(candidate) = compute_direction_row(DirectionRowInput {
                    route_id: *route_id,
                    symbol,
                    buy_ex: route_meta.buy_ex,
                    sell_ex: route_meta.sell_ex,
                    buy_quote,
                    sell_quote,
                    buy_taker_fee_pct: route_meta.buy_taker_fee_pct,
                    sell_taker_fee_pct: route_meta.sell_taker_fee_pct,
                    now_ms,
                }) {
                    append_history_sample(
                        route_history,
                        *route_id as usize,
                        candidate.net_spread_bps,
                        candidate.max_usd_notional,
                        candidate.age_ms,
                        now_ms,
                        cutoff_ms,
                    );
                    all_rows.push(candidate.clone());
                    best_row = select_best_direction(best_row, Some(candidate));
                }
            }

            all_rows.sort_unstable_by(|left, right| {
                right
                    .net_spread_bps
                    .partial_cmp(&left.net_spread_bps)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| {
                        right
                            .raw_spread_bps
                            .partial_cmp(&left.raw_spread_bps)
                            .unwrap_or(Ordering::Equal)
                    })
                    .then_with(|| left.symbol.cmp(&right.symbol))
            });
            if all_rows.is_empty() {
                self.rows_by_symbol.remove(symbol);
            } else {
                self.rows_by_symbol
                    .insert(CompactString::from(symbol), all_rows);
            }

            best_row
        };

        match best_row {
            Some(row) => {
                if let Some(existing) = self.best_rows.get_mut(symbol) {
                    *existing = row;
                } else {
                    self.best_rows.insert(CompactString::from(symbol), row);
                }
            }
            None => {
                self.best_rows.remove(symbol);
            }
        }
    }

    fn rebuild_ranked_snapshot_buf(&mut self, now_ms: i64, stale_ms: i64) {
        self.snapshot_buf.clear();

        for rows in self.rows_by_symbol.values() {
            for row in rows {
                let Some(quotes) = self.latest_quotes.get(&row.symbol) else {
                    continue;
                };

                let buy_quote = quotes.by_exchange[row.buy_ex.index()].as_ref();
                let sell_quote = quotes.by_exchange[row.sell_ex.index()].as_ref();

                let (Some(buy_quote), Some(sell_quote)) = (buy_quote, sell_quote) else {
                    continue;
                };

                let age_ms = quote_age_ms(now_ms, buy_quote, sell_quote);
                if age_ms > stale_ms {
                    continue;
                }

                let mut buy_funding_rate = None;
                let mut sell_funding_rate = None;
                let mut buy_funding_stale = false;
                let mut sell_funding_stale = false;

                if let Some(funding) = self.latest_funding.get(&row.symbol) {
                    if let Some(update) = &funding.by_exchange[row.buy_ex.index()] {
                        buy_funding_rate = Some(update.funding_rate);
                        buy_funding_stale = funding_is_stale(now_ms, update);
                    }
                    if let Some(update) = &funding.by_exchange[row.sell_ex.index()] {
                        sell_funding_rate = Some(update.funding_rate);
                        sell_funding_stale = funding_is_stale(now_ms, update);
                    }
                }

                self.snapshot_buf.push(ArbRow {
                    route_id: row.route_id,
                    symbol: row.symbol.clone(),
                    buy_ex: row.buy_ex,
                    sell_ex: row.sell_ex,
                    buy_ask: row.buy_ask,
                    sell_bid: row.sell_bid,
                    raw_spread_bps: row.raw_spread_bps,
                    net_spread_bps: row.net_spread_bps,
                    buy_funding_rate,
                    sell_funding_rate,
                    buy_funding_stale,
                    sell_funding_stale,
                    max_base_qty: row.max_base_qty,
                    max_usd_notional: row.max_usd_notional,
                    age_ms,
                    latency_ms: row.latency_ms,
                });
            }
        }
    }

    pub fn ranked_snapshot_all(&mut self, now_ms: i64, stale_ms: i64) -> &[ArbRow] {
        self.rebuild_ranked_snapshot_buf(now_ms, stale_ms);
        sort_rows(&mut self.snapshot_buf);
        &self.snapshot_buf
    }

    pub fn ranked_snapshot(&mut self, now_ms: i64, stale_ms: i64) -> &[ArbRow] {
        self.ranked_snapshot_all(now_ms, stale_ms);
        self.snapshot_buf.truncate(TOP_ROWS_LIMIT);
        &self.snapshot_buf
    }

    pub fn prune_history_all(&mut self, now_ms: i64) {
        let cutoff_ms = now_ms - HISTORY_WINDOW_MS;
        for history in &mut self.route_history {
            history.prune(cutoff_ms);
        }
    }

    pub fn history_snapshot_for_route(
        &mut self,
        route_id: u32,
        now_ms: i64,
    ) -> Option<RouteHistorySnapshot> {
        let route_meta = self.route_catalog.get(route_id as usize)?;
        let history = self.route_history.get(route_id as usize)?;

        let cutoff_ms = now_ms - HISTORY_WINDOW_MS;
        let start_idx = history.visible_start_idx_for_cutoff(cutoff_ms);
        if start_idx >= history.ts_ms.len() {
            return None;
        }

        let mut points = Vec::with_capacity(history.ts_ms.len() - start_idx);
        for idx in start_idx..history.ts_ms.len() {
            points.push(RouteHistoryPoint {
                ts_ms: history.ts_ms[idx],
                net_spread_bps: history.net_spread_bps[idx],
                max_usd_notional: history.max_usd_notional[idx],
                age_ms: history.age_ms[idx],
            });
        }

        self.detail_revision = self.detail_revision.wrapping_add(1);
        Some(RouteHistorySnapshot {
            route_id,
            key: route_meta.key.clone(),
            points,
            generated_at_ms: now_ms,
            revision: self.detail_revision,
        })
    }
}

fn append_history_sample(
    route_history: &mut [RouteHistoryColumns],
    route_idx: usize,
    net_spread_bps: f64,
    max_usd_notional: f64,
    age_ms: i64,
    now_ms: i64,
    cutoff_ms: i64,
) {
    let history = &mut route_history[route_idx];
    history.append(now_ms, net_spread_bps, max_usd_notional, age_ms);
    history.prune(cutoff_ms);
}

fn build_route_catalog(
    markets: &SymbolMarkets,
) -> (Vec<RouteMeta>, HashMap<CompactString, Vec<u32>>) {
    let mut symbols: Vec<&str> = markets.keys().map(String::as_str).collect();
    symbols.sort_unstable();

    let mut route_catalog: Vec<RouteMeta> = Vec::new();
    let mut routes_by_symbol: HashMap<CompactString, Vec<u32>> = HashMap::new();

    for symbol in symbols {
        let Some(market_metas) = markets.get(symbol) else {
            continue;
        };

        if market_metas.len() < 2 {
            continue;
        }

        let mut sorted_metas = market_metas.clone();
        sorted_metas.sort_by_key(|meta| meta.exchange.index());
        sorted_metas.dedup_by_key(|meta| meta.exchange);
        if sorted_metas.len() < 2 {
            continue;
        }

        let symbol_key = CompactString::from(symbol);
        let mut route_ids: Vec<u32> =
            Vec::with_capacity(sorted_metas.len() * (sorted_metas.len() - 1));

        for buy_meta in &sorted_metas {
            for sell_meta in &sorted_metas {
                if buy_meta.exchange == sell_meta.exchange {
                    continue;
                }

                let route_id = route_catalog.len() as u32;
                route_catalog.push(RouteMeta {
                    key: RouteKey {
                        symbol: symbol_key.clone(),
                        buy_ex: buy_meta.exchange,
                        sell_ex: sell_meta.exchange,
                    },
                    buy_ex: buy_meta.exchange,
                    sell_ex: sell_meta.exchange,
                    buy_taker_fee_pct: buy_meta.taker_fee_pct,
                    sell_taker_fee_pct: sell_meta.taker_fee_pct,
                });
                route_ids.push(route_id);
            }
        }

        routes_by_symbol.insert(symbol_key, route_ids);
    }

    (route_catalog, routes_by_symbol)
}

fn quote_age_ms(now_ms: i64, buy_quote: &QuoteUpdate, sell_quote: &QuoteUpdate) -> i64 {
    let buy_age = (now_ms - buy_quote.recv_ts_ms).max(0);
    let sell_age = (now_ms - sell_quote.recv_ts_ms).max(0);
    buy_age.max(sell_age)
}

fn normalize_exchange_ts_ms(exch_ts: i64) -> i64 {
    let abs = exch_ts.abs();
    if abs >= 100_000_000_000_000_000 {
        // Nanoseconds -> milliseconds.
        exch_ts / 1_000_000
    } else if abs >= 100_000_000_000_000 {
        // Microseconds -> milliseconds.
        exch_ts / 1_000
    } else if abs >= 100_000_000_000 {
        // Already milliseconds.
        exch_ts
    } else if abs >= 100_000_000 {
        // Seconds -> milliseconds.
        exch_ts * 1_000
    } else {
        exch_ts
    }
}

fn quote_feed_latency_ms(quote: &QuoteUpdate) -> i64 {
    let exch_ts_ms = normalize_exchange_ts_ms(quote.exch_ts_ms);
    (quote.recv_ts_ms - exch_ts_ms).saturating_abs()
}

fn row_feed_latency_ms(buy_quote: &QuoteUpdate, sell_quote: &QuoteUpdate) -> i64 {
    quote_feed_latency_ms(buy_quote).max(quote_feed_latency_ms(sell_quote))
}

fn funding_is_stale(now_ms: i64, update: &FundingUpdate) -> bool {
    match update.stale_after_ms {
        Some(stale_after_ms) => (now_ms - update.recv_ts_ms).max(0) > stale_after_ms,
        None => false,
    }
}

pub fn compute_direction_row(input: DirectionRowInput<'_>) -> Option<ArbRow> {
    if input.buy_quote.ask_px <= 0.0 || input.sell_quote.bid_px <= 0.0 {
        return None;
    }
    if input.buy_quote.ask_qty <= 0.0 || input.sell_quote.bid_qty <= 0.0 {
        return None;
    }

    let raw_spread_bps = (input.sell_quote.bid_px / input.buy_quote.ask_px - 1.0) * 10_000.0;
    let net_spread_bps = raw_spread_bps
        - 2.0 * (input.buy_taker_fee_pct * 100.0)
        - 2.0 * (input.sell_taker_fee_pct * 100.0);

    let max_base_qty = input.buy_quote.ask_qty.min(input.sell_quote.bid_qty);
    let max_usd_notional = max_base_qty * input.buy_quote.ask_px;
    let age_ms = quote_age_ms(input.now_ms, input.buy_quote, input.sell_quote);
    let latency_ms = row_feed_latency_ms(input.buy_quote, input.sell_quote);

    Some(ArbRow {
        route_id: input.route_id,
        symbol: CompactString::from(input.symbol),
        buy_ex: input.buy_ex,
        sell_ex: input.sell_ex,
        buy_ask: input.buy_quote.ask_px,
        sell_bid: input.sell_quote.bid_px,
        raw_spread_bps,
        net_spread_bps,
        buy_funding_rate: None,
        sell_funding_rate: None,
        buy_funding_stale: false,
        sell_funding_stale: false,
        max_base_qty,
        max_usd_notional,
        age_ms,
        latency_ms,
    })
}

pub fn select_best_direction(first: Option<ArbRow>, second: Option<ArbRow>) -> Option<ArbRow> {
    match (first, second) {
        (Some(a), Some(b)) => {
            let winner_is_a = if a.net_spread_bps != b.net_spread_bps {
                a.net_spread_bps > b.net_spread_bps
            } else {
                a.raw_spread_bps >= b.raw_spread_bps
            };

            if winner_is_a { Some(a) } else { Some(b) }
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

pub fn sort_rows(rows: &mut [ArbRow]) {
    rows.sort_unstable_by(|left, right| {
        right
            .net_spread_bps
            .partial_cmp(&left.net_spread_bps)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right
                    .raw_spread_bps
                    .partial_cmp(&left.raw_spread_bps)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
}

fn replace_snapshot(snapshot: &Arc<RwLock<Vec<ArbRow>>>, next_rows: &[ArbRow]) {
    match snapshot.write() {
        Ok(mut guard) => {
            guard.clear();
            guard.extend_from_slice(next_rows);
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            guard.clear();
            guard.extend_from_slice(next_rows);
        }
    }
}

fn replace_detail_snapshot(
    detail_snapshot: &Arc<RwLock<Option<RouteHistorySnapshot>>>,
    next_snapshot: Option<RouteHistorySnapshot>,
) {
    match detail_snapshot.write() {
        Ok(mut guard) => {
            *guard = next_snapshot;
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            *guard = next_snapshot;
        }
    }
}

fn with_health_write<F>(
    health_snapshot: &Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    mut f: F,
) where
    F: FnMut(&mut HashMap<Exchange, ExchangeFeedHealth>),
{
    match health_snapshot.write() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            f(&mut guard);
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PendingHealthUpdate {
    last_event_ms: Option<i64>,
    last_quote_ms: Option<i64>,
    last_funding_ms: Option<i64>,
    quote_events_inc: u64,
    funding_events_inc: u64,
}

fn flush_exchange_health_events(
    health_snapshot: &Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    pending: &mut HashMap<Exchange, PendingHealthUpdate>,
) {
    if pending.is_empty() {
        return;
    }

    let updates = std::mem::take(pending);
    with_health_write(health_snapshot, |guard| {
        for (exchange, update) in &updates {
            let health = guard.entry(*exchange).or_default();
            if let Some(last_event_ms) = update.last_event_ms {
                health.last_event_ms = Some(last_event_ms);
            }
            if let Some(last_quote_ms) = update.last_quote_ms {
                health.last_quote_ms = Some(last_quote_ms);
            }
            if let Some(last_funding_ms) = update.last_funding_ms {
                health.last_funding_ms = Some(last_funding_ms);
            }
            health.total_quote_events = health
                .total_quote_events
                .saturating_add(update.quote_events_inc);
            health.total_funding_events = health
                .total_funding_events
                .saturating_add(update.funding_events_inc);
        }
    });
}

fn update_exchange_health_rates(
    health_snapshot: &Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    window_secs: f64,
    counters: &HashMap<Exchange, (usize, usize)>,
) {
    with_health_write(health_snapshot, |guard| {
        for exchange in Exchange::all() {
            let (quote_count, funding_count) = counters.get(exchange).copied().unwrap_or((0, 0));
            let health = guard.entry(*exchange).or_default();
            health.quote_rate_per_sec = (quote_count as f64) / window_secs;
            health.funding_rate_per_sec = (funding_count as f64) / window_secs;
        }
    });
}

fn reset_exchange_health(
    health_snapshot: &Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    exchange: Exchange,
) {
    with_health_write(health_snapshot, |guard| {
        guard.insert(exchange, ExchangeFeedHealth::default());
    });
}

pub async fn run_engine(
    mut event_rx: mpsc::Receiver<MarketEvent>,
    markets: Arc<SymbolMarkets>,
    config: AppConfig,
    snapshot: Arc<RwLock<Vec<ArbRow>>>,
    health_snapshot: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    selected_route_id: Arc<AtomicU32>,
    detail_snapshot: Arc<RwLock<Option<RouteHistorySnapshot>>>,
) {
    let mut state = EngineState::new(markets);
    let publish_interval_ms = (1000_u64 / config.ui_fps.max(1) as u64).clamp(10, 100);
    let mut publish_tick = tokio::time::interval(Duration::from_millis(publish_interval_ms));
    publish_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rate_tick = tokio::time::interval(Duration::from_secs(5));
    rate_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prune_tick = tokio::time::interval(Duration::from_secs(1));
    prune_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut detail_tick = tokio::time::interval(Duration::from_millis(100));
    detail_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let rate_window_secs = 5.0;
    let mut events_in_window: usize = 0;
    let mut per_exchange_window: HashMap<Exchange, (usize, usize)> = HashMap::new();
    let mut pending_health_updates: HashMap<Exchange, PendingHealthUpdate> = HashMap::new();

    loop {
        tokio::select! {
            maybe_event = event_rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        let now = now_ms();
                        match event {
                            MarketEvent::Quote(quote) => {
                                if !state.exchange_is_enabled(quote.exchange) {
                                    continue;
                                }

                                events_in_window = events_in_window.saturating_add(1);
                                let exchange = quote.exchange;
                                {
                                    let pending = pending_health_updates.entry(exchange).or_default();
                                    pending.last_event_ms = Some(now);
                                    pending.last_quote_ms = Some(now);
                                    pending.quote_events_inc =
                                        pending.quote_events_inc.saturating_add(1);
                                }
                                {
                                    let entry = per_exchange_window.entry(exchange).or_insert((0, 0));
                                    entry.0 = entry.0.saturating_add(1);
                                }
                                state.ingest_event(MarketEvent::Quote(quote), now);
                            }
                            MarketEvent::Funding(funding) => {
                                if !state.exchange_is_enabled(funding.exchange) {
                                    continue;
                                }

                                events_in_window = events_in_window.saturating_add(1);
                                let exchange = funding.exchange;
                                {
                                    let pending = pending_health_updates.entry(exchange).or_default();
                                    pending.last_event_ms = Some(now);
                                    pending.last_funding_ms = Some(now);
                                    pending.funding_events_inc =
                                        pending.funding_events_inc.saturating_add(1);
                                }
                                {
                                    let entry = per_exchange_window.entry(exchange).or_insert((0, 0));
                                    entry.1 = entry.1.saturating_add(1);
                                }
                                state.ingest_event(MarketEvent::Funding(funding), now);
                            }
                            MarketEvent::ExchangeEnabled(exchange) => {
                                pending_health_updates.remove(&exchange);
                                per_exchange_window.remove(&exchange);
                                reset_exchange_health(&health_snapshot, exchange);
                                state.ingest_event(MarketEvent::ExchangeEnabled(exchange), now);
                            }
                            MarketEvent::ExchangeDisabled(exchange) => {
                                pending_health_updates.remove(&exchange);
                                per_exchange_window.remove(&exchange);
                                reset_exchange_health(&health_snapshot, exchange);
                                state.ingest_event(MarketEvent::ExchangeDisabled(exchange), now);
                                let rows = state.ranked_snapshot_all(now, config.stale_ms);
                                replace_snapshot(&snapshot, rows);
                                let selected = selected_route_id.load(AtomicOrdering::Relaxed);
                                let next_snapshot = if selected == NO_ROUTE_SELECTED {
                                    None
                                } else {
                                    state.history_snapshot_for_route(selected, now)
                                };
                                replace_detail_snapshot(&detail_snapshot, next_snapshot);
                            }
                        }
                    }
                    None => {
                        flush_exchange_health_events(&health_snapshot, &mut pending_health_updates);
                        let now = now_ms();
                        state.prune_history_all(now);
                        let rows = state.ranked_snapshot_all(now, config.stale_ms);
                        replace_snapshot(&snapshot, rows);
                        replace_detail_snapshot(&detail_snapshot, None);
                        tracing::info!("engine exiting: market event channel closed");
                        break;
                    }
                }
            }
            _ = publish_tick.tick() => {
                flush_exchange_health_events(&health_snapshot, &mut pending_health_updates);
                let now = now_ms();
                let rows = state.ranked_snapshot_all(now, config.stale_ms);
                replace_snapshot(&snapshot, rows);
            }
            _ = rate_tick.tick() => {
                flush_exchange_health_events(&health_snapshot, &mut pending_health_updates);
                tracing::info!(events_in_window, "engine market event rate (5s window)");
                events_in_window = 0;
                update_exchange_health_rates(
                    &health_snapshot,
                    rate_window_secs,
                    &per_exchange_window,
                );
                per_exchange_window.clear();
            }
            _ = prune_tick.tick() => {
                state.prune_history_all(now_ms());
            }
            _ = detail_tick.tick() => {
                let selected = selected_route_id.load(AtomicOrdering::Relaxed);
                if selected == NO_ROUTE_SELECTED {
                    replace_detail_snapshot(&detail_snapshot, None);
                    continue;
                }

                let now = now_ms();
                let next_snapshot = state.history_snapshot_for_route(selected, now);
                replace_detail_snapshot(&detail_snapshot, next_snapshot);
            }
        }
    }
}
