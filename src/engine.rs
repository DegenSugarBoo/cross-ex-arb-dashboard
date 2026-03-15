use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::discovery::SymbolMarkets;
use crate::model::{
    ArbRow, Exchange, ExchangeFeedHealth, FundingUpdate, MarketEvent, QuoteUpdate, now_ms,
};

#[derive(Debug, Default, Clone)]
struct SymbolQuotes {
    by_exchange: HashMap<Exchange, QuoteUpdate>,
}

#[derive(Debug, Default, Clone)]
struct SymbolFunding {
    by_exchange: HashMap<Exchange, FundingUpdate>,
}

#[derive(Debug)]
pub struct EngineState {
    markets: SymbolMarkets,
    latest_quotes: HashMap<String, SymbolQuotes>,
    latest_funding: HashMap<String, SymbolFunding>,
    best_rows: HashMap<String, ArbRow>,
}

pub struct DirectionRowInput<'a> {
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
    pub fn new(markets: SymbolMarkets) -> Self {
        Self {
            markets,
            latest_quotes: HashMap::new(),
            latest_funding: HashMap::new(),
            best_rows: HashMap::new(),
        }
    }

    pub fn ingest_event(&mut self, event: MarketEvent, now_ms: i64) {
        match event {
            MarketEvent::Quote(quote) => self.ingest_quote(quote, now_ms),
            MarketEvent::Funding(funding) => self.ingest_funding(funding),
        }
    }

    fn ingest_quote(&mut self, quote: QuoteUpdate, now_ms: i64) {
        let symbol = quote.symbol_base.clone();
        let quote_slot = self.latest_quotes.entry(symbol.clone()).or_default();
        quote_slot.by_exchange.insert(quote.exchange, quote);

        self.recompute_symbol(&symbol, now_ms);
    }

    fn ingest_funding(&mut self, funding: FundingUpdate) {
        let symbol = funding.symbol_base.clone();
        let funding_slot = self.latest_funding.entry(symbol).or_default();
        funding_slot.by_exchange.insert(funding.exchange, funding);
    }

    fn recompute_symbol(&mut self, symbol: &str, now_ms: i64) {
        let Some(market_metas) = self.markets.get(symbol) else {
            self.best_rows.remove(symbol);
            return;
        };

        if market_metas.len() < 2 {
            self.best_rows.remove(symbol);
            return;
        }

        let Some(quotes) = self.latest_quotes.get(symbol) else {
            self.best_rows.remove(symbol);
            return;
        };

        let mut best_row: Option<ArbRow> = None;

        for buy_meta in market_metas {
            let Some(buy_quote) = quotes.by_exchange.get(&buy_meta.exchange) else {
                continue;
            };

            for sell_meta in market_metas {
                if sell_meta.exchange == buy_meta.exchange {
                    continue;
                }

                let Some(sell_quote) = quotes.by_exchange.get(&sell_meta.exchange) else {
                    continue;
                };

                let candidate = compute_direction_row(DirectionRowInput {
                    symbol,
                    buy_ex: buy_meta.exchange,
                    sell_ex: sell_meta.exchange,
                    buy_quote,
                    sell_quote,
                    buy_taker_fee_pct: buy_meta.taker_fee_pct,
                    sell_taker_fee_pct: sell_meta.taker_fee_pct,
                    now_ms,
                });

                best_row = select_best_direction(best_row, candidate);
            }
        }

        match best_row {
            Some(row) => {
                self.best_rows.insert(symbol.to_owned(), row);
            }
            None => {
                self.best_rows.remove(symbol);
            }
        }
    }

    pub fn ranked_snapshot(&self, now_ms: i64, stale_ms: i64) -> Vec<ArbRow> {
        let mut rows = Vec::with_capacity(self.best_rows.len());

        for row in self.best_rows.values() {
            let Some(quotes) = self.latest_quotes.get(&row.symbol) else {
                continue;
            };

            let buy_quote = quotes.by_exchange.get(&row.buy_ex);
            let sell_quote = quotes.by_exchange.get(&row.sell_ex);

            let (Some(buy_quote), Some(sell_quote)) = (buy_quote, sell_quote) else {
                continue;
            };

            let age_ms = quote_age_ms(now_ms, buy_quote, sell_quote);
            if age_ms > stale_ms {
                continue;
            }

            let mut refreshed = row.clone();
            refreshed.age_ms = age_ms;
            refreshed.buy_funding_rate = None;
            refreshed.sell_funding_rate = None;
            refreshed.buy_funding_stale = false;
            refreshed.sell_funding_stale = false;

            if let Some(funding) = self.latest_funding.get(&row.symbol) {
                if let Some(update) = funding.by_exchange.get(&row.buy_ex) {
                    refreshed.buy_funding_rate = Some(update.funding_rate);
                    refreshed.buy_funding_stale = funding_is_stale(now_ms, update);
                }
                if let Some(update) = funding.by_exchange.get(&row.sell_ex) {
                    refreshed.sell_funding_rate = Some(update.funding_rate);
                    refreshed.sell_funding_stale = funding_is_stale(now_ms, update);
                }
            }

            if refreshed.net_spread_bps <= 0.0 {
                continue;
            }
            rows.push(refreshed);
        }

        sort_rows(&mut rows);
        rows
    }
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
    let net_spread_bps =
        raw_spread_bps - (input.buy_taker_fee_pct * 100.0) - (input.sell_taker_fee_pct * 100.0);

    let max_base_qty = input.buy_quote.ask_qty.min(input.sell_quote.bid_qty);
    let max_usd_notional = max_base_qty * input.buy_quote.ask_px;
    let age_ms = quote_age_ms(input.now_ms, input.buy_quote, input.sell_quote);
    let latency_ms = row_feed_latency_ms(input.buy_quote, input.sell_quote);

    Some(ArbRow {
        symbol: input.symbol.to_owned(),
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
    rows.sort_by(|left, right| {
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

fn replace_snapshot(snapshot: &Arc<RwLock<Vec<ArbRow>>>, next_rows: Vec<ArbRow>) {
    match snapshot.write() {
        Ok(mut guard) => {
            *guard = next_rows;
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            *guard = next_rows;
        }
    }
}

fn update_exchange_health_event(
    health_snapshot: &Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    exchange: Exchange,
    is_quote: bool,
    seen_ts_ms: i64,
) {
    match health_snapshot.write() {
        Ok(mut guard) => {
            let health = guard.entry(exchange).or_default();
            health.last_event_ms = Some(seen_ts_ms);
            if is_quote {
                health.last_quote_ms = Some(seen_ts_ms);
            } else {
                health.last_funding_ms = Some(seen_ts_ms);
            }
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            let health = guard.entry(exchange).or_default();
            health.last_event_ms = Some(seen_ts_ms);
            if is_quote {
                health.last_quote_ms = Some(seen_ts_ms);
            } else {
                health.last_funding_ms = Some(seen_ts_ms);
            }
        }
    }
}

fn update_exchange_health_rates(
    health_snapshot: &Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    window_secs: f64,
    counters: &HashMap<Exchange, (usize, usize)>,
) {
    match health_snapshot.write() {
        Ok(mut guard) => {
            for exchange in Exchange::all() {
                let (quote_count, funding_count) =
                    counters.get(exchange).copied().unwrap_or((0, 0));
                let health = guard.entry(*exchange).or_default();
                health.quote_rate_per_sec = (quote_count as f64) / window_secs;
                health.funding_rate_per_sec = (funding_count as f64) / window_secs;
            }
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            for exchange in Exchange::all() {
                let (quote_count, funding_count) =
                    counters.get(exchange).copied().unwrap_or((0, 0));
                let health = guard.entry(*exchange).or_default();
                health.quote_rate_per_sec = (quote_count as f64) / window_secs;
                health.funding_rate_per_sec = (funding_count as f64) / window_secs;
            }
        }
    }
}

pub async fn run_engine(
    mut event_rx: mpsc::Receiver<MarketEvent>,
    markets: SymbolMarkets,
    config: AppConfig,
    snapshot: Arc<RwLock<Vec<ArbRow>>>,
    health_snapshot: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
) {
    let mut state = EngineState::new(markets);
    let mut stale_tick = tokio::time::interval(Duration::from_millis(100));
    let mut rate_tick = tokio::time::interval(Duration::from_secs(5));
    let rate_window_secs = 5.0;
    let mut events_in_window: usize = 0;
    let mut per_exchange_window: HashMap<Exchange, (usize, usize)> = HashMap::new();

    loop {
        tokio::select! {
            maybe_event = event_rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        events_in_window = events_in_window.saturating_add(1);
                        let now = now_ms();
                        let (exchange, is_quote) = match &event {
                            MarketEvent::Quote(quote) => (quote.exchange, true),
                            MarketEvent::Funding(funding) => (funding.exchange, false),
                        };
                        update_exchange_health_event(&health_snapshot, exchange, is_quote, now);
                        {
                            let entry = per_exchange_window.entry(exchange).or_insert((0, 0));
                            if is_quote {
                                entry.0 = entry.0.saturating_add(1);
                            } else {
                                entry.1 = entry.1.saturating_add(1);
                            }
                        }
                        state.ingest_event(event, now);
                        let rows = state.ranked_snapshot(now, config.stale_ms);
                        replace_snapshot(&snapshot, rows);
                    }
                    None => {
                        tracing::info!("engine exiting: market event channel closed");
                        break;
                    }
                }
            }
            _ = stale_tick.tick() => {
                let now = now_ms();
                let rows = state.ranked_snapshot(now, config.stale_ms);
                replace_snapshot(&snapshot, rows);
            }
            _ = rate_tick.tick() => {
                tracing::info!(events_in_window, "engine market event rate (5s window)");
                events_in_window = 0;
                update_exchange_health_rates(
                    &health_snapshot,
                    rate_window_secs,
                    &per_exchange_window,
                );
                per_exchange_window.clear();
            }
        }
    }
}
