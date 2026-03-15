use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Lighter,
    Aster,
    Extended,
    EdgeX,
    Hyperliquid,
}

impl Exchange {
    pub fn all() -> &'static [Exchange] {
        &[
            Exchange::Lighter,
            Exchange::Aster,
            Exchange::Extended,
            Exchange::EdgeX,
            Exchange::Hyperliquid,
        ]
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lighter => "Lighter",
            Self::Aster => "Aster",
            Self::Extended => "Extended",
            Self::EdgeX => "edgeX",
            Self::Hyperliquid => "Hyperliquid",
        }
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub exchange: Exchange,
    pub symbol_base: String,
    pub exchange_symbol: String,
    pub market_id: Option<u32>,
    pub taker_fee_pct: f64,
    pub maker_fee_pct: f64,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct FundingUpdate {
    pub exchange: Exchange,
    pub symbol_base: String,
    // Fractional funding rate (e.g. 0.0001 == 0.01%).
    pub funding_rate: f64,
    pub next_funding_ts_ms: Option<i64>,
    pub recv_ts_ms: i64,
    // Suggested freshness window for this funding snapshot.
    pub stale_after_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Quote(QuoteUpdate),
    Funding(FundingUpdate),
}

#[derive(Debug, Clone)]
pub struct ArbRow {
    pub symbol: String,
    pub buy_ex: Exchange,
    pub sell_ex: Exchange,
    pub buy_ask: f64,
    pub sell_bid: f64,
    pub raw_spread_bps: f64,
    pub net_spread_bps: f64,
    pub buy_funding_rate: Option<f64>,
    pub sell_funding_rate: Option<f64>,
    pub buy_funding_stale: bool,
    pub sell_funding_stale: bool,
    pub max_base_qty: f64,
    pub max_usd_notional: f64,
    pub age_ms: i64,
    pub latency_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct ExchangeFeedHealth {
    pub last_event_ms: Option<i64>,
    pub last_quote_ms: Option<i64>,
    pub last_funding_ms: Option<i64>,
    pub quote_rate_per_sec: f64,
    pub funding_rate_per_sec: f64,
}

pub fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}
