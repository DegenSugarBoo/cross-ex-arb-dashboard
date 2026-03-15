use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::discovery::SymbolMarkets;
use crate::model::{Exchange, MarketEvent};

pub mod aster;
pub mod edge_x;
pub mod extended;
pub mod hyperliquid;
pub mod lighter;

pub const FUNDING_CHANGE_EPSILON: f64 = 1e-6;
const FIVE_MIN_MS: u64 = 5 * 60 * 1000;
const THIRTY_MIN_MS: i64 = 30 * 60 * 1000;
const FIVE_MIN_WINDOW_MS: i64 = 5 * 60 * 1000;
const ONE_MIN_MS: u64 = 60 * 1000;
const FIFTEEN_SEC_MS: u64 = 15 * 1000;
const MAX_FUNDING_TTL_MS: i64 = 15 * 60 * 1000;

pub trait ExchangeFeed: Send + Sync {
    fn exchange(&self) -> Exchange;
    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: &SymbolMarkets,
        event_tx: mpsc::Sender<MarketEvent>,
    );
}

pub fn default_feeds() -> Vec<Box<dyn ExchangeFeed>> {
    vec![
        Box::new(lighter::LighterFeed),
        Box::new(aster::AsterFeed),
        Box::new(extended::ExtendedFeed),
        Box::new(edge_x::EdgeXFeed),
        Box::new(hyperliquid::HyperliquidFeed),
    ]
}

pub fn spawn_all_feeds(
    runtime: &Runtime,
    config: &AppConfig,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) {
    for feed in default_feeds() {
        tracing::info!(exchange = %feed.exchange(), "spawning exchange feed");
        feed.spawn(runtime, config, markets, event_tx.clone());
    }
}

pub fn backoff_delay_ms(attempt: u32) -> u64 {
    match attempt {
        0 => 250,
        1 => 500,
        2 => 1_000,
        3 => 2_000,
        _ => 5_000,
    }
}

pub fn funding_baseline_poll_ms(poll_secs: u64) -> u64 {
    poll_secs.saturating_mul(1000).max(FIVE_MIN_MS)
}

pub fn aster_adaptive_poll_ms(baseline_ms: u64, min_to_next_funding_ms: Option<i64>) -> u64 {
    match min_to_next_funding_ms {
        Some(ms) if ms <= FIVE_MIN_WINDOW_MS => FIFTEEN_SEC_MS,
        Some(ms) if ms <= THIRTY_MIN_MS => ONE_MIN_MS,
        _ => baseline_ms,
    }
}

pub fn funding_cache_ttl_ms(poll_ms: u64) -> i64 {
    (poll_ms as i64).saturating_mul(2).min(MAX_FUNDING_TTL_MS)
}

pub fn jittered_poll_ms(base_ms: u64, seed_ms: i64) -> u64 {
    let positive_seed = seed_ms.max(0) as u64;
    let jitter_pct = 10 + (positive_seed % 11); // 10%..20%
    base_ms.saturating_add(base_ms.saturating_mul(jitter_pct) / 100)
}
