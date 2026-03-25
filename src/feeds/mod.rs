use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::discovery::SymbolMarkets;
use crate::model::{
    Exchange, ExchangeConnectionState, MarketEvent, SharedExchangeConnectionStates, now_ms,
};

pub mod apex;
pub mod aster;
pub mod binance;
pub mod bybit;
pub mod edge_x;
pub mod extended;
pub mod grvt;
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
        runtime: &Handle,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>>;
}

pub fn default_feeds() -> Vec<Box<dyn ExchangeFeed>> {
    vec![
        Box::new(lighter::LighterFeed),
        Box::new(aster::AsterFeed),
        Box::new(binance::BinanceFeed),
        Box::new(bybit::BybitFeed),
        Box::new(extended::ExtendedFeed),
        Box::new(edge_x::EdgeXFeed),
        Box::new(hyperliquid::HyperliquidFeed),
        Box::new(grvt::GrvtFeed),
        Box::new(apex::ApexFeed),
    ]
}

pub fn spawn_all_feeds(
    runtime: &Runtime,
    config: &AppConfig,
    markets: Arc<SymbolMarkets>,
    event_tx: mpsc::Sender<MarketEvent>,
) {
    let handle = runtime.handle().clone();
    for feed in default_feeds() {
        tracing::info!(exchange = %feed.exchange(), "spawning exchange feed");
        let _handles = feed.spawn(
            &handle,
            config,
            Arc::clone(&markets),
            event_tx.clone(),
            CancellationToken::new(),
        );
    }
}

#[derive(Debug)]
struct ExchangeTaskGroup {
    cancel_token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
}

pub type FeedSupervisorHandle = Arc<FeedSupervisor>;

pub struct FeedSupervisor {
    runtime: Handle,
    config: AppConfig,
    markets: Arc<SymbolMarkets>,
    event_tx: mpsc::Sender<MarketEvent>,
    connection_states: SharedExchangeConnectionStates,
    task_groups: RwLock<HashMap<Exchange, ExchangeTaskGroup>>,
}

impl FeedSupervisor {
    pub fn new(
        runtime: &Runtime,
        config: AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        connection_states: SharedExchangeConnectionStates,
    ) -> FeedSupervisorHandle {
        Arc::new(Self {
            runtime: runtime.handle().clone(),
            config,
            markets,
            event_tx,
            connection_states,
            task_groups: RwLock::new(HashMap::new()),
        })
    }

    pub fn start_all(&self) {
        for exchange in Exchange::all() {
            let _ = self.enable(*exchange);
        }
    }

    pub fn enable(&self, exchange: Exchange) -> bool {
        let mut groups = write_lock(&self.task_groups);
        if groups.contains_key(&exchange) && exchange_is_enabled(&self.connection_states, exchange)
        {
            return false;
        }

        if let Some(existing) = groups.remove(&exchange) {
            stop_task_group(existing);
        }

        let feed = feed_for_exchange(exchange);
        let cancel_token = CancellationToken::new();
        let handles = feed.spawn(
            &self.runtime,
            &self.config,
            Arc::clone(&self.markets),
            self.event_tx.clone(),
            cancel_token.clone(),
        );
        groups.insert(
            exchange,
            ExchangeTaskGroup {
                cancel_token,
                handles,
            },
        );
        drop(groups);

        set_exchange_connection_state(
            &self.connection_states,
            exchange,
            ExchangeConnectionState::enabled(now_ms()),
        );
        emit_control_event(
            &self.runtime,
            &self.event_tx,
            MarketEvent::ExchangeEnabled(exchange),
        );
        true
    }

    pub fn disable(&self, exchange: Exchange) -> bool {
        let task_group = {
            let mut groups = write_lock(&self.task_groups);
            groups.remove(&exchange)
        };

        let was_enabled =
            task_group.is_some() || exchange_is_enabled(&self.connection_states, exchange);
        set_exchange_connection_state(
            &self.connection_states,
            exchange,
            ExchangeConnectionState::disabled(now_ms()),
        );
        emit_control_event(
            &self.runtime,
            &self.event_tx,
            MarketEvent::ExchangeDisabled(exchange),
        );

        if let Some(group) = task_group {
            stop_task_group(group);
        }

        was_enabled
    }
}

fn feed_for_exchange(exchange: Exchange) -> Box<dyn ExchangeFeed> {
    match exchange {
        Exchange::Lighter => Box::new(lighter::LighterFeed),
        Exchange::Aster => Box::new(aster::AsterFeed),
        Exchange::Binance => Box::new(binance::BinanceFeed),
        Exchange::Bybit => Box::new(bybit::BybitFeed),
        Exchange::Extended => Box::new(extended::ExtendedFeed),
        Exchange::EdgeX => Box::new(edge_x::EdgeXFeed),
        Exchange::Hyperliquid => Box::new(hyperliquid::HyperliquidFeed),
        Exchange::Grvt => Box::new(grvt::GrvtFeed),
        Exchange::ApeX => Box::new(apex::ApexFeed),
    }
}

fn emit_control_event(runtime: &Handle, event_tx: &mpsc::Sender<MarketEvent>, event: MarketEvent) {
    if event_tx.try_send(event.clone()).is_ok() {
        return;
    }

    let event_tx = event_tx.clone();
    runtime.spawn(async move {
        if event_tx.send(event).await.is_err() {
            tracing::debug!("feed supervisor control event dropped: engine channel closed");
        }
    });
}

fn set_exchange_connection_state(
    states: &SharedExchangeConnectionStates,
    exchange: Exchange,
    next_state: ExchangeConnectionState,
) {
    let mut guard = write_lock(states);
    guard.insert(exchange, next_state);
}

fn exchange_is_enabled(states: &SharedExchangeConnectionStates, exchange: Exchange) -> bool {
    let guard = read_lock(states);
    guard
        .get(&exchange)
        .map(|state| state.enabled)
        .unwrap_or(true)
}

fn stop_task_group(group: ExchangeTaskGroup) {
    group.cancel_token.cancel();
    for handle in group.handles {
        handle.abort();
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
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
