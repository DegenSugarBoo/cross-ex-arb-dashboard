use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use fastwebsockets::{Frame, OpCode};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};
use crate::ws_fast::connect_fast_websocket;

const APEX_HEARTBEAT_SECS: u64 = 15;
const APEX_DEPTH_RESYNC_MIN_GAP_MS: i64 = 500;

pub struct ApexFeed;

impl ExchangeFeed for ApexFeed {
    fn exchange(&self) -> Exchange {
        Exchange::ApeX
    }

    fn spawn(
        &self,
        runtime: &Handle,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let ws_url = config.apex_ws_url.clone();
        let depth_rest_url = config.apex_depth_rest_url.clone();
        let timeout_secs = config.http_timeout_secs;
        let feed_markets = Arc::clone(&markets);
        let task = runtime.spawn(async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("apex feed task cancelled");
                }
                result = run_apex_feed(
                    &ws_url,
                    &depth_rest_url,
                    &feed_markets,
                    timeout_secs,
                    event_tx,
                ) => {
                    if let Err(err) = result {
                        tracing::error!(error = %err, "apex feed task terminated");
                    }
                }
            }
        });
        vec![task]
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApexBookState {
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    pub last_update_id: Option<i64>,
    pub last_sync_ms: i64,
    pub needs_resync: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResult {
    Applied,
    NeedsResync,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApexDepthEventKind {
    Snapshot,
    Delta,
}

#[derive(Debug, Clone)]
pub struct ApexDepthEvent {
    pub symbol: String,
    pub kind: ApexDepthEventKind,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub update_id: Option<i64>,
    pub prev_update_id: Option<i64>,
    pub event_ts_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum NumOrString {
    Number(f64),
    String(String),
}

impl NumOrString {
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::String(value) => value.parse::<f64>().ok(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BorrowedNumOrStr<'a> {
    Number(f64),
    String(&'a str),
}

fn de_opt_i64_from_num_or_str<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<BorrowedNumOrStr<'de>>::deserialize(deserializer)?;
    Ok(value.and_then(|entry| match entry {
        BorrowedNumOrStr::Number(num) => Some(num as i64),
        BorrowedNumOrStr::String(text) => text.parse::<i64>().ok(),
    }))
}

#[derive(Debug, Default, Deserialize)]
struct ApexDepthFastData {
    #[serde(default, rename = "s", alias = "symbol", alias = "crossSymbolName")]
    symbol: Option<String>,
    #[serde(default, rename = "b", alias = "bids")]
    bids: Vec<Vec<NumOrString>>,
    #[serde(default, rename = "a", alias = "asks")]
    asks: Vec<Vec<NumOrString>>,
    #[serde(default, rename = "u", deserialize_with = "de_opt_i64_from_num_or_str")]
    update_id: Option<i64>,
    #[serde(
        default,
        rename = "pu",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    prev_update_id: Option<i64>,
    #[serde(
        default,
        rename = "t",
        alias = "ts",
        alias = "timestamp",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    event_ts_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ApexDepthFastRoot {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default, rename = "type", alias = "event", alias = "action")]
    event_type: Option<String>,
    #[serde(default)]
    data: Option<ApexDepthFastData>,
    #[serde(default)]
    d: Option<ApexDepthFastData>,
    #[serde(default)]
    payload: Option<ApexDepthFastData>,
    #[serde(
        default,
        rename = "ts",
        alias = "timestamp",
        deserialize_with = "de_opt_i64_from_num_or_str"
    )]
    event_ts_ms: Option<i64>,
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn normalize_exchange_ts_ms(exch_ts: i64) -> i64 {
    let abs = exch_ts.abs();
    if abs >= 100_000_000_000_000_000 {
        exch_ts / 1_000_000
    } else if abs >= 100_000_000_000_000 {
        exch_ts / 1_000
    } else if abs >= 100_000_000_000 {
        exch_ts
    } else if abs >= 100_000_000 {
        exch_ts * 1_000
    } else {
        exch_ts
    }
}

fn is_apex_depth_topic(topic: &str) -> bool {
    topic.starts_with("orderBook200.H.")
}

fn is_apex_funding_topic(topic: &str) -> bool {
    topic.starts_with("instrumentInfo.H.")
}

fn symbol_from_apex_topic(topic: &str, prefix: &str) -> Option<String> {
    topic.strip_prefix(prefix).map(ToOwned::to_owned)
}

fn parse_depth_level_from_array(level: &[NumOrString]) -> Option<(f64, f64)> {
    if level.len() < 2 {
        return None;
    }
    let px = level.first()?.as_f64()?;
    let qty = level.get(1)?.as_f64()?;
    if px <= 0.0 || qty < 0.0 {
        return None;
    }
    Some((px, qty))
}

fn parse_depth_level_from_value(level: &Value) -> Option<(f64, f64)> {
    match level {
        Value::Array(values) if values.len() >= 2 => {
            let px = as_f64(values.first()?)?;
            let qty = as_f64(values.get(1)?)?;
            if px <= 0.0 || qty < 0.0 {
                return None;
            }
            Some((px, qty))
        }
        Value::Object(map) => {
            let px = map
                .get("price")
                .or_else(|| map.get("p"))
                .or_else(|| map.get("px"))
                .and_then(as_f64)?;
            let qty = map
                .get("size")
                .or_else(|| map.get("q"))
                .or_else(|| map.get("qty"))
                .or_else(|| map.get("s"))
                .and_then(as_f64)?;
            if px <= 0.0 || qty < 0.0 {
                return None;
            }
            Some((px, qty))
        }
        _ => None,
    }
}

fn parse_depth_levels_from_value(value: Option<&Value>) -> Vec<(f64, f64)> {
    value
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(parse_depth_level_from_value)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_depth_levels_from_fast(levels: &[Vec<NumOrString>]) -> Vec<(f64, f64)> {
    levels
        .iter()
        .filter_map(|level| parse_depth_level_from_array(level))
        .collect()
}

fn compact_apex_rest_symbol(symbol: &str) -> String {
    let upper = normalize_base(symbol);
    for quote in ["USDT", "USD"] {
        for separator in ["-", "_", "/"] {
            let suffix = format!("{separator}{quote}");
            if let Some(base) = upper.strip_suffix(&suffix) {
                return format!("{base}{quote}");
            }
        }
    }
    upper
}

pub fn apex_symbol_map(markets: &SymbolMarkets) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (symbol_base, per_exchange) in markets {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::ApeX)
        {
            map.insert(meta.exchange_symbol.clone(), symbol_base.clone());
            map.insert(normalize_base(&meta.exchange_symbol), symbol_base.clone());
            map.insert(
                compact_apex_rest_symbol(&meta.exchange_symbol),
                symbol_base.clone(),
            );
        }
    }
    map
}

fn apex_exchange_symbols(markets: &SymbolMarkets) -> Vec<String> {
    let mut symbols = Vec::new();
    for per_exchange in markets.values() {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::ApeX)
        {
            symbols.push(compact_apex_rest_symbol(&meta.exchange_symbol));
        }
    }
    symbols.sort_unstable();
    symbols.dedup();
    symbols
}

fn resolve_symbol_base(
    symbol_map: &HashMap<String, String>,
    exchange_symbol: &str,
) -> Option<String> {
    if let Some(symbol_base) = symbol_map.get(exchange_symbol) {
        return Some(symbol_base.clone());
    }

    let normalized = normalize_base(exchange_symbol);
    symbol_map.get(&normalized).cloned().or_else(|| {
        symbol_map
            .get(&compact_apex_rest_symbol(exchange_symbol))
            .cloned()
    })
}

pub fn apex_depth_subscribe_payload(symbol: &str) -> String {
    format!("{{\"op\":\"subscribe\",\"args\":[\"orderBook200.H.{symbol}\"]}}")
}

pub fn apex_funding_subscribe_payload(symbol: &str) -> String {
    format!("{{\"op\":\"subscribe\",\"args\":[\"instrumentInfo.H.{symbol}\"]}}")
}

fn apex_ping_payload() -> &'static str {
    "{\"op\":\"ping\"}"
}

fn apex_pong_payload() -> &'static str {
    "{\"op\":\"pong\"}"
}

fn is_server_ping_message(raw: &str) -> bool {
    (raw.contains("\"op\":\"ping\"")
        || raw.contains("\"event\":\"ping\"")
        || raw.trim().eq_ignore_ascii_case("ping"))
        && !raw.contains("\"pong\"")
}

fn is_server_pong_message(raw: &str) -> bool {
    raw.contains("\"op\":\"pong\"")
        || raw.contains("\"event\":\"pong\"")
        || raw.trim().eq_ignore_ascii_case("pong")
}

#[cfg(test)]
mod tests {
    use super::apex_exchange_symbols;
    use crate::discovery::SymbolMarkets;
    use crate::model::{Exchange, MarketMeta};

    #[test]
    fn apex_ws_subscriptions_use_compact_symbols() {
        let mut markets = SymbolMarkets::new();
        markets.insert(
            "BTC".to_owned(),
            vec![MarketMeta {
                exchange: Exchange::ApeX,
                symbol_base: "BTC".to_owned(),
                exchange_symbol: "BTC-USDT".to_owned(),
                market_id: None,
                taker_fee_pct: 0.05,
                maker_fee_pct: 0.0,
            }],
        );

        assert_eq!(apex_exchange_symbols(&markets), vec!["BTCUSDT".to_owned()]);
    }
}

pub fn parse_apex_depth_event_fast(raw: &str, recv_ts_ms: i64) -> Option<ApexDepthEvent> {
    let root: ApexDepthFastRoot = sonic_rs::from_str(raw).ok()?;
    let topic = root.topic.as_deref().or(root.channel.as_deref())?;
    if !is_apex_depth_topic(topic) {
        return None;
    }

    let payload = root.data.or(root.d).or(root.payload)?;
    let symbol = payload
        .symbol
        .or_else(|| symbol_from_apex_topic(topic, "orderBook200.H."))?;

    let bids = parse_depth_levels_from_fast(&payload.bids);
    let asks = parse_depth_levels_from_fast(&payload.asks);
    if bids.is_empty() && asks.is_empty() {
        return None;
    }

    let event_ts_ms = payload
        .event_ts_ms
        .or(root.event_ts_ms)
        .map(normalize_exchange_ts_ms)
        .unwrap_or(recv_ts_ms);
    let kind = if root
        .event_type
        .as_deref()
        .map(|kind| kind.eq_ignore_ascii_case("snapshot"))
        .unwrap_or(false)
    {
        ApexDepthEventKind::Snapshot
    } else {
        ApexDepthEventKind::Delta
    };

    Some(ApexDepthEvent {
        symbol,
        kind,
        bids,
        asks,
        update_id: payload.update_id,
        prev_update_id: payload.prev_update_id,
        event_ts_ms,
    })
}

pub fn parse_apex_depth_event_fallback(raw: &str, recv_ts_ms: i64) -> Option<ApexDepthEvent> {
    let root: Value = sonic_rs::from_str(raw).ok()?;
    let topic = root
        .get("topic")
        .or_else(|| root.get("channel"))
        .and_then(Value::as_str)?;
    if !is_apex_depth_topic(topic) {
        return None;
    }

    let payload = root
        .get("data")
        .or_else(|| root.get("d"))
        .or_else(|| root.get("payload"))
        .unwrap_or(&root);
    let payload = payload
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(payload);

    let symbol = payload
        .get("s")
        .or_else(|| payload.get("symbol"))
        .or_else(|| payload.get("crossSymbolName"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| symbol_from_apex_topic(topic, "orderBook200.H."))?;

    let bids = parse_depth_levels_from_value(payload.get("b").or_else(|| payload.get("bids")));
    let asks = parse_depth_levels_from_value(payload.get("a").or_else(|| payload.get("asks")));
    if bids.is_empty() && asks.is_empty() {
        return None;
    }

    let event_ts_ms = payload
        .get("t")
        .or_else(|| payload.get("ts"))
        .or_else(|| payload.get("timestamp"))
        .or_else(|| root.get("ts"))
        .or_else(|| root.get("timestamp"))
        .and_then(as_i64)
        .map(normalize_exchange_ts_ms)
        .unwrap_or(recv_ts_ms);

    let kind = payload
        .get("type")
        .or_else(|| root.get("type"))
        .or_else(|| root.get("event"))
        .or_else(|| root.get("action"))
        .and_then(Value::as_str)
        .map(|kind| {
            if kind.eq_ignore_ascii_case("snapshot") {
                ApexDepthEventKind::Snapshot
            } else {
                ApexDepthEventKind::Delta
            }
        })
        .unwrap_or(ApexDepthEventKind::Delta);

    Some(ApexDepthEvent {
        symbol,
        kind,
        bids,
        asks,
        update_id: payload
            .get("u")
            .or_else(|| payload.get("updateId"))
            .or_else(|| payload.get("sequence"))
            .and_then(as_i64),
        prev_update_id: payload
            .get("pu")
            .or_else(|| payload.get("prevUpdateId"))
            .and_then(as_i64),
        event_ts_ms,
    })
}

pub fn parse_apex_depth_event(raw: &str, recv_ts_ms: i64) -> Option<ApexDepthEvent> {
    parse_apex_depth_event_fast(raw, recv_ts_ms)
        .or_else(|| parse_apex_depth_event_fallback(raw, recv_ts_ms))
}

fn best_bid_ask(state: &ApexBookState) -> Option<(f64, f64, f64, f64)> {
    let best_bid = state
        .bids
        .iter()
        .rev()
        .find(|(_, qty)| **qty > 0.0)
        .map(|(px, qty)| (px.into_inner(), *qty))?;
    let best_ask = state
        .asks
        .iter()
        .find(|(_, qty)| **qty > 0.0)
        .map(|(px, qty)| (px.into_inner(), *qty))?;

    Some((best_bid.0, best_bid.1, best_ask.0, best_ask.1))
}

pub fn apply_apex_snapshot(state: &mut ApexBookState, event: &ApexDepthEvent) -> bool {
    state.bids.clear();
    state.asks.clear();

    for (price, size) in &event.bids {
        if *size > 0.0 {
            state.bids.insert(OrderedFloat(*price), *size);
        }
    }
    for (price, size) in &event.asks {
        if *size > 0.0 {
            state.asks.insert(OrderedFloat(*price), *size);
        }
    }

    state.last_update_id = event.update_id;
    state.last_sync_ms = event.event_ts_ms;
    state.needs_resync = state.bids.is_empty() || state.asks.is_empty();

    if let Some((bid_px, _, ask_px, _)) = best_bid_ask(state) {
        if bid_px >= ask_px {
            state.needs_resync = true;
        }
    }

    !state.needs_resync
}

pub fn apply_apex_delta(state: &mut ApexBookState, event: &ApexDepthEvent) -> ApplyResult {
    let Some(last_update_id) = state.last_update_id else {
        state.needs_resync = true;
        return ApplyResult::NeedsResync;
    };

    let Some(update_id) = event.update_id else {
        state.needs_resync = true;
        return ApplyResult::NeedsResync;
    };

    if update_id <= last_update_id {
        state.needs_resync = true;
        return ApplyResult::NeedsResync;
    }

    if let Some(prev_update_id) = event.prev_update_id {
        if prev_update_id != last_update_id {
            state.needs_resync = true;
            return ApplyResult::NeedsResync;
        }
    }

    for (price, size) in &event.bids {
        let key = OrderedFloat(*price);
        if *size == 0.0 {
            state.bids.remove(&key);
        } else if *size > 0.0 {
            state.bids.insert(key, *size);
        }
    }
    for (price, size) in &event.asks {
        let key = OrderedFloat(*price);
        if *size == 0.0 {
            state.asks.remove(&key);
        } else if *size > 0.0 {
            state.asks.insert(key, *size);
        }
    }

    state.last_update_id = Some(update_id);
    state.last_sync_ms = event.event_ts_ms;
    state.needs_resync = false;

    let Some((bid_px, _, ask_px, _)) = best_bid_ask(state) else {
        state.needs_resync = true;
        return ApplyResult::NeedsResync;
    };

    if bid_px >= ask_px {
        state.needs_resync = true;
        return ApplyResult::NeedsResync;
    }

    ApplyResult::Applied
}

pub fn best_quote_from_apex_state(
    state: &ApexBookState,
    symbol_base: &str,
    exch_ts_ms: i64,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let (bid_px, bid_qty, ask_px, ask_qty) = best_bid_ask(state)?;
    if bid_px <= 0.0 || ask_px <= 0.0 || bid_qty <= 0.0 || ask_qty <= 0.0 || bid_px >= ask_px {
        return None;
    }

    Some(QuoteUpdate {
        exchange: Exchange::ApeX,
        symbol_base: symbol_base.into(),
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub async fn resync_apex_symbol_depth(
    client: &reqwest::Client,
    depth_rest_url: &str,
    symbol: &str,
    state: &mut ApexBookState,
) -> anyhow::Result<()> {
    let response = client
        .get(depth_rest_url)
        .query(&[
            ("symbol", compact_apex_rest_symbol(symbol)),
            ("limit", "200".to_owned()),
        ])
        .send()
        .await
        .context("apex depth resync request failed")?
        .error_for_status()
        .context("apex depth resync non-success HTTP status")?
        .json::<Value>()
        .await
        .context("apex depth resync parse failed")?;

    let payload = response.get("data").unwrap_or(&response);
    let bids = parse_depth_levels_from_value(payload.get("b").or_else(|| payload.get("bids")));
    let asks = parse_depth_levels_from_value(payload.get("a").or_else(|| payload.get("asks")));

    let mut next_bids = BTreeMap::new();
    let mut next_asks = BTreeMap::new();
    for (price, size) in bids {
        if size > 0.0 {
            next_bids.insert(OrderedFloat(price), size);
        }
    }
    for (price, size) in asks {
        if size > 0.0 {
            next_asks.insert(OrderedFloat(price), size);
        }
    }

    if next_bids.is_empty() || next_asks.is_empty() {
        bail!("apex depth resync returned empty side");
    }

    let best_bid_px = next_bids
        .iter()
        .next_back()
        .map(|(px, _)| px.into_inner())
        .context("apex depth resync missing best bid")?;
    let best_ask_px = next_asks
        .iter()
        .next()
        .map(|(px, _)| px.into_inner())
        .context("apex depth resync missing best ask")?;
    if best_bid_px >= best_ask_px {
        bail!("apex depth resync returned crossed book");
    }

    state.bids = next_bids;
    state.asks = next_asks;
    state.last_update_id = payload
        .get("u")
        .or_else(|| payload.get("updateId"))
        .or_else(|| payload.get("sequence"))
        .and_then(as_i64);
    state.last_sync_ms = now_ms();
    state.needs_resync = false;
    Ok(())
}

pub fn parse_apex_instrument_info_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<FundingUpdate> {
    let root: Value = sonic_rs::from_str(raw).ok()?;
    let topic = root
        .get("topic")
        .or_else(|| root.get("channel"))
        .and_then(Value::as_str)?;
    if !is_apex_funding_topic(topic) {
        return None;
    }

    let payload = root
        .get("data")
        .or_else(|| root.get("d"))
        .or_else(|| root.get("payload"))
        .unwrap_or(&root);
    let payload = payload
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(payload);

    let exchange_symbol = payload
        .get("s")
        .or_else(|| payload.get("symbol"))
        .or_else(|| payload.get("crossSymbolName"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| symbol_from_apex_topic(topic, "instrumentInfo.H."))?;
    let symbol_base = resolve_symbol_base(symbol_map, &exchange_symbol)?;

    let funding_rate = payload
        .get("fundingRate")
        .or_else(|| payload.get("funding_rate"))
        .or_else(|| payload.get("r"))
        .and_then(as_f64)?;

    let next_funding_ts_ms = payload
        .get("nextFundingTime")
        .or_else(|| payload.get("nextFundingTimestamp"))
        .or_else(|| payload.get("nextFundingTs"))
        .and_then(as_i64)
        .map(normalize_exchange_ts_ms);
    let event_ts_ms = payload
        .get("t")
        .or_else(|| payload.get("ts"))
        .or_else(|| payload.get("timestamp"))
        .or_else(|| payload.get("E"))
        .or_else(|| root.get("ts"))
        .or_else(|| root.get("timestamp"))
        .and_then(as_i64)
        .map(normalize_exchange_ts_ms)
        .unwrap_or(recv_ts_ms);

    Some(FundingUpdate {
        exchange: Exchange::ApeX,
        symbol_base: symbol_base.into(),
        funding_rate,
        next_funding_ts_ms,
        recv_ts_ms: event_ts_ms,
        stale_after_ms: None,
    })
}

pub async fn run_apex_feed(
    ws_url: &str,
    depth_rest_url: &str,
    markets: &SymbolMarkets,
    timeout_secs: u64,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map = apex_symbol_map(markets);
    let exchange_symbols = apex_exchange_symbols(markets);
    if exchange_symbols.is_empty() {
        tracing::warn!("apex feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let depth_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .context("failed to build apex depth HTTP client")?;

    let mut attempt = 0u32;
    let mut funding_cache: HashMap<String, f64> = HashMap::new();

    loop {
        tracing::info!(
            attempt,
            subscriptions = exchange_symbols.len(),
            "connecting apex WS"
        );

        match connect_fast_websocket(ws_url).await {
            Ok(mut ws) => {
                tracing::info!("connected apex WS");
                attempt = 0;

                let mut subscribe_failed = false;
                for symbol in &exchange_symbols {
                    let depth_payload = apex_depth_subscribe_payload(symbol);
                    if let Err(err) = ws
                        .write_frame(Frame::text(depth_payload.into_bytes().into()))
                        .await
                    {
                        tracing::warn!(error = %err, symbol, "apex depth subscribe failed");
                        subscribe_failed = true;
                        break;
                    }

                    let funding_payload = apex_funding_subscribe_payload(symbol);
                    if let Err(err) = ws
                        .write_frame(Frame::text(funding_payload.into_bytes().into()))
                        .await
                    {
                        tracing::warn!(error = %err, symbol, "apex funding subscribe failed");
                        subscribe_failed = true;
                        break;
                    }
                }

                if !subscribe_failed {
                    if let Err(err) = ws.flush().await {
                        tracing::warn!(error = %err, "apex subscribe flush failed");
                        subscribe_failed = true;
                    }
                }

                if subscribe_failed {
                    let delay_ms = backoff_delay_ms(attempt);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }

                let mut heartbeat = tokio::time::interval(Duration::from_secs(APEX_HEARTBEAT_SECS));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                heartbeat.tick().await;

                let mut depth_states: HashMap<String, ApexBookState> = HashMap::new();
                let mut last_resync_attempt_ms: HashMap<String, i64> = HashMap::new();

                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if let Err(err) = ws
                                .write_frame(Frame::text(apex_ping_payload().as_bytes().to_vec().into()))
                                .await
                            {
                                tracing::warn!(error = %err, "apex ping send failed");
                                break;
                            }
                            if let Err(err) = ws.flush().await {
                                tracing::warn!(error = %err, "apex ping flush failed");
                                break;
                            }
                        }
                        frame_result = ws.read_frame() => {
                            match frame_result.context("apex WS read error") {
                                Ok(frame) if frame.opcode == OpCode::Text => {
                                    let raw = match std::str::from_utf8(&frame.payload) {
                                        Ok(text) => text,
                                        Err(err) => {
                                            tracing::debug!(error = %err, "apex WS text frame was not valid UTF-8");
                                            continue;
                                        }
                                    };

                                    if is_server_ping_message(raw) {
                                        if let Err(err) = ws
                                            .write_frame(Frame::text(apex_pong_payload().as_bytes().to_vec().into()))
                                            .await
                                        {
                                            tracing::warn!(error = %err, "apex pong send failed");
                                            break;
                                        }
                                        continue;
                                    }
                                    if is_server_pong_message(raw) {
                                        continue;
                                    }

                                    let recv_ts_ms = now_ms();

                                    if let Some(depth_event) = parse_apex_depth_event(raw, recv_ts_ms) {
                                        let symbol_key = normalize_base(&depth_event.symbol);
                                        let Some(symbol_base) = resolve_symbol_base(&symbol_map, &depth_event.symbol) else {
                                            continue;
                                        };

                                        let state = depth_states.entry(symbol_key.clone()).or_default();
                                        let mut applied = false;
                                        match depth_event.kind {
                                            ApexDepthEventKind::Snapshot => {
                                                applied = apply_apex_snapshot(state, &depth_event);
                                            }
                                            ApexDepthEventKind::Delta => {
                                                match apply_apex_delta(state, &depth_event) {
                                                    ApplyResult::Applied => {
                                                        applied = true;
                                                    }
                                                    ApplyResult::NeedsResync | ApplyResult::Ignored => {}
                                                }
                                            }
                                        }

                                        if state.needs_resync {
                                            let now = now_ms();
                                            let should_attempt = last_resync_attempt_ms
                                                .get(&symbol_key)
                                                .map(|last| now.saturating_sub(*last) >= APEX_DEPTH_RESYNC_MIN_GAP_MS)
                                                .unwrap_or(true);
                                            if should_attempt {
                                                last_resync_attempt_ms.insert(symbol_key.clone(), now);
                                                match resync_apex_symbol_depth(
                                                    &depth_client,
                                                    depth_rest_url,
                                                    &depth_event.symbol,
                                                    state,
                                                )
                                                .await
                                                {
                                                    Ok(_) => {
                                                        applied = true;
                                                    }
                                                    Err(err) => {
                                                        tracing::warn!(
                                                            error = %err,
                                                            symbol = %depth_event.symbol,
                                                            "apex depth resync failed"
                                                        );
                                                    }
                                                }
                                            }
                                        }

                                        if applied {
                                            let exch_ts_ms = state.last_sync_ms.max(depth_event.event_ts_ms);
                                            if let Some(quote) = best_quote_from_apex_state(
                                                state,
                                                &symbol_base,
                                                exch_ts_ms,
                                                recv_ts_ms,
                                            ) {
                                                if event_tx.send(MarketEvent::Quote(quote)).await.is_err() {
                                                    tracing::info!("apex feed stopped: market event channel closed");
                                                    return Ok(());
                                                }
                                            } else {
                                                state.needs_resync = true;
                                            }
                                        }
                                    }

                                    if let Some(funding) = parse_apex_instrument_info_message(
                                        raw,
                                        &symbol_map,
                                        recv_ts_ms,
                                    ) {
                                        let symbol = funding.symbol_base.to_string();
                                        let should_emit = funding_cache
                                            .get(&symbol)
                                            .map(|prev| {
                                                (funding.funding_rate - prev).abs() > FUNDING_CHANGE_EPSILON
                                            })
                                            .unwrap_or(true);
                                        if should_emit {
                                            funding_cache.insert(symbol, funding.funding_rate);
                                            if event_tx.send(MarketEvent::Funding(funding)).await.is_err() {
                                                tracing::info!("apex feed stopped: market event channel closed");
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                                Ok(frame) if frame.opcode == OpCode::Close => {
                                    tracing::warn!("apex WS closed by remote");
                                    break;
                                }
                                Ok(_) => {}
                                Err(err) => {
                                    tracing::warn!(error = %err, "apex WS message handling failed");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "apex WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}
