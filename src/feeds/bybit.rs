use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use fastwebsockets::{Frame, OpCode};
use serde_json::{Value, json};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};
use crate::ws_fast::connect_fast_websocket;

pub struct BybitFeed;

const BYBIT_HEARTBEAT_SECS: u64 = 20;
const BYBIT_SUBSCRIPTIONS_PER_REQUEST: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitTickerKind {
    Snapshot,
    Delta,
}

#[derive(Debug, Clone, Default)]
pub struct BybitTickerState {
    pub bid_px: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_px: Option<f64>,
    pub ask_qty: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_ts_ms: Option<i64>,
    pub last_exchange_ts_ms: Option<i64>,
    pub last_emitted_funding: Option<(f64, Option<i64>)>,
}

impl BybitTickerState {
    fn clear(&mut self) {
        *self = Self::default();
    }

    fn apply_message(
        &mut self,
        message: &BybitTickerPatch,
        symbol_base: &str,
        recv_ts_ms: i64,
    ) -> (Option<QuoteUpdate>, Option<FundingUpdate>) {
        if matches!(message.kind, BybitTickerKind::Snapshot) {
            self.clear();
        }

        let mut quote_touched = matches!(message.kind, BybitTickerKind::Snapshot);
        let mut funding_touched = matches!(message.kind, BybitTickerKind::Snapshot);

        if message.bid_px.is_some() {
            self.bid_px = message.bid_px;
            quote_touched = true;
        }
        if message.bid_qty.is_some() {
            self.bid_qty = message.bid_qty;
            quote_touched = true;
        }
        if message.ask_px.is_some() {
            self.ask_px = message.ask_px;
            quote_touched = true;
        }
        if message.ask_qty.is_some() {
            self.ask_qty = message.ask_qty;
            quote_touched = true;
        }
        if message.funding_rate.is_some() {
            self.funding_rate = message.funding_rate;
            funding_touched = true;
        }
        if message.next_funding_ts_ms.is_some() {
            self.next_funding_ts_ms = message.next_funding_ts_ms;
            funding_touched = true;
        }
        if message.exch_ts_ms > 0 {
            self.last_exchange_ts_ms = Some(message.exch_ts_ms);
        }

        let exch_ts_ms = self.last_exchange_ts_ms.unwrap_or(message.exch_ts_ms);

        let quote = if quote_touched {
            match (self.bid_px, self.bid_qty, self.ask_px, self.ask_qty) {
                (Some(bid_px), Some(bid_qty), Some(ask_px), Some(ask_qty))
                    if bid_px > 0.0
                        && ask_px > 0.0
                        && bid_qty > 0.0
                        && ask_qty > 0.0
                        && bid_px < ask_px =>
                {
                    Some(QuoteUpdate {
                        exchange: Exchange::Bybit,
                        symbol_base: symbol_base.into(),
                        bid_px,
                        bid_qty,
                        ask_px,
                        ask_qty,
                        exch_ts_ms,
                        recv_ts_ms,
                    })
                }
                _ => None,
            }
        } else {
            None
        };

        let funding = if funding_touched {
            match self.funding_rate {
                Some(funding_rate) => {
                    let next_funding_ts_ms = self.next_funding_ts_ms;
                    let should_emit = self
                        .last_emitted_funding
                        .map(|(prev_rate, prev_next)| {
                            (prev_rate - funding_rate).abs() > FUNDING_CHANGE_EPSILON
                                || prev_next != next_funding_ts_ms
                        })
                        .unwrap_or(true);

                    if should_emit {
                        self.last_emitted_funding = Some((funding_rate, next_funding_ts_ms));
                        Some(FundingUpdate {
                            exchange: Exchange::Bybit,
                            symbol_base: symbol_base.into(),
                            funding_rate,
                            next_funding_ts_ms,
                            recv_ts_ms: exch_ts_ms,
                            stale_after_ms: None,
                        })
                    } else {
                        None
                    }
                }
                None => None,
            }
        } else {
            None
        };

        (quote, funding)
    }
}

impl ExchangeFeed for BybitFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    fn spawn(
        &self,
        runtime: &Handle,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let ws_url = config.bybit_ws_url.clone();
        let feed_markets = Arc::clone(&markets);
        let task = runtime.spawn(async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("bybit feed task cancelled");
                }
                result = run_bybit_feed(&ws_url, &feed_markets, event_tx) => {
                    if let Err(err) = result {
                        tracing::error!(error = %err, "bybit feed task terminated");
                    }
                }
            }
        });
        vec![task]
    }
}

#[derive(Debug, Clone)]
pub struct BybitTickerPatch {
    kind: BybitTickerKind,
    symbol: String,
    bid_px: Option<f64>,
    bid_qty: Option<f64>,
    ask_px: Option<f64>,
    ask_qty: Option<f64>,
    funding_rate: Option<f64>,
    next_funding_ts_ms: Option<i64>,
    exch_ts_ms: i64,
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

fn first_object(value: &Value) -> Option<&Value> {
    match value {
        Value::Object(_) => Some(value),
        Value::Array(items) => items.iter().find(|item| item.is_object()),
        _ => None,
    }
}

fn extract_str<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn extract_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(parsed) = value.get(*key).and_then(as_f64) {
            return Some(parsed);
        }
    }
    None
}

fn extract_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(parsed) = value.get(*key).and_then(as_i64) {
            return Some(parsed);
        }
    }
    None
}

fn is_bybit_ticker_topic(topic: &str) -> bool {
    topic.starts_with("tickers.")
}

fn symbol_from_topic(topic: &str) -> Option<String> {
    topic.strip_prefix("tickers.").map(ToOwned::to_owned)
}

fn resolve_symbol_base(
    symbol_map: &HashMap<String, String>,
    exchange_symbol: &str,
) -> Option<String> {
    if let Some(symbol_base) = symbol_map.get(exchange_symbol) {
        return Some(symbol_base.clone());
    }

    let normalized = normalize_base(exchange_symbol);
    symbol_map.get(&normalized).cloned()
}

fn bybit_ticker_payload_from_value(
    topic: &str,
    kind: BybitTickerKind,
    value: &Value,
    recv_ts_ms: i64,
) -> Option<BybitTickerPatch> {
    let symbol = extract_str(value, &["symbol", "s"])
        .map(ToOwned::to_owned)
        .or_else(|| symbol_from_topic(topic))?;

    let bid_px = extract_f64(value, &["bid1Price", "bidPrice", "bestBidPrice", "b"]);
    let bid_qty = extract_f64(value, &["bid1Size", "bidSize", "bestBidSize", "B"]);
    let ask_px = extract_f64(value, &["ask1Price", "askPrice", "bestAskPrice", "a"]);
    let ask_qty = extract_f64(value, &["ask1Size", "askSize", "bestAskSize", "A"]);
    let funding_rate = extract_f64(value, &["fundingRate", "fr", "predictedFundingRate"]);
    let next_funding_ts_ms =
        extract_i64(value, &["nextFundingTime", "nextFundingTs", "fundingTime"]);
    let exch_ts_ms = extract_i64(value, &["ts", "timestamp", "updatedTime", "eventTime"])
        .map(normalize_exchange_ts_ms)
        .unwrap_or(recv_ts_ms);

    if bid_px.is_none()
        && bid_qty.is_none()
        && ask_px.is_none()
        && ask_qty.is_none()
        && funding_rate.is_none()
        && next_funding_ts_ms.is_none()
    {
        return None;
    }

    Some(BybitTickerPatch {
        kind,
        symbol,
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        funding_rate,
        next_funding_ts_ms,
        exch_ts_ms,
    })
}

pub fn bybit_subscribe_payload(topics: &[String], request_id: u64) -> String {
    json!({
        "op": "subscribe",
        "args": topics,
        "req_id": request_id,
    })
    .to_string()
}

pub fn bybit_ping_payload() -> &'static str {
    "{\"op\":\"ping\"}"
}

pub fn bybit_pong_payload() -> &'static str {
    "{\"op\":\"pong\"}"
}

fn pong_frame_for(frame: &Frame<'_>) -> Frame<'static> {
    Frame::pong(frame.payload.to_vec().into())
}

pub fn bybit_symbol_map(markets: &SymbolMarkets) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (symbol_base, per_exchange) in markets {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Bybit)
        {
            map.insert(meta.exchange_symbol.clone(), symbol_base.clone());
            map.insert(normalize_base(&meta.exchange_symbol), symbol_base.clone());
        }
    }
    map
}

pub fn bybit_subscription_topics(markets: &SymbolMarkets) -> Vec<String> {
    let mut topics = Vec::new();
    for per_exchange in markets.values() {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Bybit)
        {
            topics.push(format!("tickers.{}", meta.exchange_symbol));
        }
    }
    topics.sort_unstable();
    topics.dedup();
    topics
}

pub fn parse_bybit_ticker_message(raw: &str, recv_ts_ms: i64) -> Option<BybitTickerPatch> {
    let root: Value = sonic_rs::from_str(raw).ok()?;
    let topic = root
        .get("topic")
        .or_else(|| root.get("channel"))
        .and_then(Value::as_str)?;
    if !is_bybit_ticker_topic(topic) {
        return None;
    }

    let kind = root
        .get("type")
        .and_then(Value::as_str)
        .map(|value| {
            if value.eq_ignore_ascii_case("snapshot") {
                BybitTickerKind::Snapshot
            } else {
                BybitTickerKind::Delta
            }
        })
        .unwrap_or(BybitTickerKind::Delta);

    let data = root
        .get("data")
        .or_else(|| root.get("result"))
        .or_else(|| root.get("payload"))?;
    let item = first_object(data)?;

    bybit_ticker_payload_from_value(topic, kind, item, recv_ts_ms)
}

pub fn apply_bybit_ticker_patch(
    state: &mut BybitTickerState,
    message: &BybitTickerPatch,
    symbol_base: &str,
    recv_ts_ms: i64,
) -> (Option<QuoteUpdate>, Option<FundingUpdate>) {
    state.apply_message(message, symbol_base, recv_ts_ms)
}

fn bybit_is_text_ping(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed.eq_ignore_ascii_case("ping")
        || raw.contains("\"op\":\"ping\"")
        || raw.contains("\"event\":\"ping\"")
}

fn bybit_is_text_pong(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed.eq_ignore_ascii_case("pong")
        || raw.contains("\"op\":\"pong\"")
        || raw.contains("\"event\":\"pong\"")
}

pub async fn run_bybit_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map = bybit_symbol_map(markets);
    let topics = bybit_subscription_topics(markets);
    if topics.is_empty() {
        tracing::warn!("bybit feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;

    loop {
        tracing::info!(
            attempt,
            subscriptions = topics.len(),
            tracked_symbols = symbol_map.len(),
            "connecting bybit WS"
        );

        match connect_fast_websocket(ws_url).await {
            Ok(mut ws) => {
                tracing::info!("connected bybit WS");
                attempt = 0;

                let mut subscribe_failed = false;
                for (idx, chunk) in topics.chunks(BYBIT_SUBSCRIPTIONS_PER_REQUEST).enumerate() {
                    let payload = bybit_subscribe_payload(chunk, idx as u64 + 1);
                    if let Err(err) = ws
                        .write_frame(Frame::text(payload.into_bytes().into()))
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            shard = idx,
                            subscriptions = chunk.len(),
                            "bybit subscribe failed"
                        );
                        subscribe_failed = true;
                        break;
                    }
                }

                if !subscribe_failed {
                    if let Err(err) = ws.flush().await {
                        tracing::warn!(error = %err, "bybit subscribe flush failed");
                        subscribe_failed = true;
                    }
                }

                if subscribe_failed {
                    let delay_ms = backoff_delay_ms(attempt);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }

                let mut heartbeat =
                    tokio::time::interval(Duration::from_secs(BYBIT_HEARTBEAT_SECS));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                heartbeat.tick().await;

                let mut ticker_states: HashMap<String, BybitTickerState> = HashMap::new();

                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if let Err(err) = ws
                                .write_frame(Frame::text(bybit_ping_payload().as_bytes().to_vec().into()))
                                .await
                            {
                                tracing::warn!(error = %err, "bybit ping send failed");
                                break;
                            }
                            if let Err(err) = ws.flush().await {
                                tracing::warn!(error = %err, "bybit ping flush failed");
                                break;
                            }
                        }
                        frame_result = ws.read_frame() => {
                            match frame_result.context("bybit WS read error") {
                                Ok(frame) if frame.opcode == OpCode::Ping => {
                                    if let Err(err) = ws.write_frame(pong_frame_for(&frame)).await {
                                        tracing::warn!(error = %err, "bybit WS pong send failed");
                                        break;
                                    }
                                    if let Err(err) = ws.flush().await {
                                        tracing::warn!(error = %err, "bybit WS pong flush failed");
                                        break;
                                    }
                                }
                                Ok(frame) if frame.opcode == OpCode::Pong => {
                                    tracing::debug!(payload_len = frame.payload.len(), "bybit WS pong received");
                                }
                                Ok(frame) if frame.opcode == OpCode::Text => {
                                    let raw = match std::str::from_utf8(&frame.payload) {
                                        Ok(text) => text,
                                        Err(err) => {
                                            tracing::debug!(error = %err, "bybit WS text frame was not valid UTF-8");
                                            continue;
                                        }
                                    };

                                    if bybit_is_text_ping(raw) {
                                        if let Err(err) = ws
                                            .write_frame(Frame::text(bybit_pong_payload().as_bytes().to_vec().into()))
                                            .await
                                        {
                                            tracing::warn!(error = %err, "bybit text pong send failed");
                                            break;
                                        }
                                        if let Err(err) = ws.flush().await {
                                            tracing::warn!(error = %err, "bybit text pong flush failed");
                                            break;
                                        }
                                        continue;
                                    }

                                    if bybit_is_text_pong(raw) {
                                        continue;
                                    }

                                    let recv_ts_ms = now_ms();
                                    let Some(message) = parse_bybit_ticker_message(raw, recv_ts_ms) else {
                                        continue;
                                    };

                                    let Some(symbol_base) =
                                        resolve_symbol_base(&symbol_map, &message.symbol)
                                    else {
                                        continue;
                                    };

                                    let state = ticker_states.entry(message.symbol.clone()).or_default();
                                    let (quote, funding) = state.apply_message(&message, &symbol_base, recv_ts_ms);

                                    if let Some(quote) = quote {
                                        if event_tx.send(MarketEvent::Quote(quote)).await.is_err() {
                                            tracing::info!("bybit feed stopped: market event channel closed");
                                            return Ok(());
                                        }
                                    }

                                    if let Some(funding) = funding {
                                        if event_tx.send(MarketEvent::Funding(funding)).await.is_err() {
                                            tracing::info!("bybit feed stopped: market event channel closed");
                                            return Ok(());
                                        }
                                    }
                                }
                                Ok(frame) if frame.opcode == OpCode::Close => {
                                    tracing::warn!("bybit WS closed by remote");
                                    break;
                                }
                                Ok(_) => {}
                                Err(err) => {
                                    tracing::warn!(error = %err, "bybit WS message handling failed");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "bybit WS connect failed");
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}
