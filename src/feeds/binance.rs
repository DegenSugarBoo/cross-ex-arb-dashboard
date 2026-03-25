use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use fastwebsockets::{Frame, OpCode};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{ExchangeFeed, backoff_delay_ms};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};
use crate::ws_fast::connect_fast_websocket;

pub struct BinanceFeed;

const BINANCE_STREAMS_PER_CONNECTION: usize = 400;
const BINANCE_BOOK_TICKER_SUFFIX: &str = "@bookTicker";
const BINANCE_MARK_PRICE_SUFFIX: &str = "@markPrice";

#[derive(Debug, Clone, Copy)]
enum BinanceStreamKind {
    BookTicker,
    MarkPrice,
}

impl BinanceStreamKind {
    fn label(self) -> &'static str {
        match self {
            Self::BookTicker => "bookTicker",
            Self::MarkPrice => "markPrice",
        }
    }

    fn stream_suffix(self) -> &'static str {
        match self {
            Self::BookTicker => BINANCE_BOOK_TICKER_SUFFIX,
            Self::MarkPrice => BINANCE_MARK_PRICE_SUFFIX,
        }
    }
}

fn pong_frame_for(frame: &Frame<'_>) -> Frame<'static> {
    Frame::pong(frame.payload.to_vec().into())
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

fn binance_symbol_from_stream(stream: &str) -> Option<String> {
    stream
        .split_once('@')
        .map(|(symbol, _)| symbol.to_ascii_uppercase())
}

fn resolve_symbol_base(
    symbol_map: &HashMap<String, String>,
    exchange_symbol: Option<&str>,
    stream: Option<&str>,
) -> Option<String> {
    if let Some(symbol) = exchange_symbol {
        if let Some(symbol_base) = symbol_map.get(symbol) {
            return Some(symbol_base.clone());
        }

        let normalized = normalize_base(symbol);
        if let Some(symbol_base) = symbol_map.get(&normalized) {
            return Some(symbol_base.clone());
        }

        let lowered = normalized.to_ascii_lowercase();
        if let Some(symbol_base) = symbol_map.get(&lowered) {
            return Some(symbol_base.clone());
        }
    }

    stream
        .and_then(binance_symbol_from_stream)
        .and_then(|symbol| resolve_symbol_base(symbol_map, Some(&symbol), None))
}

fn binance_payload<'a>(root: &'a Value) -> Option<(Option<&'a str>, &'a Value)> {
    let object = root.as_object()?;
    let stream = object.get("stream").and_then(Value::as_str);

    if let Some(data) = object.get("data") {
        if data.is_object() {
            return Some((stream, data));
        }
    }

    Some((stream, root))
}

fn binance_symbol_map(markets: &SymbolMarkets) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (symbol_base, per_exchange) in markets {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Binance)
        {
            let normalized = normalize_base(&meta.exchange_symbol);
            map.insert(meta.exchange_symbol.clone(), symbol_base.clone());
            map.insert(normalized.clone(), symbol_base.clone());
            map.insert(normalized.to_ascii_lowercase(), symbol_base.clone());
        }
    }
    map
}

fn binance_subscription_streams(markets: &SymbolMarkets, kind: BinanceStreamKind) -> Vec<String> {
    let mut streams = Vec::new();
    for per_exchange in markets.values() {
        if let Some(meta) = per_exchange
            .iter()
            .find(|meta| meta.exchange == Exchange::Binance)
        {
            let symbol = normalize_base(&meta.exchange_symbol).to_ascii_lowercase();
            streams.push(format!("{symbol}{}", kind.stream_suffix()));
        }
    }
    streams.sort_unstable();
    streams.dedup();
    streams
}

fn binance_ws_url_with_streams(base_ws_url: &str, streams: &[String]) -> String {
    let separator = if base_ws_url.contains('?') { "&" } else { "?" };
    format!("{base_ws_url}{separator}streams={}", streams.join("/"))
}

pub fn parse_binance_book_ticker_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: Value = sonic_rs::from_str(raw).ok()?;
    let (stream, payload) = binance_payload(&root)?;

    if payload.get("e").and_then(Value::as_str) != Some("bookTicker") {
        return None;
    }

    let exchange_symbol = payload.get("s").and_then(Value::as_str);
    let symbol_base = resolve_symbol_base(symbol_map, exchange_symbol, stream)?;

    let bid_px = payload.get("b").and_then(as_f64)?;
    let bid_qty = payload.get("B").and_then(as_f64)?;
    let ask_px = payload.get("a").and_then(as_f64)?;
    let ask_qty = payload.get("A").and_then(as_f64)?;

    if bid_px <= 0.0 || bid_qty <= 0.0 || ask_px <= 0.0 || ask_qty <= 0.0 {
        return None;
    }

    let exch_ts_ms = payload
        .get("E")
        .or_else(|| payload.get("T"))
        .and_then(as_i64)
        .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Binance,
        symbol_base: symbol_base.into(),
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub fn parse_binance_mark_price_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<FundingUpdate> {
    let root: Value = sonic_rs::from_str(raw).ok()?;
    let (stream, payload) = binance_payload(&root)?;

    if payload.get("e").and_then(Value::as_str) != Some("markPriceUpdate") {
        return None;
    }

    let exchange_symbol = payload.get("s").and_then(Value::as_str);
    let symbol_base = resolve_symbol_base(symbol_map, exchange_symbol, stream)?;
    let funding_rate = payload.get("r").and_then(as_f64)?;

    Some(FundingUpdate {
        exchange: Exchange::Binance,
        symbol_base: symbol_base.into(),
        funding_rate,
        next_funding_ts_ms: payload.get("T").and_then(as_i64),
        recv_ts_ms: payload.get("E").and_then(as_i64).unwrap_or(recv_ts_ms),
        stale_after_ms: None,
    })
}

async fn run_binance_stream_feed(
    ws_url: &str,
    symbol_map: &HashMap<String, String>,
    streams: Vec<String>,
    shard_id: usize,
    kind: BinanceStreamKind,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let stream_ws_url = binance_ws_url_with_streams(ws_url, &streams);
    let mut attempt = 0u32;

    loop {
        tracing::info!(
            shard_id,
            kind = kind.label(),
            subscriptions = streams.len(),
            tracked_symbols = symbol_map.len(),
            "connecting binance WS"
        );

        match connect_fast_websocket(&stream_ws_url).await {
            Ok(mut ws) => {
                tracing::info!(shard_id, kind = kind.label(), "connected binance WS");
                attempt = 0;

                loop {
                    match ws.read_frame().await.context("binance WS read error") {
                        Ok(frame) if frame.opcode == OpCode::Ping => {
                            if let Err(err) = ws.write_frame(pong_frame_for(&frame)).await {
                                tracing::warn!(
                                    error = %err,
                                    shard_id,
                                    kind = kind.label(),
                                    "binance WS pong send failed"
                                );
                                break;
                            }
                            if let Err(err) = ws.flush().await {
                                tracing::warn!(
                                    error = %err,
                                    shard_id,
                                    kind = kind.label(),
                                    "binance WS pong flush failed"
                                );
                                break;
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Pong => {
                            tracing::debug!(
                                payload_len = frame.payload.len(),
                                shard_id,
                                kind = kind.label(),
                                "binance WS pong received"
                            );
                        }
                        Ok(frame) if frame.opcode == OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    tracing::debug!(
                                        error = %err,
                                        shard_id,
                                        kind = kind.label(),
                                        "binance WS text frame was not valid UTF-8"
                                    );
                                    continue;
                                }
                            };
                            let recv_ts_ms = now_ms();

                            match kind {
                                BinanceStreamKind::BookTicker => {
                                    if let Some(update) = parse_binance_book_ticker_message(
                                        text, symbol_map, recv_ts_ms,
                                    ) {
                                        if event_tx.send(MarketEvent::Quote(update)).await.is_err()
                                        {
                                            tracing::info!(
                                                shard_id,
                                                kind = kind.label(),
                                                "binance feed stopped: market event channel closed"
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                                BinanceStreamKind::MarkPrice => {
                                    if let Some(update) = parse_binance_mark_price_message(
                                        text, symbol_map, recv_ts_ms,
                                    ) {
                                        if event_tx
                                            .send(MarketEvent::Funding(update))
                                            .await
                                            .is_err()
                                        {
                                            tracing::info!(
                                                shard_id,
                                                kind = kind.label(),
                                                "binance feed stopped: market event channel closed"
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                        Ok(frame) if frame.opcode == OpCode::Close => {
                            tracing::warn!(
                                shard_id,
                                kind = kind.label(),
                                "binance WS closed by remote"
                            );
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(
                                error = ?err,
                                shard_id,
                                kind = kind.label(),
                                "binance WS message handling failed"
                            );
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    shard_id,
                    kind = kind.label(),
                    "binance WS connect failed"
                );
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

fn spawn_binance_kind_tasks(
    runtime: &Handle,
    ws_url: String,
    symbol_map: HashMap<String, String>,
    streams: Vec<String>,
    kind: BinanceStreamKind,
    event_tx: mpsc::Sender<MarketEvent>,
    cancel_token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    if streams.is_empty() {
        tracing::warn!(
            kind = kind.label(),
            "binance feed skipped: no matched symbols"
        );
        return handles;
    }

    for (shard_id, shard_streams) in streams.chunks(BINANCE_STREAMS_PER_CONNECTION).enumerate() {
        let shard_streams = shard_streams.to_vec();
        let shard_ws_url = ws_url.clone();
        let shard_symbol_map = symbol_map.clone();
        let shard_event_tx = event_tx.clone();
        let shard_cancel = cancel_token.clone();
        tracing::info!(
            shard_id,
            kind = kind.label(),
            subscriptions = shard_streams.len(),
            "prepared binance shard"
        );

        let handle = runtime.spawn(async move {
            tokio::select! {
                _ = shard_cancel.cancelled() => {
                    tracing::info!(shard_id, kind = kind.label(), "binance feed task cancelled");
                }
                result = run_binance_stream_feed(
                    &shard_ws_url,
                    &shard_symbol_map,
                    shard_streams,
                    shard_id,
                    kind,
                    shard_event_tx,
                ) => {
                    if let Err(err) = result {
                        tracing::error!(error = %err, shard_id, kind = kind.label(), "binance feed task terminated");
                    }
                }
            }
        });
        handles.push(handle);
    }

    handles
}

impl ExchangeFeed for BinanceFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn spawn(
        &self,
        runtime: &Handle,
        config: &AppConfig,
        markets: Arc<SymbolMarkets>,
        event_tx: mpsc::Sender<MarketEvent>,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let ws_url = config.binance_ws_url.clone();
        let symbol_map = binance_symbol_map(&markets);
        let quote_streams = binance_subscription_streams(&markets, BinanceStreamKind::BookTicker);
        let funding_streams = binance_subscription_streams(&markets, BinanceStreamKind::MarkPrice);

        let mut handles = Vec::new();
        handles.extend(spawn_binance_kind_tasks(
            runtime,
            ws_url.clone(),
            symbol_map.clone(),
            quote_streams,
            BinanceStreamKind::BookTicker,
            event_tx.clone(),
            cancel_token.clone(),
        ));
        handles.extend(spawn_binance_kind_tasks(
            runtime,
            ws_url,
            symbol_map,
            funding_streams,
            BinanceStreamKind::MarkPrice,
            event_tx,
            cancel_token,
        ));

        handles
    }
}
