use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{ACCEPT, ACCEPT_LANGUAGE, ORIGIN, USER_AGENT};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::AppConfig;
use crate::discovery::{SymbolMarkets, normalize_base};
use crate::feeds::{ExchangeFeed, FUNDING_CHANGE_EPSILON, backoff_delay_ms};
use crate::model::{Exchange, FundingUpdate, MarketEvent, QuoteUpdate, now_ms};

pub struct ExtendedFeed;

impl ExchangeFeed for ExtendedFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Extended
    }

    fn spawn(
        &self,
        runtime: &Runtime,
        config: &AppConfig,
        markets: &SymbolMarkets,
        event_tx: mpsc::Sender<MarketEvent>,
    ) {
        let ws_url = config.extended_ws_url.clone();
        let feed_markets = markets.clone();
        let quote_event_tx = event_tx.clone();
        runtime.spawn(async move {
            if let Err(err) = run_extended_feed(&ws_url, &feed_markets, quote_event_tx).await {
                tracing::error!(error = %err, "extended feed task terminated");
            }
        });

        if config.funding_poll_secs > 0 {
            let funding_ws_url = config.extended_funding_ws_url.clone();
            let funding_markets = markets.clone();
            runtime.spawn(async move {
                if let Err(err) =
                    run_extended_funding_feed(&funding_ws_url, &funding_markets, event_tx).await
                {
                    tracing::error!(error = %err, "extended funding feed task terminated");
                }
            });
        }
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

fn extract_string<'a>(value: &'a Value, paths: &[&[&str]]) -> Option<&'a str> {
    for path in paths {
        let mut current = value;
        let mut matched = true;
        for key in *path {
            match current.get(*key) {
                Some(next) => current = next,
                None => {
                    matched = false;
                    break;
                }
            }
        }
        if !matched {
            continue;
        }
        if let Some(text) = current.as_str() {
            return Some(text);
        }
    }

    None
}

fn extract_i64(value: &Value, paths: &[&[&str]]) -> Option<i64> {
    for path in paths {
        let mut current = value;
        let mut matched = true;
        for key in *path {
            match current.get(*key) {
                Some(next) => current = next,
                None => {
                    matched = false;
                    break;
                }
            }
        }
        if matched {
            if let Some(parsed) = as_i64(current) {
                return Some(parsed);
            }
        }
    }

    None
}

fn top_of_side(levels: &Value, is_bid: bool) -> Option<(f64, f64)> {
    let entries = levels.as_array()?;

    let mut best_px: Option<f64> = None;
    let mut best_qty: Option<f64> = None;

    for level in entries {
        let (px, qty) = match level {
            Value::Array(items) if items.len() >= 2 => {
                let px = as_f64(items.first()?)?;
                let qty = as_f64(items.get(1)?)?;
                (px, qty)
            }
            Value::Object(map) => {
                let px = map
                    .get("p")
                    .or_else(|| map.get("price"))
                    .or_else(|| map.get("px"))
                    .and_then(as_f64)?;
                let qty = map
                    .get("q")
                    .or_else(|| map.get("size"))
                    .or_else(|| map.get("s"))
                    .or_else(|| map.get("amount"))
                    .or_else(|| map.get("qty"))
                    .and_then(as_f64)?;
                (px, qty)
            }
            _ => continue,
        };

        if px <= 0.0 || qty <= 0.0 {
            continue;
        }

        match best_px {
            None => {
                best_px = Some(px);
                best_qty = Some(qty);
            }
            Some(current_best) => {
                let better = if is_bid {
                    px > current_best
                } else {
                    px < current_best
                };
                if better {
                    best_px = Some(px);
                    best_qty = Some(qty);
                }
            }
        }
    }

    Some((best_px?, best_qty?))
}

pub fn parse_extended_orderbook_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<QuoteUpdate> {
    let root: Value = serde_json::from_str(raw).ok()?;

    let market = extract_string(
        &root,
        &[
            &["market"],
            &["m"],
            &["symbol"],
            &["data", "market"],
            &["data", "m"],
            &["data", "symbol"],
        ],
    )?;

    let symbol_base = symbol_map.get(&normalize_base(market))?.clone();

    let bids = root
        .get("bids")
        .or_else(|| root.get("b"))
        .or_else(|| root.get("data").and_then(|data| data.get("bids")))
        .or_else(|| root.get("data").and_then(|data| data.get("b")))?;

    let asks = root
        .get("asks")
        .or_else(|| root.get("a"))
        .or_else(|| root.get("data").and_then(|data| data.get("asks")))
        .or_else(|| root.get("data").and_then(|data| data.get("a")))?;

    let (bid_px, bid_qty) = top_of_side(bids, true)?;
    let (ask_px, ask_qty) = top_of_side(asks, false)?;

    let exch_ts_ms = extract_i64(
        &root,
        &[
            &["timestamp"],
            &["ts"],
            &["t"],
            &["data", "timestamp"],
            &["data", "ts"],
            &["data", "t"],
        ],
    )
    .unwrap_or(recv_ts_ms);

    Some(QuoteUpdate {
        exchange: Exchange::Extended,
        symbol_base,
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        exch_ts_ms,
        recv_ts_ms,
    })
}

pub fn parse_extended_funding_message(
    raw: &str,
    symbol_map: &HashMap<String, String>,
    recv_ts_ms: i64,
) -> Option<FundingUpdate> {
    let root: Value = serde_json::from_str(raw).ok()?;

    let market = extract_string(
        &root,
        &[
            &["market"],
            &["m"],
            &["symbol"],
            &["data", "market"],
            &["data", "m"],
            &["data", "symbol"],
        ],
    )?;

    let symbol_base = symbol_map.get(&normalize_base(market))?.clone();

    let funding_rate = root
        .get("fundingRate")
        .or_else(|| root.get("funding_rate"))
        .or_else(|| root.get("r"))
        .or_else(|| root.get("f"))
        .or_else(|| root.get("data").and_then(|data| data.get("fundingRate")))
        .or_else(|| root.get("data").and_then(|data| data.get("funding_rate")))
        .or_else(|| root.get("data").and_then(|data| data.get("r")))
        .or_else(|| root.get("data").and_then(|data| data.get("f")))
        .and_then(as_f64)?;

    let next_funding_ts_ms = extract_i64(
        &root,
        &[
            &["nextFundingTimestamp"],
            &["nextFundingTime"],
            &["next_funding_ts"],
            &["T"],
            &["data", "nextFundingTimestamp"],
            &["data", "nextFundingTime"],
            &["data", "next_funding_ts"],
            &["data", "T"],
        ],
    );

    let exch_ts_ms = extract_i64(
        &root,
        &[
            &["timestamp"],
            &["ts"],
            &["t"],
            &["data", "timestamp"],
            &["data", "ts"],
            &["data", "t"],
        ],
    )
    .unwrap_or(recv_ts_ms);

    Some(FundingUpdate {
        exchange: Exchange::Extended,
        symbol_base,
        funding_rate,
        next_funding_ts_ms,
        recv_ts_ms: exch_ts_ms,
        stale_after_ms: None,
    })
}

fn extended_symbol_map(markets: &SymbolMarkets) -> HashMap<String, String> {
    let discovered: HashMap<String, String> = markets
        .iter()
        .filter_map(|(symbol_base, per_exchange)| {
            per_exchange
                .iter()
                .find(|meta| meta.exchange == Exchange::Extended)
                .map(|meta| (normalize_base(&meta.exchange_symbol), symbol_base.clone()))
        })
        .collect();

    if !discovered.is_empty() {
        return discovered;
    }

    // Fallback when Extended REST discovery is blocked (e.g. 403): infer common
    // perp naming to keep WS integration alive.
    tracing::warn!("extended symbol discovery unavailable; using inferred *-USD mapping fallback");
    let mut inferred = HashMap::new();
    for symbol_base in markets.keys() {
        inferred.insert(
            normalize_base(&format!("{symbol_base}-USD")),
            symbol_base.clone(),
        );
        inferred.insert(
            normalize_base(&format!("{symbol_base}/USD")),
            symbol_base.clone(),
        );
    }
    inferred
}

fn extended_ws_request(
    ws_url: &str,
) -> anyhow::Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut request = ws_url
        .into_client_request()
        .context("failed to build extended websocket request")?;
    let headers = request.headers_mut();
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("Mozilla/5.0 (compatible; cross-ex-arb/0.1)"),
    );
    headers.insert(
        ORIGIN,
        HeaderValue::from_static("https://app.extended.exchange"),
    );
    Ok(request)
}

fn extended_connect_backoff_ms(err: &WsError, attempt: u32, forbidden_streak: &mut u32) -> u64 {
    if let WsError::Http(response) = err {
        if response.status() == StatusCode::FORBIDDEN {
            *forbidden_streak = forbidden_streak.saturating_add(1);
            let exp = (*forbidden_streak).min(4);
            return 30_000u64.saturating_mul(1u64 << exp);
        }
    }

    *forbidden_streak = 0;
    backoff_delay_ms(attempt)
}

pub async fn run_extended_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map = extended_symbol_map(markets);

    if symbol_map.is_empty() {
        tracing::warn!("extended feed skipped: no matched symbols to subscribe");
        return Ok(());
    }

    let mut attempt = 0u32;
    let mut forbidden_streak = 0u32;

    loop {
        tracing::info!(attempt, "connecting extended WS");
        let request = match extended_ws_request(ws_url) {
            Ok(req) => req,
            Err(err) => {
                tracing::warn!(error = %err, "extended WS request build failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        match connect_async(request).await {
            Ok((stream, _)) => {
                tracing::info!("connected extended WS");
                attempt = 0;
                forbidden_streak = 0;

                let (mut write_half, mut read_half) = stream.split();

                while let Some(message_result) = read_half.next().await {
                    match message_result.context("extended WS read error") {
                        Ok(Message::Text(text)) => {
                            let recv_ts_ms = now_ms();
                            if let Some(update) = parse_extended_orderbook_message(
                                text.as_ref(),
                                &symbol_map,
                                recv_ts_ms,
                            ) {
                                if event_tx.send(MarketEvent::Quote(update)).await.is_err() {
                                    tracing::info!(
                                        "extended feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if let Err(err) = write_half.send(Message::Pong(payload)).await {
                                tracing::warn!(error = %err, "extended WS pong failed");
                                break;
                            }
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(?frame, "extended WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "extended WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "extended WS connect failed");
                let delay_ms = extended_connect_backoff_ms(&err, attempt, &mut forbidden_streak);
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                continue;
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

pub async fn run_extended_funding_feed(
    ws_url: &str,
    markets: &SymbolMarkets,
    event_tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let symbol_map = extended_symbol_map(markets);

    if symbol_map.is_empty() {
        tracing::warn!("extended funding feed skipped: no matched symbols");
        return Ok(());
    }

    let mut attempt = 0u32;
    let mut forbidden_streak = 0u32;
    let mut cache: HashMap<String, f64> = HashMap::new();

    loop {
        tracing::info!(attempt, "connecting extended funding WS");
        let request = match extended_ws_request(ws_url) {
            Ok(req) => req,
            Err(err) => {
                tracing::warn!(error = %err, "extended funding WS request build failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        match connect_async(request).await {
            Ok((stream, _)) => {
                tracing::info!("connected extended funding WS");
                attempt = 0;
                forbidden_streak = 0;

                let (mut write_half, mut read_half) = stream.split();

                while let Some(message_result) = read_half.next().await {
                    match message_result.context("extended funding WS read error") {
                        Ok(Message::Text(text)) => {
                            let recv_ts_ms = now_ms();

                            if let Some(update) = parse_extended_funding_message(
                                text.as_ref(),
                                &symbol_map,
                                recv_ts_ms,
                            ) {
                                let should_emit = cache
                                    .get(&update.symbol_base)
                                    .map(|prev| {
                                        (update.funding_rate - prev).abs() > FUNDING_CHANGE_EPSILON
                                    })
                                    .unwrap_or(true);

                                if !should_emit {
                                    continue;
                                }

                                cache.insert(update.symbol_base.clone(), update.funding_rate);

                                if event_tx.send(MarketEvent::Funding(update)).await.is_err() {
                                    tracing::info!(
                                        "extended funding feed stopped: market event channel closed"
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if let Err(err) = write_half.send(Message::Pong(payload)).await {
                                tracing::warn!(error = %err, "extended funding WS pong failed");
                                break;
                            }
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(?frame, "extended funding WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "extended funding WS message handling failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "extended funding WS connect failed");
                let delay_ms = extended_connect_backoff_ms(&err, attempt, &mut forbidden_streak);
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                continue;
            }
        }

        let delay_ms = backoff_delay_ms(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}
